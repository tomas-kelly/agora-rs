use anyhow::{Context, Result};
use async_trait::async_trait;
use std::{process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
};
use tracing::{debug, warn};

use crate::command::split_command_line;

const ACP_CALL_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Clone)]
pub struct AcpResult {
    pub text: String,
}

#[async_trait]
pub trait AcpClient: Send + 'static {
    async fn session_new(&mut self, session_id: &str) -> Result<()>;
    async fn session_load(&mut self, session_id: &str) -> Result<()>;
    async fn session_prompt(
        &mut self,
        session_id: &str,
        messages: &[serde_json::Value],
    ) -> Result<AcpResult>;
    async fn session_close(&mut self, session_id: &str) -> Result<()>;
}

// ---------------------------------------------------------------- Mock client

/// Returns canned plausible JSON after a brief delay.
pub struct MockAcpClient {
    agent_name: String,
}

impl MockAcpClient {
    pub fn new(agent_name: impl Into<String>) -> Self {
        Self {
            agent_name: agent_name.into(),
        }
    }
}

#[async_trait]
impl AcpClient for MockAcpClient {
    async fn session_new(&mut self, _session_id: &str) -> Result<()> {
        Ok(())
    }
    async fn session_load(&mut self, _session_id: &str) -> Result<()> {
        Ok(())
    }

    async fn session_prompt(
        &mut self,
        _session_id: &str,
        messages: &[serde_json::Value],
    ) -> Result<AcpResult> {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let prompt: String = messages
            .iter()
            .filter_map(|m| m.get("text").and_then(|t| t.as_str()))
            .collect::<Vec<_>>()
            .join(" ");
        let trimmed = &prompt[..prompt.len().min(120)];
        let text = serde_json::json!({
            "summary": format!("[mock/{}] {}", self.agent_name, trimmed),
        })
        .to_string();
        Ok(AcpResult { text })
    }

    async fn session_close(&mut self, _session_id: &str) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------- Kiro client

/// JSON-RPC 2.0 over stdio to a `kiro-cli acp` subprocess. Implements the
/// Agent Client Protocol (https://github.com/zed-industries/agent-client-protocol).
///
/// Protocol notes (learned the hard way):
///
/// * `initialize` MUST be the first request. Without it, kiro silently
///   drops subsequent requests and exits on EOF.
/// * `session/new` takes `{cwd, mcpServers}` and returns `{sessionId}` —
///   kiro mints its own UUID, ignoring whatever ID we have in mind.
///   We map agora-side session ids → kiro session ids in `sessions`.
/// * `session/prompt` doesn't return the assistant's text in its result —
///   it returns `{stopReason: "..."}`. The actual content streams in via
///   `session/update` notifications with `update.kind == "agent_message_chunk"`
///   between the request and the final result, all sharing the kiro
///   sessionId.
pub struct KiroAcpClient {
    agent_name: String,
    command: String,
    cwd: String,
    proc: Option<KiroProcess>,
    next_id: i64,
    initialized: bool,
    /// agora session id → kiro session id (UUID)
    sessions: std::collections::HashMap<String, String>,
}

struct KiroProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl KiroAcpClient {
    pub fn new(agent_name: impl Into<String>, command: &str) -> Self {
        let cwd = std::env::current_dir()
            .ok()
            .and_then(|p| p.to_str().map(String::from))
            .unwrap_or_else(|| ".".to_string());
        Self {
            agent_name: agent_name.into(),
            command: command.to_string(),
            cwd,
            proc: None,
            next_id: 1,
            initialized: false,
            sessions: std::collections::HashMap::new(),
        }
    }

    /// Drop the subprocess + every cached session id. Used when a pipe
    /// breaks so the next call re-spawns kiro fresh.
    fn reset(&mut self) {
        self.proc = None;
        self.initialized = false;
        self.sessions.clear();
    }

    /// Ensure subprocess is up AND `initialize` has been handshaken.
    async fn ensure_initialized(&mut self) -> Result<()> {
        if self.initialized {
            return Ok(());
        }
        self.ensure_started().await?;
        self.call_inner(
            "initialize",
            serde_json::json!({
                "protocolVersion": 1,
                "clientCapabilities": {
                    "fs": { "readTextFile": true, "writeTextFile": true },
                    "terminal": true,
                }
            }),
        )
        .await?;
        self.initialized = true;
        debug!(agent = %self.agent_name, "kiro ACP handshake complete");
        Ok(())
    }

    async fn ensure_started(&mut self) -> Result<()> {
        if self.proc.is_some() {
            return Ok(());
        }
        let command = split_command_line(&self.command)?;
        let (head, tail) = command.split_first().context("kiro_command is empty")?;
        let agent_name = self.agent_name.clone();
        let mut child = Command::new(head)
            .args(tail)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to spawn {head}"))?;
        let stdin = child.stdin.take().context("kiro stdin missing")?;
        let stdout = BufReader::new(child.stdout.take().context("kiro stdout missing")?);
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                loop {
                    match lines.next_line().await {
                        Ok(Some(line)) => warn!(agent = %agent_name, "kiro stderr: {line}"),
                        Ok(None) => break,
                        Err(e) => {
                            warn!(agent = %agent_name, "kiro stderr read failed: {e}");
                            break;
                        }
                    }
                }
            });
        }
        debug!(agent = %self.agent_name, "spawned kiro-cli");
        self.proc = Some(KiroProcess {
            child,
            stdin,
            stdout,
        });
        Ok(())
    }

    /// Send a JSON-RPC request and return the matching result. On ANY
    /// failure (pipe broken, kiro exited, malformed reply), reset state so
    /// the next call respawns a fresh subprocess.
    async fn call_inner(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let proc = self.proc.as_mut().context("kiro subprocess not started")?;

        let id = self.next_id;
        self.next_id += 1;

        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": id,
        });
        let line = format!("{}\n", request);
        proc.stdin.write_all(line.as_bytes()).await?;
        proc.stdin.flush().await?;

        loop {
            let mut buf = String::new();
            let n = proc.stdout.read_line(&mut buf).await?;
            if n == 0 {
                anyhow::bail!("kiro stdout closed unexpectedly");
            }
            let trimmed = buf.trim();
            if trimmed.is_empty() {
                continue;
            }
            let msg: serde_json::Value = match serde_json::from_str(trimmed) {
                Ok(v) => v,
                Err(e) => {
                    // kiro emits its own structured logs on stdout (e.g. from
                    // fig_telemetry). They're not JSON-RPC; skip and keep reading.
                    debug!("non-JSON-RPC line from kiro stdout: {trimmed} ({e})");
                    continue;
                }
            };
            if msg.get("id").and_then(|v| v.as_i64()) == Some(id) {
                if let Some(err) = msg.get("error") {
                    anyhow::bail!("kiro returned error for {method}: {err}");
                }
                return Ok(msg
                    .get("result")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null));
            }
            // Otherwise it's a notification or out-of-order reply; ignore.
        }
    }

    /// Public call wrapper — auto-resets on failure so a dead subprocess
    /// gets respawned on the next attempt instead of looping forever on
    /// EPIPE.
    async fn call(&mut self, method: &str, params: serde_json::Value) -> Result<serde_json::Value> {
        self.ensure_initialized().await?;
        match self.call_inner(method, params).await {
            Ok(v) => Ok(v),
            Err(e) => {
                warn!(agent = %self.agent_name, method, "kiro call failed; recycling subprocess: {e}");
                self.reset();
                Err(e)
            }
        }
    }

    /// Send `session/prompt`, then collect `session/update` notifications
    /// (`agent_message_chunk`s) until the prompt's final result arrives.
    /// The accumulated text is the assistant's full reply.
    async fn prompt_streaming(
        &mut self,
        kiro_session_id: &str,
        messages: &[serde_json::Value],
    ) -> Result<String> {
        self.ensure_initialized().await?;
        let proc = self.proc.as_mut().context("kiro subprocess not started")?;

        let id = self.next_id;
        self.next_id += 1;

        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/prompt",
            "params": {
                "sessionId": kiro_session_id,
                "prompt": messages,
            },
            "id": id,
        });
        proc.stdin
            .write_all(format!("{}\n", request).as_bytes())
            .await?;
        proc.stdin.flush().await?;

        let mut accumulated = String::new();
        loop {
            let mut buf = String::new();
            let n = proc.stdout.read_line(&mut buf).await?;
            if n == 0 {
                anyhow::bail!("kiro stdout closed mid-prompt");
            }
            let trimmed = buf.trim();
            if trimmed.is_empty() {
                continue;
            }
            let msg: serde_json::Value = match serde_json::from_str(trimmed) {
                Ok(v) => v,
                Err(_) => continue,
            };

            // Final result for our prompt — return what we accumulated.
            if msg.get("id").and_then(|v| v.as_i64()) == Some(id) {
                if let Some(err) = msg.get("error") {
                    anyhow::bail!("kiro session/prompt error: {err}");
                }
                // Some agents put the text in the result rather than streaming.
                if accumulated.is_empty() {
                    if let Some(text) = msg.get("result").and_then(extract_result_text) {
                        accumulated = text;
                    }
                }
                return Ok(accumulated);
            }

            // Streaming chunk for our session?
            if msg.get("method").and_then(|v| v.as_str()) == Some("session/update") {
                let same_session = msg.pointer("/params/sessionId").and_then(|v| v.as_str())
                    == Some(kiro_session_id);
                if same_session {
                    if let Some(chunk) = extract_update_chunk(&msg) {
                        accumulated.push_str(&chunk);
                    }
                }
            }
        }
    }
}

/// Pull text out of a `session/update` notification when the update is an
/// `agent_message_chunk`. Other kinds (`tool_call`, `tool_call_update`,
/// `agent_thought_chunk`, `plan`) are ignored — they're not part of the
/// final reply.
fn extract_update_chunk(msg: &serde_json::Value) -> Option<String> {
    let update = msg.pointer("/params/update")?;
    let kind = update
        .get("sessionUpdate")
        .or_else(|| update.get("kind"))
        .and_then(|v| v.as_str())?;
    if kind != "agent_message_chunk" {
        return None;
    }
    // content can be either a bare string, {type:"text", text:"..."},
    // or omitted in favor of a top-level `text` field.
    let content = update.get("content").or_else(|| update.get("text"))?;
    if let Some(s) = content.as_str() {
        return Some(s.to_string());
    }
    if let Some(t) = content.get("text").and_then(|v| v.as_str()) {
        return Some(t.to_string());
    }
    None
}

/// Fallback: if kiro returned the text directly in the prompt result
/// instead of streaming it.
fn extract_result_text(result: &serde_json::Value) -> Option<String> {
    if let Some(s) = result.get("text").and_then(|v| v.as_str()) {
        return Some(s.to_string());
    }
    if let Some(s) = result.get("response").and_then(|v| v.as_str()) {
        return Some(s.to_string());
    }
    None
}

#[async_trait]
impl AcpClient for KiroAcpClient {
    async fn session_new(&mut self, session_id: &str) -> Result<()> {
        let cwd = self.cwd.clone();
        let result = tokio::time::timeout(
            ACP_CALL_TIMEOUT,
            self.call(
                "session/new",
                serde_json::json!({ "cwd": cwd, "mcpServers": [] }),
            ),
        )
        .await
        .context("kiro session/new timed out")??;

        let kiro_sid = result
            .get("sessionId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                anyhow::anyhow!("kiro session/new response missing sessionId: {result}")
            })?
            .to_string();

        debug!(
            agent = %self.agent_name,
            agora_session = session_id,
            kiro_session = %kiro_sid,
            "kiro session created"
        );
        self.sessions.insert(session_id.to_string(), kiro_sid);
        Ok(())
    }

    /// "Load" semantics for a kiro-backed agent: if this process already
    /// has a kiro session mapped for this agora session id, it's live and
    /// usable. Otherwise we report an error so the caller falls back to
    /// `session_new`. (ACP standard `session/load` exists for persisted
    /// kiro sessions but we don't persist the kiro UUID across process
    /// restarts, so it would never succeed for us.)
    async fn session_load(&mut self, session_id: &str) -> Result<()> {
        if self.sessions.contains_key(session_id) {
            Ok(())
        } else {
            anyhow::bail!("agora session {session_id} not loaded in this kiro process")
        }
    }

    async fn session_prompt(
        &mut self,
        session_id: &str,
        messages: &[serde_json::Value],
    ) -> Result<AcpResult> {
        let kiro_sid = self
            .sessions
            .get(session_id)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "agora session {session_id} has no kiro session — call session_new first"
                )
            })?
            .clone();

        let text = match tokio::time::timeout(
            ACP_CALL_TIMEOUT,
            self.prompt_streaming(&kiro_sid, messages),
        )
        .await
        .context("kiro session/prompt timed out")?
        {
            Ok(t) => t,
            Err(e) => {
                // Pipe broke or kiro errored mid-prompt — recycle.
                warn!(agent = %self.agent_name, "prompt failed; recycling subprocess: {e}");
                self.reset();
                return Err(e);
            }
        };

        Ok(AcpResult { text })
    }

    async fn session_close(&mut self, _session_id: &str) -> Result<()> {
        if let Some(mut p) = self.proc.take() {
            let _ = p.child.kill().await;
        }
        Ok(())
    }
}
