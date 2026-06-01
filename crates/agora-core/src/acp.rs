use anyhow::{Context, Result};
use async_trait::async_trait;
use std::{process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
};
use tracing::{debug, warn};

use crate::command::split_command_line;

pub const DEFAULT_ACP_CALL_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
pub struct AcpResult {
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct AcpPermissionOption {
    pub option_id: String,
    pub name: String,
    pub kind: String,
}

#[derive(Debug, Clone)]
pub struct AcpPermissionRequest {
    pub acp_session_id: String,
    pub tool_call: serde_json::Value,
    pub options: Vec<AcpPermissionOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcpPermissionDecision {
    Selected { option_id: String },
    Cancelled,
}

#[async_trait]
pub trait AcpPermissionHandler: Send {
    async fn request_permission(
        &mut self,
        request: AcpPermissionRequest,
    ) -> Result<AcpPermissionDecision>;
}

#[async_trait]
pub trait AcpOutputHandler: Send {
    async fn output_chunk(&mut self, chunk: &str) -> Result<()>;
}

#[async_trait]
pub trait AcpClient: Send + 'static {
    async fn session_new(&mut self, session_id: &str) -> Result<()>;
    async fn session_load(&mut self, session_id: &str) -> Result<()>;
    async fn session_prompt_with_output(
        &mut self,
        session_id: &str,
        messages: &[serde_json::Value],
        output_handler: Option<&mut dyn AcpOutputHandler>,
        permission_handler: Option<&mut dyn AcpPermissionHandler>,
    ) -> Result<AcpResult>;
    async fn session_prompt(
        &mut self,
        session_id: &str,
        messages: &[serde_json::Value],
        permission_handler: Option<&mut dyn AcpPermissionHandler>,
    ) -> Result<AcpResult> {
        self.session_prompt_with_output(session_id, messages, None, permission_handler)
            .await
    }
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

    async fn session_prompt_with_output(
        &mut self,
        _session_id: &str,
        messages: &[serde_json::Value],
        output_handler: Option<&mut dyn AcpOutputHandler>,
        _permission_handler: Option<&mut dyn AcpPermissionHandler>,
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
        if let Some(handler) = output_handler {
            handler.output_chunk(&text).await?;
        }
        Ok(AcpResult { text })
    }

    async fn session_close(&mut self, _session_id: &str) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------- ACP client

/// JSON-RPC 2.0 over stdio to a configured ACP subprocess. Implements the
/// Agent Client Protocol (https://github.com/zed-industries/agent-client-protocol).
///
/// Protocol notes (learned the hard way):
///
/// * `initialize` MUST be the first request. Without it, the subprocess may
///   drop subsequent requests and exit on EOF.
/// * `session/new` takes `{cwd, mcpServers}` and returns `{sessionId}` —
///   the subprocess mints its own UUID, ignoring whatever ID we have in mind.
///   We map Agora session ids to ACP session ids in `sessions`.
/// * `session/prompt` doesn't return the assistant's text in its result —
///   it returns `{stopReason: "..."}`. The actual content streams in via
///   `session/update` notifications with `update.kind == "agent_message_chunk"`
///   between the request and the final result, all sharing the ACP
///   sessionId.
pub struct StdioAcpClient {
    agent_name: String,
    command: String,
    cwd: String,
    call_timeout: Duration,
    proc: Option<StdioProcess>,
    next_id: i64,
    initialized: bool,
    /// Agora session id -> ACP session id.
    sessions: std::collections::HashMap<String, String>,
}

struct StdioProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl StdioAcpClient {
    pub fn new(agent_name: impl Into<String>, command: &str) -> Self {
        Self::new_with_timeout(agent_name, command, DEFAULT_ACP_CALL_TIMEOUT)
    }

    pub fn new_with_timeout(
        agent_name: impl Into<String>,
        command: &str,
        call_timeout: Duration,
    ) -> Self {
        let cwd = std::env::current_dir()
            .ok()
            .and_then(|p| p.to_str().map(String::from))
            .unwrap_or_else(|| ".".to_string());
        let call_timeout = if call_timeout.is_zero() {
            DEFAULT_ACP_CALL_TIMEOUT
        } else {
            call_timeout
        };
        Self {
            agent_name: agent_name.into(),
            command: command.to_string(),
            cwd,
            call_timeout,
            proc: None,
            next_id: 1,
            initialized: false,
            sessions: std::collections::HashMap::new(),
        }
    }

    /// Kill the subprocess and drop every cached session id. Used when a pipe
    /// breaks or a request times out so the next call re-spawns stdio fresh.
    fn reset(&mut self) {
        if let Some(proc) = self.proc.take() {
            let StdioProcess { mut child, .. } = proc;
            if let Err(e) = child.start_kill() {
                warn!(agent = %self.agent_name, "failed to kill ACP subprocess during reset: {e}");
            }
            tokio::spawn(async move {
                let _ = child.wait().await;
            });
        }
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
        debug!(agent = %self.agent_name, "stdio ACP handshake complete");
        Ok(())
    }

    async fn ensure_started(&mut self) -> Result<()> {
        if self.proc.is_some() {
            return Ok(());
        }
        let command = split_command_line(&self.command)?;
        let (head, tail) = command.split_first().context("acp_command is empty")?;
        let agent_name = self.agent_name.clone();
        let mut child = Command::new(head)
            .args(tail)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("failed to spawn {head}"))?;
        let stdin = child.stdin.take().context("stdio stdin missing")?;
        let stdout = BufReader::new(child.stdout.take().context("stdio stdout missing")?);
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                loop {
                    match lines.next_line().await {
                        Ok(Some(line)) => warn!(agent = %agent_name, "stdio stderr: {line}"),
                        Ok(None) => break,
                        Err(e) => {
                            warn!(agent = %agent_name, "stdio stderr read failed: {e}");
                            break;
                        }
                    }
                }
            });
        }
        debug!(agent = %self.agent_name, command = %self.command, "spawned ACP subprocess");
        self.proc = Some(StdioProcess {
            child,
            stdin,
            stdout,
        });
        Ok(())
    }

    /// Send a JSON-RPC request and return the matching result. On ANY
    /// failure (pipe broken, stdio exited, malformed reply), reset state so
    /// the next call respawns a fresh subprocess.
    async fn call_inner(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let proc = self.proc.as_mut().context("stdio subprocess not started")?;

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
                anyhow::bail!("stdio stdout closed unexpectedly");
            }
            let trimmed = buf.trim();
            if trimmed.is_empty() {
                continue;
            }
            let msg: serde_json::Value = match serde_json::from_str(trimmed) {
                Ok(v) => v,
                Err(e) => {
                    // stdio emits its own structured logs on stdout (e.g. from
                    // fig_telemetry). They're not JSON-RPC; skip and keep reading.
                    debug!("non-JSON-RPC line from stdio stdout: {trimmed} ({e})");
                    continue;
                }
            };
            if msg.get("id").and_then(|v| v.as_i64()) == Some(id) {
                if let Some(err) = msg.get("error") {
                    anyhow::bail!("stdio returned error for {method}: {err}");
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
                warn!(agent = %self.agent_name, method, "stdio call failed; recycling subprocess: {e}");
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
        acp_session_id: &str,
        messages: &[serde_json::Value],
        mut output_handler: Option<&mut dyn AcpOutputHandler>,
        mut permission_handler: Option<&mut dyn AcpPermissionHandler>,
    ) -> Result<String> {
        self.ensure_initialized().await?;

        let id = self.next_id;
        self.next_id += 1;

        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/prompt",
            "params": {
                "sessionId": acp_session_id,
                "prompt": messages,
            },
            "id": id,
        });
        {
            let proc = self.proc.as_mut().context("stdio subprocess not started")?;
            proc.stdin
                .write_all(format!("{}\n", request).as_bytes())
                .await?;
            proc.stdin.flush().await?;
        }

        let mut accumulated = String::new();
        loop {
            let mut buf = String::new();
            let n = {
                let proc = self.proc.as_mut().context("stdio subprocess not started")?;
                proc.stdout.read_line(&mut buf).await?
            };
            if n == 0 {
                anyhow::bail!("stdio stdout closed mid-prompt");
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
                    anyhow::bail!("stdio session/prompt error: {err}");
                }
                // Some agents put the text in the result rather than streaming.
                if accumulated.is_empty() {
                    if let Some(text) = msg.get("result").and_then(extract_result_text) {
                        if let Some(handler) = output_handler.as_mut() {
                            (*handler).output_chunk(&text).await?;
                        }
                        accumulated = text;
                    }
                }
                return Ok(accumulated);
            }

            // Streaming chunk for our session?
            if msg.get("method").and_then(|v| v.as_str()) == Some("session/update") {
                let same_session = msg.pointer("/params/sessionId").and_then(|v| v.as_str())
                    == Some(acp_session_id);
                if same_session {
                    if let Some(chunk) = extract_update_chunk(&msg) {
                        accumulated.push_str(&chunk);
                        if let Some(handler) = output_handler.as_mut() {
                            (*handler).output_chunk(&chunk).await?;
                        }
                    }
                }
                continue;
            }

            if let Some((request_id, request)) = parse_permission_request(&msg) {
                let decision = match permission_handler.as_mut() {
                    Some(handler) => (*handler).request_permission(request).await?,
                    None => AcpPermissionDecision::Cancelled,
                };
                self.write_json_rpc_result(request_id, permission_result(decision))
                    .await?;
            }
        }
    }

    async fn write_json_rpc_result(
        &mut self,
        id: serde_json::Value,
        result: serde_json::Value,
    ) -> Result<()> {
        let proc = self.proc.as_mut().context("stdio subprocess not started")?;
        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result,
        });
        proc.stdin
            .write_all(format!("{}\n", response).as_bytes())
            .await?;
        proc.stdin.flush().await?;
        Ok(())
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

fn parse_permission_request(
    msg: &serde_json::Value,
) -> Option<(serde_json::Value, AcpPermissionRequest)> {
    if msg.get("method").and_then(|v| v.as_str()) != Some("session/request_permission") {
        return None;
    }

    let id = msg.get("id")?.clone();
    let params = msg.get("params")?;
    let acp_session_id = params
        .get("sessionId")
        .or_else(|| params.get("session_id"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let tool_call = params
        .get("toolCall")
        .or_else(|| params.get("tool_call"))
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let options = params
        .get("options")
        .and_then(|v| v.as_array())
        .map(|values| {
            values
                .iter()
                .filter_map(parse_permission_option)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Some((
        id,
        AcpPermissionRequest {
            acp_session_id,
            tool_call,
            options,
        },
    ))
}

fn parse_permission_option(value: &serde_json::Value) -> Option<AcpPermissionOption> {
    Some(AcpPermissionOption {
        option_id: value
            .get("optionId")
            .or_else(|| value.get("option_id"))
            .and_then(|v| v.as_str())?
            .to_string(),
        name: value
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        kind: value
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
    })
}

fn permission_result(decision: AcpPermissionDecision) -> serde_json::Value {
    match decision {
        AcpPermissionDecision::Selected { option_id } => serde_json::json!({
            "outcome": {
                "outcome": "selected",
                "optionId": option_id,
            }
        }),
        AcpPermissionDecision::Cancelled => serde_json::json!({
            "outcome": {
                "outcome": "cancelled",
            }
        }),
    }
}

/// Fallback: if stdio returned the text directly in the prompt result
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
impl AcpClient for StdioAcpClient {
    async fn session_new(&mut self, session_id: &str) -> Result<()> {
        let cwd = self.cwd.clone();
        let result = match tokio::time::timeout(
            self.call_timeout,
            self.call(
                "session/new",
                serde_json::json!({ "cwd": cwd, "mcpServers": [] }),
            ),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                warn!(agent = %self.agent_name, "stdio session/new timed out; recycling subprocess");
                self.reset();
                anyhow::bail!("stdio session/new timed out");
            }
        };

        let acp_sid = result
            .get("sessionId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                anyhow::anyhow!("stdio session/new response missing sessionId: {result}")
            })?
            .to_string();

        debug!(
            agent = %self.agent_name,
            agora_session = session_id,
            acp_session = %acp_sid,
            "stdio session created"
        );
        self.sessions.insert(session_id.to_string(), acp_sid);
        Ok(())
    }

    /// "Load" semantics for a stdio-backed agent: if this process already
    /// has a stdio session mapped for this agora session id, it's live and
    /// usable. Otherwise we report an error so the caller falls back to
    /// `session_new`. (ACP standard `session/load` exists for persisted
    /// stdio sessions but we don't persist the stdio UUID across process
    /// restarts, so it would never succeed for us.)
    async fn session_load(&mut self, session_id: &str) -> Result<()> {
        if self.sessions.contains_key(session_id) {
            Ok(())
        } else {
            anyhow::bail!("agora session {session_id} not loaded in this stdio process")
        }
    }

    async fn session_prompt_with_output(
        &mut self,
        session_id: &str,
        messages: &[serde_json::Value],
        output_handler: Option<&mut dyn AcpOutputHandler>,
        permission_handler: Option<&mut dyn AcpPermissionHandler>,
    ) -> Result<AcpResult> {
        let acp_sid = self
            .sessions
            .get(session_id)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "agora session {session_id} has no stdio session — call session_new first"
                )
            })?
            .clone();

        let text = match tokio::time::timeout(
            self.call_timeout,
            self.prompt_streaming(&acp_sid, messages, output_handler, permission_handler),
        )
        .await
        {
            Ok(Ok(t)) => t,
            Ok(Err(e)) => {
                // Pipe broke or stdio errored mid-prompt — recycle.
                warn!(agent = %self.agent_name, "prompt failed; recycling subprocess: {e}");
                self.reset();
                return Err(e);
            }
            Err(_) => {
                warn!(agent = %self.agent_name, "stdio session/prompt timed out; recycling subprocess");
                self.reset();
                anyhow::bail!("stdio session/prompt timed out");
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

#[cfg(test)]
mod tests {
    use super::{
        parse_permission_request, permission_result, AcpClient, AcpOutputHandler,
        AcpPermissionDecision, AcpPermissionHandler, AcpPermissionRequest, StdioAcpClient,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use std::{
        io::{BufRead, Write},
        sync::{Arc, Mutex},
        time::Duration,
    };

    #[test]
    fn parses_acp_permission_request() {
        let msg = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "session/request_permission",
            "params": {
                "sessionId": "stdio-session",
                "toolCall": {
                    "toolCallId": "call_1",
                    "title": "Run cargo test"
                },
                "options": [
                    {
                        "optionId": "allow-once",
                        "name": "Allow once",
                        "kind": "allow_once"
                    }
                ]
            }
        });

        let (id, request) = parse_permission_request(&msg).unwrap();

        assert_eq!(id, serde_json::json!(5));
        assert_eq!(request.acp_session_id, "stdio-session");
        assert_eq!(request.tool_call["toolCallId"], "call_1");
        assert_eq!(request.options[0].option_id, "allow-once");
    }

    #[test]
    fn permission_result_uses_acp_shape() {
        let selected = permission_result(AcpPermissionDecision::Selected {
            option_id: "allow-once".into(),
        });
        let cancelled = permission_result(AcpPermissionDecision::Cancelled);

        assert_eq!(selected["outcome"]["outcome"], "selected");
        assert_eq!(selected["outcome"]["optionId"], "allow-once");
        assert_eq!(cancelled["outcome"]["outcome"], "cancelled");
    }

    #[test]
    fn malformed_permission_options_are_ignored() {
        let msg = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "perm-1",
            "method": "session/request_permission",
            "params": {
                "sessionId": "stdio-session",
                "options": [{ "name": "missing id" }]
            }
        });

        let (_id, request) = parse_permission_request(&msg).unwrap();

        assert!(request.options.is_empty());
    }

    #[test]
    fn stdio_client_uses_configured_timeout() {
        let client =
            StdioAcpClient::new_with_timeout("test-agent", "fake-acp", Duration::from_secs(42));

        assert_eq!(client.call_timeout, Duration::from_secs(42));
    }

    #[tokio::test]
    async fn stdio_client_handles_permission_requests_from_fake_acp() {
        let exe = std::env::current_exe().unwrap();
        let command = format!(
            "env AGORA_CORE_FAKE_ACP_CHILD=1 \"{}\" --exact acp::tests::fake_acp_child --nocapture",
            exe.display()
        );
        let mut client = StdioAcpClient::new("test-agent", &command);
        let seen = Arc::new(Mutex::new(None));
        let mut handler = RecordingPermissionHandler { seen: seen.clone() };
        let chunks = Arc::new(Mutex::new(String::new()));
        let mut output = RecordingOutputHandler {
            chunks: chunks.clone(),
        };

        client.session_new("sess_runtime").await.unwrap();
        let result = client
            .session_prompt_with_output(
                "sess_runtime",
                &[serde_json::json!({ "type": "text", "text": "please use the tool" })],
                Some(&mut output),
                Some(&mut handler),
            )
            .await
            .unwrap();

        assert_eq!(result.text, "approved: allow-once");
        assert_eq!(*chunks.lock().unwrap(), "approved: allow-once");
        let request = seen.lock().unwrap().clone().unwrap();
        assert_eq!(request.acp_session_id, "fake-acp-session");
        assert_eq!(request.tool_call["title"], "Run cargo test");
        assert_eq!(request.options[0].option_id, "allow-once");

        client.session_close("sess_runtime").await.unwrap();
    }

    struct RecordingPermissionHandler {
        seen: Arc<Mutex<Option<AcpPermissionRequest>>>,
    }

    #[async_trait]
    impl AcpPermissionHandler for RecordingPermissionHandler {
        async fn request_permission(
            &mut self,
            request: AcpPermissionRequest,
        ) -> Result<AcpPermissionDecision> {
            *self.seen.lock().unwrap() = Some(request);
            Ok(AcpPermissionDecision::Selected {
                option_id: "allow-once".into(),
            })
        }
    }

    struct RecordingOutputHandler {
        chunks: Arc<Mutex<String>>,
    }

    #[async_trait]
    impl AcpOutputHandler for RecordingOutputHandler {
        async fn output_chunk(&mut self, chunk: &str) -> Result<()> {
            self.chunks.lock().unwrap().push_str(chunk);
            Ok(())
        }
    }

    #[test]
    fn fake_acp_child() {
        if std::env::var("AGORA_CORE_FAKE_ACP_CHILD").as_deref() != Ok("1") {
            return;
        }
        run_fake_acp_child().unwrap();
    }

    fn run_fake_acp_child() -> Result<()> {
        let stdin = std::io::stdin();
        let mut lines = stdin.lock().lines();
        let mut stdout = std::io::stdout();

        while let Some(line) = lines.next() {
            let msg: serde_json::Value = serde_json::from_str(&line?)?;
            let id = msg.get("id").cloned().unwrap_or(serde_json::Value::Null);
            let method = msg.get("method").and_then(|v| v.as_str()).unwrap_or("");

            match method {
                "initialize" => {
                    write_json_rpc_result(&mut stdout, id, serde_json::json!({}))?;
                }
                "session/new" => {
                    write_json_rpc_result(
                        &mut stdout,
                        id,
                        serde_json::json!({ "sessionId": "fake-acp-session" }),
                    )?;
                }
                "session/prompt" => {
                    write_session_update(&mut stdout, "approved: ")?;
                    writeln!(
                        stdout,
                        "{}",
                        serde_json::json!({
                            "jsonrpc": "2.0",
                            "id": "perm-1",
                            "method": "session/request_permission",
                            "params": {
                                "sessionId": "fake-acp-session",
                                "toolCall": {
                                    "toolCallId": "call_1",
                                    "title": "Run cargo test"
                                },
                                "options": [
                                    {
                                        "optionId": "allow-once",
                                        "name": "Allow once",
                                        "kind": "allow_once"
                                    },
                                    {
                                        "optionId": "reject-once",
                                        "name": "Reject once",
                                        "kind": "reject_once"
                                    }
                                ]
                            }
                        })
                    )?;
                    stdout.flush()?;

                    let response_line = lines.next().expect("permission response")?;
                    let response: serde_json::Value = serde_json::from_str(&response_line)?;
                    let option_id = response
                        .pointer("/result/outcome/optionId")
                        .and_then(|v| v.as_str())
                        .unwrap_or("missing-option");
                    write_session_update(&mut stdout, option_id)?;
                    write_json_rpc_result(
                        &mut stdout,
                        id,
                        serde_json::json!({ "stopReason": "end_turn" }),
                    )?;
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn write_json_rpc_result(
        stdout: &mut std::io::Stdout,
        id: serde_json::Value,
        result: serde_json::Value,
    ) -> Result<()> {
        writeln!(
            stdout,
            "{}",
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": result,
            })
        )?;
        stdout.flush()?;
        Ok(())
    }

    fn write_session_update(stdout: &mut std::io::Stdout, content: &str) -> Result<()> {
        writeln!(
            stdout,
            "{}",
            serde_json::json!({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": "fake-acp-session",
                    "update": {
                        "kind": "agent_message_chunk",
                        "content": content,
                    }
                }
            })
        )?;
        stdout.flush()?;
        Ok(())
    }
}
