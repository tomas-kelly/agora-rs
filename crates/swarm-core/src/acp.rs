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

/// JSON-RPC 2.0 over stdio to a `kiro-cli acp` subprocess.
pub struct KiroAcpClient {
    agent_name: String,
    command: String,
    proc: Option<KiroProcess>,
    next_id: i64,
}

struct KiroProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl KiroAcpClient {
    pub fn new(agent_name: impl Into<String>, command: &str) -> Self {
        Self {
            agent_name: agent_name.into(),
            command: command.to_string(),
            proc: None,
            next_id: 1,
        }
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

    async fn call(&mut self, method: &str, params: serde_json::Value) -> Result<serde_json::Value> {
        self.ensure_started().await?;
        let proc = self.proc.as_mut().unwrap();

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
            let msg: serde_json::Value = match serde_json::from_str(buf.trim()) {
                Ok(v) => v,
                Err(e) => {
                    warn!("non-JSON line from kiro: {} ({})", buf.trim(), e);
                    continue;
                }
            };
            if msg.get("id").and_then(|v| v.as_i64()) == Some(id) {
                if let Some(err) = msg.get("error") {
                    anyhow::bail!("kiro returned error: {err}");
                }
                return Ok(msg
                    .get("result")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null));
            }
            // Otherwise it's a notification or out-of-order reply; ignore.
        }
    }
}

#[async_trait]
impl AcpClient for KiroAcpClient {
    async fn session_new(&mut self, session_id: &str) -> Result<()> {
        tokio::time::timeout(
            ACP_CALL_TIMEOUT,
            self.call(
                "session/new",
                serde_json::json!({ "session_id": session_id }),
            ),
        )
        .await
        .context("kiro session/new timed out")??;
        Ok(())
    }

    async fn session_load(&mut self, session_id: &str) -> Result<()> {
        tokio::time::timeout(
            ACP_CALL_TIMEOUT,
            self.call(
                "session/load",
                serde_json::json!({ "session_id": session_id }),
            ),
        )
        .await
        .context("kiro session/load timed out")??;
        Ok(())
    }

    async fn session_prompt(
        &mut self,
        session_id: &str,
        messages: &[serde_json::Value],
    ) -> Result<AcpResult> {
        let result = tokio::time::timeout(
            ACP_CALL_TIMEOUT,
            self.call(
                "session/prompt",
                serde_json::json!({ "session_id": session_id, "prompt": messages }),
            ),
        )
        .await
        .context("kiro session/prompt timed out")??;

        // Try common shapes for the text payload.
        let text = result
            .get("text")
            .and_then(|v| v.as_str())
            .or_else(|| result.get("response").and_then(|v| v.as_str()))
            .map(String::from)
            .unwrap_or_else(|| result.to_string());

        Ok(AcpResult { text })
    }

    async fn session_close(&mut self, _session_id: &str) -> Result<()> {
        if let Some(mut p) = self.proc.take() {
            let _ = p.child.kill().await;
        }
        Ok(())
    }
}
