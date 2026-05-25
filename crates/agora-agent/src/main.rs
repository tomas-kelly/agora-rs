//! Generic config-driven agent runner. One process per agent.
//!
//! ```text
//! agora-agent --config agents.local.json --agent architect-daemon
//! ```
//!
//! Reads its `AgentSpec` from the topology file, opens a kiro-cli (or mock)
//! ACP session, subscribes to its declared topics, renders the configured
//! prompt template against each inbound envelope, ships it to ACP, and
//! publishes the response.
//!
//! Response handling:
//! - If kiro returns valid JSON, or valid JSON on the final non-empty line,
//!   that becomes the published event's `data`.
//! - Otherwise the response text is wrapped as `{"summary": "<text>"}`.
//! - If the JSON contains `_topic`, that overrides the subscription's
//!   default emit topic (the actual data is taken from `_data` if present,
//!   otherwise from the whole response minus `_topic`).

use agora_core::{
    acp::{AcpClient, KiroAcpClient, MockAcpClient},
    agent_spec::{AgentSpec, SubscriptionSpec},
    daemon::{Agent, DaemonConfig, DaemonRunner, Publisher},
    envelope::Envelope,
    manifest::{PublishedEvent, Subscription},
    topics::{direct_inbox_topic, CODE_CHANGED},
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use clap::Parser;
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use tracing::{info, warn};

#[derive(Parser)]
#[command(name = "agora-agent")]
struct Args {
    /// Path to the topology JSON
    #[arg(long)]
    config: PathBuf,
    /// Agent name (must match an entry in `agents[]` of the topology)
    #[arg(long)]
    agent: String,
    /// Override ACP backend: "mock" or "kiro". Falls back to topology `default_acp`, then "mock".
    #[arg(long)]
    acp: Option<String>,
    #[arg(long, default_value = "nats://127.0.0.1:4222")]
    bus_url: String,
    #[arg(long, default_value = ".kiro/session_token")]
    key_path: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt().with_env_filter("info").init();

    let raw = std::fs::read_to_string(&args.config)
        .with_context(|| format!("cannot read {}", args.config.display()))?;
    let topology: serde_json::Value = serde_json::from_str(&raw)
        .with_context(|| format!("cannot parse {}", args.config.display()))?;

    let agents = topology
        .get("agents")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("topology has no `agents` array"))?;
    let spec_json = agents
        .iter()
        .find(|a| a.get("name").and_then(|v| v.as_str()) == Some(&args.agent))
        .ok_or_else(|| anyhow!("agent `{}` not in topology", args.agent))?;
    let spec: AgentSpec = serde_json::from_value(spec_json.clone())
        .with_context(|| format!("invalid agent spec for `{}`", args.agent))?;

    let topology_default_acp = topology
        .get("default_acp")
        .or_else(|| topology.get("defaultAcp"))
        .and_then(|v| v.as_str())
        .unwrap_or("mock");
    let acp_mode = args
        .acp
        .clone()
        .or_else(|| spec.acp.clone())
        .unwrap_or_else(|| topology_default_acp.to_string());

    let acp: Box<dyn AcpClient> = match acp_mode.as_str() {
        "mock" => Box::new(MockAcpClient::new(&spec.name)),
        "kiro" => {
            let cmd = spec
                .kiro_command
                .clone()
                .ok_or_else(|| anyhow!("agent `{}` has no `kiro_command`", spec.name))?;
            Box::new(KiroAcpClient::new(&spec.name, &cmd))
        }
        other => anyhow::bail!("unknown acp mode: {other}"),
    };

    info!(agent = %spec.name, mode = %acp_mode, "starting");

    let agent = ConfigAgent::new(spec, acp);
    let config = DaemonConfig {
        bus_url: args.bus_url,
        key_path: args.key_path.into(),
    };
    DaemonRunner::new(agent, config).run().await
}

// ------------------------------------------------------------- ConfigAgent

struct ConfigAgent {
    spec: AgentSpec,
    acp: Box<dyn AcpClient>,
    subs_by_topic: HashMap<String, SubscriptionSpec>,
}

impl ConfigAgent {
    fn new(spec: AgentSpec, acp: Box<dyn AcpClient>) -> Self {
        let mut subs_by_topic: HashMap<String, SubscriptionSpec> = spec
            .subscriptions
            .iter()
            .map(|s| (s.topic.clone(), s.clone()))
            .collect();
        let inbox = direct_inbox_topic(&spec.name);
        subs_by_topic.insert(
            inbox.clone(),
            SubscriptionSpec {
                topic: inbox,
                required_scopes: vec!["agent:message".to_string()],
                prompt_template: "Agora session {{session_id}}. Direct {{data.messageType}} message for this agent: {{data.message}}".to_string(),
                emit: None,
            },
        );
        Self {
            spec,
            acp,
            subs_by_topic,
        }
    }

    fn render_prompt(&self, template: &str, envelope: &Envelope) -> String {
        let mut out = String::with_capacity(template.len());
        let bytes = template.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if i + 1 < bytes.len() && bytes[i] == b'{' && bytes[i + 1] == b'{' {
                if let Some(end) = template[i + 2..].find("}}") {
                    let key = template[i + 2..i + 2 + end].trim();
                    out.push_str(&resolve_key(key, envelope));
                    i += 2 + end + 2;
                    continue;
                }
            }
            out.push(bytes[i] as char);
            i += 1;
        }
        out
    }
}

fn resolve_key(key: &str, envelope: &Envelope) -> String {
    match key {
        "topic" => envelope.topic.clone(),
        "session_id" | "sessionId" => envelope.context.session_id.clone(),
        "event_id" | "eventId" => envelope.event_id.clone(),
        "data" => envelope.data.to_string(),
        k if k.starts_with("data.") => {
            let mut current = &envelope.data;
            for part in k[5..].split('.') {
                current = current.get(part).unwrap_or(&serde_json::Value::Null);
            }
            match current {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Null => String::new(),
                v => v.to_string(),
            }
        }
        _ => String::new(),
    }
}

fn parse_agent_response(text: &str) -> serde_json::Value {
    let trimmed = text.trim();
    if let Ok(value) = serde_json::from_str(trimmed) {
        return value;
    }

    for line in text
        .lines()
        .rev()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        if let Ok(value) = serde_json::from_str(line) {
            return value;
        }
    }

    if let Some(value) = parse_fenced_json(text) {
        return value;
    }

    serde_json::json!({ "summary": text })
}

fn parse_fenced_json(text: &str) -> Option<serde_json::Value> {
    let mut parts = text.split("```");
    parts.next();

    let mut parsed = None;
    while let Some(block) = parts.next() {
        let content = strip_fence_language(block.trim());
        if let Ok(value) = serde_json::from_str(content.trim()) {
            parsed = Some(value);
        }
        parts.next();
    }
    parsed
}

fn strip_fence_language(block: &str) -> &str {
    let Some((first, rest)) = block.split_once('\n') else {
        return block;
    };
    match first.trim() {
        "json" | "JSON" => rest,
        _ => block,
    }
}

fn output_topic_and_data(
    parsed: serde_json::Value,
    emit_topic: &str,
) -> (String, serde_json::Value) {
    if let Some(topic) = parsed.get("_topic").and_then(|v| v.as_str()) {
        let mut data = parsed.get("_data").cloned().unwrap_or_else(|| {
            let mut value = parsed.clone();
            if let serde_json::Value::Object(ref mut object) = value {
                object.remove("_topic");
            }
            value
        });
        remove_control_fields(&mut data);
        return (topic.to_string(), data);
    }

    let mut data = parsed;
    remove_control_fields(&mut data);
    (emit_topic.to_string(), data)
}

fn remove_control_fields(value: &mut serde_json::Value) {
    if let serde_json::Value::Object(object) = value {
        object.remove("_topic");
        object.remove("_data");
    }
}

fn affected_files_for_output(topic: &str, data: &serde_json::Value) -> Option<Vec<String>> {
    if topic != CODE_CHANGED {
        return None;
    }

    for field in [
        "changedFiles",
        "affectedFiles",
        "changed_files",
        "affected_files",
    ] {
        if let Some(values) = data.get(field).and_then(|value| value.as_array()) {
            return Some(string_array(values));
        }
    }

    None
}

fn string_array(values: &[serde_json::Value]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values
        .iter()
        .filter_map(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter_map(|value| {
            let value = value.to_string();
            seen.insert(value.clone()).then_some(value)
        })
        .collect()
}

#[async_trait]
impl Agent for ConfigAgent {
    fn agent_name(&self) -> &str {
        &self.spec.name
    }
    fn port(&self) -> u16 {
        self.spec.port
    }
    fn capabilities(&self) -> Vec<String> {
        self.spec.capabilities.clone()
    }

    fn subscriptions(&self) -> Vec<Subscription> {
        let mut subscriptions: Vec<Subscription> = self
            .spec
            .subscriptions
            .iter()
            .map(|s| Subscription {
                topic: s.topic.clone(),
                description: String::new(),
                required_scopes: s.required_scopes.clone(),
            })
            .collect();
        subscriptions.push(Subscription {
            topic: direct_inbox_topic(&self.spec.name),
            description: "Direct steering and queued messages for this agent".into(),
            required_scopes: vec!["agent:message".into()],
        });
        subscriptions
    }

    fn published_events(&self) -> Vec<PublishedEvent> {
        let mut by_topic: HashMap<String, PublishedEvent> = HashMap::new();
        for p in &self.spec.publishes {
            by_topic.insert(
                p.topic.clone(),
                PublishedEvent {
                    topic: p.topic.clone(),
                    description: String::new(),
                    required_scopes: p.required_scopes.clone(),
                },
            );
        }
        for s in &self.spec.subscriptions {
            if let Some(e) = &s.emit {
                by_topic.entry(e.topic.clone()).or_insert(PublishedEvent {
                    topic: e.topic.clone(),
                    description: String::new(),
                    required_scopes: e.required_scopes.clone(),
                });
            }
        }
        by_topic.into_values().collect()
    }

    async fn on_event(&mut self, envelope: Envelope, publisher: Publisher) -> Result<()> {
        let sub = match self.subs_by_topic.get(&envelope.topic).cloned() {
            Some(s) => s,
            None => {
                warn!(topic = %envelope.topic, "no subscription spec matched");
                return Ok(());
            }
        };

        let session_id = publisher.session_id().to_string();
        let rendered = self.render_prompt(&sub.prompt_template, &envelope);
        let preview = &rendered[..rendered.len().min(120)];
        info!(topic = %envelope.topic, prompt = %preview, "prompting ACP");

        let _ = publisher
            .emit_telemetry(
                "INFO",
                "prompt_sent",
                serde_json::json!({
                    "triggerEventId": envelope.event_id,
                    "triggerTopic": envelope.topic,
                    "prompt": rendered,
                }),
            )
            .await;

        if self.acp.session_load(&session_id).await.is_err() {
            self.acp.session_new(&session_id).await?;
        }
        let result = self
            .acp
            .session_prompt(
                &session_id,
                &[serde_json::json!({ "type": "text", "text": rendered })],
            )
            .await?;

        let _ = publisher
            .emit_telemetry(
                "INFO",
                "response_received",
                serde_json::json!({
                    "triggerEventId": envelope.event_id,
                    "response": result.text,
                }),
            )
            .await;

        let parsed = parse_agent_response(&result.text);

        if let Some(emit) = &sub.emit {
            let (out_topic, out_data) = output_topic_and_data(parsed, &emit.topic);
            let affected_files = affected_files_for_output(&out_topic, &out_data);
            publisher
                .publish(&out_topic, out_data, affected_files)
                .await?;
        }

        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.acp.session_close("").await
    }
}

#[cfg(test)]
mod tests {
    use super::{
        affected_files_for_output, output_topic_and_data, parse_agent_response, resolve_key,
        ConfigAgent,
    };
    use agora_core::{
        acp::MockAcpClient,
        agent_spec::{AgentSpec, EmitSpec, SubscriptionSpec},
        envelope::Envelope,
    };

    fn spec_with_sub(template: &str) -> AgentSpec {
        AgentSpec {
            name: "test-agent".into(),
            port: 1234,
            capabilities: vec![],
            kiro_command: None,
            acp: None,
            publishes: vec![],
            subscriptions: vec![SubscriptionSpec {
                topic: "workspace.event.submitted".into(),
                required_scopes: vec!["workspace:read".into()],
                prompt_template: template.into(),
                emit: Some(EmitSpec {
                    topic: "workspace.design.finalized".into(),
                    required_scopes: vec!["workspace:write".into()],
                }),
            }],
        }
    }

    fn envelope(data: serde_json::Value) -> Envelope {
        Envelope::build(
            "workspace.event.submitted",
            "agora-console",
            0,
            "tok",
            "sess_abc",
            data,
            None,
            vec![],
        )
    }

    #[test]
    fn resolves_top_level_keys() {
        let env = envelope(serde_json::json!({ "text": "x" }));
        assert_eq!(resolve_key("topic", &env), "workspace.event.submitted");
        assert_eq!(resolve_key("session_id", &env), "sess_abc");
        assert_eq!(resolve_key("sessionId", &env), "sess_abc");
        assert_eq!(resolve_key("event_id", &env), env.event_id);
        assert_eq!(resolve_key("data.text", &env), "x");
    }

    #[test]
    fn unknown_keys_resolve_empty() {
        let env = envelope(serde_json::json!({}));
        assert_eq!(resolve_key("nope", &env), "");
        assert_eq!(resolve_key("data.missing", &env), "");
    }

    #[test]
    fn resolves_nested_data_paths() {
        let env = envelope(serde_json::json!({ "a": { "b": { "c": "deep" } } }));
        assert_eq!(resolve_key("data.a.b.c", &env), "deep");
    }

    #[test]
    fn renders_prompt_template_substituting_placeholders() {
        let agent = ConfigAgent::new(
            spec_with_sub("event={{data.text}} in {{session_id}} ({{topic}})"),
            Box::new(MockAcpClient::new("test-agent")),
        );
        let env = envelope(serde_json::json!({ "text": "ship it" }));
        let rendered =
            agent.render_prompt("event={{data.text}} in {{session_id}} ({{topic}})", &env);
        assert_eq!(
            rendered,
            "event=ship it in sess_abc (workspace.event.submitted)"
        );
    }

    #[test]
    fn render_prompt_tolerates_no_placeholders() {
        let agent = ConfigAgent::new(
            spec_with_sub("plain text"),
            Box::new(MockAcpClient::new("test-agent")),
        );
        let env = envelope(serde_json::json!({}));
        assert_eq!(agent.render_prompt("plain text", &env), "plain text");
    }

    #[test]
    fn render_prompt_leaves_unterminated_braces_alone() {
        let agent = ConfigAgent::new(
            spec_with_sub("oops {{noend"),
            Box::new(MockAcpClient::new("test-agent")),
        );
        let env = envelope(serde_json::json!({}));
        // No closing `}}` — entire run passes through.
        assert!(agent.render_prompt("oops {{noend", &env).contains("oops"));
    }

    #[test]
    fn parses_exact_json_response() {
        let parsed = parse_agent_response(r#"{"summary":"ok"}"#);
        assert_eq!(parsed["summary"], "ok");
    }

    #[test]
    fn parses_json_from_final_non_empty_line() {
        let parsed = parse_agent_response(
            "I checked the files first.\n\n{\"summary\":\"done\",\"changedFiles\":[\"src/lib.rs\"]}",
        );
        assert_eq!(parsed["summary"], "done");
        assert_eq!(parsed["changedFiles"][0], "src/lib.rs");
    }

    #[test]
    fn parses_last_fenced_json_as_development_fallback() {
        let parsed = parse_agent_response(
            "Result:\n```json\n{\"summary\":\"fenced\",\"status\":\"succeeded\"}\n```\n",
        );
        assert_eq!(parsed["summary"], "fenced");
        assert_eq!(parsed["status"], "succeeded");
    }

    #[test]
    fn wraps_plain_text_response() {
        let parsed = parse_agent_response("plain notes only");
        assert_eq!(parsed["summary"], "plain notes only");
    }

    #[test]
    fn routes_topic_override_and_strips_control_fields() {
        let parsed = serde_json::json!({
            "_topic": "test.failed",
            "_data": {
                "summary": "failed",
                "_topic": "ignored",
                "_data": {}
            }
        });

        let (topic, data) = output_topic_and_data(parsed, "test.passed");

        assert_eq!(topic, "test.failed");
        assert_eq!(data["summary"], "failed");
        assert!(data.get("_topic").is_none());
        assert!(data.get("_data").is_none());
    }

    #[test]
    fn extracts_changed_files_for_code_changed() {
        let data = serde_json::json!({
            "summary": "changed",
            "changedFiles": ["src/lib.rs", " src/main.rs ", "", "src/lib.rs", 7]
        });

        let files = affected_files_for_output("code.changed", &data).unwrap();

        assert_eq!(files, vec!["src/lib.rs", "src/main.rs"]);
    }

    #[test]
    fn ignores_changed_files_for_other_topics() {
        let data = serde_json::json!({ "changedFiles": ["src/lib.rs"] });
        assert!(affected_files_for_output("test.passed", &data).is_none());
    }
}
