use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, path::Path};

use agora_core::agent_spec::AgentSpec;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsConfig {
    pub command: String,
    #[serde(default = "default_nats_log")]
    pub log_file: String,
}

fn default_nats_log() -> String {
    ".agora/logs/nats.log".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_telemetry_log")]
    pub log_file: String,
}

fn default_telemetry_log() -> String {
    ".agora/logs/telemetry.jsonl".into()
}
fn default_true() -> bool {
    true
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            log_file: default_telemetry_log(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopologyConfig {
    pub name: String,
    #[serde(default = "default_bus_url")]
    pub bus_url: String,
    #[serde(default = "default_pid_dir")]
    pub pid_dir: String,
    #[serde(default = "default_log_dir")]
    pub log_dir: String,
    pub nats: NatsConfig,
    #[serde(default)]
    pub telemetry: TelemetryConfig,
    /// Default ACP mode for agents that don't override it.
    #[serde(default = "default_acp", alias = "default_acp")]
    pub default_acp: String,
    pub agents: Vec<AgentSpec>,
}

fn default_bus_url() -> String {
    "nats://127.0.0.1:4222".into()
}
fn default_pid_dir() -> String {
    ".agora/pids".into()
}
fn default_log_dir() -> String {
    ".agora/logs".into()
}
fn default_acp() -> String {
    "mock".into()
}

impl TopologyConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let p = path.as_ref();
        let raw = std::fs::read_to_string(p)
            .with_context(|| format!("Cannot read config {}", p.display()))?;
        let config: Self = serde_json::from_str(&raw)
            .with_context(|| format!("Cannot parse config {}", p.display()))?;
        config.validate()?;
        Ok(config)
    }

    /// Catch typos and structural mistakes at startup so the swarm doesn't
    /// silently degrade. Called automatically from [`TopologyConfig::load`].
    pub fn validate(&self) -> Result<()> {
        if self.agents.is_empty() {
            bail!("topology has no agents");
        }

        // 1. Agent names must be unique.
        let mut seen = HashSet::new();
        for agent in &self.agents {
            if !seen.insert(agent.name.as_str()) {
                bail!("duplicate agent name: {}", agent.name);
            }
            if agent.name.is_empty() {
                bail!("agent has empty name");
            }
        }

        // 2. Every subscription's `emit.topic` must be present in `publishes`
        //    so `Publisher::publish` will accept it. Implicit-from-emit
        //    discovery (in agora-agent) handles this at runtime, but
        //    declaring explicitly catches drift if the agent uses `_topic`
        //    overrides to publish to topics the runtime doesn't know about.
        for agent in &self.agents {
            let declared: HashSet<&str> = agent
                .publishes
                .iter()
                .map(|p| p.topic.as_str())
                .chain(
                    agent
                        .subscriptions
                        .iter()
                        .filter_map(|s| s.emit.as_ref().map(|e| e.topic.as_str())),
                )
                .collect();

            for sub in &agent.subscriptions {
                if let Some(emit) = &sub.emit {
                    if !declared.contains(emit.topic.as_str()) {
                        bail!(
                            "agent `{}` emits to `{}` from subscription `{}`, but `{}` isn't listed in `publishes`",
                            agent.name, emit.topic, sub.topic, emit.topic
                        );
                    }
                }
            }
        }

        // 3. kiro_command required when the agent will use the kiro backend.
        let default_acp = self.default_acp.as_str();
        for agent in &self.agents {
            let mode = agent.acp.as_deref().unwrap_or(default_acp);
            match mode {
                "mock" | "kiro" => {}
                other => bail!(
                    "agent `{}` has unsupported acp mode `{}`",
                    agent.name,
                    other
                ),
            }
            if mode == "kiro"
                && agent
                    .kiro_command
                    .as_deref()
                    .unwrap_or("")
                    .trim()
                    .is_empty()
            {
                bail!(
                    "agent `{}` uses kiro ACP but has no `kiro_command`",
                    agent.name
                );
            }
        }

        // 4. Two agents subscribing to the same topic with conflicting
        //    `required_scopes` is almost certainly a config bug.
        let mut scopes_by_topic: std::collections::HashMap<&str, &Vec<String>> =
            std::collections::HashMap::new();
        for agent in &self.agents {
            for sub in &agent.subscriptions {
                if let Some(prev) = scopes_by_topic.get(sub.topic.as_str()) {
                    if **prev != sub.required_scopes {
                        bail!(
                            "topic `{}` is subscribed with conflicting required_scopes (one agent: {:?}, another: {:?})",
                            sub.topic, prev, sub.required_scopes
                        );
                    }
                } else {
                    scopes_by_topic.insert(sub.topic.as_str(), &sub.required_scopes);
                }
            }
        }

        // 5. Every topic any agent publishes (or subscribes to) must be
        //    covered by the JetStream stream's subject filters, or the
        //    publish will succeed at the NATS layer but fail on the ack
        //    with "Publish ack failed" and the agent will loop forever.
        let stream_subjects = swarm_core_stream_subjects();
        let mut all_topics: HashSet<&str> = HashSet::new();
        for agent in &self.agents {
            for p in &agent.publishes {
                all_topics.insert(p.topic.as_str());
            }
            for s in &agent.subscriptions {
                all_topics.insert(s.topic.as_str());
                if let Some(emit) = &s.emit {
                    all_topics.insert(emit.topic.as_str());
                }
            }
        }
        for topic in &all_topics {
            if !stream_subjects.iter().any(|filter| subject_matches(filter, topic)) {
                bail!(
                    "topic `{topic}` is referenced by the topology but not covered by any JetStream \
                     subject filter ({stream_subjects:?}). Add a matching pattern to \
                     `EVENT_STREAM_SUBJECTS` in agora-core/src/topics.rs and restart."
                );
            }
        }

        Ok(())
    }
}

fn swarm_core_stream_subjects() -> &'static [&'static str] {
    agora_core::topics::EVENT_STREAM_SUBJECTS
}

/// Match a NATS subject filter (with `.` separators and `>` / `*` wildcards)
/// against a concrete topic. `>` matches all remaining tokens; `*` matches
/// exactly one.
fn subject_matches(filter: &str, topic: &str) -> bool {
    let f: Vec<&str> = filter.split('.').collect();
    let t: Vec<&str> = topic.split('.').collect();
    let mut i = 0;
    while i < f.len() {
        if f[i] == ">" {
            return true;
        }
        if i >= t.len() {
            return false;
        }
        if f[i] != "*" && f[i] != t[i] {
            return false;
        }
        i += 1;
    }
    i == t.len()
}

#[cfg(test)]
mod tests {
    use super::TopologyConfig;
    use std::path::Path;

    fn parse(json: &str) -> super::Result<TopologyConfig> {
        let cfg: TopologyConfig = serde_json::from_str(json)?;
        cfg.validate()?;
        Ok(cfg)
    }

    const MINIMAL_NATS: &str = r#""nats": { "command": "nats-server -js" }"#;

    #[test]
    fn rejects_empty_agents() {
        let err = parse(&format!(
            r#"{{ "name": "t", {MINIMAL_NATS}, "agents": [] }}"#
        ))
        .unwrap_err();
        assert!(err.to_string().contains("no agents"));
    }

    #[test]
    fn rejects_duplicate_agent_names() {
        let err = parse(&format!(
            r#"{{
                "name": "t", {MINIMAL_NATS},
                "agents": [
                    {{ "name": "a", "port": 1, "subscriptions": [] }},
                    {{ "name": "a", "port": 2, "subscriptions": [] }}
                ]
            }}"#
        ))
        .unwrap_err();
        assert!(err.to_string().contains("duplicate agent name"));
    }

    #[test]
    fn rejects_kiro_without_command() {
        let err = parse(&format!(
            r#"{{
                "name": "t", {MINIMAL_NATS},
                "default_acp": "kiro",
                "agents": [
                    {{ "name": "a", "port": 1, "subscriptions": [] }}
                ]
            }}"#
        ))
        .unwrap_err();
        assert!(err.to_string().contains("kiro"));
    }

    #[test]
    fn rejects_unknown_acp_mode() {
        let err = parse(&format!(
            r#"{{
                "name": "t", {MINIMAL_NATS},
                "agents": [
                    {{ "name": "a", "port": 1, "acp": "potato", "subscriptions": [] }}
                ]
            }}"#
        ))
        .unwrap_err();
        assert!(err.to_string().contains("unsupported acp"));
    }

    #[test]
    fn rejects_conflicting_subscription_scopes() {
        let err = parse(&format!(
            r#"{{
                "name": "t", {MINIMAL_NATS},
                "agents": [
                    {{ "name": "a", "port": 1, "subscriptions": [
                        {{ "topic": "workspace.idea.submitted", "required_scopes": ["x"], "prompt_template": "_" }}
                    ] }},
                    {{ "name": "b", "port": 2, "subscriptions": [
                        {{ "topic": "workspace.idea.submitted", "required_scopes": ["y"], "prompt_template": "_" }}
                    ] }}
                ]
            }}"#
        ))
        .unwrap_err();
        assert!(err.to_string().contains("conflicting required_scopes"));
    }

    #[test]
    fn local_topology_wires_workspace_kiro_agents() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let topology = TopologyConfig::load(root.join("agents.local.json")).unwrap();

        assert_eq!(topology.default_acp, "kiro");

        let expected = [
            "backend-coder",
            "frontend-coder",
            "product-manager",
            "quality-assurance",
            "security-engineer",
            "system-architect",
        ];
        let mut actual = topology
            .agents
            .iter()
            .map(|agent| agent.name.as_str())
            .collect::<Vec<_>>();
        actual.sort_unstable();

        assert_eq!(actual, expected);

        for agent in &topology.agents {
            let command = agent.kiro_command.as_deref().unwrap_or_default();
            assert!(
                command.contains(&format!("--agent {}", agent.name)),
                "{} has unexpected kiro_command: {command}",
                agent.name
            );
            assert!(
                root.join(".kiro/agents")
                    .join(format!("{}.json", agent.name))
                    .exists(),
                "{} has no matching workspace Kiro config",
                agent.name
            );
        }
    }
}
