//! Lightweight topology readers used by clients (CLI + console) that need
//! to know which topics the running swarm understands and what scopes
//! must be on a publisher's actor token for downstream agents to accept
//! the event.
//!
//! The agora supervisor crate has a richer `TopologyConfig` with startup
//! validation — this module deliberately stays minimal and dependency-free
//! so the console can load a topology snapshot without pulling in the
//! supervisor.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

/// Read-only view of which topics the running swarm declares and the
/// minimum set of actor-token scopes required for a publisher to land an
/// event on each one.
///
/// A human submitting from the console or CLI must mint a token whose
/// scopes are a superset of `scopes_for(topic)`, or every subscribing
/// agent will reject the event during JWT authorization.
#[derive(Debug, Clone, Default)]
pub struct TopicCatalog {
    /// Every topic name declared anywhere in the topology, plus the
    /// synthetic `agent.inbox.<name>` entries that the agent runtime
    /// adds at startup.
    pub known: BTreeSet<String>,
    /// Per topic, the union of `required_scopes` across every
    /// subscribing agent.
    pub subscriber_scopes: BTreeMap<String, Vec<String>>,
}

impl TopicCatalog {
    pub fn knows(&self, topic: &str) -> bool {
        self.known.contains(topic)
    }

    pub fn scopes_for(&self, topic: &str) -> &[String] {
        self.subscriber_scopes
            .get(topic)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Suggestions for a topic-completion prompt: every known topic
    /// whose name starts with `prefix`, sorted.
    pub fn matching(&self, prefix: &str) -> Vec<String> {
        self.known
            .range(prefix.to_string()..)
            .take_while(|t| t.starts_with(prefix))
            .cloned()
            .collect()
    }
}

/// Subset of the topology JSON needed to build a [`TopicCatalog`]. Both
/// the agora supervisor (via its richer `TopologyConfig`) and the
/// console parse this same JSON shape.
#[derive(Debug, Clone, Deserialize)]
pub struct TopologySnapshot {
    #[serde(default)]
    pub agents: Vec<AgentSnapshot>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentSnapshot {
    pub name: String,
    #[serde(default)]
    pub publishes: Vec<TopicSnapshot>,
    #[serde(default)]
    pub subscriptions: Vec<SubscriptionSnapshot>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TopicSnapshot {
    pub topic: String,
    #[serde(default)]
    pub required_scopes: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SubscriptionSnapshot {
    pub topic: String,
    #[serde(default)]
    pub required_scopes: Vec<String>,
    #[serde(default)]
    pub emit: Option<TopicSnapshot>,
}

impl TopologySnapshot {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let p = path.as_ref();
        let raw = std::fs::read_to_string(p)
            .with_context(|| format!("cannot read topology {}", p.display()))?;
        let snap: Self = serde_json::from_str(&raw)
            .with_context(|| format!("cannot parse topology {}", p.display()))?;
        Ok(snap)
    }

    pub fn topic_catalog(&self) -> TopicCatalog {
        let mut known = BTreeSet::new();
        let mut subscriber_scopes: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

        for agent in &self.agents {
            // Synthetic per-agent inbox — added by the agent runtime at
            // startup; clients need to know it's valid to publish there.
            let inbox = format!("agent.inbox.{}", agent.name);
            known.insert(inbox.clone());
            subscriber_scopes
                .entry(inbox)
                .or_default()
                .insert("agent:message".to_string());

            for p in &agent.publishes {
                known.insert(p.topic.clone());
            }
            for s in &agent.subscriptions {
                known.insert(s.topic.clone());
                if let Some(emit) = &s.emit {
                    known.insert(emit.topic.clone());
                }
                let entry = subscriber_scopes.entry(s.topic.clone()).or_default();
                for scope in &s.required_scopes {
                    entry.insert(scope.clone());
                }
            }
        }

        TopicCatalog {
            known,
            subscriber_scopes: subscriber_scopes
                .into_iter()
                .map(|(k, v)| (k, v.into_iter().collect()))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_unions_subscriber_scopes_across_agents() {
        let snap: TopologySnapshot = serde_json::from_str(
            r#"{
                "agents": [
                    {
                        "name": "a",
                        "subscriptions": [
                            {"topic": "t1", "required_scopes": ["x"]}
                        ]
                    },
                    {
                        "name": "b",
                        "subscriptions": [
                            {"topic": "t1", "required_scopes": ["y"]}
                        ]
                    }
                ]
            }"#,
        )
        .unwrap();
        let catalog = snap.topic_catalog();
        let scopes = catalog.scopes_for("t1");
        assert!(scopes.contains(&"x".to_string()));
        assert!(scopes.contains(&"y".to_string()));
    }

    #[test]
    fn synthetic_inbox_topics_are_known() {
        let snap: TopologySnapshot =
            serde_json::from_str(r#"{"agents": [{"name": "product-manager"}]}"#).unwrap();
        let catalog = snap.topic_catalog();
        assert!(catalog.knows("agent.inbox.product-manager"));
        assert!(catalog
            .scopes_for("agent.inbox.product-manager")
            .contains(&"agent:message".to_string()));
    }

    #[test]
    fn matching_prefixes_for_completion() {
        let snap: TopologySnapshot = serde_json::from_str(
            r#"{
                "agents": [{
                    "name": "pm",
                    "publishes": [
                        {"topic": "code.changed"},
                        {"topic": "code.reviewed"},
                        {"topic": "test.passed"}
                    ]
                }]
            }"#,
        )
        .unwrap();
        let catalog = snap.topic_catalog();
        let matches = catalog.matching("code.");
        assert_eq!(matches, vec!["code.changed", "code.reviewed"]);
    }
}
