//! Declarative agent definition — what an agent subscribes to, what prompt to
//! render for each topic, and what to emit. Used by `agora-agent` (the
//! generic runner) and by the supervisor to enumerate agents.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmitSpec {
    pub topic: String,
    #[serde(default)]
    pub required_scopes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionSpec {
    pub topic: String,
    #[serde(default)]
    pub required_scopes: Vec<String>,
    pub prompt_template: String,
    /// Default topic + scopes for the event this subscription emits.
    /// The actual emit topic may be overridden by a `_topic` key in the
    /// ACP response.
    pub emit: Option<EmitSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishedEventSpec {
    pub topic: String,
    #[serde(default)]
    pub required_scopes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSpec {
    pub name: String,
    pub port: u16,
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Shell command that launches the configured ACP server for this agent.
    /// Required when `acp == "stdio"`.
    pub acp_command: Option<String>,
    /// ACP backend override. If unset, falls back to the topology-level
    /// `default_acp`.
    pub acp: Option<String>,
    /// Optional per-agent timeout, in seconds, for ACP requests.
    #[serde(default, alias = "acpTimeoutSecs")]
    pub acp_timeout_secs: Option<u64>,
    pub subscriptions: Vec<SubscriptionSpec>,
    /// Optional explicit list. The runner also auto-includes every emit
    /// topic from `subscriptions`, so this only needs entries for topics
    /// reachable via the `_topic` override.
    #[serde(default)]
    pub publishes: Vec<PublishedEventSpec>,
}
