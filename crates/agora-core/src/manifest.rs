use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const AGENT_STALE_AFTER_SECS: i64 = 15;
pub const AGENT_DOWN_AFTER_SECS: i64 = 60;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentStatus {
    Starting,
    Ready,
    Busy,
    Stale,
    Draining,
    Down,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Subscription {
    pub topic: String,
    pub description: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub required_scopes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishedEvent {
    pub topic: String,
    pub description: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub required_scopes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentManifest {
    pub agent_name: String,
    pub port: u16,
    pub endpoint: String,
    pub capabilities: Vec<String>,
    pub subscribes_to: Vec<String>,
    pub publishes: Vec<String>,
    pub status: AgentStatus,
    pub last_seen: String,
}

impl AgentManifest {
    pub fn new(
        agent_name: impl Into<String>,
        port: u16,
        capabilities: Vec<String>,
        subscribes_to: Vec<String>,
        publishes: Vec<String>,
    ) -> Self {
        let agent_name = agent_name.into();
        let endpoint = format!("http://127.0.0.1:{}/acp/v1", port);
        Self {
            agent_name,
            port,
            endpoint,
            capabilities,
            subscribes_to,
            publishes,
            status: AgentStatus::Starting,
            last_seen: Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        }
    }

    pub fn with_status(mut self, status: AgentStatus) -> Self {
        self.status = status;
        self.last_seen = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        self
    }

    pub fn observed_status(&self) -> AgentStatus {
        self.observed_status_at(Utc::now())
    }

    pub fn observed_status_at(&self, now: DateTime<Utc>) -> AgentStatus {
        if self.status == AgentStatus::Down {
            return AgentStatus::Down;
        }

        let Ok(last_seen) = DateTime::parse_from_rfc3339(&self.last_seen) else {
            return AgentStatus::Stale;
        };
        let age = now.signed_duration_since(last_seen.with_timezone(&Utc));
        if age.num_seconds() >= AGENT_DOWN_AFTER_SECS {
            AgentStatus::Down
        } else if age.num_seconds() >= AGENT_STALE_AFTER_SECS {
            AgentStatus::Stale
        } else {
            self.status.clone()
        }
    }

    pub fn to_bytes(&self) -> anyhow::Result<bytes::Bytes> {
        Ok(bytes::Bytes::from(serde_json::to_vec(self)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn observed_status_marks_old_heartbeats_stale_and_down() {
        let now = Utc::now();
        let mut manifest = AgentManifest::new("agent", 4001, vec![], vec![], vec![])
            .with_status(AgentStatus::Ready);

        manifest.last_seen = (now - Duration::seconds(AGENT_STALE_AFTER_SECS + 1))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
        assert_eq!(manifest.observed_status_at(now), AgentStatus::Stale);

        manifest.last_seen = (now - Duration::seconds(AGENT_DOWN_AFTER_SECS + 1))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
        assert_eq!(manifest.observed_status_at(now), AgentStatus::Down);
    }
}
