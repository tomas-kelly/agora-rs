use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

fn new_event_id() -> String {
    format!("evt_{}", Ulid::new())
}

fn utcnow_iso() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sender {
    pub agent_name: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Security {
    pub actor_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Context {
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_commit: Option<String>,
    #[serde(default)]
    pub affected_files: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Envelope {
    pub event_id: String,
    pub timestamp: String,
    pub topic: String,
    pub sender: Sender,
    pub security: Security,
    pub context: Context,
    #[serde(default)]
    pub data: serde_json::Value,
}

impl Envelope {
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        topic: impl Into<String>,
        agent_name: impl Into<String>,
        port: u16,
        actor_token: impl Into<String>,
        session_id: impl Into<String>,
        data: serde_json::Value,
        git_commit: Option<String>,
        affected_files: Vec<String>,
    ) -> Self {
        Self {
            event_id: new_event_id(),
            timestamp: utcnow_iso(),
            topic: topic.into(),
            sender: Sender {
                agent_name: agent_name.into(),
                port,
            },
            security: Security {
                actor_token: actor_token.into(),
            },
            context: Context {
                session_id: session_id.into(),
                git_commit,
                affected_files,
                tags: vec![],
            },
            data,
        }
    }

    /// Build a child envelope that inherits session_id and actor_token.
    pub fn child(
        &self,
        topic: impl Into<String>,
        agent_name: impl Into<String>,
        port: u16,
        data: serde_json::Value,
        affected_files: Option<Vec<String>>,
        git_commit: Option<String>,
    ) -> Self {
        let mut child = Self::build(
            topic,
            agent_name,
            port,
            &self.security.actor_token,
            &self.context.session_id,
            data,
            git_commit.or_else(|| self.context.git_commit.clone()),
            affected_files.unwrap_or_else(|| self.context.affected_files.clone()),
        );
        child.context.tags = self.context.tags.clone();
        child
    }

    pub fn to_bytes(&self) -> Result<bytes::Bytes> {
        let json = serde_json::to_vec(self)?;
        Ok(bytes::Bytes::from(json))
    }

    pub fn from_bytes(raw: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(raw)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> Envelope {
        Envelope::build(
            "workspace.event.submitted",
            "agora-console",
            0,
            "tok",
            "sess_abc",
            serde_json::json!({ "text": "build a thing" }),
            Some("deadbeef".into()),
            vec!["src/main.rs".into()],
        )
    }

    #[test]
    fn round_trips_through_bytes() {
        let env = fixture();
        let bytes = env.to_bytes().unwrap();
        let decoded = Envelope::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.topic, env.topic);
        assert_eq!(decoded.event_id, env.event_id);
        assert_eq!(decoded.context.session_id, "sess_abc");
        assert_eq!(decoded.context.affected_files, vec!["src/main.rs"]);
        assert_eq!(decoded.data["text"], "build a thing");
    }

    #[test]
    fn serializes_to_camel_case_keys() {
        let env = fixture();
        let bytes = env.to_bytes().unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(json.get("eventId").is_some(), "eventId key missing");
        assert!(json.get("event_id").is_none(), "snake_case key leaked");
        assert!(json["sender"].get("agentName").is_some());
        assert!(json["context"].get("sessionId").is_some());
        assert!(json["context"].get("affectedFiles").is_some());
    }

    #[test]
    fn child_inherits_session_and_token() {
        let parent = fixture();
        let child = parent.child(
            "code.changed",
            "backend-coder",
            42,
            serde_json::json!({ "summary": "ok" }),
            None,
            None,
        );
        assert_eq!(child.context.session_id, parent.context.session_id);
        assert_eq!(child.security.actor_token, parent.security.actor_token);
        assert_eq!(child.context.affected_files, parent.context.affected_files);
        assert_eq!(child.context.git_commit, parent.context.git_commit);
        assert_ne!(child.event_id, parent.event_id, "child must mint new id");
        assert_eq!(child.sender.agent_name, "backend-coder");
        assert_eq!(child.topic, "code.changed");
    }

    #[test]
    fn child_can_override_affected_files() {
        let parent = fixture();
        let child = parent.child(
            "code.changed",
            "x",
            0,
            serde_json::json!({}),
            Some(vec!["src/other.rs".into()]),
            None,
        );
        assert_eq!(child.context.affected_files, vec!["src/other.rs"]);
    }
}
