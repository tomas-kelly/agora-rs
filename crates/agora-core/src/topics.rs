// Lifecycle / registry
pub const AGENT_REGISTRY_HEARTBEAT: &str = "agent.registry.heartbeat";
pub const AGENT_TELEMETRY_LOGS: &str = "agent.telemetry.logs";

// Workspace flow
pub const WORKSPACE_IDEA_SUBMITTED: &str = "workspace.idea.submitted";
pub const WORKSPACE_DESIGN_FINALIZED: &str = "workspace.design.finalized";

// Code & feedback loops
pub const CODE_CHANGED: &str = "code.changed";
pub const TEST_PASSED: &str = "test.passed";
pub const TEST_FAILED: &str = "test.failed";
pub const SECURITY_ALERT_FOUND: &str = "security.alert.found";
pub const SECURITY_SCAN_CLEAN: &str = "security.scan.clean";

// Human-in-the-loop
pub const HUMAN_INTERACTION_REQUEST: &str = "human.interaction.request";

// Session metadata
pub const SESSION_NAMED: &str = "session.named";
pub const SESSION_DELETED: &str = "session.deleted";

// JetStream
pub const EVENT_STREAM: &str = "AGORA_EVENTS";
/// Subjects captured by the `AGORA_EVENTS` JetStream stream.
///
/// **Any topic an agent publishes must match one of these patterns**, or the
/// publish will succeed at the NATS layer but JetStream will return "no
/// stream matches the subject" on the ack and the agent will loop forever
/// on `Publish ack failed`. Adding a new topic family means appending it
/// here AND restarting the swarm so `Bus::ensure_event_stream` updates the
/// live stream to include the new subjects.
pub const EVENT_STREAM_SUBJECTS: &[&str] = &[
    "workspace.>",
    "product.>",
    "code.>",
    "test.>",
    "security.>",
    "human.>",
    "agent.>",
    "event.>",
    "session.>",
];

// KV bucket for agent registry
pub const KV_AGENT_REGISTRY: &str = "AGORA_AGENT_REGISTRY";

pub fn direct_inbox_topic(agent_name: &str) -> String {
    format!("agent.inbox.{}", agent_name)
}

pub fn consumer_name(agent_name: &str, topic: &str) -> String {
    format!(
        "{}_{}",
        sanitize_consumer_token(agent_name),
        topic.replace('.', "_")
    )
}

fn sanitize_consumer_token(input: &str) -> String {
    let sanitized: String = input
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect();

    if sanitized.is_empty() {
        "agent".to_string()
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use super::consumer_name;

    #[test]
    fn consumer_names_are_nats_safe() {
        assert_eq!(
            consumer_name("backend-developer-daemon", "workspace.design.finalized"),
            "backend_developer_daemon_workspace_design_finalized"
        );
    }
}
