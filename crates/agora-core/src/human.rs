use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HumanInteractionRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub question: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub choices: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HumanInteractionResponse {
    pub correlation_id: String,
    pub answer: String,
    pub responded_by: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_round_trips() {
        let req = HumanInteractionRequest {
            kind: None,
            question: "Pick a color".into(),
            choices: Some(vec!["red".into(), "blue".into()]),
            timeout_secs: Some(60),
            details: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: HumanInteractionRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.question, "Pick a color");
        assert_eq!(decoded.choices.unwrap(), vec!["red", "blue"]);
        assert_eq!(decoded.timeout_secs.unwrap(), 60);
    }

    #[test]
    fn request_omits_none_fields() {
        let req = HumanInteractionRequest {
            kind: None,
            question: "Yes or no?".into(),
            choices: None,
            timeout_secs: None,
            details: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(!json.contains("choices"));
        assert!(!json.contains("timeoutSecs"));
        assert!(!json.contains("kind"));
        assert!(!json.contains("details"));
    }

    #[test]
    fn request_supports_structured_tool_approval_details() {
        let req = HumanInteractionRequest {
            kind: Some("tool_approval".into()),
            question: "Approve tool call?".into(),
            choices: Some(vec!["allow-once".into(), "reject-once".into()]),
            timeout_secs: Some(300),
            details: Some(serde_json::json!({
                "toolCall": {
                    "toolCallId": "call_1",
                    "title": "Run cargo test"
                }
            })),
        };

        let json = serde_json::to_value(&req).unwrap();

        assert_eq!(json["kind"], "tool_approval");
        assert_eq!(json["details"]["toolCall"]["toolCallId"], "call_1");
    }

    #[test]
    fn response_round_trips() {
        let resp = HumanInteractionResponse {
            correlation_id: "evt_abc".into(),
            answer: "yes".into(),
            responded_by: "cli".into(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: HumanInteractionResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.correlation_id, "evt_abc");
        assert_eq!(decoded.answer, "yes");
        assert_eq!(decoded.responded_by, "cli");
    }

    #[tokio::test]
    async fn timeout_produces_timeout_answer() {
        tokio::time::pause();
        let (tx, rx) = tokio::sync::oneshot::channel::<HumanInteractionResponse>();
        let timeout_dur = std::time::Duration::from_secs(120);
        let result = tokio::time::timeout(timeout_dur, rx);
        // Advance time past the timeout
        tokio::time::advance(std::time::Duration::from_secs(121)).await;
        let outcome = result.await;
        assert!(outcome.is_err()); // Elapsed = timeout
        drop(tx);
    }
}
