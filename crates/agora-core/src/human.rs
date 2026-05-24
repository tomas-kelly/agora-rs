use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HumanInteractionRequest {
    pub question: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub choices: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
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
            question: "Pick a color".into(),
            choices: Some(vec!["red".into(), "blue".into()]),
            timeout_secs: Some(60),
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
            question: "Yes or no?".into(),
            choices: None,
            timeout_secs: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(!json.contains("choices"));
        assert!(!json.contains("timeoutSecs"));
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
