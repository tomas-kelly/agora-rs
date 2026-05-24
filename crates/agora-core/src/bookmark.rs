use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BookmarkEvent {
    pub target_event_id: String,
    pub label: Option<String>,
    pub actor: String,
}

pub fn validate_bookmark_label(input: &str) -> anyhow::Result<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        anyhow::bail!("label cannot be empty");
    }
    if trimmed.len() > 128 {
        anyhow::bail!("label exceeds 128 characters");
    }
    if !trimmed
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == ' ' || c == '_' || c == '-')
    {
        anyhow::bail!(
            "label may only contain alphanumeric characters, spaces, dashes, and underscores"
        );
    }
    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_labels() {
        assert_eq!(
            validate_bookmark_label("key decision").unwrap(),
            "key decision"
        );
        assert_eq!(validate_bookmark_label("bug-123").unwrap(), "bug-123");
        assert_eq!(
            validate_bookmark_label("test_failure").unwrap(),
            "test_failure"
        );
    }

    #[test]
    fn rejects_empty() {
        assert!(validate_bookmark_label("").is_err());
        assert!(validate_bookmark_label("   ").is_err());
    }

    #[test]
    fn rejects_too_long() {
        let long = "a".repeat(129);
        assert!(validate_bookmark_label(&long).is_err());
    }

    #[test]
    fn rejects_special_chars() {
        assert!(validate_bookmark_label("<script>alert(1)</script>").is_err());
        assert!(validate_bookmark_label("has\nnewline").is_err());
        assert!(validate_bookmark_label("has\ttab").is_err());
    }

    #[test]
    fn bookmark_event_serde_roundtrip() {
        let ev = BookmarkEvent {
            target_event_id: "evt_123".into(),
            label: Some("important".into()),
            actor: "console-user".into(),
        };
        let json = serde_json::to_string(&ev).unwrap();
        let back: BookmarkEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back.target_event_id, "evt_123");
        assert_eq!(back.label.as_deref(), Some("important"));
        assert_eq!(back.actor, "console-user");
    }
}
