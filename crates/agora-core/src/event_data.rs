use anyhow::{bail, Result};

pub const DEFAULT_EVENT_TEXT_FIELD: &str = "text";

/// Convert user-facing event data into an envelope payload.
///
/// Valid JSON is passed through as-is. Plain text is wrapped under
/// `text_field`, which keeps CLI/TUI submission ergonomic while preserving a
/// deterministic JSON payload shape for agents.
pub fn event_data_from_input(input: &str, text_field: &str) -> Result<serde_json::Value> {
    let text_field = text_field.trim();
    if text_field.is_empty() {
        bail!("event text field cannot be empty");
    }

    let trimmed = input.trim();
    match serde_json::from_str::<serde_json::Value>(trimmed) {
        Ok(value) => Ok(value),
        Err(_) => Ok(serde_json::json!({ text_field: input })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_json_event_data() {
        let data = event_data_from_input(r#"{"kind":"task","priority":2}"#, "text").unwrap();
        assert_eq!(data["kind"], "task");
        assert_eq!(data["priority"], 2);
    }

    #[test]
    fn wraps_plain_text_event_data() {
        let data = event_data_from_input("Build a task tracker", "text").unwrap();
        assert_eq!(data["text"], "Build a task tracker");
    }

    #[test]
    fn supports_custom_text_field() {
        let data = event_data_from_input("Build a task tracker", "request").unwrap();
        assert_eq!(data["request"], "Build a task tracker");
    }

    #[test]
    fn rejects_empty_text_field() {
        let err = event_data_from_input("hello", "  ").unwrap_err();
        assert!(err.to_string().contains("cannot be empty"));
    }
}
