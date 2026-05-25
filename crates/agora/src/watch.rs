use anyhow::{Context, Result};
use clap::Args;
use futures::StreamExt;
use std::io::IsTerminal;
use tokio::sync::mpsc;

use agora_core::{bus::Bus, envelope::Envelope, topics::EVENT_STREAM_SUBJECTS};

#[derive(Debug, Clone, Args)]
pub struct WatchArgs {
    #[arg(long)]
    pub session_id: Option<String>,
    #[arg(long)]
    pub agent: Option<String>,
    #[arg(long)]
    pub json: bool,
    #[arg(long, default_value = "nats://127.0.0.1:4222")]
    pub bus_url: String,
}

pub async fn run_watch(args: &WatchArgs) -> Result<()> {
    let bus = Bus::connect(&args.bus_url).await?;
    let (tx, mut rx) = mpsc::channel::<Envelope>(256);

    for subject in EVENT_STREAM_SUBJECTS {
        let mut sub = bus
            .client
            .subscribe((*subject).to_string())
            .await
            .with_context(|| format!("subscribe {subject}"))?;
        let tx = tx.clone();
        tokio::spawn(async move {
            while let Some(msg) = sub.next().await {
                if let Ok(env) = Envelope::from_bytes(&msg.payload) {
                    if tx.send(env).await.is_err() {
                        break;
                    }
                }
            }
        });
    }
    drop(tx);

    loop {
        tokio::select! {
            Some(event) = rx.recv() => {
                if !event_matches(&event, args) {
                    continue;
                }
                print_watch_event(&event, args.json)?;
            }
            _ = tokio::signal::ctrl_c() => {
                return Ok(());
            }
        }
    }
}

fn print_watch_event(event: &Envelope, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(event)?);
    } else {
        println!(
            "{}",
            format_watch_line(event, std::io::stdout().is_terminal())
        );
    }
    Ok(())
}

pub fn format_watch_line(event: &Envelope, color: bool) -> String {
    let time = if event.timestamp.len() >= 19 {
        &event.timestamp[11..19]
    } else {
        &event.timestamp
    };

    let topic_str = if color {
        let c = topic_color(&event.topic);
        if c.is_empty() {
            event.topic.clone()
        } else {
            format!("{c}{}\x1b[0m", event.topic)
        }
    } else {
        event.topic.clone()
    };

    let sender = &event.sender.agent_name;
    let session_short = if event.context.session_id.len() > 12 {
        &event.context.session_id[..12]
    } else {
        &event.context.session_id
    };

    let mut out = format!("{time}  {topic_str:<40}  {sender:<20}  {session_short}");

    if !event.data.is_null() && event.data != serde_json::json!({}) {
        if let Ok(s) = serde_json::to_string(&event.data) {
            let truncated = if s.len() > 120 { &s[..120] } else { &s };
            out.push_str(&format!("\n  {truncated}"));
        }
    }

    out
}

fn event_matches(event: &Envelope, args: &WatchArgs) -> bool {
    if args
        .session_id
        .as_deref()
        .is_some_and(|sid| event.context.session_id != sid)
    {
        return false;
    }
    if args
        .agent
        .as_deref()
        .is_some_and(|a| event.sender.agent_name != a)
    {
        return false;
    }
    true
}

fn topic_color(topic: &str) -> &'static str {
    if topic.starts_with("workspace.") {
        "\x1b[36m"
    } else if topic.starts_with("code.") {
        "\x1b[33m"
    } else if topic == "test.passed" {
        "\x1b[32m"
    } else if topic == "test.failed" {
        "\x1b[31m"
    } else if topic.starts_with("security.") {
        if topic.contains("clean") {
            "\x1b[32m"
        } else {
            "\x1b[31m"
        }
    } else if topic.starts_with("human.") {
        "\x1b[35m"
    } else if topic.starts_with("agent.") {
        "\x1b[34m"
    } else {
        ""
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_envelope(topic: &str) -> Envelope {
        Envelope::build(
            topic,
            "test-agent",
            0,
            "tok",
            "sess_01ABCDEF",
            serde_json::json!({ "summary": "All tests passing" }),
            None,
            vec![],
        )
    }

    #[test]
    fn format_line_colored_test_passed() {
        let env = test_envelope("test.passed");
        let output = format_watch_line(&env, true);
        assert!(output.contains("\x1b[32m"), "should contain green");
        assert!(output.contains("\x1b[0m"), "should contain reset");
        assert!(output.contains("test.passed"));
    }

    #[test]
    fn format_line_no_color_when_disabled() {
        let env = test_envelope("test.passed");
        let output = format_watch_line(&env, false);
        assert!(
            !output.contains("\x1b["),
            "should not contain escape sequences"
        );
        assert!(output.contains("test.passed"));
    }

    #[test]
    fn json_roundtrip() {
        let env = test_envelope("test.passed");
        let json = serde_json::to_string(&env).unwrap();
        let decoded: Envelope = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.topic, "test.passed");
        assert_eq!(decoded.context.session_id, "sess_01ABCDEF");
    }

    #[test]
    fn color_mapping_workspace_cyan() {
        let env = test_envelope("workspace.event.submitted");
        let output = format_watch_line(&env, true);
        assert!(output.contains("\x1b[36m"));
    }

    #[test]
    fn color_mapping_code_yellow() {
        let env = test_envelope("code.changed");
        let output = format_watch_line(&env, true);
        assert!(output.contains("\x1b[33m"));
    }

    #[test]
    fn color_mapping_security_clean_green() {
        let env = test_envelope("security.scan.clean");
        let output = format_watch_line(&env, true);
        assert!(output.contains("\x1b[32m"));
    }

    #[test]
    fn color_mapping_security_alert_red() {
        let env = test_envelope("security.alert.found");
        let output = format_watch_line(&env, true);
        assert!(output.contains("\x1b[31m"));
    }
}
