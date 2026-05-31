use std::{
    net::TcpListener,
    process::{Child, Command, Output, Stdio},
    time::{Duration, Instant},
};

use agora_core::{
    bus::Bus,
    daemon::{Agent, DaemonConfig, DaemonRunner, Publisher},
    envelope::Envelope,
    manifest::{PublishedEvent, Subscription},
    tokens::{load_signing_key, verify_actor_token},
    topics::{consumer_name, CODE_CHANGED, WORKSPACE_EVENT_SUBMITTED},
};
use async_trait::async_trait;

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.0.kill().ok();
        self.0.wait().ok();
    }
}

#[test]
fn cli_mutating_commands_work_against_isolated_nats() {
    let Some(nats) = which("nats-server") else {
        eprintln!("skipping CLI smoke test: nats-server is not on PATH");
        return;
    };
    let temp = tempfile::tempdir().expect("tempdir");
    let port = free_port();
    let bus_url = format!("nats://127.0.0.1:{port}");
    let store_dir = temp.path().join("nats-store");
    let key_path = temp.path().join("session_token");
    let topology_path = temp.path().join("topology.json");

    std::fs::write(
        &topology_path,
        serde_json::json!({
            "name": "cli-smoke",
            "busUrl": bus_url,
            "nats": { "command": "nats-server -js" },
            "agents": [
                {
                    "name": "product-manager",
                    "port": 4100,
                    "subscriptions": []
                }
            ]
        })
        .to_string(),
    )
    .expect("write topology");

    let mut child = Command::new(nats)
        .args(["-js", "-p", &port.to_string(), "-sd"])
        .arg(&store_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start nats-server");
    wait_for_nats(&mut child, &bus_url);
    let _guard = ChildGuard(child);

    run_ok(["bootstrap", "--key-path", key_path.to_str().unwrap()]);

    let out = run_ok([
        "session",
        "new",
        "Smoke Session",
        "--bus-url",
        &bus_url,
        "--key-path",
        key_path.to_str().unwrap(),
    ]);
    let stdout = String::from_utf8(out.stdout).unwrap();
    let session_id = stdout
        .split_whitespace()
        .find(|part| part.starts_with("sess_"))
        .expect("session id")
        .trim_end_matches(':')
        .to_string();

    run_ok([
        "session",
        "rename",
        &session_id,
        "Renamed Smoke",
        "--bus-url",
        &bus_url,
        "--key-path",
        key_path.to_str().unwrap(),
    ]);
    run_ok([
        "submit",
        "workspace.event.submitted",
        "Build smoke path",
        "--sessionId",
        &session_id,
        "--bus-url",
        &bus_url,
        "--key-path",
        key_path.to_str().unwrap(),
    ]);
    let missing_session = run(["message", "--agent", "product-manager", "unscoped"]);
    assert!(
        !missing_session.status.success(),
        "message without --sessionId should fail"
    );
    assert!(
        String::from_utf8_lossy(&missing_session.stderr).contains("--sessionId"),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&missing_session.stderr)
    );
    let legacy_session_flag = run([
        "message",
        "--agent",
        "product-manager",
        "--session-id",
        &session_id,
        "legacy",
    ]);
    assert!(
        !legacy_session_flag.status.success(),
        "legacy --session-id should be rejected for message"
    );
    assert!(
        String::from_utf8_lossy(&legacy_session_flag.stderr).contains("--session-id"),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&legacy_session_flag.stderr)
    );
    run_ok([
        "message",
        "--agent",
        "product-manager",
        "--sessionId",
        &session_id,
        "hello",
        "agent",
        "--bus-url",
        &bus_url,
        "--key-path",
        key_path.to_str().unwrap(),
    ]);

    let sessions = run_ok(["sessions", "--bus-url", &bus_url]);
    let sessions = String::from_utf8(sessions.stdout).unwrap();
    assert!(sessions.contains("Renamed Smoke"));

    let replay = run_ok(["replay", "--sessionId", &session_id, "--bus-url", &bus_url]);
    let replay = String::from_utf8(replay.stdout).unwrap();
    assert!(replay.contains("workspace.event.submitted"));
    assert!(replay.contains("agent.inbox.product-manager"));

    let events = run_ok(["events", "--sessionId", &session_id, "--bus-url", &bus_url]);
    let events = String::from_utf8(events.stdout).unwrap();
    assert!(events.contains("workspace.event.submitted"));
    assert!(events.contains("agent.inbox.product-manager"));

    let event_json = run_ok([
        "events",
        "--json",
        "--sessionId",
        &session_id,
        "--bus-url",
        &bus_url,
    ]);
    let event_json = String::from_utf8(event_json.stdout).unwrap();
    let inbox_event: serde_json::Value = event_json
        .lines()
        .find(|line| line.contains("agent.inbox.product-manager"))
        .map(|line| serde_json::from_str(line).unwrap())
        .expect("agent inbox event");
    assert_eq!(inbox_event["context"]["sessionId"], session_id);
    assert_eq!(inbox_event["data"]["sessionId"], session_id);

    let events_positional = run_ok(["events", &session_id, "--tail", "2", "--bus-url", &bus_url]);
    let events_positional = String::from_utf8(events_positional.stdout).unwrap();
    assert!(events_positional.contains(&session_id));

    let events_named = run_ok([
        "events",
        "Renamed Smoke",
        "--tail",
        "4",
        "--bus-url",
        &bus_url,
    ]);
    let events_named = String::from_utf8(events_named.stdout).unwrap();
    assert!(events_named.contains("workspace.event.submitted"));
    assert!(events_named.contains("agent.inbox.product-manager"));

    let session_logs = run_ok([
        "logs",
        "Renamed Smoke",
        "--tail",
        "4",
        "--bus-url",
        &bus_url,
        "--config",
        topology_path.to_str().unwrap(),
    ]);
    let session_logs = String::from_utf8(session_logs.stdout).unwrap();
    assert!(session_logs.contains("workspace.event.submitted"));
    assert!(session_logs.contains("agent.inbox.product-manager"));

    let session_ls = run_ok(["session", "ls", "--bus-url", &bus_url]);
    let session_ls = String::from_utf8(session_ls.stdout).unwrap();
    assert!(session_ls.contains("Renamed Smoke"));

    let session_inspect = run_ok([
        "session",
        "inspect",
        &session_id,
        "--json",
        "--bus-url",
        &bus_url,
    ]);
    let session_inspect = String::from_utf8(session_inspect.stdout).unwrap();
    assert!(session_inspect.contains("Renamed Smoke"));

    let session_history = run_ok([
        "session",
        "history",
        &session_id,
        "--json",
        "--bus-url",
        &bus_url,
    ]);
    let session_history = String::from_utf8(session_history.stdout).unwrap();
    assert!(session_history.contains("workspace.event.submitted"));

    run_ok([
        "session",
        "delete",
        &session_id,
        "--bus-url",
        &bus_url,
        "--key-path",
        key_path.to_str().unwrap(),
    ]);
    let session_ls = run_ok(["session", "ls", "--bus-url", &bus_url]);
    let session_ls = String::from_utf8(session_ls.stdout).unwrap();
    assert!(
        !session_ls.contains("Renamed Smoke"),
        "deleted session leaked into default list"
    );
    let session_ls_deleted = run_ok(["session", "ls", "--include-deleted", "--bus-url", &bus_url]);
    let session_ls_deleted = String::from_utf8(session_ls_deleted.stdout).unwrap();
    assert!(session_ls_deleted.contains("Renamed Smoke"));
    assert!(session_ls_deleted.contains("deleted"));

    let deleted_inspect = run_ok([
        "session",
        "inspect",
        &session_id,
        "--json",
        "--bus-url",
        &bus_url,
    ]);
    let deleted_inspect: serde_json::Value =
        serde_json::from_slice(&deleted_inspect.stdout).unwrap();
    assert_eq!(deleted_inspect["summary"]["deleted"], true);
    assert_eq!(deleted_inspect["summary"]["name"], "Renamed Smoke");
    assert!(deleted_inspect["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["topic"] == "workspace.event.submitted"));

    let deleted_history = run_ok([
        "session",
        "history",
        &session_id,
        "--json",
        "--bus-url",
        &bus_url,
    ]);
    let deleted_history: serde_json::Value =
        serde_json::from_slice(&deleted_history.stdout).unwrap();
    assert!(deleted_history["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["topic"] == "agent.inbox.product-manager"));

    // --json replay: NDJSON output with camelCase keys
    let replay_json = run_ok([
        "replay",
        "--json",
        "--sessionId",
        &session_id,
        "--bus-url",
        &bus_url,
    ]);
    let replay_json = String::from_utf8(replay_json.stdout).unwrap();
    let lines: Vec<&str> = replay_json.lines().filter(|l| !l.is_empty()).collect();
    assert!(
        lines.len() >= 2,
        "expected at least 2 NDJSON lines, got {}",
        lines.len()
    );
    assert!(
        !lines[0].starts_with("Found"),
        "header leaked into --json output"
    );
    for line in &lines {
        let v: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("invalid JSON line: {e}\nline: {line}"));
        assert!(v.get("eventId").is_some(), "missing eventId in: {line}");
        assert!(v.get("topic").is_some(), "missing topic in: {line}");
    }
    let submitted_line = lines
        .iter()
        .find(|l| l.contains("workspace.event.submitted"))
        .expect("no workspace.event.submitted line in --json replay");
    let submitted: serde_json::Value = serde_json::from_str(submitted_line).unwrap();
    assert_eq!(submitted["data"]["text"], "Build smoke path");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn submit_drives_mock_agent_and_preserves_event_context() {
    let Some(nats) = which("nats-server") else {
        eprintln!("skipping mock-agent integration test: nats-server is not on PATH");
        return;
    };
    let temp = tempfile::tempdir().expect("tempdir");
    let port = free_port();
    let bus_url = format!("nats://127.0.0.1:{port}");
    let store_dir = temp.path().join("nats-store");
    let key_path = temp.path().join("session_token");
    let topology_path = temp.path().join("topology.json");
    let session_id = "sess_mock_agent";

    let mut child = Command::new(nats)
        .args(["-js", "-p", &port.to_string(), "-sd"])
        .arg(&store_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start nats-server");
    wait_for_nats(&mut child, &bus_url);
    let _guard = ChildGuard(child);

    run_ok(["bootstrap", "--key-path", key_path.to_str().unwrap()]);
    std::fs::write(
        &topology_path,
        serde_json::json!({
            "agents": [
                {
                    "name": "mock-coder",
                    "publishes": [
                        { "topic": CODE_CHANGED, "required_scopes": ["workspace:write"] }
                    ],
                    "subscriptions": [
                        {
                            "topic": WORKSPACE_EVENT_SUBMITTED,
                            "required_scopes": ["workspace:submit"],
                            "emit": { "topic": CODE_CHANGED, "required_scopes": ["workspace:write"] }
                        }
                    ]
                }
            ]
        })
        .to_string(),
    )
    .expect("write topology");

    let runner = tokio::spawn(
        DaemonRunner::new(
            MockChangeAgent,
            DaemonConfig {
                bus_url: bus_url.clone(),
                key_path: key_path.clone(),
            },
        )
        .run(),
    );

    let bus = Bus::connect(&bus_url).await.expect("connect bus");
    wait_for_consumer(
        &bus,
        &consumer_name("mock-coder", WORKSPACE_EVENT_SUBMITTED),
    )
    .await;

    let rejected = run([
        "submit",
        "workspace.event.typo",
        "bad topic",
        "--sessionId",
        session_id,
        "--bus-url",
        &bus_url,
        "--key-path",
        key_path.to_str().unwrap(),
        "--config",
        topology_path.to_str().unwrap(),
    ]);
    assert!(
        !rejected.status.success(),
        "unknown topology topic was accepted"
    );
    assert!(
        String::from_utf8_lossy(&rejected.stderr).contains("Unknown topic"),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&rejected.stderr)
    );

    run_ok([
        "submit",
        WORKSPACE_EVENT_SUBMITTED,
        "Build from integration",
        "--sessionId",
        session_id,
        "--bus-url",
        &bus_url,
        "--key-path",
        key_path.to_str().unwrap(),
        "--config",
        topology_path.to_str().unwrap(),
    ]);

    let changed = wait_for_event(&bus, session_id, CODE_CHANGED).await;
    assert_eq!(changed.sender.agent_name, "mock-coder");
    assert_eq!(changed.context.affected_files, vec!["src/lib.rs"]);
    assert_eq!(changed.data["changedFiles"][0], "src/lib.rs");

    let submitted = wait_for_event(&bus, session_id, WORKSPACE_EVENT_SUBMITTED).await;
    let key = load_signing_key(&key_path).expect("load key");
    let claims =
        verify_actor_token(&submitted.security.actor_token, &key).expect("verify submitted token");
    assert!(claims.has_scope("workspace:submit"));
    assert_eq!(claims.sid, session_id);

    let health = bus.event_stream_health().await.expect("stream health");
    let consumer = health
        .consumers
        .iter()
        .find(|consumer| consumer.name == consumer_name("mock-coder", WORKSPACE_EVENT_SUBMITTED))
        .expect("mock durable consumer");
    assert_eq!(consumer.filter_subject, WORKSPACE_EVENT_SUBMITTED);
    assert_eq!(consumer.redelivered, 0);

    runner.abort();
}

struct MockChangeAgent;

#[async_trait]
impl Agent for MockChangeAgent {
    fn agent_name(&self) -> &str {
        "mock-coder"
    }

    fn port(&self) -> u16 {
        4101
    }

    fn capabilities(&self) -> Vec<String> {
        vec!["mock".into(), "code".into()]
    }

    fn subscriptions(&self) -> Vec<Subscription> {
        vec![Subscription {
            topic: WORKSPACE_EVENT_SUBMITTED.into(),
            description: "Integration test input".into(),
            required_scopes: vec!["workspace:submit".into()],
        }]
    }

    fn published_events(&self) -> Vec<PublishedEvent> {
        vec![PublishedEvent {
            topic: CODE_CHANGED.into(),
            description: "Mock code change".into(),
            required_scopes: vec!["workspace:write".into()],
        }]
    }

    async fn on_event(&mut self, envelope: Envelope, publisher: Publisher) -> anyhow::Result<()> {
        let changed_files = vec!["src/lib.rs".to_string()];
        publisher
            .publish(
                CODE_CHANGED,
                serde_json::json!({
                    "summary": format!("mock handled {}", envelope.event_id),
                    "changedFiles": changed_files,
                }),
                Some(vec!["src/lib.rs".to_string()]),
            )
            .await?;
        Ok(())
    }
}

async fn wait_for_consumer(bus: &Bus, name: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if bus
            .event_stream_health()
            .await
            .map(|health| {
                health
                    .consumers
                    .iter()
                    .any(|consumer| consumer.name == name)
            })
            .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("consumer {name} did not appear");
}

async fn wait_for_event(bus: &Bus, session_id: &str, topic: &str) -> Envelope {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let events = bus.read_all_events().await.expect("read events");
        if let Some(event) = events
            .into_iter()
            .find(|event| event.context.session_id == session_id && event.topic == topic)
        {
            return event;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("event {topic} did not appear in session {session_id}");
}

fn run_ok<const N: usize>(args: [&str; N]) -> Output {
    let output = run(args);
    assert!(
        output.status.success(),
        "agora failed\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn run<const N: usize>(args: [&str; N]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_agora"))
        .args(args)
        .output()
        .expect("run agora")
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind port")
        .local_addr()
        .expect("local addr")
        .port()
}

fn wait_for_nats(child: &mut Child, bus_url: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("nats status") {
            panic!("nats-server exited early: {status}");
        }
        if Command::new(env!("CARGO_BIN_EXE_agora"))
            .args(["sessions", "--bus-url", bus_url])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("nats-server did not become ready");
}

fn which(binary: &str) -> Option<String> {
    let path = Command::new("which").arg(binary).output().ok()?;
    path.status.success().then(|| {
        String::from_utf8(path.stdout)
            .expect("which utf8")
            .trim()
            .to_string()
    })
}
