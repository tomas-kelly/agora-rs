use std::{
    net::TcpListener,
    process::{Child, Command, Output, Stdio},
    time::{Duration, Instant},
};

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.0.kill().ok();
        self.0.wait().ok();
    }
}

#[test]
#[ignore = "starts an isolated local nats-server and exercises mutating CLI commands"]
fn cli_mutating_commands_work_against_isolated_nats() {
    let nats = which("nats-server");
    let temp = tempfile::tempdir().expect("tempdir");
    let port = free_port();
    let bus_url = format!("nats://127.0.0.1:{port}");
    let store_dir = temp.path().join("nats-store");
    let key_path = temp.path().join("session_token");

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
        "Build smoke path",
        "--session-id",
        &session_id,
        "--bus-url",
        &bus_url,
        "--key-path",
        key_path.to_str().unwrap(),
    ]);
    run_ok([
        "message",
        "product-manager",
        "hello",
        "agent",
        "--session-id",
        &session_id,
        "--bus-url",
        &bus_url,
        "--key-path",
        key_path.to_str().unwrap(),
    ]);

    let sessions = run_ok(["sessions", "--bus-url", &bus_url]);
    let sessions = String::from_utf8(sessions.stdout).unwrap();
    assert!(sessions.contains("Renamed Smoke"));

    let replay = run_ok(["replay", "--session-id", &session_id, "--bus-url", &bus_url]);
    let replay = String::from_utf8(replay.stdout).unwrap();
    assert!(replay.contains("workspace.idea.submitted"));
    assert!(replay.contains("agent.inbox.product-manager"));

    let events = run_ok(["events", "--session-id", &session_id, "--bus-url", &bus_url]);
    let events = String::from_utf8(events.stdout).unwrap();
    assert!(events.contains("workspace.idea.submitted"));
    assert!(events.contains("agent.inbox.product-manager"));

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
    assert!(session_history.contains("workspace.idea.submitted"));

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

    // --json replay: NDJSON output with camelCase keys
    let replay_json = run_ok([
        "replay",
        "--json",
        "--session-id",
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
    let idea_line = lines
        .iter()
        .find(|l| l.contains("workspace.idea.submitted"))
        .expect("no workspace.idea.submitted line in --json replay");
    let idea: serde_json::Value = serde_json::from_str(idea_line).unwrap();
    assert_eq!(idea["data"]["idea"], "Build smoke path");
}

fn run_ok<const N: usize>(args: [&str; N]) -> Output {
    let output = Command::new(env!("CARGO_BIN_EXE_agora"))
        .args(args)
        .output()
        .expect("run agora");
    assert!(
        output.status.success(),
        "agora failed\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
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

fn which(binary: &str) -> String {
    let path = Command::new("which").arg(binary).output().expect("which");
    assert!(
        path.status.success(),
        "`{binary}` is required for ignored CLI smoke test"
    );
    String::from_utf8(path.stdout)
        .expect("which utf8")
        .trim()
        .to_string()
}
