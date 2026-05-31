use std::{
    net::TcpListener,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use agora_core::{
    bus::{Bus, DurableConsumerOptions, DURABLE_ACK_WAIT, DURABLE_MAX_DELIVER},
    daemon::{Agent, DaemonConfig, DaemonRunner, Publisher},
    envelope::Envelope,
    manifest::{PublishedEvent, Subscription},
    tokens::{mint_actor_token, DEFAULT_TTL_SECS},
    topics::{
        consumer_name, CODE_CHANGED, HUMAN_INTERACTION_REQUEST, HUMAN_INTERACTION_RESPONSE,
        WORKSPACE_EVENT_SUBMITTED,
    },
};
use async_trait::async_trait;

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.0.kill().ok();
        self.0.wait().ok();
    }
}

struct RuntimeHarness {
    _temp: tempfile::TempDir,
    _guard: ChildGuard,
    bus_url: String,
    key_path: std::path::PathBuf,
    signing_key: Vec<u8>,
    bus: Bus,
}

impl RuntimeHarness {
    async fn start(test_name: &str) -> Option<Self> {
        let Some(nats) = which("nats-server") else {
            eprintln!("skipping runtime reliability test: nats-server is not on PATH");
            return None;
        };

        let temp = tempfile::tempdir().expect("tempdir");
        let port = free_port();
        let bus_url = format!("nats://127.0.0.1:{port}");
        let store_dir = temp.path().join("nats-store");
        let key_path = temp.path().join("session_token");
        let signing_key = format!("{test_name}-runtime-signing-key").into_bytes();
        std::fs::write(&key_path, &signing_key).expect("write signing key");

        let mut child = Command::new(nats)
            .args(["-js", "-p", &port.to_string(), "-sd"])
            .arg(&store_dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start nats-server");
        let bus = wait_for_nats(&mut child, &bus_url).await;

        Some(Self {
            _temp: temp,
            _guard: ChildGuard(child),
            bus_url,
            key_path,
            signing_key,
            bus,
        })
    }

    async fn publish_workspace_event(&self, session_id: &str, text: &str) -> Envelope {
        let token = mint_actor_token(
            "runtime-test",
            &["workspace:submit"],
            session_id,
            &self.signing_key,
            DEFAULT_TTL_SECS,
        )
        .expect("mint submit token");
        let envelope = Envelope::build(
            WORKSPACE_EVENT_SUBMITTED,
            "runtime-test",
            0,
            token,
            session_id,
            serde_json::json!({ "text": text }),
            None,
            vec![],
        );
        self.bus.publish(&envelope).await.expect("publish input");
        envelope
    }

    async fn publish_human_response(&self, session_id: &str, request_id: &str, answer: &str) {
        let token = mint_actor_token(
            "runtime-test",
            &["workspace:read", "workspace:write"],
            session_id,
            &self.signing_key,
            DEFAULT_TTL_SECS,
        )
        .expect("mint response token");
        let envelope = Envelope::build(
            HUMAN_INTERACTION_RESPONSE,
            "runtime-test",
            0,
            token,
            session_id,
            serde_json::json!({
                "correlationId": request_id,
                "answer": answer,
                "respondedBy": "runtime-test",
            }),
            None,
            vec![],
        );
        self.bus
            .publish(&envelope)
            .await
            .expect("publish human response");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_agent_failure_is_naked_and_redelivered() {
    let Some(harness) = RuntimeHarness::start("redelivery").await else {
        return;
    };
    let attempts = Arc::new(AtomicUsize::new(0));
    let runner = tokio::spawn(
        DaemonRunner::new(
            FlakyAgent {
                attempts: attempts.clone(),
            },
            DaemonConfig {
                bus_url: harness.bus_url.clone(),
                key_path: harness.key_path.clone(),
            },
        )
        .run(),
    );
    wait_for_consumer(
        &harness.bus,
        &consumer_name("flaky-agent", WORKSPACE_EVENT_SUBMITTED),
    )
    .await;

    harness
        .publish_workspace_event("sess_redelivery", "exercise retry")
        .await;

    let changed = wait_for_event(&harness.bus, "sess_redelivery", CODE_CHANGED).await;
    assert_eq!(changed.sender.agent_name, "flaky-agent");
    assert_eq!(changed.data["attempt"], 2);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    let health = harness.bus.event_stream_health().await.expect("health");
    let consumer = health
        .consumers
        .iter()
        .find(|consumer| consumer.name == consumer_name("flaky-agent", WORKSPACE_EVENT_SUBMITTED))
        .expect("flaky consumer");
    assert_eq!(consumer.max_deliver, DURABLE_MAX_DELIVER);
    assert_eq!(consumer.ack_wait_secs, DURABLE_ACK_WAIT.as_secs());

    runner.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn human_interaction_request_resumes_agent_after_response() {
    let Some(harness) = RuntimeHarness::start("human").await else {
        return;
    };
    let runner = tokio::spawn(
        DaemonRunner::new(
            HumanInputAgent,
            DaemonConfig {
                bus_url: harness.bus_url.clone(),
                key_path: harness.key_path.clone(),
            },
        )
        .run(),
    );
    wait_for_consumer(
        &harness.bus,
        &consumer_name("human-agent", WORKSPACE_EVENT_SUBMITTED),
    )
    .await;

    harness
        .publish_workspace_event("sess_human", "ask before finishing")
        .await;
    let request = wait_for_event(&harness.bus, "sess_human", HUMAN_INTERACTION_REQUEST).await;
    assert_eq!(request.sender.agent_name, "human-agent");
    assert_eq!(request.data["question"], "Pick a release mode");
    assert_eq!(request.data["choices"][1], "safe");

    harness
        .publish_human_response("sess_human", &request.event_id, "safe")
        .await;

    let changed = wait_for_event(&harness.bus, "sess_human", CODE_CHANGED).await;
    assert_eq!(changed.sender.agent_name, "human-agent");
    assert_eq!(changed.data["answer"], "safe");
    assert_eq!(changed.data["correlationId"], request.event_id);

    runner.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_event_ids_are_deduplicated_by_jetstream() {
    let Some(harness) = RuntimeHarness::start("dedupe").await else {
        return;
    };

    let event = harness
        .publish_workspace_event("sess_dedupe", "publish once")
        .await;
    harness
        .bus
        .publish(&event)
        .await
        .expect("duplicate publish");

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let events = harness.bus.read_all_events().await.expect("read events");
        let matching = events
            .iter()
            .filter(|candidate| candidate.event_id == event.event_id)
            .count();
        if matching == 1 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let events = harness.bus.read_all_events().await.expect("read events");
    let matching = events
        .iter()
        .filter(|candidate| candidate.event_id == event.event_id)
        .count();
    assert_eq!(matching, 1, "duplicate event was stored more than once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn long_handler_progress_ack_prevents_redelivery() {
    let Some(harness) = RuntimeHarness::start("progress").await else {
        return;
    };

    let attempts = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(tokio::sync::Notify::new());
    let subscriber_bus = Bus::connect(&harness.bus_url)
        .await
        .expect("subscriber bus");
    let attempts_for_handler = attempts.clone();
    let done_for_handler = done.clone();

    let subscriber = tokio::spawn(async move {
        subscriber_bus
            .subscribe_durable_with_options(
                WORKSPACE_EVENT_SUBMITTED,
                "slow-progress-agent",
                DurableConsumerOptions {
                    ack_wait: Duration::from_millis(500),
                    ack_progress_interval: Duration::from_millis(100),
                    max_deliver: 3,
                    nak_delay: Duration::from_millis(50),
                },
                move |_env| {
                    let attempts = attempts_for_handler.clone();
                    let done = done_for_handler.clone();
                    async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(1200)).await;
                        done.notify_one();
                        Ok(())
                    }
                },
            )
            .await
    });

    wait_for_consumer(
        &harness.bus,
        &consumer_name("slow-progress-agent", WORKSPACE_EVENT_SUBMITTED),
    )
    .await;

    harness
        .publish_workspace_event("sess_progress", "work longer than ack_wait")
        .await;

    tokio::time::timeout(Duration::from_secs(5), done.notified())
        .await
        .expect("slow handler finished");
    tokio::time::sleep(Duration::from_millis(800)).await;

    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    let health = harness.bus.event_stream_health().await.expect("health");
    let consumer = health
        .consumers
        .iter()
        .find(|consumer| {
            consumer.name == consumer_name("slow-progress-agent", WORKSPACE_EVENT_SUBMITTED)
        })
        .expect("slow progress consumer");
    assert_eq!(consumer.redelivered, 0);

    subscriber.abort();
}

struct FlakyAgent {
    attempts: Arc<AtomicUsize>,
}

#[async_trait]
impl Agent for FlakyAgent {
    fn agent_name(&self) -> &str {
        "flaky-agent"
    }

    fn port(&self) -> u16 {
        4201
    }

    fn capabilities(&self) -> Vec<String> {
        vec!["runtime-test".into()]
    }

    fn subscriptions(&self) -> Vec<Subscription> {
        vec![workspace_submission_subscription()]
    }

    fn published_events(&self) -> Vec<PublishedEvent> {
        vec![code_changed_publication()]
    }

    async fn on_event(&mut self, _envelope: Envelope, publisher: Publisher) -> anyhow::Result<()> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt == 1 {
            anyhow::bail!("intentional transient failure");
        }
        publisher
            .publish(
                CODE_CHANGED,
                serde_json::json!({
                    "summary": "handled after redelivery",
                    "attempt": attempt,
                    "changedFiles": ["src/lib.rs"],
                }),
                Some(vec!["src/lib.rs".into()]),
            )
            .await?;
        Ok(())
    }
}

struct HumanInputAgent;

#[async_trait]
impl Agent for HumanInputAgent {
    fn agent_name(&self) -> &str {
        "human-agent"
    }

    fn port(&self) -> u16 {
        4202
    }

    fn capabilities(&self) -> Vec<String> {
        vec!["runtime-test".into(), "human-input".into()]
    }

    fn subscriptions(&self) -> Vec<Subscription> {
        vec![workspace_submission_subscription()]
    }

    fn published_events(&self) -> Vec<PublishedEvent> {
        vec![code_changed_publication()]
    }

    async fn on_event(&mut self, _envelope: Envelope, publisher: Publisher) -> anyhow::Result<()> {
        let response = publisher
            .ask_human(
                "Pick a release mode",
                Some(vec!["fast".into(), "safe".into()]),
                Some(10),
            )
            .await?;
        publisher
            .publish(
                CODE_CHANGED,
                serde_json::json!({
                    "summary": "continued after human input",
                    "answer": response.answer,
                    "correlationId": response.correlation_id,
                    "changedFiles": ["src/lib.rs"],
                }),
                Some(vec!["src/lib.rs".into()]),
            )
            .await?;
        Ok(())
    }
}

fn workspace_submission_subscription() -> Subscription {
    Subscription {
        topic: WORKSPACE_EVENT_SUBMITTED.into(),
        description: "Runtime reliability test input".into(),
        required_scopes: vec!["workspace:submit".into()],
    }
}

fn code_changed_publication() -> PublishedEvent {
    PublishedEvent {
        topic: CODE_CHANGED.into(),
        description: "Runtime reliability output".into(),
        required_scopes: vec!["workspace:write".into()],
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

async fn wait_for_nats(child: &mut Child, bus_url: &str) -> Bus {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("nats status") {
            panic!("nats-server exited early: {status}");
        }
        if let Ok(bus) = Bus::connect(bus_url).await {
            return bus;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("nats-server did not become ready");
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind port")
        .local_addr()
        .expect("local addr")
        .port()
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
