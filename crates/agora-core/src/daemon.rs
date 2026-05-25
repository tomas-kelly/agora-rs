use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::StreamExt;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{oneshot, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    bus::Bus,
    envelope::Envelope,
    human::{HumanInteractionRequest, HumanInteractionResponse},
    manifest::{AgentManifest, AgentStatus, PublishedEvent, Subscription},
    tokens::{load_signing_key, mint_actor_token, verify_actor_token},
    topics::{
        AGENT_REGISTRY_HEARTBEAT, AGENT_TELEMETRY_LOGS, HUMAN_INTERACTION_REQUEST,
        HUMAN_INTERACTION_RESPONSE,
    },
};

/// Shared map of pending human interaction requests awaiting responses.
pub type PendingInteractions =
    Arc<Mutex<HashMap<String, oneshot::Sender<HumanInteractionResponse>>>>;

/// Handle passed to `on_event` so agents can publish child events.
#[derive(Clone)]
pub struct Publisher {
    bus: Arc<Bus>,
    agent_name: String,
    port: u16,
    inbound: Arc<Envelope>,
    published_topics: Arc<HashMap<String, Vec<String>>>,
    signing_key: Arc<Vec<u8>>,
    pending_interactions: PendingInteractions,
}

impl Publisher {
    pub async fn publish(
        &self,
        topic: &str,
        data: serde_json::Value,
        affected_files: Option<Vec<String>>,
    ) -> Result<Envelope> {
        // Security: code.changed events MUST declare at least one affected file.
        // Empty changesets bypass security review and are rejected at publish time.
        if topic == crate::topics::CODE_CHANGED {
            let files = affected_files.as_deref().unwrap_or(&[]);
            if files.is_empty() {
                anyhow::bail!(
                    "{} attempted to publish code.changed with no affected files — \
                     empty changesets bypass security review",
                    self.agent_name
                );
            }
        }

        let scopes = self.published_topics.get(topic).ok_or_else(|| {
            anyhow::anyhow!(
                "{} attempted to publish undeclared topic: {topic}",
                self.agent_name
            )
        })?;

        let scope_refs: Vec<&str> = scopes.iter().map(String::as_str).collect();
        let token = mint_actor_token(
            &self.agent_name,
            &scope_refs,
            &self.inbound.context.session_id,
            &self.signing_key,
            crate::tokens::DEFAULT_TTL_SECS,
        )?;
        let envelope = self.inbound.child(
            topic,
            &self.agent_name,
            self.port,
            data,
            affected_files,
            None,
        );

        // Override the actor token with a freshly minted one for this agent
        let mut env = envelope;
        env.security.actor_token = token;

        self.bus.publish(&env).await?;
        info!(topic, event_id = %env.event_id, "published");
        Ok(env)
    }

    pub fn session_id(&self) -> &str {
        &self.inbound.context.session_id
    }

    pub fn inbound(&self) -> &Envelope {
        &self.inbound
    }

    pub async fn emit_telemetry(
        &self,
        level: &str,
        action: &str,
        telemetry: serde_json::Value,
    ) -> Result<()> {
        // Telemetry now flows through JetStream so consoles that connect
        // later replay past prompts/responses on startup. The legacy
        // payload shape is preserved inside `data` so daemon-telemetry's
        // JSONL output and the console's `TelemetryEntry` decoding both
        // keep working — they just unwrap `envelope.data` first.
        let payload = serde_json::json!({
            "timestamp": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            "sessionId": self.inbound.context.session_id,
            "agent": self.agent_name,
            "level": level,
            "action": action,
            "telemetry": telemetry,
        });

        // Telemetry envelopes need a valid actor token because every
        // subscriber that reads from JetStream verifies the JWT.
        let token = mint_actor_token(
            &self.agent_name,
            &["agent:telemetry"],
            &self.inbound.context.session_id,
            &self.signing_key,
            crate::tokens::DEFAULT_TTL_SECS,
        )?;
        let mut envelope = self.inbound.child(
            AGENT_TELEMETRY_LOGS,
            &self.agent_name,
            self.port,
            payload,
            Some(vec![]),
            None,
        );
        envelope.security.actor_token = token;
        self.bus.publish(&envelope).await
    }

    /// Ask a human a question and block until a response arrives or timeout.
    pub async fn ask_human(
        &self,
        question: &str,
        choices: Option<Vec<String>>,
        timeout_secs: Option<u64>,
    ) -> Result<HumanInteractionResponse> {
        let timeout_secs = timeout_secs.unwrap_or(120);
        let request = HumanInteractionRequest {
            question: question.to_string(),
            choices,
            timeout_secs: Some(timeout_secs),
        };

        // Publish the request envelope
        let token = mint_actor_token(
            &self.agent_name,
            &["workspace:read", "workspace:write"],
            &self.inbound.context.session_id,
            &self.signing_key,
            crate::tokens::DEFAULT_TTL_SECS,
        )?;
        let env = self.inbound.child(
            HUMAN_INTERACTION_REQUEST,
            &self.agent_name,
            self.port,
            serde_json::to_value(&request)?,
            None,
            None,
        );
        let mut env = env;
        env.security.actor_token = token;
        let event_id = env.event_id.clone();

        // Register the pending interaction before publishing
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending_interactions.lock().await;
            pending.insert(event_id.clone(), tx);
        }

        self.bus.publish(&env).await?;
        info!(event_id = %event_id, "published human.interaction.request");

        // Wait for response with timeout
        let duration = std::time::Duration::from_secs(timeout_secs);
        match tokio::time::timeout(duration, rx).await {
            Ok(Ok(response)) => Ok(response),
            _ => {
                // Remove pending entry on timeout/channel drop
                self.pending_interactions.lock().await.remove(&event_id);
                Ok(HumanInteractionResponse {
                    correlation_id: event_id,
                    answer: "[timeout]".to_string(),
                    responded_by: "system".to_string(),
                })
            }
        }
    }
}

/// Implement this trait for each agent daemon.
#[async_trait]
pub trait Agent: Send + 'static {
    fn agent_name(&self) -> &str;
    fn port(&self) -> u16;
    fn capabilities(&self) -> Vec<String>;
    fn subscriptions(&self) -> Vec<Subscription>;
    fn published_events(&self) -> Vec<PublishedEvent>;

    async fn on_event(&mut self, envelope: Envelope, publisher: Publisher) -> Result<()>;

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Configuration for `DaemonRunner::run`.
pub struct DaemonConfig {
    pub bus_url: String,
    pub key_path: std::path::PathBuf,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            bus_url: "nats://127.0.0.1:4222".to_string(),
            key_path: crate::tokens::default_key_path(),
        }
    }
}

/// Wraps an `Agent` implementation and drives the full event loop:
/// connects to NATS, subscribes to all declared topics, emits heartbeats,
/// and calls `on_event` sequentially (one event at a time via a mutex).
pub struct DaemonRunner<A: Agent> {
    agent: Arc<Mutex<A>>,
    config: DaemonConfig,
}

struct WorkItem {
    envelope: Envelope,
    done: oneshot::Sender<Result<()>>,
}

impl<A: Agent> DaemonRunner<A> {
    pub fn new(agent: A, config: DaemonConfig) -> Self {
        Self {
            agent: Arc::new(Mutex::new(agent)),
            config,
        }
    }

    pub async fn run(self) -> Result<()> {
        let bus = Arc::new(Bus::connect(&self.config.bus_url).await?);
        let signing_key = Arc::new(load_signing_key(&self.config.key_path)?);

        let agent_name;
        let port;
        let capabilities;
        let subscriptions;
        let published_events;

        {
            let a = self.agent.lock().await;
            agent_name = a.agent_name().to_string();
            port = a.port();
            capabilities = a.capabilities();
            subscriptions = a.subscriptions();
            published_events = a.published_events();
        }

        let published_topics: Arc<HashMap<String, Vec<String>>> = Arc::new(
            published_events
                .iter()
                .map(|e| (e.topic.clone(), e.required_scopes.clone()))
                .collect(),
        );
        let subscription_scopes: Arc<HashMap<String, Vec<String>>> = Arc::new(
            subscriptions
                .iter()
                .map(|s| (s.topic.clone(), s.required_scopes.clone()))
                .collect(),
        );

        info!(agent = %agent_name, port, "daemon starting");

        // Publish initial manifest to KV
        self.update_registry(
            &bus,
            &agent_name,
            port,
            &capabilities,
            &subscriptions,
            &published_events,
            AgentStatus::Starting,
        )
        .await;

        let shutdown = CancellationToken::new();
        let pending_interactions: PendingInteractions = Arc::new(Mutex::new(HashMap::new()));

        // Heartbeat task
        {
            let bus = bus.clone();
            let agent_name = agent_name.clone();
            let capabilities = capabilities.clone();
            let subs = subscriptions.clone();
            let pubs = published_events.clone();
            let shutdown = shutdown.clone();

            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = interval.tick() => {
                            Self::heartbeat(&bus, &agent_name, port, &capabilities, &subs, &pubs, AgentStatus::Ready).await;
                        }
                    }
                }
            });
        }

        // Signal handler
        let shutdown_signal = shutdown.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutdown signal received");
            shutdown_signal.cancel();
        });

        // Human interaction responses are delivered back to the requesting
        // agent by correlation id. This live subscriber completes the pending
        // `ask_human` oneshot without routing the response through `on_event`.
        {
            let client = bus.client.clone();
            let pending_interactions = pending_interactions.clone();
            let shutdown = shutdown.clone();

            tokio::spawn(async move {
                let mut sub = match client
                    .subscribe(HUMAN_INTERACTION_RESPONSE.to_string())
                    .await
                {
                    Ok(sub) => sub,
                    Err(e) => {
                        warn!("Human interaction response subscriber failed to start: {e}");
                        return;
                    }
                };

                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        maybe_msg = sub.next() => {
                            let Some(msg) = maybe_msg else { break };
                            let envelope = match Envelope::from_bytes(&msg.payload) {
                                Ok(envelope) => envelope,
                                Err(e) => {
                                    warn!("Failed to deserialize human interaction response envelope: {e}");
                                    continue;
                                }
                            };
                            let response = match serde_json::from_value::<HumanInteractionResponse>(envelope.data) {
                                Ok(response) => response,
                                Err(e) => {
                                    warn!("Failed to decode human interaction response: {e}");
                                    continue;
                                }
                            };
                            let correlation_id = response.correlation_id.clone();
                            let sender = {
                                let mut pending = pending_interactions.lock().await;
                                pending.remove(&correlation_id)
                            };
                            if let Some(sender) = sender {
                                let _ = sender.send(response);
                            } else {
                                warn!(%correlation_id, "Human interaction response had no pending request");
                            }
                        }
                    }
                }
            });
        }

        // One subscriber task per subscription, all feeding a shared channel
        let (tx, mut rx) = tokio::sync::mpsc::channel::<WorkItem>(64);

        for sub in &subscriptions {
            let topic = sub.topic.clone();
            let bus = bus.clone();
            let agent_name = agent_name.clone();
            let tx = tx.clone();
            let shutdown = shutdown.clone();

            tokio::spawn(async move {
                let mut retry_delay = std::time::Duration::from_secs(1);

                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        result = bus.subscribe_durable(&topic, &agent_name, |env| {
                            let tx = tx.clone();
                            async move {
                                let (done, wait) = oneshot::channel();
                                tx.send(WorkItem { envelope: env, done })
                                    .await
                                    .context("agent work queue closed")?;
                                wait.await.context("agent work result dropped")?
                            }
                        }) => {
                            match result {
                                Ok(()) => warn!("Subscriber for {topic} stopped; retrying"),
                                Err(e) => error!("Subscriber for {topic} failed; retrying: {e:#}"),
                            }
                        }
                    }

                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = tokio::time::sleep(retry_delay) => {}
                    }
                    retry_delay = (retry_delay * 2).min(std::time::Duration::from_secs(30));
                }
            });
        }
        drop(tx); // all senders now live in subscriber tasks; drop the original

        self.update_registry(
            &bus,
            &agent_name,
            port,
            &capabilities,
            &subscriptions,
            &published_events,
            AgentStatus::Ready,
        )
        .await;
        info!(agent = %agent_name, "ready");

        // Main sequential processing loop
        while let Some(work) = rx.recv().await {
            let envelope = work.envelope;
            let topic = envelope.topic.clone();
            let event_id = envelope.event_id.clone();
            info!(%topic, %event_id, "processing");

            if let Err(e) = Self::authorize_event(
                &envelope,
                &signing_key,
                subscription_scopes
                    .get(&topic)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
            ) {
                error!(%topic, %event_id, "dropping unauthorized event: {e}");
                let _ = work.done.send(Ok(()));
                continue;
            }

            let publisher = Publisher {
                bus: bus.clone(),
                agent_name: agent_name.clone(),
                port,
                inbound: Arc::new(envelope.clone()),
                published_topics: published_topics.clone(),
                signing_key: signing_key.clone(),
                pending_interactions: pending_interactions.clone(),
            };

            let mut agent = self.agent.lock().await;
            let result = agent.on_event(envelope, publisher).await;
            if let Err(e) = &result {
                error!(%topic, %event_id, "on_event error: {e}");
            }
            let _ = work.done.send(result);
        }

        {
            let mut agent = self.agent.lock().await;
            if let Err(e) = agent.shutdown().await {
                error!(agent = %agent_name, "agent shutdown error: {e}");
            }
        }
        shutdown.cancel();
        info!(agent = %agent_name, "daemon stopped");
        Ok(())
    }

    fn authorize_event(
        envelope: &Envelope,
        signing_key: &[u8],
        required_scopes: &[String],
    ) -> Result<()> {
        let claims = verify_actor_token(&envelope.security.actor_token, signing_key)?;
        if claims.sid != envelope.context.session_id {
            anyhow::bail!(
                "token session {} does not match envelope session {}",
                claims.sid,
                envelope.context.session_id
            );
        }
        for required in required_scopes {
            if !claims.has_scope(required) {
                anyhow::bail!("token missing required scope {required}");
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn update_registry(
        &self,
        bus: &Bus,
        agent_name: &str,
        port: u16,
        capabilities: &[String],
        subscriptions: &[Subscription],
        published_events: &[PublishedEvent],
        status: AgentStatus,
    ) {
        let manifest = AgentManifest::new(
            agent_name,
            port,
            capabilities.to_vec(),
            subscriptions.iter().map(|s| s.topic.clone()).collect(),
            published_events.iter().map(|p| p.topic.clone()).collect(),
        )
        .with_status(status);

        match bus.get_or_create_kv().await {
            Ok(kv) => {
                if let Ok(bytes) = manifest.to_bytes() {
                    kv.put(agent_name, bytes).await.ok();
                }
            }
            Err(e) => error!("KV registry update failed: {e}"),
        }

        // Also publish heartbeat on the bus
        Self::heartbeat(
            bus,
            agent_name,
            port,
            capabilities,
            subscriptions,
            published_events,
            manifest.status,
        )
        .await;
    }

    async fn heartbeat(
        bus: &Bus,
        agent_name: &str,
        port: u16,
        capabilities: &[String],
        subscriptions: &[Subscription],
        published_events: &[PublishedEvent],
        status: AgentStatus,
    ) {
        let manifest = AgentManifest::new(
            agent_name,
            port,
            capabilities.to_vec(),
            subscriptions.iter().map(|s| s.topic.clone()).collect(),
            published_events.iter().map(|p| p.topic.clone()).collect(),
        )
        .with_status(status);

        if let Ok(payload) = manifest.to_bytes() {
            bus.publish_raw(AGENT_REGISTRY_HEARTBEAT, payload)
                .await
                .ok();
        }
    }
}
