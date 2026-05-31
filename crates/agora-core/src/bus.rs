use anyhow::{Context, Result};
use async_nats::jetstream::{self, consumer::pull, stream, AckKind};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use serde::Serialize;
use std::{
    collections::{HashSet, VecDeque},
    future::Future,
    time::Duration,
};
use tracing::{debug, warn};

use crate::{
    envelope::Envelope,
    manifest::AgentManifest,
    topics::{consumer_name, EVENT_STREAM, EVENT_STREAM_SUBJECTS, KV_AGENT_REGISTRY},
};

pub const DURABLE_ACK_WAIT: Duration = Duration::from_secs(30);
pub const DURABLE_ACK_PROGRESS_INTERVAL: Duration = Duration::from_secs(10);
pub const DURABLE_MAX_DELIVER: i64 = 5;
pub const DURABLE_NAK_DELAY: Duration = Duration::from_secs(1);
const HANDLED_EVENT_CACHE_LIMIT: usize = 10_000;

#[derive(Debug, Clone, Copy)]
pub struct DurableConsumerOptions {
    pub ack_wait: Duration,
    pub ack_progress_interval: Duration,
    pub max_deliver: i64,
    pub nak_delay: Duration,
}

impl Default for DurableConsumerOptions {
    fn default() -> Self {
        Self {
            ack_wait: DURABLE_ACK_WAIT,
            ack_progress_interval: DURABLE_ACK_PROGRESS_INTERVAL,
            max_deliver: DURABLE_MAX_DELIVER,
            nak_delay: DURABLE_NAK_DELAY,
        }
    }
}

#[derive(Default)]
struct HandledEventCache {
    ids: HashSet<String>,
    order: VecDeque<String>,
}

impl HandledEventCache {
    fn contains(&self, event_id: &str) -> bool {
        self.ids.contains(event_id)
    }

    fn insert(&mut self, event_id: String) {
        if !self.ids.insert(event_id.clone()) {
            return;
        }

        self.order.push_back(event_id);
        while self.order.len() > HANDLED_EVENT_CACHE_LIMIT {
            if let Some(oldest) = self.order.pop_front() {
                self.ids.remove(&oldest);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct AgentRegistryRecord {
    pub key: String,
    pub manifest: Option<AgentManifest>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventStreamHealth {
    pub name: String,
    pub subjects: Vec<String>,
    pub messages: u64,
    pub bytes: u64,
    pub consumers: Vec<ConsumerHealth>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerHealth {
    pub name: String,
    pub filter_subject: String,
    pub ack_wait_secs: u64,
    pub max_deliver: i64,
    pub delivered_stream_sequence: u64,
    pub acknowledged_stream_sequence: u64,
    pub pending: u64,
    pub ack_pending: usize,
    pub redelivered: usize,
    pub waiting: usize,
}

pub struct Bus {
    pub client: async_nats::Client,
    pub js: jetstream::Context,
}

impl Bus {
    pub async fn connect(url: &str) -> Result<Self> {
        let client = async_nats::connect(url)
            .await
            .with_context(|| format!("Failed to connect to NATS at {url}"))?;
        let js = jetstream::new(client.clone());
        let bus = Self { client, js };
        bus.ensure_event_stream().await?;
        Ok(bus)
    }

    async fn ensure_event_stream(&self) -> Result<()> {
        let subjects: Vec<String> = EVENT_STREAM_SUBJECTS
            .iter()
            .map(|s| s.to_string())
            .collect();
        let config = stream::Config {
            name: EVENT_STREAM.to_string(),
            subjects: subjects.clone(),
            storage: stream::StorageType::File,
            duplicate_window: Duration::from_secs(120),
            ..Default::default()
        };

        match self.js.get_stream(EVENT_STREAM).await {
            Ok(existing) => {
                use std::collections::HashSet;
                let have: HashSet<&str> = existing
                    .cached_info()
                    .config
                    .subjects
                    .iter()
                    .map(|s| s.as_str())
                    .collect();
                let want: HashSet<&str> = EVENT_STREAM_SUBJECTS.iter().copied().collect();
                if have != want {
                    self.js
                        .update_stream(&config)
                        .await
                        .context("Failed to update AGORA_EVENTS stream subjects")?;
                }
            }
            Err(_) => {
                self.js
                    .create_stream(config)
                    .await
                    .context("Failed to create AGORA_EVENTS stream")?;
            }
        }
        Ok(())
    }

    pub async fn publish(&self, envelope: &Envelope) -> Result<()> {
        let topic = envelope.topic.clone();
        let payload = envelope.to_bytes()?;

        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", envelope.event_id.as_str());

        self.js
            .publish_with_headers(topic, headers, payload)
            .await
            .context("Publish failed")?
            .await
            .context("Publish ack failed")?;
        Ok(())
    }

    pub async fn publish_raw(&self, subject: &str, payload: Bytes) -> Result<()> {
        self.client
            .publish(subject.to_string(), payload)
            .await
            .context("Raw publish failed")?;
        Ok(())
    }

    pub async fn event_stream_health(&self) -> Result<EventStreamHealth> {
        let stream = self.js.get_stream(EVENT_STREAM).await?;
        let info = stream.cached_info();
        let name = info.config.name.clone();
        let subjects = info.config.subjects.clone();
        let messages = info.state.messages;
        let bytes = info.state.bytes;

        let mut consumers = stream.consumers();
        let mut consumer_health = Vec::new();
        while let Some(info) = consumers.try_next().await? {
            consumer_health.push(ConsumerHealth {
                name: info.name,
                filter_subject: info.config.filter_subject,
                ack_wait_secs: info.config.ack_wait.as_secs(),
                max_deliver: info.config.max_deliver,
                delivered_stream_sequence: info.delivered.stream_sequence,
                acknowledged_stream_sequence: info.ack_floor.stream_sequence,
                pending: info.num_pending,
                ack_pending: info.num_ack_pending,
                redelivered: info.num_redelivered,
                waiting: info.num_waiting,
            });
        }
        consumer_health.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(EventStreamHealth {
            name,
            subjects,
            messages,
            bytes,
            consumers: consumer_health,
        })
    }

    /// Create (or get existing) durable pull consumer for a topic, then call
    /// `handler` for each message. Messages are acked only after the handler
    /// succeeds, with periodic progress acks while long handlers are running.
    /// Handler errors are NAKed so JetStream can redeliver them.
    pub async fn subscribe_durable<F, Fut>(
        &self,
        topic: &str,
        agent_name: &str,
        handler: F,
    ) -> Result<()>
    where
        F: FnMut(Envelope) -> Fut + Send,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        self.subscribe_durable_with_options(
            topic,
            agent_name,
            DurableConsumerOptions::default(),
            handler,
        )
        .await
    }

    pub async fn subscribe_durable_with_options<F, Fut>(
        &self,
        topic: &str,
        agent_name: &str,
        mut options: DurableConsumerOptions,
        mut handler: F,
    ) -> Result<()>
    where
        F: FnMut(Envelope) -> Fut + Send,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        if options.ack_progress_interval.is_zero() {
            options.ack_progress_interval = Duration::from_millis(1);
        }
        let name = consumer_name(agent_name, topic);
        let stream = self.js.get_stream(EVENT_STREAM).await?;
        let consumer: jetstream::consumer::Consumer<pull::Config> = stream
            .get_or_create_consumer(
                &name,
                pull::Config {
                    durable_name: Some(name.clone()),
                    filter_subject: topic.to_string(),
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    // Start at "new" so first-time agents don't replay the
                    // entire backlog. Durable consumers persist their ack
                    // position across restarts, so this only affects the
                    // initial creation — once acked, the consumer resumes
                    // from its stored position regardless of this setting.
                    deliver_policy: jetstream::consumer::DeliverPolicy::New,
                    max_deliver: options.max_deliver,
                    ack_wait: options.ack_wait,
                    ..Default::default()
                },
            )
            .await
            .with_context(|| format!("Failed to create consumer {name}"))?;

        let mut messages = consumer
            .messages()
            .await
            .with_context(|| format!("Failed to open message stream for {name}"))?;
        let mut handled_events = HandledEventCache::default();

        while let Some(result) = messages.next().await {
            match result {
                Ok(msg) => match Envelope::from_bytes(&msg.payload) {
                    Ok(envelope) => {
                        let event_id = envelope.event_id.clone();
                        debug!(topic = %envelope.topic, event_id = %event_id, "received");
                        if handled_events.contains(&event_id) {
                            debug!(
                                topic = %envelope.topic,
                                event_id = %event_id,
                                "received duplicate delivery for already handled event"
                            );
                            if let Err(e) = msg.ack().await {
                                warn!("Failed to ack duplicate {event_id}: {e}");
                            }
                            continue;
                        }

                        match handle_with_ack_progress(
                            &msg,
                            &event_id,
                            options.ack_progress_interval,
                            handler(envelope),
                        )
                        .await
                        {
                            Ok(()) => {
                                handled_events.insert(event_id.clone());
                                if let Err(e) = msg.ack().await {
                                    warn!("Failed to ack {event_id}: {e}");
                                }
                            }
                            Err(e) => {
                                warn!("Handler failed for {event_id}; requesting redelivery: {e}");
                                msg.ack_with(AckKind::Nak(Some(options.nak_delay)))
                                    .await
                                    .ok();
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to deserialize envelope: {e}");
                        msg.ack_with(AckKind::Term).await.ok();
                    }
                },
                Err(e) => {
                    warn!("Message stream error on {name}: {e}");
                }
            }
        }
        Ok(())
    }

    /// Read all messages from the AGORA_EVENTS stream in sequence order.
    pub async fn read_all_events(&self) -> Result<Vec<Envelope>> {
        let stream = self.js.get_stream(EVENT_STREAM).await?;
        let total = stream.cached_info().state.messages;
        if total == 0 {
            return Ok(vec![]);
        }

        // Ephemeral pull consumer: no durable name, deliver all existing messages
        let consumer: jetstream::consumer::Consumer<pull::Config> = stream
            .create_consumer(pull::Config {
                deliver_policy: jetstream::consumer::DeliverPolicy::All,
                ack_policy: jetstream::consumer::AckPolicy::None,
                ..Default::default()
            })
            .await?;

        let mut messages = consumer.messages().await?;
        let mut events = Vec::with_capacity(total as usize);
        let mut collected = 0u64;

        while collected < total {
            match tokio::time::timeout(std::time::Duration::from_secs(5), messages.next()).await {
                Ok(Some(Ok(msg))) => {
                    if let Ok(env) = Envelope::from_bytes(&msg.payload) {
                        events.push(env);
                    }
                    collected += 1;
                }
                _ => break,
            }
        }
        Ok(events)
    }

    /// Read all raw messages currently stored for a specific subject.
    pub async fn read_raw_subject(&self, subject: &str) -> Result<Vec<Bytes>> {
        let stream = self.js.get_stream(EVENT_STREAM).await?;
        let consumer: jetstream::consumer::Consumer<pull::Config> = stream
            .create_consumer(pull::Config {
                deliver_policy: jetstream::consumer::DeliverPolicy::All,
                ack_policy: jetstream::consumer::AckPolicy::None,
                filter_subject: subject.to_string(),
                ..Default::default()
            })
            .await?;

        let mut messages = consumer.messages().await?;
        let mut payloads = Vec::new();

        loop {
            match tokio::time::timeout(std::time::Duration::from_millis(250), messages.next()).await
            {
                Ok(Some(Ok(msg))) => payloads.push(msg.payload.clone()),
                Ok(Some(Err(e))) => warn!("Raw message stream error on {subject}: {e}"),
                Ok(None) | Err(_) => break,
            }
        }

        Ok(payloads)
    }

    pub async fn get_or_create_kv(&self) -> Result<jetstream::kv::Store> {
        match self.js.get_key_value(KV_AGENT_REGISTRY).await {
            Ok(store) => Ok(store),
            Err(_) => self
                .js
                .create_key_value(jetstream::kv::Config {
                    bucket: KV_AGENT_REGISTRY.to_string(),
                    ..Default::default()
                })
                .await
                .context("Failed to create KV bucket"),
        }
    }

    pub async fn read_agent_registry(&self) -> Result<Vec<AgentManifest>> {
        Ok(self
            .read_agent_registry_records()
            .await?
            .into_iter()
            .filter_map(|record| record.manifest)
            .collect())
    }

    pub async fn read_agent_registry_records(&self) -> Result<Vec<AgentRegistryRecord>> {
        let kv = self.get_or_create_kv().await?;
        let mut keys = kv
            .keys()
            .await
            .context("Failed to list agent registry keys")?;
        let mut records = Vec::new();

        while let Some(result) = keys.next().await {
            let key = match result {
                Ok(key) => key,
                Err(e) => {
                    warn!("Failed to read agent registry key: {e}");
                    continue;
                }
            };

            match kv.get(key.clone()).await {
                Ok(Some(bytes)) => match serde_json::from_slice::<AgentManifest>(&bytes) {
                    Ok(manifest) => records.push(AgentRegistryRecord {
                        key,
                        manifest: Some(manifest),
                        error: None,
                    }),
                    Err(e) => {
                        debug!(key = %key, "Ignoring malformed agent registry entry: {e}");
                        records.push(AgentRegistryRecord {
                            key,
                            manifest: None,
                            error: Some(e.to_string()),
                        });
                    }
                },
                Ok(None) => {}
                Err(e) => {
                    warn!(key = %key, "Failed to read agent registry entry: {e}");
                    records.push(AgentRegistryRecord {
                        key,
                        manifest: None,
                        error: Some(e.to_string()),
                    });
                }
            }
        }

        Ok(records)
    }

    pub async fn purge_agent_registry_key(&self, key: &str) -> Result<()> {
        let kv = self.get_or_create_kv().await?;
        kv.purge(key)
            .await
            .with_context(|| format!("Failed to purge agent registry key {key}"))
    }
}

async fn handle_with_ack_progress<Fut>(
    msg: &jetstream::Message,
    event_id: &str,
    progress_interval: Duration,
    handler: Fut,
) -> Result<()>
where
    Fut: Future<Output = Result<()>>,
{
    let mut progress = tokio::time::interval(progress_interval);
    progress.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    progress.tick().await;
    tokio::pin!(handler);

    loop {
        tokio::select! {
            result = &mut handler => return result,
            _ = progress.tick() => {
                if let Err(e) = msg.ack_with(AckKind::Progress).await {
                    warn!("Failed to send in-progress ack for {event_id}: {e}");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{HandledEventCache, HANDLED_EVENT_CACHE_LIMIT};

    #[test]
    fn handled_event_cache_remembers_seen_events() {
        let mut cache = HandledEventCache::default();

        cache.insert("event-1".to_string());
        cache.insert("event-1".to_string());

        assert!(cache.contains("event-1"));
        assert_eq!(cache.ids.len(), 1);
        assert_eq!(cache.order.len(), 1);
    }

    #[test]
    fn handled_event_cache_evicts_oldest_event() {
        let mut cache = HandledEventCache::default();

        for i in 0..=HANDLED_EVENT_CACHE_LIMIT {
            cache.insert(format!("event-{i}"));
        }

        assert!(!cache.contains("event-0"));
        assert!(cache.contains("event-1"));
        assert!(cache.contains(&format!("event-{HANDLED_EVENT_CACHE_LIMIT}")));
        assert_eq!(cache.ids.len(), HANDLED_EVENT_CACHE_LIMIT);
        assert_eq!(cache.order.len(), HANDLED_EVENT_CACHE_LIMIT);
    }
}
