mod config;
mod supervisor;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use serde::Deserialize;
use std::collections::BTreeMap;
use tokio_util::sync::CancellationToken;
use tracing::info;

use config::TopologyConfig;
use supervisor::Supervisor;
use swarm_core::{
    bus::Bus,
    envelope::Envelope,
    manifest::AgentManifest,
    tokens::{default_key_path, load_signing_key, mint_actor_token},
    topics::{direct_inbox_topic, AGENT_TELEMETRY_LOGS, SESSION_NAMED, WORKSPACE_IDEA_SUBMITTED},
};

#[derive(Parser)]
#[command(name = "agora", about = "Rust event-driven agent swarm on NATS")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Start the full swarm from a topology config
    Run { config: std::path::PathBuf },
    /// Submit an idea to a running swarm (fires workspace.idea.submitted)
    Submit {
        /// The idea text
        idea: String,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
        #[arg(long, default_value = ".kiro/session_token")]
        key_path: String,
    },
    /// List sessions seen in the event stream
    Sessions {
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
    },
    /// Create or rename session metadata
    Session {
        #[command(subcommand)]
        command: SessionCmd,
    },
    /// List agents currently registered in the swarm
    Agents {
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
    },
    /// Show an agent manifest and optional session activity
    Status {
        agent: String,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
    },
    /// Show an agent's event and telemetry history in one session
    History {
        agent: String,
        #[arg(long)]
        session_id: String,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
    },
    /// Send a direct steering or queue message to an agent
    Message {
        agent: String,
        #[arg(required = true, trailing_var_arg = true)]
        message: Vec<String>,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long, default_value = "steer")]
        message_type: String,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
        #[arg(long, default_value = ".kiro/session_token")]
        key_path: String,
    },
    /// Print events from the AGORA_EVENTS stream
    Replay {
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
    },
    /// Bootstrap the signing key
    Bootstrap {
        #[arg(long, default_value = ".kiro/session_token")]
        key_path: String,
    },
    /// Check that everything the swarm needs is in place
    Doctor {
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
        #[arg(long, default_value = ".kiro/session_token")]
        key_path: String,
    },
}

#[derive(Subcommand)]
enum SessionCmd {
    /// Create a named session without submitting work
    New {
        name: String,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
        #[arg(long, default_value = ".kiro/session_token")]
        key_path: String,
    },
    /// Rename an existing session
    Rename {
        session_id: String,
        name: String,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
        #[arg(long, default_value = ".kiro/session_token")]
        key_path: String,
    },
}

#[derive(Debug, Clone)]
struct SessionSummary {
    session_id: String,
    name: Option<String>,
    started_at: String,
    last_topic: String,
    event_count: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TelemetryEntry {
    timestamp: String,
    #[serde(rename = "sessionId")]
    session_id: String,
    agent: String,
    #[serde(default)]
    level: String,
    action: String,
    #[serde(default)]
    telemetry: serde_json::Value,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let default_log_filter = if matches!(&cli.command, Cmd::Run { .. }) {
        "info"
    } else {
        "warn"
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| default_log_filter.into()),
        )
        .init();

    match cli.command {
        Cmd::Run { config } => run_swarm(config).await,
        Cmd::Submit {
            idea,
            session_id,
            bus_url,
            key_path,
        } => submit_idea(&idea, session_id.as_deref(), &bus_url, &key_path).await,
        Cmd::Sessions { bus_url } => list_sessions(&bus_url).await,
        Cmd::Session { command } => match command {
            SessionCmd::New {
                name,
                bus_url,
                key_path,
            } => create_session(&name, &bus_url, &key_path).await,
            SessionCmd::Rename {
                session_id,
                name,
                bus_url,
                key_path,
            } => rename_session(&session_id, &name, &bus_url, &key_path).await,
        },
        Cmd::Agents { bus_url } => list_agents(&bus_url).await,
        Cmd::Status {
            agent,
            session_id,
            bus_url,
        } => show_status(&agent, session_id.as_deref(), &bus_url).await,
        Cmd::History {
            agent,
            session_id,
            bus_url,
        } => show_history(&agent, &session_id, &bus_url).await,
        Cmd::Message {
            agent,
            message,
            session_id,
            message_type,
            bus_url,
            key_path,
        } => {
            send_message(
                &agent,
                &message.join(" "),
                session_id.as_deref(),
                &message_type,
                &bus_url,
                &key_path,
            )
            .await
        }
        Cmd::Replay {
            session_id,
            bus_url,
        } => replay(&session_id, &bus_url).await,
        Cmd::Bootstrap { key_path } => bootstrap(&key_path),
        Cmd::Doctor { config, key_path } => doctor(&config, &key_path).await,
    }
}

/// Run a sequence of preflight checks and exit non-zero on any failure.
async fn doctor(config_path: &std::path::Path, key_path: &str) -> Result<()> {
    use std::io::IsTerminal;

    let tty = std::io::stdout().is_terminal();
    let paint = |s: &str, code: &str| -> String {
        if tty {
            format!("\x1b[{code}m{s}\x1b[0m")
        } else {
            s.to_string()
        }
    };
    let ok = |label: &str, detail: &str| {
        println!("  {} {label} — {detail}", paint("✓", "32;1"));
    };
    let warn = |label: &str, detail: &str| {
        println!("  {} {label} — {detail}", paint("!", "33;1"));
    };
    let fail = |label: &str, detail: &str| {
        println!("  {} {label} — {detail}", paint("✗", "31;1"));
    };

    println!("agora doctor");
    println!();

    let mut failures = 0u32;
    let mut warnings = 0u32;

    // 1. nats-server on PATH
    match which::which("nats-server") {
        Ok(p) => ok("nats-server", &format!("found at {}", p.display())),
        Err(_) => {
            fail(
                "nats-server",
                "not on PATH. Install with `brew install nats-server` (macOS) or download from https://nats.io",
            );
            failures += 1;
        }
    }

    // 2. Topology load + validate
    let topology = match TopologyConfig::load(config_path) {
        Ok(t) => {
            ok(
                "topology",
                &format!(
                    "{} validates ({} agents)",
                    config_path.display(),
                    t.agents.len()
                ),
            );
            Some(t)
        }
        Err(e) => {
            fail("topology", &format!("{}: {e:#}", config_path.display()));
            failures += 1;
            None
        }
    };

    // 3. kiro-cli on PATH (only if any agent will use it)
    let uses_kiro = topology
        .as_ref()
        .map(|t| {
            let default = t.default_acp.as_str();
            t.agents
                .iter()
                .any(|a| a.acp.as_deref().unwrap_or(default) == "kiro")
        })
        .unwrap_or(true);
    if uses_kiro {
        match which::which("kiro-cli") {
            Ok(p) => ok("kiro-cli", &format!("found at {}", p.display())),
            Err(_) => {
                fail(
                    "kiro-cli",
                    "not on PATH but topology uses kiro ACP. Install kiro-cli and run `kiro-cli login`",
                );
                failures += 1;
            }
        }
    } else {
        ok("kiro-cli", "not required (topology uses mock ACP)");
    }

    // 4. Signing key exists + non-empty
    let key_p = std::path::Path::new(key_path);
    match std::fs::metadata(key_p) {
        Ok(meta) if meta.len() > 0 => ok(
            "signing key",
            &format!("{} ({} bytes)", key_p.display(), meta.len()),
        ),
        Ok(_) => {
            fail(
                "signing key",
                &format!("{} is empty. Re-run `agora bootstrap`", key_p.display()),
            );
            failures += 1;
        }
        Err(_) => {
            fail(
                "signing key",
                &format!(
                    "{} missing. Run `./scripts/bootstrap.sh` or `agora bootstrap`",
                    key_p.display()
                ),
            );
            failures += 1;
        }
    }

    // 5. NATS reachable + JetStream stream healthy
    if let Some(topology) = &topology {
        let bus_url = &topology.bus_url;
        match Bus::connect(bus_url).await {
            Ok(bus) => {
                ok("NATS bus", &format!("reachable at {bus_url}"));
                match bus.js.get_stream(swarm_core::topics::EVENT_STREAM).await {
                    Ok(stream) => {
                        let info = stream.cached_info();
                        ok(
                            "JetStream",
                            &format!(
                                "stream {} present ({} subjects, {} messages)",
                                info.config.name,
                                info.config.subjects.len(),
                                info.state.messages
                            ),
                        );
                    }
                    Err(e) => {
                        warn(
                            "JetStream",
                            &format!(
                                "could not load stream info: {e}. Will be recreated next startup"
                            ),
                        );
                        warnings += 1;
                    }
                }
            }
            Err(_) => {
                warn(
                    "NATS bus",
                    &format!(
                        "not reachable at {bus_url} (this is expected if the swarm isn't running yet)"
                    ),
                );
                warnings += 1;
            }
        }
    }

    println!();
    if failures == 0 && warnings == 0 {
        println!("{}", paint("All checks passed.", "32;1"));
        Ok(())
    } else if failures == 0 {
        println!(
            "{} {warnings} warning{}",
            paint("Ready, with", "33;1"),
            if warnings == 1 { "" } else { "s" },
        );
        Ok(())
    } else {
        bail!(
            "{failures} check{} failed (warnings: {warnings})",
            if failures == 1 { "" } else { "s" }
        );
    }
}

async fn run_swarm(config_path: std::path::PathBuf) -> Result<()> {
    let cfg = TopologyConfig::load(&config_path)?;
    info!(name = %cfg.name, agents = cfg.agents.len(), "starting swarm");

    std::fs::create_dir_all(&cfg.log_dir)?;
    std::fs::create_dir_all(&cfg.pid_dir)?;
    ensure_signing_key(&default_key_path())?;

    let shutdown = CancellationToken::new();
    let mut sup = Supervisor::new(shutdown.clone());

    let topology_path = config_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("topology path is not valid UTF-8"))?;
    sup.start_all(topology_path, &cfg).await?;

    let sd = shutdown.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Ctrl-C received — shutting down");
        sd.cancel();
    });

    info!("Swarm running. Press Ctrl-C to stop.");
    sup.wait_for_shutdown().await;
    Ok(())
}

async fn submit_idea(
    idea: &str,
    session_id: Option<&str>,
    bus_url: &str,
    key_path: &str,
) -> Result<()> {
    let signing_key = load_signing_key(key_path)?;
    let session_id = session_id
        .map(String::from)
        .unwrap_or_else(|| format!("sess_{}", ulid::Ulid::new()));
    let token = mint_actor_token(
        "cli-user",
        &["workspace:read", "workspace:write"],
        &session_id,
        &signing_key,
        900,
    )?;

    let bus = Bus::connect(bus_url).await?;
    let envelope = Envelope::build(
        WORKSPACE_IDEA_SUBMITTED,
        "agora-cli",
        0u16,
        token,
        session_id.clone(),
        serde_json::json!({ "idea": idea }),
        None,
        vec![],
    );

    bus.publish(&envelope).await?;
    println!("Submitted session {session_id}: {idea}");
    Ok(())
}

async fn create_session(name: &str, bus_url: &str, key_path: &str) -> Result<()> {
    let session_id = format!("sess_{}", ulid::Ulid::new());
    publish_session_named(&session_id, name, bus_url, key_path).await?;
    println!("Created session {session_id}: {name}");
    Ok(())
}

async fn rename_session(session_id: &str, name: &str, bus_url: &str, key_path: &str) -> Result<()> {
    publish_session_named(session_id, name, bus_url, key_path).await?;
    println!("Renamed session {session_id}: {name}");
    Ok(())
}

async fn publish_session_named(
    session_id: &str,
    name: &str,
    bus_url: &str,
    key_path: &str,
) -> Result<()> {
    let signing_key = load_signing_key(key_path)?;
    let token = mint_actor_token(
        "cli-user",
        &["workspace:read"],
        session_id,
        &signing_key,
        900,
    )?;
    let bus = Bus::connect(bus_url).await?;
    let env = Envelope::build(
        SESSION_NAMED,
        "agora-cli",
        0,
        token,
        session_id,
        serde_json::json!({ "name": name }),
        None,
        vec![],
    );
    bus.publish(&env).await
}

async fn list_sessions(bus_url: &str) -> Result<()> {
    let bus = Bus::connect(bus_url).await?;
    let sessions = load_sessions(&bus).await?;

    if sessions.is_empty() {
        println!("No sessions found.");
        return Ok(());
    }

    let mut rows: Vec<_> = sessions.values().collect();
    rows.sort_by(|a, b| {
        b.started_at
            .cmp(&a.started_at)
            .then_with(|| a.session_id.cmp(&b.session_id))
    });

    println!(
        "{:<32} {:<24} {:>6}  {:<22} LAST TOPIC",
        "SESSION", "NAME", "EVENTS", "STARTED"
    );
    for session in rows {
        println!(
            "{:<32} {:<24} {:>6}  {:<22} {}",
            session.session_id,
            session.name.as_deref().unwrap_or("-"),
            session.event_count,
            session.started_at,
            if session.last_topic.is_empty() {
                "-"
            } else {
                &session.last_topic
            }
        );
    }
    Ok(())
}

async fn list_agents(bus_url: &str) -> Result<()> {
    let bus = Bus::connect(bus_url).await?;
    let mut agents = bus.read_agent_registry().await?;
    agents.sort_by(|a, b| a.agent_name.cmp(&b.agent_name));

    if agents.is_empty() {
        println!("No agents registered.");
        return Ok(());
    }

    println!(
        "{:<30} {:<10} {:>5}  {:<22} CAPABILITIES",
        "AGENT", "STATUS", "PORT", "LAST SEEN"
    );
    for agent in agents {
        println!(
            "{:<30} {:<10} {:>5}  {:<22} {}",
            agent.agent_name,
            format!("{:?}", agent.status).to_lowercase(),
            agent.port,
            agent.last_seen,
            agent.capabilities.join(",")
        );
    }
    Ok(())
}

async fn show_status(agent_name: &str, session_id: Option<&str>, bus_url: &str) -> Result<()> {
    let bus = Bus::connect(bus_url).await?;
    let agents = bus.read_agent_registry().await?;
    let manifest = agents.iter().find(|agent| agent.agent_name == agent_name);

    print_agent_manifest(agent_name, manifest);

    let Some(session_id) = session_id else {
        return Ok(());
    };

    let events = bus.read_all_events().await?;
    let published: Vec<_> = events
        .iter()
        .filter(|e| e.context.session_id == session_id && e.sender.agent_name == agent_name)
        .collect();

    println!();
    println!("Session {session_id}:");
    if published.is_empty() {
        println!("  no published events from this agent");
    } else {
        for event in published {
            println!(
                "  {}  {}  {}",
                time_only(&event.timestamp),
                event.topic,
                short(&event.event_id, 18)
            );
        }
    }
    Ok(())
}

async fn show_history(agent_name: &str, session_id: &str, bus_url: &str) -> Result<()> {
    let bus = Bus::connect(bus_url).await?;
    let agents = bus.read_agent_registry().await?;
    let subscribed = agents
        .iter()
        .find(|agent| agent.agent_name == agent_name)
        .map(|agent| agent.subscribes_to.clone())
        .unwrap_or_default();

    let events = bus.read_all_events().await?;
    let telemetry = read_telemetry(&bus).await?;
    let session_names = load_session_names(&events);
    let label = session_names
        .get(session_id)
        .map(String::as_str)
        .unwrap_or(session_id);

    #[derive(Clone)]
    enum Item {
        Received(Envelope),
        Prompt(String),
        Response(String),
        Published(Envelope),
        Other(TelemetryEntry),
    }

    let mut items: Vec<(String, Item)> = Vec::new();

    for event in events {
        if event.context.session_id != session_id {
            continue;
        }
        if event.sender.agent_name == agent_name {
            items.push((event.timestamp.clone(), Item::Published(event)));
        } else if subscribed
            .iter()
            .any(|pattern| topic_matches(pattern, &event.topic))
        {
            items.push((event.timestamp.clone(), Item::Received(event)));
        }
    }

    for entry in telemetry {
        if entry.session_id != session_id || entry.agent != agent_name {
            continue;
        }

        match entry.action.as_str() {
            "prompt_sent" | "prompt" => {
                let text = entry
                    .telemetry
                    .get("prompt")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                items.push((entry.timestamp.clone(), Item::Prompt(text)));
            }
            "response_received" | "response" => {
                let text = entry
                    .telemetry
                    .get("response")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                items.push((entry.timestamp.clone(), Item::Response(text)));
            }
            _ => items.push((entry.timestamp.clone(), Item::Other(entry))),
        }
    }

    items.sort_by(|a, b| a.0.cmp(&b.0));

    println!("History: {agent_name} in \"{label}\"");
    println!("{} item(s)", items.len());
    println!();

    if items.is_empty() {
        println!("No activity found.");
    }

    for (_, item) in items {
        match item {
            Item::Received(event) => {
                println!(
                    "{}  <- received  {}  [{}]",
                    time_only(&event.timestamp),
                    event.topic,
                    short(&event.event_id, 18)
                );
                print_indented_json(&event.data, "         ")?;
            }
            Item::Prompt(text) => {
                println!(".....  prompt to ACP  .....");
                push_indented_stdout(&text, "  ");
            }
            Item::Response(text) => {
                println!(".....  response from ACP  .....");
                push_indented_stdout(&text, "  ");
            }
            Item::Published(event) => {
                println!(
                    "{}  -> published {}  [{}]",
                    time_only(&event.timestamp),
                    event.topic,
                    short(&event.event_id, 18)
                );
                print_indented_json(&event.data, "         ")?;
            }
            Item::Other(entry) => {
                println!(
                    "{}  . {}  {}",
                    time_only(&entry.timestamp),
                    entry.level,
                    entry.action
                );
                push_indented_stdout(&entry.telemetry.to_string(), "         ");
            }
        }
        println!();
    }

    Ok(())
}

async fn send_message(
    agent: &str,
    message: &str,
    session_id: Option<&str>,
    message_type: &str,
    bus_url: &str,
    key_path: &str,
) -> Result<()> {
    if message_type != "steer" && message_type != "queue" {
        bail!("--message-type must be 'steer' or 'queue'");
    }

    let session_id = session_id
        .map(String::from)
        .unwrap_or_else(|| format!("sess_{}", ulid::Ulid::new()));
    let signing_key = load_signing_key(key_path)?;
    let token = mint_actor_token(
        "cli-user",
        &["agent:message"],
        &session_id,
        &signing_key,
        900,
    )?;
    let bus = Bus::connect(bus_url).await?;
    let env = Envelope::build(
        direct_inbox_topic(agent),
        "agora-cli",
        0,
        token,
        session_id.clone(),
        serde_json::json!({
            "messageType": message_type,
            "recipient": agent,
            "message": message,
        }),
        None,
        vec![],
    );
    bus.publish(&env).await?;
    println!("[{message_type}] @{agent}: {message}");
    println!("session={session_id}");
    Ok(())
}

async fn replay(session_id: &Option<String>, bus_url: &str) -> Result<()> {
    let bus = Bus::connect(bus_url).await?;
    let events = bus.read_all_events().await?;

    let filtered: Vec<_> = events
        .iter()
        .filter(|e| {
            session_id
                .as_deref()
                .is_none_or(|sid| e.context.session_id == sid)
        })
        .collect();

    println!("Found {} events", filtered.len());
    for e in filtered {
        println!(
            "[{}] {} | {} | session={}",
            e.timestamp, e.event_id, e.topic, e.context.session_id
        );
        if !e.data.is_null() {
            println!("  data: {}", serde_json::to_string(&e.data)?);
        }
    }
    Ok(())
}

async fn load_sessions(bus: &Bus) -> Result<BTreeMap<String, SessionSummary>> {
    let events = bus.read_all_events().await?;
    Ok(build_sessions(&events))
}

fn build_sessions(events: &[Envelope]) -> BTreeMap<String, SessionSummary> {
    let mut sessions = BTreeMap::new();

    for event in events {
        let session_id = event.context.session_id.clone();
        let entry = sessions
            .entry(session_id.clone())
            .or_insert_with(|| SessionSummary {
                session_id,
                name: None,
                started_at: event.timestamp.clone(),
                last_topic: String::new(),
                event_count: 0,
            });

        if event.timestamp < entry.started_at {
            entry.started_at = event.timestamp.clone();
        }

        if event.topic == SESSION_NAMED {
            if let Some(name) = event.data.get("name").and_then(|v| v.as_str()) {
                entry.name = Some(name.to_string());
            }
            continue;
        }

        entry.last_topic = event.topic.clone();
        entry.event_count += 1;
    }

    sessions
}

fn load_session_names(events: &[Envelope]) -> BTreeMap<String, String> {
    let mut names = BTreeMap::new();
    for event in events {
        if event.topic == SESSION_NAMED {
            if let Some(name) = event.data.get("name").and_then(|v| v.as_str()) {
                names.insert(event.context.session_id.clone(), name.to_string());
            }
        }
    }
    names
}

async fn read_telemetry(bus: &Bus) -> Result<Vec<TelemetryEntry>> {
    let payloads = bus.read_raw_subject(AGENT_TELEMETRY_LOGS).await?;
    let mut entries = Vec::new();

    for payload in payloads {
        if let Ok(entry) = serde_json::from_slice::<TelemetryEntry>(&payload) {
            entries.push(entry);
        }
    }

    Ok(entries)
}

fn print_agent_manifest(agent_name: &str, manifest: Option<&AgentManifest>) {
    println!("{agent_name}");
    let Some(manifest) = manifest else {
        println!("  no manifest registered");
        return;
    };

    println!("  status:        {:?}", manifest.status);
    println!("  port:          {}", manifest.port);
    println!("  endpoint:      {}", manifest.endpoint);
    println!("  last seen:     {}", manifest.last_seen);
    println!("  capabilities:  {}", manifest.capabilities.join(", "));
    println!("  subscribes:    {}", manifest.subscribes_to.join(", "));
    println!("  publishes:     {}", manifest.publishes.join(", "));
}

fn print_indented_json(value: &serde_json::Value, indent: &str) -> Result<()> {
    if value.is_null() {
        return Ok(());
    }

    let text = serde_json::to_string_pretty(value)?;
    push_indented_stdout(&text, indent);
    Ok(())
}

fn push_indented_stdout(text: &str, indent: &str) {
    for line in text.lines() {
        println!("{indent}{line}");
    }
}

fn time_only(timestamp: &str) -> &str {
    if timestamp.len() >= 19 {
        &timestamp[11..19]
    } else {
        timestamp
    }
}

fn short(value: &str, len: usize) -> String {
    if value.len() <= len {
        value.to_string()
    } else {
        value[..len].to_string()
    }
}

fn topic_matches(pattern: &str, topic: &str) -> bool {
    let pattern_parts: Vec<_> = pattern.split('.').collect();
    let topic_parts: Vec<_> = topic.split('.').collect();
    let mut p = 0;
    let mut t = 0;

    while p < pattern_parts.len() {
        match pattern_parts[p] {
            ">" => return p == pattern_parts.len() - 1,
            "*" => {
                if t >= topic_parts.len() {
                    return false;
                }
                p += 1;
                t += 1;
            }
            literal => {
                if t >= topic_parts.len() || literal != topic_parts[t] {
                    return false;
                }
                p += 1;
                t += 1;
            }
        }
    }

    t == topic_parts.len()
}

fn bootstrap(key_path: &str) -> Result<()> {
    let path = std::path::Path::new(key_path);
    if path.exists() {
        println!("Key already exists at {key_path}");
        return Ok(());
    }
    write_new_signing_key(path)?;
    println!("Signing key written to {key_path}");
    Ok(())
}

fn ensure_signing_key(path: &std::path::Path) -> Result<()> {
    if path.exists() {
        return Ok(());
    }
    write_new_signing_key(path)?;
    info!(path = %path.display(), "generated missing signing key");
    Ok(())
}

fn write_new_signing_key(path: &std::path::Path) -> Result<()> {
    use std::io::Write;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Cross-platform CSPRNG: `getrandom` reads from /dev/urandom on Linux,
    // /dev/random on macOS, and BCryptGenRandom on Windows.
    let mut key = [0u8; 32];
    getrandom::getrandom(&mut key).context("failed to read OS random bytes")?;
    let hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut f = options.open(path)?;
    writeln!(f, "{hex}")?;
    Ok(())
}
