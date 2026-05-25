mod config;
mod lifecycle;
mod supervisor;
mod watch;

use anyhow::{bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use agora_core::{
    bookmark::{validate_bookmark_label, BookmarkEvent},
    bus::{AgentRegistryRecord, Bus},
    envelope::Envelope,
    event_data_from_input,
    manifest::{AgentManifest, AgentStatus},
    tags::validate_tag,
    tokens::{default_key_path, load_signing_key, mint_actor_token},
    topics::{
        direct_inbox_topic, AGENT_TELEMETRY_LOGS, EVENT_BOOKMARKED, EVENT_STREAM,
        EVENT_STREAM_SUBJECTS, EVENT_UNBOOKMARKED, HUMAN_INTERACTION_REQUEST,
        HUMAN_INTERACTION_RESPONSE, SESSION_DELETED, SESSION_NAMED, SESSION_TAGGED,
        SESSION_UNTAGGED,
    },
};
use config::TopologyConfig;
use lifecycle::{
    expected_processes, legacy_swarm_processes, log_path_for_target, print_logs,
    print_process_stats, print_process_table, print_process_top, process_for_target,
    process_metrics, restart_agent, runtime_statuses, status_for, stop_legacy_swarm_processes,
    stop_runtime, write_pid, LogOptions, ProcessState, RuntimeProcessStatus,
};
use supervisor::Supervisor;

#[derive(Parser)]
#[command(name = "agora", about = "Rust event-driven agent swarm on NATS")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Start the full swarm from a topology config
    Start {
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
        /// Start the supervisor in the background and write logs to the runtime log directory
        #[arg(short = 'd', long)]
        detach: bool,
    },
    /// Legacy alias for `start --config <CONFIG>`
    #[command(hide = true)]
    Run {
        config: std::path::PathBuf,
        #[arg(short = 'd', long)]
        detach: bool,
    },
    /// Open the terminal console for the running swarm
    Console(agora_console::ConsoleArgs),
    /// Submit an event to a running swarm
    Submit {
        /// Event topic to publish
        topic: String,
        /// Event data as JSON, or plain text wrapped as {"text": "..."}
        data: String,
        /// Field name used when DATA is plain text
        #[arg(long, default_value = "text")]
        field: String,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
        #[arg(long, default_value = ".kiro/session_token")]
        key_path: String,
        /// Topology file. Used to reject unknown topics and derive
        /// actor-token scopes. Pass `--no-topology` to skip both.
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
        /// Skip topology-aware validation and scope derivation.
        #[arg(long)]
        no_topology: bool,
    },
    /// List sessions seen in the event stream
    Sessions {
        #[arg(long)]
        include_deleted: bool,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
    },
    /// Manage session metadata and history
    Session {
        #[command(subcommand)]
        command: SessionCmd,
    },
    /// Manage agents
    Agent {
        #[command(subcommand)]
        command: AgentCmd,
    },
    /// List agents currently registered in the swarm
    Agents {
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
    },
    /// Inspect and maintain the local runtime
    System {
        #[command(subcommand)]
        command: SystemCmd,
    },
    /// Show local supervisor, service, and agent process status
    Ps {
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
    },
    /// Print log targets or tail a process/agent log
    Logs {
        target: Option<String>,
        #[arg(long = "tail", visible_alias = "lines", default_value_t = 80)]
        tail: usize,
        #[arg(short = 'f', long)]
        follow: bool,
        #[arg(long)]
        since: Option<String>,
        #[arg(short = 't', long)]
        timestamps: bool,
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
    },
    /// Show events from the swarm event stream
    Events(EventArgs),
    /// Stream formatted events to stdout in real-time
    Watch(watch::WatchArgs),
    /// Inspect an agent, session, process, or runtime object
    Inspect {
        target: String,
        #[arg(long)]
        json: bool,
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
        #[arg(long)]
        bus_url: Option<String>,
    },
    /// Show local process resource usage
    Stats {
        target: Option<String>,
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
    },
    /// Show the local process tree for an agent or service
    Top {
        target: String,
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
    },
    /// Show runtime, bus, registry, and topology information
    Info {
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
        #[arg(long)]
        bus_url: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Print CLI version information
    Version {
        #[arg(long)]
        json: bool,
    },
    /// Stop the running swarm, a single process, or legacy Python publishers
    Stop {
        target: Option<String>,
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
        #[arg(long)]
        force: bool,
        #[arg(long)]
        legacy: bool,
    },
    /// Restart one agent managed by `agora start`
    Restart {
        agent: String,
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
        #[arg(long)]
        force: bool,
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
        #[arg(required = true, num_args = 1..)]
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
        #[arg(long)]
        json: bool,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
    },
    /// Bootstrap the signing key
    Bootstrap {
        #[arg(long, default_value = ".kiro/session_token")]
        key_path: String,
    },
    /// Inspect and clean the agent registry
    Registry {
        #[command(subcommand)]
        command: RegistryCmd,
    },
    /// Check that everything the swarm needs is in place
    Doctor {
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
        #[arg(long, default_value = ".kiro/session_token")]
        key_path: String,
    },
    /// Manage event bookmarks
    Bookmark {
        #[command(subcommand)]
        command: BookmarkCmd,
    },
    /// Respond to a pending human.interaction.request
    Respond {
        /// Event ID of the human.interaction.request to respond to
        event_id: String,
        /// Response text (reads from stdin if omitted)
        answer: Option<String>,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
        #[arg(long, default_value = ".kiro/session_token")]
        key_path: String,
    },
}

#[derive(Subcommand)]
enum BookmarkCmd {
    /// Bookmark an event
    Add {
        event_id: String,
        #[arg(long)]
        label: Option<String>,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
        #[arg(long, default_value = ".kiro/session_token")]
        key_path: String,
    },
    /// Remove a bookmark from an event
    Remove {
        event_id: String,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
        #[arg(long, default_value = ".kiro/session_token")]
        key_path: String,
    },
    /// List bookmarked events
    Ls {
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long)]
        json: bool,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
    },
}

#[derive(Debug, Clone, Args)]
struct EventArgs {
    #[arg(long)]
    session_id: Option<String>,
    #[arg(long)]
    agent: Option<String>,
    #[arg(long)]
    topic: Option<String>,
    #[arg(short = 'f', long)]
    follow: bool,
    #[arg(long)]
    json: bool,
    #[arg(long, default_value = "nats://127.0.0.1:4222")]
    bus_url: String,
}

#[derive(Subcommand)]
enum RegistryCmd {
    /// Prune stale or malformed registry entries outside the topology
    Prune {
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
        #[arg(long)]
        bus_url: Option<String>,
        /// Actually purge entries. Without this, the command is a dry run.
        #[arg(long)]
        apply: bool,
    },
}

#[derive(Subcommand)]
enum SessionCmd {
    /// List sessions seen in the event stream
    Ls {
        #[arg(long)]
        include_deleted: bool,
        #[arg(long)]
        tag: Option<String>,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
    },
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
    /// Hide a session from default lists without erasing its events
    Delete {
        session_id: String,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
        #[arg(long, default_value = ".kiro/session_token")]
        key_path: String,
    },
    /// Add a tag to a session
    Tag {
        session_id: String,
        label: String,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
        #[arg(long, default_value = ".kiro/session_token")]
        key_path: String,
    },
    /// Remove a tag from a session
    Untag {
        session_id: String,
        label: String,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
        #[arg(long, default_value = ".kiro/session_token")]
        key_path: String,
    },
    /// Inspect a session
    Inspect {
        session_id: String,
        #[arg(long)]
        json: bool,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
    },
    /// Show all events in a session
    History {
        session_id: String,
        #[arg(long)]
        json: bool,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
    },
}

#[derive(Subcommand)]
enum AgentCmd {
    /// List registered agents
    Ls {
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
    },
    /// Inspect one agent
    Inspect {
        agent: String,
        #[arg(long)]
        json: bool,
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
        #[arg(long)]
        bus_url: Option<String>,
    },
    /// Show one agent's logs
    Logs {
        agent: String,
        #[arg(long = "tail", visible_alias = "lines", default_value_t = 80)]
        tail: usize,
        #[arg(short = 'f', long)]
        follow: bool,
        #[arg(long)]
        since: Option<String>,
        #[arg(short = 't', long)]
        timestamps: bool,
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
    },
    /// Show one agent's manifest and optional session activity
    Status {
        agent: String,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
    },
    /// Show one agent's event and telemetry history in one session
    History {
        agent: String,
        #[arg(long)]
        session_id: String,
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        bus_url: String,
    },
    /// Show one agent's local resource usage
    Stats {
        agent: String,
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
    },
    /// Show one agent's local process tree
    Top {
        agent: String,
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
    },
    /// Restart one agent
    Restart {
        agent: String,
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
        #[arg(long)]
        force: bool,
    },
    /// Stop one agent
    Stop {
        agent: String,
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
        #[arg(long)]
        force: bool,
    },
    /// Send a direct steering or queue message
    Message {
        agent: String,
        #[arg(required = true, num_args = 1..)]
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
}

#[derive(Subcommand)]
enum SystemCmd {
    /// Show runtime, bus, registry, and topology information
    Info {
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
        #[arg(long)]
        bus_url: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Show events from the swarm event stream
    Events(EventArgs),
    /// Show local supervisor, service, and agent process status
    Ps {
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
    },
    /// Show local process resource usage
    Stats {
        target: Option<String>,
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
    },
    /// Prune stale or malformed registry entries outside the topology
    Prune {
        #[arg(long, default_value = "agents.local.json")]
        config: std::path::PathBuf,
        #[arg(long)]
        bus_url: Option<String>,
        #[arg(long)]
        apply: bool,
    },
}

#[derive(Debug, Clone, Serialize)]
struct SessionSummary {
    session_id: String,
    name: Option<String>,
    started_at: String,
    last_topic: String,
    event_count: usize,
    deleted: bool,
    deleted_at: Option<String>,
    tags: BTreeSet<String>,
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
    let default_log_filter = if matches!(&cli.command, Cmd::Start { .. } | Cmd::Run { .. }) {
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
        Cmd::Start { config, detach } => {
            if detach {
                start_detached(config)
            } else {
                run_swarm(config).await
            }
        }
        Cmd::Run { config, detach } => {
            if detach {
                start_detached(config)
            } else {
                run_swarm(config).await
            }
        }
        Cmd::Console(args) => agora_console::run(args).await,
        Cmd::Submit {
            topic,
            data,
            field,
            session_id,
            bus_url,
            key_path,
            config,
            no_topology,
        } => {
            let catalog = if no_topology {
                agora_core::TopicCatalog::default()
            } else {
                match agora_core::TopologySnapshot::load(&config) {
                    Ok(snap) => snap.topic_catalog(),
                    Err(e) => {
                        warn!(
                            "Could not load topology from {} ({e}); falling back to legacy scopes",
                            config.display()
                        );
                        agora_core::TopicCatalog::default()
                    }
                }
            };
            submit_event(
                &topic,
                &data,
                &field,
                session_id.as_deref(),
                &bus_url,
                &key_path,
                &catalog,
            )
            .await
        }
        Cmd::Sessions {
            include_deleted,
            bus_url,
        } => list_sessions(&bus_url, include_deleted, None).await,
        Cmd::Session { command } => match command {
            SessionCmd::Ls {
                include_deleted,
                tag,
                bus_url,
            } => list_sessions(&bus_url, include_deleted, tag.as_deref()).await,
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
            SessionCmd::Delete {
                session_id,
                bus_url,
                key_path,
            } => delete_session(&session_id, &bus_url, &key_path).await,
            SessionCmd::Tag {
                session_id,
                label,
                bus_url,
                key_path,
            } => tag_session(&session_id, &label, &bus_url, &key_path).await,
            SessionCmd::Untag {
                session_id,
                label,
                bus_url,
                key_path,
            } => untag_session(&session_id, &label, &bus_url, &key_path).await,
            SessionCmd::Inspect {
                session_id,
                json,
                bus_url,
            } => inspect_session(&session_id, &bus_url, json).await,
            SessionCmd::History {
                session_id,
                json,
                bus_url,
            } => show_session_history(&session_id, &bus_url, json).await,
        },
        Cmd::Agent { command } => match command {
            AgentCmd::Ls { bus_url } => list_agents(&bus_url).await,
            AgentCmd::Inspect {
                agent,
                json,
                config,
                bus_url,
            } => inspect_target(&agent, &config, bus_url.as_deref(), json).await,
            AgentCmd::Logs {
                agent,
                tail,
                follow,
                since,
                timestamps,
                config,
            } => {
                let cfg = TopologyConfig::load(config)?;
                print_logs(
                    &cfg,
                    Some(&agent),
                    &LogOptions {
                        tail,
                        follow,
                        since,
                        timestamps,
                    },
                )
            }
            AgentCmd::Status {
                agent,
                session_id,
                bus_url,
            } => show_status(&agent, session_id.as_deref(), &bus_url).await,
            AgentCmd::History {
                agent,
                session_id,
                bus_url,
            } => show_history(&agent, &session_id, &bus_url).await,
            AgentCmd::Stats { agent, config } => {
                let cfg = TopologyConfig::load(config)?;
                print_process_stats(&cfg, Some(&agent))
            }
            AgentCmd::Top { agent, config } => {
                let cfg = TopologyConfig::load(config)?;
                print_process_top(&cfg, &agent)
            }
            AgentCmd::Restart {
                agent,
                config,
                force,
            } => {
                let cfg = TopologyConfig::load(config)?;
                restart_agent(&cfg, &agent, force)
            }
            AgentCmd::Stop {
                agent,
                config,
                force,
            } => {
                let cfg = TopologyConfig::load(config)?;
                stop_runtime(&cfg, Some(&agent), force)
            }
            AgentCmd::Message {
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
        },
        Cmd::Agents { bus_url } => list_agents(&bus_url).await,
        Cmd::System { command } => match command {
            SystemCmd::Info {
                config,
                bus_url,
                json,
            } => show_info(&config, bus_url.as_deref(), json).await,
            SystemCmd::Events(args) => show_events(&args).await,
            SystemCmd::Ps { config } => {
                let cfg = TopologyConfig::load(config)?;
                print_process_table(&cfg);
                Ok(())
            }
            SystemCmd::Stats { target, config } => {
                let cfg = TopologyConfig::load(config)?;
                print_process_stats(&cfg, target.as_deref())
            }
            SystemCmd::Prune {
                config,
                bus_url,
                apply,
            } => prune_registry(&config, bus_url.as_deref(), apply).await,
        },
        Cmd::Ps { config } => {
            let cfg = TopologyConfig::load(config)?;
            print_process_table(&cfg);
            Ok(())
        }
        Cmd::Logs {
            target,
            tail,
            follow,
            since,
            timestamps,
            config,
        } => {
            let cfg = TopologyConfig::load(config)?;
            print_logs(
                &cfg,
                target.as_deref(),
                &LogOptions {
                    tail,
                    follow,
                    since,
                    timestamps,
                },
            )
        }
        Cmd::Events(args) => show_events(&args).await,
        Cmd::Watch(args) => watch::run_watch(&args).await,
        Cmd::Inspect {
            target,
            json,
            config,
            bus_url,
        } => inspect_target(&target, &config, bus_url.as_deref(), json).await,
        Cmd::Stats { target, config } => {
            let cfg = TopologyConfig::load(config)?;
            print_process_stats(&cfg, target.as_deref())
        }
        Cmd::Top { target, config } => {
            let cfg = TopologyConfig::load(config)?;
            print_process_top(&cfg, &target)
        }
        Cmd::Info {
            config,
            bus_url,
            json,
        } => show_info(&config, bus_url.as_deref(), json).await,
        Cmd::Version { json } => {
            print_version(json)?;
            Ok(())
        }
        Cmd::Stop {
            target,
            config,
            force,
            legacy,
        } => {
            if legacy {
                stop_legacy_swarm_processes(force)
            } else {
                let cfg = TopologyConfig::load(config)?;
                stop_runtime(&cfg, target.as_deref(), force)
            }
        }
        Cmd::Restart {
            agent,
            config,
            force,
        } => {
            let cfg = TopologyConfig::load(config)?;
            restart_agent(&cfg, &agent, force)
        }
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
            json,
            bus_url,
        } => replay(&session_id, &bus_url, json).await,
        Cmd::Bootstrap { key_path } => bootstrap(&key_path),
        Cmd::Registry { command } => match command {
            RegistryCmd::Prune {
                config,
                bus_url,
                apply,
            } => prune_registry(&config, bus_url.as_deref(), apply).await,
        },
        Cmd::Doctor { config, key_path } => doctor(&config, &key_path).await,
        Cmd::Bookmark { command } => match command {
            BookmarkCmd::Add {
                event_id,
                label,
                session_id,
                bus_url,
                key_path,
            } => {
                add_bookmark(
                    &event_id,
                    label.as_deref(),
                    session_id.as_deref(),
                    &bus_url,
                    &key_path,
                )
                .await
            }
            BookmarkCmd::Remove {
                event_id,
                session_id,
                bus_url,
                key_path,
            } => remove_bookmark(&event_id, session_id.as_deref(), &bus_url, &key_path).await,
            BookmarkCmd::Ls {
                session_id,
                json,
                bus_url,
            } => list_bookmarks(session_id.as_deref(), &bus_url, json).await,
        },
        Cmd::Respond {
            event_id,
            answer,
            bus_url,
            key_path,
        } => respond_to_interaction(&event_id, answer.as_deref(), &bus_url, &key_path).await,
    }
}

async fn respond_to_interaction(
    event_id: &str,
    answer: Option<&str>,
    bus_url: &str,
    key_path: &str,
) -> Result<()> {
    let answer = match answer {
        Some(answer) => answer.to_string(),
        None => {
            let mut input = String::new();
            std::io::Read::read_to_string(&mut std::io::stdin(), &mut input)?;
            input.trim_end_matches('\n').to_string()
        }
    };

    let bus = Bus::connect(bus_url).await?;
    let events = bus.read_all_events().await?;
    let request = events
        .iter()
        .find(|event| event.event_id == event_id)
        .with_context(|| format!("event `{event_id}` not found in stream"))?;
    if request.topic != HUMAN_INTERACTION_REQUEST {
        bail!(
            "event `{event_id}` is `{}`, not `{HUMAN_INTERACTION_REQUEST}`",
            request.topic
        );
    }

    let signing_key = load_signing_key(key_path)?;
    let token = mint_actor_token(
        "cli-user",
        &["workspace:read", "workspace:write"],
        &request.context.session_id,
        &signing_key,
        900,
    )?;
    let env = Envelope::build(
        HUMAN_INTERACTION_RESPONSE,
        "agora-cli",
        0,
        token,
        request.context.session_id.clone(),
        serde_json::json!({
            "correlationId": event_id,
            "answer": answer,
            "respondedBy": "agora-cli",
        }),
        None,
        vec![],
    );
    bus.publish(&env).await?;
    println!("Responded to {event_id}");
    Ok(())
}

async fn add_bookmark(
    event_id: &str,
    label: Option<&str>,
    session_id: Option<&str>,
    bus_url: &str,
    key_path: &str,
) -> Result<()> {
    if let Some(l) = label {
        validate_bookmark_label(l)?;
    }
    let bus = Bus::connect(bus_url).await?;
    let events = bus.read_all_events().await?;
    let target = events
        .iter()
        .find(|e| e.event_id == event_id)
        .with_context(|| format!("event `{event_id}` not found in stream"))?;
    let sid = session_id.unwrap_or(&target.context.session_id);

    let signing_key = load_signing_key(key_path)?;
    let token = mint_actor_token("cli-user", &["workspace:write"], sid, &signing_key, 900)?;
    let bookmark = BookmarkEvent {
        target_event_id: event_id.to_string(),
        label: label.map(String::from),
        actor: "agora-cli".to_string(),
    };
    let env = Envelope::build(
        EVENT_BOOKMARKED,
        "agora-cli",
        0u16,
        token,
        sid,
        serde_json::to_value(&bookmark)?,
        None,
        vec![],
    );
    bus.publish(&env).await?;
    println!("Bookmarked {event_id}");
    Ok(())
}

async fn remove_bookmark(
    event_id: &str,
    session_id: Option<&str>,
    bus_url: &str,
    key_path: &str,
) -> Result<()> {
    let bus = Bus::connect(bus_url).await?;
    let events = bus.read_all_events().await?;
    let target = events
        .iter()
        .find(|e| e.event_id == event_id)
        .with_context(|| format!("event `{event_id}` not found in stream"))?;
    let sid = session_id.unwrap_or(&target.context.session_id);

    let signing_key = load_signing_key(key_path)?;
    let token = mint_actor_token("cli-user", &["workspace:write"], sid, &signing_key, 900)?;
    let bookmark = BookmarkEvent {
        target_event_id: event_id.to_string(),
        label: None,
        actor: "agora-cli".to_string(),
    };
    let env = Envelope::build(
        EVENT_UNBOOKMARKED,
        "agora-cli",
        0u16,
        token,
        sid,
        serde_json::to_value(&bookmark)?,
        None,
        vec![],
    );
    bus.publish(&env).await?;
    println!("Unbookmarked {event_id}");
    Ok(())
}

async fn list_bookmarks(session_id: Option<&str>, bus_url: &str, json: bool) -> Result<()> {
    let bus = Bus::connect(bus_url).await?;
    let events = bus.read_all_events().await?;
    let mut bookmarks: BTreeMap<String, BookmarkEvent> = BTreeMap::new();

    for event in &events {
        if let Some(sid) = session_id {
            if event.context.session_id != sid {
                continue;
            }
        }
        if event.topic == EVENT_BOOKMARKED {
            if let Ok(bm) = serde_json::from_value::<BookmarkEvent>(event.data.clone()) {
                bookmarks.insert(bm.target_event_id.clone(), bm);
            }
        } else if event.topic == EVENT_UNBOOKMARKED {
            if let Ok(bm) = serde_json::from_value::<BookmarkEvent>(event.data.clone()) {
                bookmarks.remove(&bm.target_event_id);
            }
        }
    }

    if json {
        println!("{}", serde_json::to_string_pretty(&bookmarks)?);
        return Ok(());
    }

    if bookmarks.is_empty() {
        println!("No bookmarks found.");
        return Ok(());
    }

    println!("{:<24} {:<16} LABEL", "EVENT", "ACTOR");
    for (event_id, bm) in &bookmarks {
        println!(
            "{:<24} {:<16} {}",
            short(event_id, 24),
            bm.actor,
            bm.label.as_deref().unwrap_or("-")
        );
    }
    Ok(())
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

    let legacy_publishers = legacy_swarm_processes();
    if !legacy_publishers.is_empty() {
        warn(
            "legacy publishers",
            &format!(
                "{} old Python swarm process{} detected; these can repopulate stale registry keys",
                legacy_publishers.len(),
                if legacy_publishers.len() == 1 {
                    ""
                } else {
                    "es"
                }
            ),
        );
        for process in legacy_publishers.iter().take(4) {
            println!("      {} {}", process.pid, process.command);
        }
        warnings += 1;
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
        let processes = expected_processes(topology);
        let supervisor = processes.iter().find(|process| process.name == "agora");
        match supervisor.map(|process| status_for(process.clone())) {
            Some(status) if status.state == ProcessState::Running => ok(
                "agora supervisor",
                &format!("running as pid {}", status.pid.unwrap_or_default()),
            ),
            _ => {
                warn(
                    "agora supervisor",
                    "not running. Use `agora start --config agents.local.json` to own the swarm lifecycle",
                );
                warnings += 1;
            }
        }

        let bus_url = &topology.bus_url;
        match Bus::connect(bus_url).await {
            Ok(bus) => {
                ok("NATS bus", &format!("reachable at {bus_url}"));
                match bus.js.get_stream(agora_core::topics::EVENT_STREAM).await {
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

                match bus.read_agent_registry_records().await {
                    Ok(records) => {
                        let current = current_registry_count(&records, topology);
                        let stale = registry_prune_candidates(&records, topology);
                        let unhealthy = unhealthy_registry_count(&records, topology);
                        let missing = topology.agents.len().saturating_sub(current);
                        if missing == 0 && stale.is_empty() && unhealthy == 0 {
                            ok(
                                "agent registry",
                                &format!(
                                    "{current}/{} topology agents registered; no stale entries",
                                    topology.agents.len()
                                ),
                            );
                        } else {
                            warn(
                                "agent registry",
                                &format!(
                                    "{current}/{} topology agents registered; {missing} missing; {unhealthy} stale/down; {} stale/malformed entr{}",
                                    topology.agents.len(),
                                    stale.len(),
                                    if stale.len() == 1 { "y" } else { "ies" }
                                ),
                            );
                            if !stale.is_empty() {
                                println!(
                                    "      run `agora registry prune --apply` after stopping stale publishers"
                                );
                            }
                            warnings += 1;
                        }
                    }
                    Err(e) => {
                        warn("agent registry", &format!("could not read registry: {e}"));
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

#[derive(Debug, Clone)]
struct RegistryPruneCandidate {
    key: String,
    agent_name: Option<String>,
    reason: String,
}

async fn prune_registry(
    config_path: &std::path::Path,
    bus_url: Option<&str>,
    apply: bool,
) -> Result<()> {
    let topology = TopologyConfig::load(config_path)?;
    let bus_url = bus_url.unwrap_or(&topology.bus_url);
    let bus = Bus::connect(bus_url).await?;
    let records = bus.read_agent_registry_records().await?;
    let candidates = registry_prune_candidates(&records, &topology);
    let current = current_registry_count(&records, &topology);

    println!(
        "Registry at {bus_url}: {} entr{}",
        records.len(),
        if records.len() == 1 { "y" } else { "ies" }
    );
    println!(
        "Topology {}: {current}/{} current agents registered",
        topology.name,
        topology.agents.len()
    );

    if candidates.is_empty() {
        println!("No stale or malformed registry entries found.");
        return Ok(());
    }

    println!();
    println!(
        "{} prune candidate{}:",
        candidates.len(),
        if candidates.len() == 1 { "" } else { "s" }
    );
    for candidate in &candidates {
        match &candidate.agent_name {
            Some(agent) => println!(
                "  {:<36} agent={:<30} {}",
                candidate.key, agent, candidate.reason
            ),
            None => println!("  {:<36} {}", candidate.key, candidate.reason),
        }
    }

    if !apply {
        println!();
        println!("Dry run only. Re-run with `--apply` to purge these registry keys.");
        return Ok(());
    }

    println!();
    for candidate in candidates {
        bus.purge_agent_registry_key(&candidate.key).await?;
        println!("purged {}", candidate.key);
    }

    let records = bus.read_agent_registry_records().await?;
    let remaining = registry_prune_candidates(&records, &topology);
    if remaining.is_empty() {
        println!("Registry is clean.");
    } else {
        println!();
        println!(
            "{} prune candidate{} remain after purge.",
            remaining.len(),
            if remaining.len() == 1 { "" } else { "s" }
        );
        println!("If they reappear immediately, stop the process that is republishing them and rerun this command.");
    }

    Ok(())
}

fn current_registry_count(records: &[AgentRegistryRecord], topology: &TopologyConfig) -> usize {
    let expected = topology_agent_names(topology);
    records
        .iter()
        .filter(|record| {
            record.manifest.as_ref().is_some_and(|manifest| {
                expected.contains(manifest.agent_name.as_str())
                    && record.key.as_str() == manifest.agent_name.as_str()
            })
        })
        .count()
}

fn unhealthy_registry_count(records: &[AgentRegistryRecord], topology: &TopologyConfig) -> usize {
    let expected = topology_agent_names(topology);
    records
        .iter()
        .filter_map(|record| {
            record.manifest.as_ref().filter(|manifest| {
                expected.contains(manifest.agent_name.as_str())
                    && record.key.as_str() == manifest.agent_name.as_str()
            })
        })
        .filter(|manifest| {
            matches!(
                manifest.observed_status(),
                AgentStatus::Stale | AgentStatus::Down
            )
        })
        .count()
}

fn registry_prune_candidates(
    records: &[AgentRegistryRecord],
    topology: &TopologyConfig,
) -> Vec<RegistryPruneCandidate> {
    let expected = topology_agent_names(topology);
    let mut candidates = Vec::new();

    for record in records {
        match &record.manifest {
            Some(manifest)
                if expected.contains(manifest.agent_name.as_str())
                    && record.key.as_str() == manifest.agent_name.as_str() =>
            {
                continue;
            }
            Some(manifest) => {
                let reason = if expected.contains(manifest.agent_name.as_str()) {
                    format!(
                        "key does not match manifest agent `{}`",
                        manifest.agent_name
                    )
                } else {
                    format!("agent is not in topology `{}`", topology.name)
                };
                candidates.push(RegistryPruneCandidate {
                    key: record.key.clone(),
                    agent_name: Some(manifest.agent_name.clone()),
                    reason,
                });
            }
            None => {
                let reason = record
                    .error
                    .as_deref()
                    .map(|e| format!("malformed registry entry: {e}"))
                    .unwrap_or_else(|| "empty registry entry".to_string());
                candidates.push(RegistryPruneCandidate {
                    key: record.key.clone(),
                    agent_name: None,
                    reason,
                });
            }
        }
    }

    candidates.sort_by(|a, b| a.key.cmp(&b.key));
    candidates
}

fn topology_agent_names(topology: &TopologyConfig) -> BTreeSet<&str> {
    topology
        .agents
        .iter()
        .map(|agent| agent.name.as_str())
        .collect()
}

async fn run_swarm(config_path: std::path::PathBuf) -> Result<()> {
    let cfg = TopologyConfig::load(&config_path)?;
    info!(name = %cfg.name, agents = cfg.agents.len(), "starting swarm");

    std::fs::create_dir_all(&cfg.log_dir)?;
    std::fs::create_dir_all(&cfg.pid_dir)?;
    ensure_signing_key(&default_key_path())?;
    let supervisor_pid = std::path::Path::new(&cfg.pid_dir).join("agora.pid");
    let _pid_guard = PidFileGuard::write(supervisor_pid)?;

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

    #[cfg(unix)]
    {
        let sd = shutdown.clone();
        tokio::spawn(async move {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut sigterm) => {
                    sigterm.recv().await;
                    info!("SIGTERM received — shutting down");
                    sd.cancel();
                }
                Err(e) => warn!("Failed to install SIGTERM handler: {e}"),
            }
        });
    }

    info!("Swarm running. Press Ctrl-C to stop.");
    sup.wait_for_shutdown().await;
    Ok(())
}

fn start_detached(config_path: std::path::PathBuf) -> Result<()> {
    let cfg = TopologyConfig::load(&config_path)?;
    std::fs::create_dir_all(&cfg.log_dir)?;
    std::fs::create_dir_all(&cfg.pid_dir)?;
    ensure_signing_key(&default_key_path())?;

    let supervisor = process_for_target(&cfg, "agora")?;
    let status = status_for(supervisor.clone());
    if status.state == ProcessState::Running {
        let pid = status
            .pid
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        bail!(
            "agora supervisor is already running as pid {pid}; stop it with `agora stop --config {}`",
            config_path.display()
        );
    }

    let log_path = Path::new(&cfg.log_dir).join("agora.log");
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("open {}", log_path.display()))?;
    let stderr = log
        .try_clone()
        .with_context(|| format!("clone {}", log_path.display()))?;

    let exe = std::env::current_exe().context("resolve current agora executable")?;
    let mut command = Command::new(exe);
    command
        .arg("start")
        .arg("--config")
        .arg(&config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr));

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }

    let mut child = command
        .spawn()
        .with_context(|| format!("start detached supervisor for {}", config_path.display()))?;
    let child_pid = child.id();
    let deadline = Instant::now() + Duration::from_secs(5);

    while Instant::now() < deadline {
        if let Some(exit) = child.try_wait()? {
            bail!(
                "detached supervisor exited early with {exit}; see {}",
                log_path.display()
            );
        }

        let started = status_for(supervisor.clone());
        if started.state == ProcessState::Running && started.pid == Some(child_pid) {
            println!(
                "Started agora supervisor detached for {} (pid {child_pid})",
                config_path.display()
            );
            println!("Logs: {}", log_path.display());
            println!("Stop: agora stop --config {}", config_path.display());
            return Ok(());
        }

        std::thread::sleep(Duration::from_millis(100));
    }

    bail!(
        "detached supervisor did not write {} within 5s; see {}",
        supervisor.pid_file.display(),
        log_path.display()
    )
}

struct PidFileGuard {
    path: std::path::PathBuf,
}

impl PidFileGuard {
    fn write(path: std::path::PathBuf) -> Result<Self> {
        write_pid(&path, std::process::id())?;
        Ok(Self { path })
    }
}

impl Drop for PidFileGuard {
    fn drop(&mut self) {
        std::fs::remove_file(&self.path).ok();
    }
}

async fn submit_event(
    topic: &str,
    data: &str,
    text_field: &str,
    session_id: Option<&str>,
    bus_url: &str,
    key_path: &str,
    catalog: &agora_core::TopicCatalog,
) -> Result<()> {
    let topic = topic.trim();
    if topic.is_empty() {
        bail!("event topic cannot be empty");
    }

    // Topology-aware validation. Empty catalog → skip (legacy fallback).
    if !catalog.known.is_empty() && !catalog.knows(topic) {
        let hint = match catalog.matching(topic).first() {
            Some(near) => format!(" Did you mean `{near}`?"),
            None => String::new(),
        };
        bail!("Unknown topic `{topic}` — not declared in topology.{hint} Use --no-topology to override.");
    }

    let data = event_data_from_input(data, text_field)?;
    let signing_key = load_signing_key(key_path)?;
    let session_id = session_id
        .map(String::from)
        .unwrap_or_else(|| format!("sess_{}", ulid::Ulid::new()));

    // Derive scopes from the catalog, falling back to legacy read/write
    // when subscribers haven't declared any (or topology was skipped).
    let catalog_scopes: Vec<&str> = catalog
        .scopes_for(topic)
        .iter()
        .map(String::as_str)
        .collect();
    let fallback = ["workspace:read", "workspace:write"];
    let scopes: &[&str] = if catalog_scopes.is_empty() {
        &fallback
    } else {
        &catalog_scopes
    };
    let token = mint_actor_token("cli-user", scopes, &session_id, &signing_key, 900)?;

    let bus = Bus::connect(bus_url).await?;
    let envelope = Envelope::build(
        topic,
        "agora-cli",
        0u16,
        token,
        session_id.clone(),
        data,
        None,
        vec![],
    );

    bus.publish(&envelope).await?;
    println!("Submitted event {topic} in session {session_id}");
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

async fn delete_session(session_id: &str, bus_url: &str, key_path: &str) -> Result<()> {
    publish_session_deleted(session_id, bus_url, key_path).await?;
    println!("Deleted session {session_id} (history retained)");
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

async fn publish_session_deleted(session_id: &str, bus_url: &str, key_path: &str) -> Result<()> {
    let signing_key = load_signing_key(key_path)?;
    let token = mint_actor_token(
        "cli-user",
        &["workspace:read", "workspace:write"],
        session_id,
        &signing_key,
        900,
    )?;
    let bus = Bus::connect(bus_url).await?;
    let env = Envelope::build(
        SESSION_DELETED,
        "agora-cli",
        0,
        token,
        session_id,
        serde_json::json!({ "sessionId": session_id, "deletedBy": "agora-cli" }),
        None,
        vec![],
    );
    bus.publish(&env).await
}

async fn tag_session(session_id: &str, label: &str, bus_url: &str, key_path: &str) -> Result<()> {
    let tag = validate_tag(label)?;
    let signing_key = load_signing_key(key_path)?;
    let token = mint_actor_token(
        "cli-user",
        &["workspace:write"],
        session_id,
        &signing_key,
        900,
    )?;
    let bus = Bus::connect(bus_url).await?;
    let env = Envelope::build(
        SESSION_TAGGED,
        "agora-cli",
        0,
        token,
        session_id,
        serde_json::json!({ "tag": tag, "actor": "agora-cli" }),
        None,
        vec![],
    );
    bus.publish(&env).await?;
    println!("Tagged {session_id}: {tag}");
    Ok(())
}

async fn untag_session(session_id: &str, label: &str, bus_url: &str, key_path: &str) -> Result<()> {
    let tag = validate_tag(label)?;
    let signing_key = load_signing_key(key_path)?;
    let token = mint_actor_token(
        "cli-user",
        &["workspace:write"],
        session_id,
        &signing_key,
        900,
    )?;
    let bus = Bus::connect(bus_url).await?;
    let env = Envelope::build(
        SESSION_UNTAGGED,
        "agora-cli",
        0,
        token,
        session_id,
        serde_json::json!({ "tag": tag, "actor": "agora-cli" }),
        None,
        vec![],
    );
    bus.publish(&env).await?;
    println!("Untagged {session_id}: {tag}");
    Ok(())
}

async fn list_sessions(bus_url: &str, include_deleted: bool, tag: Option<&str>) -> Result<()> {
    let bus = Bus::connect(bus_url).await?;
    let events = bus.read_all_events().await?;
    let sessions = build_sessions(&events);

    let mut rows: Vec<_> = sessions
        .values()
        .filter(|session| include_deleted || !session.deleted)
        .filter(|session| tag.map(|t| session.tags.contains(t)).unwrap_or(true))
        .collect();

    if rows.is_empty() {
        println!("No sessions found.");
        return Ok(());
    }

    rows.sort_by(|a, b| {
        b.started_at
            .cmp(&a.started_at)
            .then_with(|| a.session_id.cmp(&b.session_id))
    });

    println!(
        "{:<32} {:<24} {:>6}  {:<9} {:<22} {:<20} LAST TOPIC",
        "SESSION", "NAME", "EVENTS", "STATE", "STARTED", "TAGS"
    );
    for session in rows {
        let tags_str = if session.tags.is_empty() {
            "-".to_string()
        } else {
            session.tags.iter().cloned().collect::<Vec<_>>().join(",")
        };
        println!(
            "{:<32} {:<24} {:>6}  {:<9} {:<22} {:<20} {}",
            session.session_id,
            session.name.as_deref().unwrap_or("-"),
            session.event_count,
            if session.deleted { "deleted" } else { "active" },
            session.started_at,
            tags_str,
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
            format!("{:?}", agent.observed_status()).to_lowercase(),
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

async fn replay(session_id: &Option<String>, bus_url: &str, json: bool) -> Result<()> {
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

    if !json {
        println!("Found {} events", filtered.len());
    }
    for e in filtered {
        print_event(e, json)?;
    }
    Ok(())
}

async fn show_events(args: &EventArgs) -> Result<()> {
    let bus = Bus::connect(&args.bus_url).await?;
    let events = bus.read_all_events().await?;

    for event in events.iter().filter(|event| event_matches(event, args)) {
        print_event(event, args.json)?;
    }

    if args.follow {
        follow_events(&bus, args.clone()).await?;
    }

    Ok(())
}

async fn follow_events(bus: &Bus, args: EventArgs) -> Result<()> {
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

    while let Some(event) = rx.recv().await {
        if event_matches(&event, &args) {
            print_event(&event, args.json)?;
        }
    }

    Ok(())
}

fn event_matches(event: &Envelope, args: &EventArgs) -> bool {
    if args
        .session_id
        .as_deref()
        .is_some_and(|session_id| event.context.session_id != session_id)
    {
        return false;
    }
    if args
        .agent
        .as_deref()
        .is_some_and(|agent| event.sender.agent_name != agent)
    {
        return false;
    }
    if let Some(topic) = args.topic.as_deref() {
        if topic.contains('*') || topic.contains('>') {
            return topic_matches(topic, &event.topic);
        }
        return event.topic == topic;
    }
    true
}

fn print_event(event: &Envelope, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(event)?);
        return Ok(());
    }

    println!(
        "[{}] {} | {} | {} | session={}",
        event.timestamp,
        event.event_id,
        event.topic,
        event.sender.agent_name,
        event.context.session_id
    );
    if !event.data.is_null() {
        println!("  data: {}", serde_json::to_string(&event.data)?);
    }
    Ok(())
}

async fn inspect_target(
    target: &str,
    config_path: &Path,
    bus_url: Option<&str>,
    json: bool,
) -> Result<()> {
    let cfg = TopologyConfig::load(config_path)?;
    let bus_url = bus_url.unwrap_or(&cfg.bus_url);

    if matches!(target, "runtime" | "system" | "topology") {
        return show_info(config_path, Some(bus_url), json).await;
    }

    if cfg.agents.iter().any(|agent| agent.name == target) {
        return inspect_agent(target, &cfg, bus_url, json).await;
    }

    if process_for_target(&cfg, target).is_ok() {
        return inspect_process(target, &cfg, json);
    }

    if target.starts_with("sess_") {
        return inspect_session(target, bus_url, json).await;
    }

    if let Ok(bus) = Bus::connect(bus_url).await {
        let sessions = load_sessions(&bus).await?;
        if let Some((session_id, _)) = sessions
            .iter()
            .find(|(_, session)| session.name.as_deref() == Some(target))
        {
            return inspect_session(session_id, bus_url, json).await;
        }
    }

    bail!("unknown inspect target `{target}`")
}

async fn inspect_agent(
    agent_name: &str,
    cfg: &TopologyConfig,
    bus_url: &str,
    json: bool,
) -> Result<()> {
    let spec = cfg.agents.iter().find(|agent| agent.name == agent_name);
    let process = process_for_target(cfg, agent_name)
        .ok()
        .map(status_for)
        .map(|status| process_status_json(&status));
    let log_file = log_path_for_target(cfg, agent_name).map(|path| path.display().to_string());

    let (manifest, bus_error) = match Bus::connect(bus_url).await {
        Ok(bus) => {
            let manifests = bus.read_agent_registry().await?;
            (
                manifests
                    .into_iter()
                    .find(|manifest| manifest.agent_name == agent_name),
                None,
            )
        }
        Err(e) => (None, Some(e.to_string())),
    };

    if json {
        let value = serde_json::json!({
            "kind": "agent",
            "name": agent_name,
            "topology": spec,
            "manifest": manifest,
            "process": process,
            "logFile": log_file,
            "busUrl": bus_url,
            "busError": bus_error,
        });
        print_json(&value)?;
        return Ok(());
    }

    print_agent_manifest(agent_name, manifest.as_ref());
    if let Some(process) = process {
        println!();
        print_process_json_human(&process);
    }
    if let Some(log_file) = log_file {
        println!("  log:           {log_file}");
    }
    if let Some(error) = bus_error {
        println!("  bus:           unavailable ({error})");
    }
    Ok(())
}

fn inspect_process(target: &str, cfg: &TopologyConfig, json: bool) -> Result<()> {
    let status = status_for(process_for_target(cfg, target)?);
    let value = process_status_json(&status);
    if json {
        print_json(&value)
    } else {
        print_process_json_human(&value);
        Ok(())
    }
}

async fn inspect_session(session_id: &str, bus_url: &str, json: bool) -> Result<()> {
    let bus = Bus::connect(bus_url).await?;
    let events = bus.read_all_events().await?;
    let sessions = build_sessions(&events);
    let Some(summary) = sessions.get(session_id) else {
        bail!("session `{session_id}` not found");
    };
    let session_events: Vec<_> = events
        .iter()
        .filter(|event| event.context.session_id == session_id)
        .collect();

    if json {
        let value = serde_json::json!({
            "kind": "session",
            "summary": summary,
            "events": session_events,
        });
        print_json(&value)?;
        return Ok(());
    }

    print_session_summary(summary);
    println!();
    println!("Recent events:");
    for event in session_events.iter().rev().take(10).rev() {
        println!(
            "  {}  {:<32} {:<22} {}",
            time_only(&event.timestamp),
            event.topic,
            event.sender.agent_name,
            short(&event.event_id, 18)
        );
    }
    Ok(())
}

async fn show_session_history(session_id: &str, bus_url: &str, json: bool) -> Result<()> {
    let bus = Bus::connect(bus_url).await?;
    let events = bus.read_all_events().await?;
    let session_events: Vec<_> = events
        .iter()
        .filter(|event| event.context.session_id == session_id)
        .collect();

    if json {
        print_json(&serde_json::json!({
            "kind": "sessionHistory",
            "sessionId": session_id,
            "events": session_events,
        }))?;
        return Ok(());
    }

    if session_events.is_empty() {
        println!("No events found for session {session_id}.");
        return Ok(());
    }

    println!("Session {session_id}");
    println!("{:<10} {:<34} {:<24} EVENT", "TIME", "TOPIC", "SENDER");
    for event in session_events {
        println!(
            "{:<10} {:<34} {:<24} {}",
            time_only(&event.timestamp),
            event.topic,
            event.sender.agent_name,
            short(&event.event_id, 18)
        );
    }
    Ok(())
}

async fn show_info(config_path: &Path, bus_url: Option<&str>, json: bool) -> Result<()> {
    let cfg = TopologyConfig::load(config_path)?;
    let bus_url = bus_url.unwrap_or(&cfg.bus_url);
    let statuses = runtime_statuses(&cfg);
    let running = statuses
        .iter()
        .filter(|status| status.state == ProcessState::Running)
        .count();
    let stale_pid = statuses
        .iter()
        .filter(|status| status.state == ProcessState::StalePid)
        .count();

    let mut bus_state = serde_json::json!({
        "url": bus_url,
        "reachable": false,
    });
    let mut registry_state = serde_json::json!(null);
    let mut stream_state = serde_json::json!(null);
    let mut session_count = None;

    if let Ok(bus) = Bus::connect(bus_url).await {
        bus_state["reachable"] = serde_json::json!(true);
        if let Ok(stream) = bus.js.get_stream(EVENT_STREAM).await {
            let info = stream.cached_info();
            stream_state = serde_json::json!({
                "name": info.config.name,
                "subjects": info.config.subjects,
                "messages": info.state.messages,
                "bytes": info.state.bytes,
            });
        }
        if let Ok(records) = bus.read_agent_registry_records().await {
            registry_state = serde_json::json!({
                "entries": records.len(),
                "current": current_registry_count(&records, &cfg),
                "unhealthy": unhealthy_registry_count(&records, &cfg),
                "pruneCandidates": registry_prune_candidates(&records, &cfg).len(),
            });
        }
        if let Ok(sessions) = load_sessions(&bus).await {
            session_count = Some(sessions.len());
        }
    }

    let value = serde_json::json!({
        "name": "agora",
        "version": env!("CARGO_PKG_VERSION"),
        "topology": {
            "name": &cfg.name,
            "config": config_path.display().to_string(),
            "busUrl": &cfg.bus_url,
            "pidDir": &cfg.pid_dir,
            "logDir": &cfg.log_dir,
            "agents": cfg.agents.len(),
            "defaultAcp": &cfg.default_acp,
        },
        "bus": bus_state,
        "stream": stream_state,
        "registry": registry_state,
        "sessions": session_count,
        "processes": {
            "total": statuses.len(),
            "running": running,
            "stalePid": stale_pid,
            "stopped": statuses.len().saturating_sub(running + stale_pid),
        },
    });

    if json {
        print_json(&value)?;
        return Ok(());
    }

    println!("agora {}", env!("CARGO_PKG_VERSION"));
    println!("topology:  {} ({})", cfg.name, config_path.display());
    println!("bus:       {bus_url}");
    println!("pid dir:   {}", cfg.pid_dir);
    println!("log dir:   {}", cfg.log_dir);
    println!(
        "agents:    {} configured, default ACP {}",
        cfg.agents.len(),
        cfg.default_acp
    );
    println!(
        "processes: {running}/{} running, {stale_pid} stale pid",
        statuses.len()
    );
    println!(
        "nats:      {}",
        if value["bus"]["reachable"].as_bool().unwrap_or(false) {
            "reachable"
        } else {
            "unreachable"
        }
    );
    if let Some(messages) = value["stream"]["messages"].as_u64() {
        println!("stream:    {EVENT_STREAM} ({messages} messages)");
    }
    if let Some(entries) = value["registry"]["entries"].as_u64() {
        println!(
            "registry:  {entries} entries, {} current, {} unhealthy, {} prune candidates",
            value["registry"]["current"].as_u64().unwrap_or(0),
            value["registry"]["unhealthy"].as_u64().unwrap_or(0),
            value["registry"]["pruneCandidates"].as_u64().unwrap_or(0)
        );
    }
    if let Some(sessions) = session_count {
        println!("sessions:  {sessions}");
    }
    Ok(())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct VersionInfo {
    name: &'static str,
    version: &'static str,
}

fn print_version(json: bool) -> Result<()> {
    let info = VersionInfo {
        name: "agora",
        version: env!("CARGO_PKG_VERSION"),
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&info)?);
    } else {
        println!("agora {}", env!("CARGO_PKG_VERSION"));
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
                deleted: false,
                deleted_at: None,
                tags: BTreeSet::new(),
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
        if event.topic == SESSION_DELETED {
            entry.deleted = true;
            entry.deleted_at = Some(event.timestamp.clone());
            continue;
        }
        if event.topic == SESSION_TAGGED {
            if let Some(t) = event.data.get("tag").and_then(|v| v.as_str()) {
                if entry.tags.len() < agora_core::tags::MAX_TAGS_PER_SESSION {
                    entry.tags.insert(t.to_string());
                }
            }
            continue;
        }
        if event.topic == SESSION_UNTAGGED {
            if let Some(t) = event.data.get("tag").and_then(|v| v.as_str()) {
                entry.tags.remove(t);
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
    let observed = manifest.observed_status();
    if observed != manifest.status {
        println!("  observed:      {:?}", observed);
    }
    println!("  port:          {}", manifest.port);
    println!("  endpoint:      {}", manifest.endpoint);
    println!("  last seen:     {}", manifest.last_seen);
    println!("  capabilities:  {}", manifest.capabilities.join(", "));
    println!("  subscribes:    {}", manifest.subscribes_to.join(", "));
    println!("  publishes:     {}", manifest.publishes.join(", "));
}

fn process_status_json(status: &RuntimeProcessStatus) -> serde_json::Value {
    let metrics = status.pid.and_then(process_metrics);
    serde_json::json!({
        "kind": status.process.kind,
        "name": &status.process.name,
        "state": status.state.as_str(),
        "pid": status.pid,
        "age": status.elapsed.as_deref(),
        "command": status.command.as_deref(),
        "pidFile": status.process.pid_file.display().to_string(),
        "logFile": status.process.log_file.as_ref().map(|path| path.display().to_string()),
        "metrics": {
            "cpuPercent": metrics.as_ref().and_then(|metric| metric.cpu_percent.clone()),
            "memPercent": metrics.as_ref().and_then(|metric| metric.mem_percent.clone()),
            "rssKb": metrics.as_ref().and_then(|metric| metric.rss_kb),
        }
    })
}

fn print_process_json_human(value: &serde_json::Value) {
    println!("{}", value["name"].as_str().unwrap_or("process"));
    println!("  kind:          {}", value["kind"].as_str().unwrap_or("-"));
    println!(
        "  state:         {}",
        value["state"].as_str().unwrap_or("-")
    );
    println!(
        "  pid:           {}",
        value["pid"]
            .as_u64()
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!("  age:           {}", value["age"].as_str().unwrap_or("-"));
    println!(
        "  cpu/mem:       {}/{}",
        value["metrics"]["cpuPercent"].as_str().unwrap_or("-"),
        value["metrics"]["memPercent"].as_str().unwrap_or("-")
    );
    println!(
        "  pid file:      {}",
        value["pidFile"].as_str().unwrap_or("-")
    );
    println!(
        "  log file:      {}",
        value["logFile"].as_str().unwrap_or("-")
    );
    println!(
        "  command:       {}",
        value["command"].as_str().unwrap_or("-")
    );
}

fn print_session_summary(summary: &SessionSummary) {
    println!("{}", summary.session_id);
    println!(
        "  name:          {}",
        summary.name.as_deref().unwrap_or("-")
    );
    println!("  started:       {}", summary.started_at);
    println!(
        "  state:         {}",
        if summary.deleted { "deleted" } else { "active" }
    );
    if let Some(deleted_at) = &summary.deleted_at {
        println!("  deleted:       {deleted_at}");
    }
    println!("  events:        {}", summary.event_count);
    println!(
        "  last topic:    {}",
        if summary.last_topic.is_empty() {
            "-"
        } else {
            &summary.last_topic
        }
    );
}

fn print_json(value: &serde_json::Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use agora_core::topics::WORKSPACE_EVENT_SUBMITTED;

    fn event(topic: &str, session_id: &str, data: serde_json::Value) -> Envelope {
        Envelope::build(topic, "test", 0, "tok", session_id, data, None, vec![])
    }

    #[test]
    fn session_deleted_marks_summary_without_erasing_history() {
        let events = vec![
            event(
                SESSION_NAMED,
                "sess_delete",
                serde_json::json!({ "name": "Delete me" }),
            ),
            event(
                WORKSPACE_EVENT_SUBMITTED,
                "sess_delete",
                serde_json::json!({ "text": "keep history" }),
            ),
            event(
                SESSION_DELETED,
                "sess_delete",
                serde_json::json!({ "sessionId": "sess_delete" }),
            ),
        ];

        let sessions = build_sessions(&events);
        let summary = sessions.get("sess_delete").expect("session summary");
        assert_eq!(summary.name.as_deref(), Some("Delete me"));
        assert_eq!(summary.event_count, 1);
        assert_eq!(summary.last_topic, WORKSPACE_EVENT_SUBMITTED);
        assert!(summary.deleted);
        assert!(summary.deleted_at.is_some());
    }
}
