//! Telemetry log appender — subscribes to `agent.telemetry.logs` and writes
//! newline-delimited JSON to `.agora/logs/telemetry.jsonl`.

use agora_core::bus::Bus;
use anyhow::{Context, Result};
use clap::Parser;
use futures::StreamExt;
use std::{io::Write, path::PathBuf};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "nats://127.0.0.1:4222")]
    bus_url: String,
    #[arg(long, default_value = ".agora/logs/telemetry.jsonl")]
    log_file: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt().with_env_filter("info").init();

    // Ensure log directory exists
    if let Some(parent) = args.log_file.parent() {
        std::fs::create_dir_all(parent).context("Failed to create log directory")?;
    }

    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&args.log_file)
        .with_context(|| format!("Cannot open {}", args.log_file.display()))?;
    let file = std::sync::Arc::new(std::sync::Mutex::new(file));

    info!(path = %args.log_file.display(), "telemetry appender started");

    let bus = Bus::connect(&args.bus_url).await?;
    let shutdown = CancellationToken::new();

    let sd = shutdown.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        sd.cancel();
    });

    // Plain NATS subscribe (not durable) for telemetry — best effort
    let mut sub = bus
        .client
        .subscribe(agora_core::AGENT_TELEMETRY_LOGS.to_string())
        .await
        .context("Failed to subscribe to agent.telemetry.logs")?;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            msg = sub.next() => {
                match msg {
                    Some(msg) => {
                        match serde_json::from_slice::<serde_json::Value>(&msg.payload) {
                            Ok(payload) => {
                                let line = serde_json::to_string(&payload).unwrap_or_default();
                                let mut f = file.lock().unwrap();
                                if let Err(e) = writeln!(f, "{line}") {
                                    error!("Failed to write telemetry: {e}");
                                }
                            }
                            Err(e) => warn!("Could not parse telemetry payload: {e}"),
                        }
                    }
                    None => break,
                }
            }
        }
    }

    info!("telemetry appender stopped");
    Ok(())
}
