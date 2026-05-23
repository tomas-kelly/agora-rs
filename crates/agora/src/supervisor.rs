use anyhow::{Context, Result};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
};
use tokio::process::{Child, Command};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::{NatsConfig, TelemetryConfig, TopologyConfig};
use swarm_core::{bus::Bus, command::split_command_line};

const MAX_RESTARTS: u32 = 3;

#[derive(Clone)]
struct ProcessSpec {
    name: String,
    command: String,
    args: Vec<String>,
    log_file: PathBuf,
    pid_file: PathBuf,
}

struct ManagedChild {
    spec: ProcessSpec,
    child: Child,
    restarts: u32,
}

pub struct Supervisor {
    shutdown: CancellationToken,
    children: HashMap<String, ManagedChild>,
}

impl Supervisor {
    pub fn new(shutdown: CancellationToken) -> Self {
        Self {
            shutdown,
            children: HashMap::new(),
        }
    }

    pub async fn start_nats(
        &mut self,
        cfg: &NatsConfig,
        log_dir: &str,
        pid_dir: &str,
        bus_url: &str,
    ) -> Result<()> {
        std::fs::create_dir_all(log_dir)?;
        if Bus::connect(bus_url).await.is_ok() {
            info!("NATS already available at {bus_url}; using existing server");
            return Ok(());
        }

        let parts = split_command_line(&cfg.command)?;
        let (cmd, args) = parts
            .split_first()
            .context("nats command must contain executable")?;

        info!("Starting NATS: {}", cfg.command);
        self.start_process(ProcessSpec {
            name: "nats".into(),
            command: cmd.clone(),
            args: args.to_vec(),
            log_file: PathBuf::from(&cfg.log_file),
            pid_file: Path::new(pid_dir).join("nats.pid"),
        })?;

        wait_for_nats(bus_url).await?;
        info!("NATS started");
        Ok(())
    }

    pub async fn start_telemetry(
        &mut self,
        cfg: &TelemetryConfig,
        bus_url: &str,
        log_dir: &str,
        pid_dir: &str,
    ) -> Result<()> {
        if !cfg.enabled {
            return Ok(());
        }
        let proc_log = format!("{}/daemon-telemetry.log", log_dir);

        info!("Starting daemon-telemetry");
        self.start_process(binary_spec(
            "daemon-telemetry",
            vec!["--bus-url", bus_url, "--log-file", &cfg.log_file],
            &proc_log,
            pid_dir,
        )?)?;
        Ok(())
    }

    pub async fn start_agent(
        &mut self,
        topology_path: &str,
        agent_name: &str,
        bus_url: &str,
        log_dir: &str,
        pid_dir: &str,
    ) -> Result<()> {
        let proc_log = format!("{}/{}.log", log_dir, agent_name);

        info!("Starting agent: {}", agent_name);
        self.start_process(
            binary_spec(
                "agora-agent",
                vec![
                    "--config",
                    topology_path,
                    "--agent",
                    agent_name,
                    "--bus-url",
                    bus_url,
                ],
                &proc_log,
                pid_dir,
            )?
            .named(agent_name),
        )?;
        Ok(())
    }

    pub async fn start_all(&mut self, topology_path: &str, cfg: &TopologyConfig) -> Result<()> {
        self.start_nats(&cfg.nats, &cfg.log_dir, &cfg.pid_dir, &cfg.bus_url)
            .await?;
        self.start_telemetry(&cfg.telemetry, &cfg.bus_url, &cfg.log_dir, &cfg.pid_dir)
            .await?;
        for agent in &cfg.agents {
            self.start_agent(
                topology_path,
                &agent.name,
                &cfg.bus_url,
                &cfg.log_dir,
                &cfg.pid_dir,
            )
            .await?;
        }
        Ok(())
    }

    pub async fn wait_for_shutdown(&mut self) {
        let mut monitor = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => break,
                _ = monitor.tick() => self.restart_exited_children(),
            }
        }
        info!("Shutting down all processes...");
        self.stop_all().await;
    }

    fn start_process(&mut self, spec: ProcessSpec) -> Result<()> {
        let child = spawn_process(&spec)?;
        let pid = child.id();
        if let Some(pid) = pid {
            write_pid(&spec.pid_file, pid)?;
        }
        info!("{} started (pid: {:?})", spec.name, pid);
        self.children.insert(
            spec.name.clone(),
            ManagedChild {
                spec,
                child,
                restarts: 0,
            },
        );
        Ok(())
    }

    fn restart_exited_children(&mut self) {
        let mut exited = Vec::new();
        for (name, managed) in &mut self.children {
            match managed.child.try_wait() {
                Ok(Some(status)) => {
                    warn!("{name} exited unexpectedly: {status}");
                    exited.push(name.clone());
                }
                Ok(None) => {}
                Err(e) => {
                    error!("{name} status check failed: {e}");
                    exited.push(name.clone());
                }
            }
        }

        for name in exited {
            let Some(mut managed) = self.children.remove(&name) else {
                continue;
            };
            if managed.restarts >= MAX_RESTARTS {
                error!("{name} exceeded restart limit; shutting down swarm");
                self.shutdown.cancel();
                continue;
            }
            managed.restarts += 1;
            match spawn_process(&managed.spec) {
                Ok(child) => {
                    let pid = child.id();
                    if let Some(pid) = pid {
                        if let Err(e) = write_pid(&managed.spec.pid_file, pid) {
                            warn!("Failed to write pid for {name}: {e}");
                        }
                    }
                    info!(
                        "{name} restarted (attempt {}, pid: {:?})",
                        managed.restarts, pid
                    );
                    managed.child = child;
                    self.children.insert(name, managed);
                }
                Err(e) => {
                    error!("Failed to restart {name}: {e}");
                    self.shutdown.cancel();
                }
            }
        }
    }

    async fn stop_all(&mut self) {
        for (name, managed) in &mut self.children {
            info!("Stopping {name}");
            if let Err(e) = managed.child.kill().await {
                warn!("Failed to kill {name}: {e}");
            }
        }
        for (name, managed) in &mut self.children {
            match managed.child.wait().await {
                Ok(status) => info!("{name} exited: {status}"),
                Err(e) => error!("{name} wait error: {e}"),
            }
            std::fs::remove_file(&managed.spec.pid_file).ok();
        }
        self.children.clear();
    }
}

impl ProcessSpec {
    fn named(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self.pid_file = self
            .pid_file
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(format!("{name}.pid"));
        self
    }
}

fn binary_spec(
    bin_name: &str,
    args: Vec<&str>,
    log_file: &str,
    pid_dir: &str,
) -> Result<ProcessSpec> {
    let sibling = std::env::current_exe()
        .ok()
        .map(|path| path.with_file_name(bin_name))
        .filter(|path| path.exists());

    let (command, full_args) = match sibling {
        Some(path) => (
            path.to_string_lossy().to_string(),
            args.into_iter().map(String::from).collect(),
        ),
        None => {
            let mut cargo_args = vec![
                "run".to_string(),
                "-p".to_string(),
                bin_name.to_string(),
                "--".to_string(),
            ];
            cargo_args.extend(args.into_iter().map(String::from));
            ("cargo".to_string(), cargo_args)
        }
    };

    Ok(ProcessSpec {
        name: bin_name.to_string(),
        command,
        args: full_args,
        log_file: PathBuf::from(log_file),
        pid_file: Path::new(pid_dir).join(format!("{bin_name}.pid")),
    })
}

fn spawn_process(spec: &ProcessSpec) -> Result<Child> {
    if let Some(parent) = spec.log_file.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&spec.log_file)?;

    Command::new(&spec.command)
        .args(&spec.args)
        .stdout(Stdio::from(log_file.try_clone()?))
        .stderr(Stdio::from(log_file))
        .spawn()
        .with_context(|| format!("failed to start {}", spec.name))
}

fn write_pid(path: &Path, pid: u32) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, format!("{pid}\n"))?;
    Ok(())
}

async fn wait_for_nats(bus_url: &str) -> Result<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut last_error = None;

    while std::time::Instant::now() < deadline {
        match Bus::connect(bus_url).await {
            Ok(_) => return Ok(()),
            Err(e) => last_error = Some(e),
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    match last_error {
        Some(e) => Err(e).context("NATS did not become ready"),
        None => anyhow::bail!("NATS did not become ready"),
    }
}
