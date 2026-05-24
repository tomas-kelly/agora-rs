use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use std::{
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, Instant},
};

use crate::config::TopologyConfig;

#[derive(Debug, Clone)]
pub struct RuntimeProcess {
    pub name: String,
    pub kind: &'static str,
    pub pid_file: PathBuf,
    pub log_file: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct RuntimeProcessStatus {
    pub process: RuntimeProcess,
    pub pid: Option<u32>,
    pub state: ProcessState,
    pub elapsed: Option<String>,
    pub command: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessState {
    Running,
    StalePid,
    NotStarted,
}

impl ProcessState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::StalePid => "stale-pid",
            Self::NotStarted => "stopped",
        }
    }
}

#[derive(Debug, Clone)]
pub struct LegacyProcess {
    pub pid: u32,
    pub command: String,
}

#[derive(Debug, Clone)]
pub struct LogOptions {
    pub tail: usize,
    pub follow: bool,
    pub since: Option<String>,
    pub timestamps: bool,
}

#[derive(Debug, Clone)]
pub struct ProcessMetrics {
    pub cpu_percent: Option<String>,
    pub mem_percent: Option<String>,
    pub rss_kb: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ProcessTreeRow {
    pub pid: u32,
    pub ppid: u32,
    pub stat: String,
    pub elapsed: String,
    pub command: String,
}

pub fn expected_processes(cfg: &TopologyConfig) -> Vec<RuntimeProcess> {
    let pid_dir = Path::new(&cfg.pid_dir);
    let log_dir = Path::new(&cfg.log_dir);
    let mut processes = Vec::new();

    processes.push(RuntimeProcess {
        name: "agora".to_string(),
        kind: "supervisor",
        pid_file: pid_dir.join("agora.pid"),
        log_file: None,
    });
    processes.push(RuntimeProcess {
        name: "nats".to_string(),
        kind: "service",
        pid_file: pid_dir.join("nats.pid"),
        log_file: Some(PathBuf::from(&cfg.nats.log_file)),
    });

    if cfg.telemetry.enabled {
        processes.push(RuntimeProcess {
            name: "daemon-telemetry".to_string(),
            kind: "service",
            pid_file: pid_dir.join("daemon-telemetry.pid"),
            log_file: Some(log_dir.join("daemon-telemetry.log")),
        });
    }

    for agent in &cfg.agents {
        processes.push(RuntimeProcess {
            name: agent.name.clone(),
            kind: "agent",
            pid_file: pid_dir.join(format!("{}.pid", agent.name)),
            log_file: Some(log_dir.join(format!("{}.log", agent.name))),
        });
    }

    processes
}

pub fn runtime_statuses(cfg: &TopologyConfig) -> Vec<RuntimeProcessStatus> {
    expected_processes(cfg)
        .into_iter()
        .map(status_for)
        .collect()
}

pub fn status_for(process: RuntimeProcess) -> RuntimeProcessStatus {
    let pid = read_pid_file(&process.pid_file).ok();
    let Some(pid) = pid else {
        return RuntimeProcessStatus {
            process,
            pid: None,
            state: ProcessState::NotStarted,
            elapsed: None,
            command: None,
        };
    };

    if !pid_alive(pid) {
        return RuntimeProcessStatus {
            process,
            pid: Some(pid),
            state: ProcessState::StalePid,
            elapsed: None,
            command: None,
        };
    }

    let (elapsed, command) = ps_details(pid);
    RuntimeProcessStatus {
        process,
        pid: Some(pid),
        state: ProcessState::Running,
        elapsed,
        command,
    }
}

pub fn print_process_table(cfg: &TopologyConfig) {
    println!(
        "{:<22} {:<12} {:<10} {:>8} {:<12} {:<32} COMMAND",
        "NAME", "KIND", "STATE", "PID", "AGE", "LOG"
    );
    for status in expected_processes(cfg).into_iter().map(status_for) {
        let log = status
            .process
            .log_file
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "-".to_string());
        println!(
            "{:<22} {:<12} {:<10} {:>8} {:<12} {:<32} {}",
            status.process.name,
            status.process.kind,
            status.state.as_str(),
            status
                .pid
                .map(|pid| pid.to_string())
                .unwrap_or_else(|| "-".to_string()),
            status.elapsed.unwrap_or_else(|| "-".to_string()),
            log,
            status.command.unwrap_or_else(|| "-".to_string())
        );
    }

    let legacy = legacy_swarm_processes();
    if !legacy.is_empty() {
        println!();
        println!("Legacy Python swarm publishers:");
        for process in legacy {
            println!("  {:>8}  {}", process.pid, process.command);
        }
    }
}

pub fn print_log_targets(cfg: &TopologyConfig) {
    println!("{:<22} LOG", "TARGET");
    for process in expected_processes(cfg) {
        if let Some(path) = process.log_file {
            println!("{:<22} {}", process.name, path.display());
        }
    }
    println!("{:<22} {}", "telemetry-jsonl", cfg.telemetry.log_file);
}

pub fn print_logs(cfg: &TopologyConfig, target: Option<&str>, options: &LogOptions) -> Result<()> {
    let Some(target) = target else {
        print_log_targets(cfg);
        return Ok(());
    };

    let path = log_path_for_target(cfg, target)
        .with_context(|| format!("unknown log target `{target}`"))?;
    let since = options
        .since
        .as_deref()
        .map(parse_since)
        .transpose()
        .with_context(|| {
            format!(
                "invalid --since value `{}`; use RFC3339 or a duration like 10m, 2h, 1d",
                options.since.as_deref().unwrap_or_default()
            )
        })?;
    let bytes = std::fs::read(&path).with_context(|| format!("cannot read {}", path.display()))?;
    let text = String::from_utf8_lossy(&bytes);
    let mut matching = Vec::new();
    for line in text.lines() {
        if line_matches_since(line, since) {
            matching.push(line);
        }
    }
    let start = matching.len().saturating_sub(options.tail);

    println!("==> {} <==", path.display());
    for line in &matching[start..] {
        print_log_line(line, options.timestamps);
    }

    if options.follow {
        follow_log_file(&path, since, options.timestamps)?;
    }
    Ok(())
}

pub fn print_process_stats(cfg: &TopologyConfig, target: Option<&str>) -> Result<()> {
    if let Some(target) = target {
        process_for_target(cfg, target)?;
    }

    println!(
        "{:<22} {:<12} {:<10} {:>8} {:>7} {:>7} {:>10} COMMAND",
        "NAME", "KIND", "STATE", "PID", "CPU%", "MEM%", "RSS"
    );

    for status in runtime_statuses(cfg) {
        if target.is_some_and(|target| status.process.name != target) {
            continue;
        }
        let metrics = status.pid.and_then(process_metrics);
        println!(
            "{:<22} {:<12} {:<10} {:>8} {:>7} {:>7} {:>10} {}",
            status.process.name,
            status.process.kind,
            status.state.as_str(),
            status
                .pid
                .map(|pid| pid.to_string())
                .unwrap_or_else(|| "-".to_string()),
            metrics
                .as_ref()
                .and_then(|m| m.cpu_percent.clone())
                .unwrap_or_else(|| "-".to_string()),
            metrics
                .as_ref()
                .and_then(|m| m.mem_percent.clone())
                .unwrap_or_else(|| "-".to_string()),
            metrics
                .as_ref()
                .and_then(|m| m.rss_kb)
                .map(format_kb)
                .unwrap_or_else(|| "-".to_string()),
            status.command.unwrap_or_else(|| "-".to_string())
        );
    }
    Ok(())
}

pub fn print_process_top(cfg: &TopologyConfig, target: &str) -> Result<()> {
    let status = status_for(process_for_target(cfg, target)?);
    let Some(pid) = status.pid else {
        bail!("{target} is not running");
    };
    if status.state != ProcessState::Running {
        bail!("{target} is not running (state: {})", status.state.as_str());
    }

    let rows = process_tree(pid);
    if rows.is_empty() {
        println!("No process details found for {target} ({pid}).");
        return Ok(());
    }

    println!(
        "{:<8} {:<8} {:<8} {:<12} COMMAND",
        "PID", "PPID", "STAT", "AGE"
    );
    for row in rows {
        println!(
            "{:<8} {:<8} {:<8} {:<12} {}",
            row.pid, row.ppid, row.stat, row.elapsed, row.command
        );
    }
    Ok(())
}

pub fn process_for_target(cfg: &TopologyConfig, target: &str) -> Result<RuntimeProcess> {
    expected_processes(cfg)
        .into_iter()
        .find(|process| process.name == target)
        .with_context(|| format!("unknown process `{target}`"))
}

pub fn process_metrics(pid: u32) -> Option<ProcessMetrics> {
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "%cpu=,%mem=,rss="])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut parts = text.split_whitespace();
    let cpu_percent = parts.next().map(ToString::to_string);
    let mem_percent = parts.next().map(ToString::to_string);
    let rss_kb = parts.next().and_then(|value| value.parse::<u64>().ok());
    Some(ProcessMetrics {
        cpu_percent,
        mem_percent,
        rss_kb,
    })
}

pub fn stop_runtime(cfg: &TopologyConfig, target: Option<&str>, force: bool) -> Result<()> {
    if let Some(target) = target {
        let process = expected_processes(cfg)
            .into_iter()
            .find(|process| process.name == target)
            .with_context(|| format!("unknown process `{target}`"))?;
        stop_process(&process, force, true)?;
        return Ok(());
    }

    let processes = expected_processes(cfg);
    let supervisor = processes
        .iter()
        .find(|process| process.name == "agora")
        .cloned();

    if let Some(supervisor) = supervisor {
        let status = status_for(supervisor.clone());
        if status.state == ProcessState::Running {
            stop_process(&supervisor, force, true)?;
            wait_for_all_stopped(&processes, Duration::from_secs(10));
        }
    }

    for process in processes.iter().rev() {
        if process.name == "agora" {
            continue;
        }
        let status = status_for(process.clone());
        if status.state != ProcessState::NotStarted {
            stop_process(process, force, true)?;
        }
    }

    Ok(())
}

pub fn restart_agent(cfg: &TopologyConfig, agent: &str, force: bool) -> Result<()> {
    if !cfg.agents.iter().any(|spec| spec.name == agent) {
        bail!("unknown agent `{agent}`");
    }

    let processes = expected_processes(cfg);
    let supervisor = processes
        .iter()
        .find(|process| process.name == "agora")
        .cloned()
        .context("missing supervisor process spec")?;
    let supervisor_status = status_for(supervisor);
    if supervisor_status.state != ProcessState::Running {
        bail!("agora supervisor is not running; start it with `agora run agents.local.json`");
    }

    let process = processes
        .into_iter()
        .find(|process| process.name == agent)
        .context("missing agent process spec")?;
    let before = status_for(process.clone());
    let Some(old_pid) = before.pid else {
        bail!("agent `{agent}` has no pid file; wait for `agora run` to start it");
    };
    if before.state != ProcessState::Running {
        bail!(
            "agent `{agent}` is not running (state: {})",
            before.state.as_str()
        );
    }

    stop_pid(old_pid, force, Duration::from_secs(5))?;
    wait_for_pid_change(&process.pid_file, old_pid, Duration::from_secs(15))
        .with_context(|| format!("agent `{agent}` did not restart"))?;
    let after = status_for(process);
    match after.pid {
        Some(pid) if after.state == ProcessState::Running => {
            println!("restarted {agent}: {old_pid} -> {pid}");
            Ok(())
        }
        _ => bail!("agent `{agent}` restart did not produce a running process"),
    }
}

pub fn stop_legacy_swarm_processes(force: bool) -> Result<()> {
    let legacy = legacy_swarm_processes();
    if legacy.is_empty() {
        println!("No legacy Python swarm processes found.");
        return Ok(());
    }

    for process in legacy {
        stop_pid(process.pid, force, Duration::from_secs(5))
            .with_context(|| format!("failed to stop legacy process {}", process.pid))?;
        println!("stopped legacy process {}", process.pid);
    }
    Ok(())
}

pub fn legacy_swarm_processes() -> Vec<LegacyProcess> {
    #[cfg(unix)]
    {
        let output = Command::new("ps").args(["-axo", "pid=,command="]).output();
        let Ok(output) = output else {
            return Vec::new();
        };
        if !output.status.success() {
            return Vec::new();
        }

        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(parse_legacy_process)
            .collect()
    }

    #[cfg(not(unix))]
    {
        Vec::new()
    }
}

pub fn write_pid(path: &Path, pid: u32) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, format!("{pid}\n"))?;
    Ok(())
}

fn stop_process(process: &RuntimeProcess, force: bool, remove_pid: bool) -> Result<()> {
    let status = status_for(process.clone());
    match (status.pid, status.state) {
        (Some(pid), ProcessState::Running) => {
            stop_pid(pid, force, Duration::from_secs(8))
                .with_context(|| format!("failed to stop {}", process.name))?;
            if remove_pid {
                std::fs::remove_file(&process.pid_file).ok();
            }
            println!("stopped {} ({pid})", process.name);
        }
        (Some(pid), ProcessState::StalePid) => {
            if remove_pid {
                std::fs::remove_file(&process.pid_file).ok();
            }
            println!("removed stale pid for {} ({pid})", process.name);
        }
        (None, ProcessState::NotStarted) => {
            println!("{} is not running", process.name);
        }
        _ => {}
    }
    Ok(())
}

fn stop_pid(pid: u32, force: bool, timeout: Duration) -> Result<()> {
    signal_pid(pid, "-TERM")?;
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !pid_alive(pid) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }

    if force {
        signal_pid(pid, "-KILL")?;
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if !pid_alive(pid) {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    if pid_alive(pid) {
        bail!("pid {pid} is still running");
    }
    Ok(())
}

fn wait_for_all_stopped(processes: &[RuntimeProcess], timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if processes
            .iter()
            .all(|process| status_for(process.clone()).state != ProcessState::Running)
        {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn wait_for_pid_change(path: &Path, old_pid: u32, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        match read_pid_file(path) {
            Ok(pid) if pid != old_pid && pid_alive(pid) => return Ok(()),
            _ => thread::sleep(Duration::from_millis(100)),
        }
    }
    bail!("pid file {} did not change from {old_pid}", path.display())
}

pub fn log_path_for_target(cfg: &TopologyConfig, target: &str) -> Option<PathBuf> {
    if target == "nats" {
        return Some(PathBuf::from(&cfg.nats.log_file));
    }
    if target == "daemon-telemetry" {
        return Some(Path::new(&cfg.log_dir).join("daemon-telemetry.log"));
    }
    if target == "telemetry-jsonl" {
        return Some(PathBuf::from(&cfg.telemetry.log_file));
    }
    cfg.agents
        .iter()
        .find(|agent| agent.name == target)
        .map(|agent| Path::new(&cfg.log_dir).join(format!("{}.log", agent.name)))
}

fn follow_log_file(path: &Path, since: Option<DateTime<Utc>>, timestamps: bool) -> Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .open(path)
        .with_context(|| format!("cannot open {}", path.display()))?;
    let mut offset = file.metadata()?.len();

    loop {
        thread::sleep(Duration::from_millis(500));
        let metadata = match file.metadata() {
            Ok(metadata) => metadata,
            Err(e) => {
                eprintln!("log follow failed for {}: {e}", path.display());
                continue;
            }
        };
        let len = metadata.len();
        if len < offset {
            file.seek(SeekFrom::Start(0))?;
            offset = 0;
        }
        if len == offset {
            continue;
        }

        file.seek(SeekFrom::Start(offset))?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        offset = file.stream_position()?;
        for line in buf.lines() {
            if line_matches_since(line, since) {
                print_log_line(line, timestamps);
            }
        }
    }
}

fn parse_since(value: &str) -> Result<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Ok(dt.with_timezone(&Utc));
    }

    let value = value.trim();
    if value.is_empty() {
        bail!("empty duration");
    }

    let split = value
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(value.len());
    let amount: i64 = value[..split].parse()?;
    let unit = &value[split..];
    let duration = match unit {
        "" | "s" | "sec" | "secs" => ChronoDuration::seconds(amount),
        "m" | "min" | "mins" => ChronoDuration::minutes(amount),
        "h" | "hr" | "hrs" => ChronoDuration::hours(amount),
        "d" | "day" | "days" => ChronoDuration::days(amount),
        _ => bail!("unknown duration unit `{unit}`"),
    };
    Ok(Utc::now() - duration)
}

fn line_matches_since(line: &str, since: Option<DateTime<Utc>>) -> bool {
    match since {
        Some(since) => line_timestamp(line).is_some_and(|timestamp| timestamp >= since),
        None => true,
    }
}

fn line_timestamp(line: &str) -> Option<DateTime<Utc>> {
    for token in line.split_whitespace().take(4) {
        let token = token.trim_matches(|ch| matches!(ch, '[' | ']' | '(' | ')' | ','));
        if let Ok(dt) = DateTime::parse_from_rfc3339(token) {
            return Some(dt.with_timezone(&Utc));
        }
    }
    None
}

fn print_log_line(line: &str, timestamps: bool) {
    if timestamps && line_timestamp(line).is_none() {
        println!("{} {line}", Utc::now().format("%Y-%m-%dT%H:%M:%SZ"));
    } else {
        println!("{line}");
    }
}

fn format_kb(kb: u64) -> String {
    if kb >= 1024 * 1024 {
        format!("{:.1}g", kb as f64 / 1024.0 / 1024.0)
    } else if kb >= 1024 {
        format!("{:.1}m", kb as f64 / 1024.0)
    } else {
        format!("{kb}k")
    }
}

fn process_tree(root_pid: u32) -> Vec<ProcessTreeRow> {
    #[cfg(unix)]
    {
        let output = Command::new("ps")
            .args(["-axo", "pid=,ppid=,stat=,etime=,command="])
            .output();
        let Ok(output) = output else {
            return Vec::new();
        };
        if !output.status.success() {
            return Vec::new();
        }

        let rows: Vec<_> = String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(parse_process_tree_row)
            .collect();
        let mut out = Vec::new();
        collect_process_tree(root_pid, &rows, &mut out);
        out
    }

    #[cfg(not(unix))]
    {
        Vec::new()
    }
}

fn collect_process_tree(root_pid: u32, rows: &[ProcessTreeRow], out: &mut Vec<ProcessTreeRow>) {
    if let Some(root) = rows.iter().find(|row| row.pid == root_pid) {
        out.push(root.clone());
    }
    let mut children: Vec<_> = rows
        .iter()
        .filter(|row| row.ppid == root_pid)
        .cloned()
        .collect();
    children.sort_by_key(|row| row.pid);
    for child in children {
        collect_process_tree(child.pid, rows, out);
    }
}

fn parse_process_tree_row(line: &str) -> Option<ProcessTreeRow> {
    let mut parts = line.trim().splitn(5, char::is_whitespace);
    let pid = parts.next()?.trim().parse().ok()?;
    let ppid = parts.next()?.trim().parse().ok()?;
    let stat = parts.next()?.trim().to_string();
    let elapsed = parts.next()?.trim().to_string();
    let command = parts.next()?.trim().to_string();
    Some(ProcessTreeRow {
        pid,
        ppid,
        stat,
        elapsed,
        command,
    })
}

fn read_pid_file(path: &Path) -> Result<u32> {
    let raw = std::fs::read_to_string(path)?;
    raw.trim()
        .parse::<u32>()
        .with_context(|| format!("invalid pid file {}", path.display()))
}

fn pid_alive(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn signal_pid(pid: u32, signal: &str) -> Result<()> {
    let status = Command::new("kill")
        .args([signal, &pid.to_string()])
        .status()
        .with_context(|| format!("failed to invoke kill for pid {pid}"))?;
    if !status.success() {
        bail!("kill {signal} {pid} failed with {status}");
    }
    Ok(())
}

fn ps_details(pid: u32) -> (Option<String>, Option<String>) {
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "etime=,command="])
        .output();
    let Ok(output) = output else {
        return (None, None);
    };
    if !output.status.success() {
        return (None, None);
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let line = text.lines().next().map(str::trim).unwrap_or("");
    if line.is_empty() {
        return (None, None);
    }
    let mut parts = line.splitn(2, char::is_whitespace);
    let elapsed = parts.next().map(str::trim).filter(|s| !s.is_empty());
    let command = parts.next().map(str::trim).filter(|s| !s.is_empty());
    (
        elapsed.map(ToString::to_string),
        command.map(ToString::to_string),
    )
}

fn parse_legacy_process(line: &str) -> Option<LegacyProcess> {
    let trimmed = line.trim();
    let (pid_raw, command) = trimmed.split_once(char::is_whitespace)?;
    if !(command.contains("src/daemons/") || command.contains("swarm_sdk.cli")) {
        return None;
    }
    let pid = pid_raw.parse().ok()?;
    Some(LegacyProcess {
        pid,
        command: command.trim().to_string(),
    })
}
