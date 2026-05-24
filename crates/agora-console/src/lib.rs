//! Terminal UI for the agora swarm.
//!
//! Connects to the NATS bus, subscribes to all application topics + the
//! agent registry, and renders a multi-pane view (sessions / events /
//! agents) with a composer at the bottom. Type an idea + Enter to publish
//! `workspace.idea.submitted`, or `@agent <msg>` to send a direct message.

mod app;
mod ui;

use agora_core::{
    bus::Bus,
    envelope::Envelope,
    manifest::AgentManifest,
    tokens::load_signing_key,
    topics::{AGENT_REGISTRY_HEARTBEAT, AGENT_TELEMETRY_LOGS},
};
use anyhow::{Context, Result};
use app::{App, AppEvent, InputMode, Panel, PendingAction, TelemetryEntry};
use clap::Args;
use crossterm::{
    event::{
        DisableMouseCapture, EnableMouseCapture, Event as CtEvent, EventStream, KeyCode, KeyEvent,
        KeyEventKind, KeyModifiers, MouseEvent, MouseEventKind,
    },
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures::StreamExt;
use ratatui::{backend::CrosstermBackend, Terminal};
use std::{
    io::{self, Write as _},
    process::Stdio,
    sync::Arc,
};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Args)]
pub struct ConsoleArgs {
    #[arg(long, default_value = "nats://127.0.0.1:4222")]
    bus_url: String,
    #[arg(long, default_value = ".kiro/session_token")]
    key_path: String,
    /// Skip the JetStream replay on startup (start with an empty event list)
    #[arg(long)]
    no_replay: bool,
}

pub async fn run(args: ConsoleArgs) -> Result<()> {
    let bus = Arc::new(
        Bus::connect(&args.bus_url)
            .await
            .with_context(|| format!("cannot connect to {}", args.bus_url))?,
    );
    let signing_key = load_signing_key(&args.key_path)?;

    let mut app = App::new(bus.clone(), signing_key, args.bus_url.clone());

    let (tx, mut rx) = mpsc::channel::<AppEvent>(256);
    spawn_subscribers(bus.clone(), tx.clone());

    match bus.read_agent_registry().await {
        Ok(manifests) => {
            for manifest in manifests {
                app.handle_app_event(AppEvent::Heartbeat(manifest));
            }
        }
        Err(e) => {
            app.status_msg = Some(format!("Agent registry load failed: {e}"));
        }
    }

    if !args.no_replay {
        match bus.read_all_events().await {
            Ok(events) => {
                for env in events {
                    app.handle_envelope(env);
                }
            }
            Err(e) => {
                app.status_msg = Some(format!("Replay failed: {e}"));
            }
        }
    }

    // Terminal setup — from here on, panics MUST restore the terminal.
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen, DisableMouseCapture);
        original_hook(info);
    }));

    let mut term_events = EventStream::new();
    let result = run_loop(&mut terminal, &mut app, &mut term_events, &mut rx).await;

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    result
}

async fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
    term_events: &mut EventStream,
    bus_rx: &mut mpsc::Receiver<AppEvent>,
) -> Result<()> {
    loop {
        terminal.draw(|f| ui::render(f, app))?;

        tokio::select! {
            maybe_term = term_events.next() => {
                match maybe_term {
                    Some(Ok(CtEvent::Key(key)))
                        if key.kind == KeyEventKind::Press && handle_key(key, app).await? =>
                    {
                        return Ok(());
                    }
                    Some(Ok(CtEvent::Key(_))) => {}
                    Some(Ok(CtEvent::Mouse(m))) => handle_mouse(m, app),
                    Some(Ok(CtEvent::Resize(_, _))) => { /* redraw on next loop */ }
                    Some(Err(_)) | None => return Ok(()),
                    _ => {}
                }
            }
            maybe_app = bus_rx.recv() => {
                match maybe_app {
                    Some(event) => app.handle_app_event(event),
                    None => return Ok(()),
                }
            }
        }

        while let Some(action) = app.take_pending_action() {
            match action {
                PendingAction::Exit => return Ok(()),
                PendingAction::OpenEditor { initial } => {
                    match run_in_external_program(terminal, |_| run_editor(&initial)) {
                        Ok(new_text) => {
                            app.replace_input(new_text);
                            app.status_msg = Some(
                                "Editor closed — Enter to submit, edit, or !editor again".into(),
                            );
                        }
                        Err(e) => app.status_msg = Some(format!("Editor failed: {e}")),
                    }
                }
                PendingAction::OpenPager { content } => {
                    if let Err(e) = run_in_external_program(terminal, |_| run_pager(&content)) {
                        app.status_msg = Some(format!("Pager failed: {e}"));
                    }
                }
            }
        }
    }
}

/// Suspend the TUI, run `f`, restore the TUI. Used for `$EDITOR` and `$PAGER`.
fn run_in_external_program<T, F>(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    f: F,
) -> Result<T>
where
    F: FnOnce(&mut Terminal<CrosstermBackend<io::Stdout>>) -> Result<T>,
{
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;

    let result = f(terminal);

    enable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        EnterAlternateScreen,
        EnableMouseCapture
    )?;
    terminal.clear()?;

    result
}

fn run_editor(initial: &str) -> Result<String> {
    let editor = std::env::var("EDITOR")
        .or_else(|_| std::env::var("VISUAL"))
        .unwrap_or_else(|_| "vi".into());
    let tmp = tempfile::Builder::new()
        .prefix("agora-")
        .suffix(".txt")
        .tempfile()?;
    std::fs::write(tmp.path(), initial)?;
    let status = std::process::Command::new(&editor)
        .arg(tmp.path())
        .status()
        .with_context(|| format!("failed to spawn {editor}"))?;
    if !status.success() {
        anyhow::bail!("editor exited with {status}");
    }
    let content = std::fs::read_to_string(tmp.path())?;
    Ok(content.trim_end_matches('\n').to_string())
}

fn run_pager(content: &str) -> Result<()> {
    let pager = std::env::var("PAGER").unwrap_or_else(|_| "less".into());
    let mut child = std::process::Command::new(&pager)
        .stdin(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to spawn {pager}"))?;
    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(content.as_bytes())?;
    }
    child.wait()?;
    Ok(())
}

async fn handle_key(key: KeyEvent, app: &mut App) -> Result<bool> {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);

    // Modal modes have their own key handling. Esc cancels; Enter confirms.
    if app.mode != InputMode::Normal {
        match key.code {
            KeyCode::Esc => app.cancel_modal(),
            KeyCode::Enter => app.confirm_modal().await?,
            KeyCode::Backspace => {
                app.pop_input_char();
            }
            KeyCode::Char('c') if ctrl => return Ok(true),
            KeyCode::Char(c) if !ctrl => app.push_input_char(c),
            _ => {}
        }
        return Ok(false);
    }

    // Normal mode
    match key.code {
        KeyCode::Esc => {
            // Cascade: dismiss command output panel → otherwise quit
            if app.dismiss_command_output() {
                return Ok(false);
            }
            return Ok(true);
        }
        KeyCode::Char('c') if ctrl => return Ok(true),
        KeyCode::Char('q') if app.input.is_empty() => return Ok(true),
        KeyCode::Char('n') if ctrl => app.enter_naming_new(),
        KeyCode::Char('r') if ctrl => app.enter_renaming(),
        KeyCode::Char('x') if ctrl => app.clear_active(),
        KeyCode::F(1) => app.toggle_panel(Panel::Sessions),
        KeyCode::F(2) => app.toggle_panel(Panel::Events),
        KeyCode::F(3) => app.toggle_panel(Panel::Agents),
        KeyCode::F(4) => app.toggle_panel(Panel::Detail),
        KeyCode::Tab => {
            if let Some(completion) = app::autocomplete_agent(&app.input, &app.agents) {
                app.replace_input(completion);
            } else {
                app.cycle_session(true);
            }
        }
        KeyCode::BackTab => app.cycle_session(false),
        // Shift+Enter, Alt+Enter, and Ctrl-J insert a newline.
        // (Terminals that don't distinguish Shift+Enter from Enter need Alt+Enter.)
        KeyCode::Enter
            if key.modifiers.contains(KeyModifiers::SHIFT)
                || key.modifiers.contains(KeyModifiers::ALT) =>
        {
            app.push_input_newline();
        }
        KeyCode::Char('j') if ctrl => app.push_input_newline(),
        KeyCode::Enter => {
            let text = std::mem::take(&mut app.input);
            app.reset_input_scroll();
            app.submit(text).await?;
        }
        KeyCode::Backspace => {
            app.pop_input_char();
        }
        KeyCode::PageUp => {
            if app.command_output.is_some() {
                app.scroll_output(-5);
            } else if app.input_overflows() || app.input_scroll > 0 {
                app.scroll_input(-5);
            } else {
                for _ in 0..5 {
                    app.scroll_up();
                }
            }
        }
        KeyCode::PageDown => {
            if app.command_output.is_some() {
                app.scroll_output(5);
            } else if app.input_overflows() || app.input_scroll > 0 {
                app.scroll_input(5);
            } else {
                for _ in 0..5 {
                    app.scroll_down();
                }
            }
        }
        KeyCode::Up => app.scroll_up(),
        KeyCode::Down => app.scroll_down(),
        KeyCode::End => {
            if app.input_scroll > 0 {
                app.reset_input_scroll();
            } else {
                app.end_scroll();
            }
        }
        KeyCode::Char(c) if !ctrl => app.push_input_char(c),
        _ => {}
    }
    Ok(false)
}

fn handle_mouse(m: MouseEvent, app: &mut App) {
    match m.kind {
        MouseEventKind::ScrollUp => {
            if app.mouse_over_input(m.row) && (app.input_overflows() || app.input_scroll > 0) {
                app.scroll_input(-2);
            } else if app.command_output.is_some() {
                app.scroll_output(-2);
            } else {
                app.scroll_up();
                app.scroll_up();
            }
        }
        MouseEventKind::ScrollDown => {
            if app.mouse_over_input(m.row) && (app.input_overflows() || app.input_scroll > 0) {
                app.scroll_input(2);
            } else if app.command_output.is_some() {
                app.scroll_output(2);
            } else {
                app.scroll_down();
                app.scroll_down();
            }
        }
        _ => {}
    }
}

fn spawn_subscribers(bus: Arc<Bus>, tx: mpsc::Sender<AppEvent>) {
    let app_subjects = [
        "workspace.>",
        "code.>",
        "test.>",
        "security.>",
        "human.>",
        "agent.inbox.>",
        "event.>",
        "session.>",
    ];

    for subj in app_subjects {
        let client = bus.client.clone();
        let tx = tx.clone();
        let s = subj.to_string();
        tokio::spawn(async move {
            let mut sub = match client.subscribe(s.clone()).await {
                Ok(s) => s,
                Err(e) => {
                    let _ = tx
                        .send(AppEvent::Disconnect(format!("subscribe {s}: {e}")))
                        .await;
                    return;
                }
            };
            while let Some(msg) = sub.next().await {
                if let Ok(env) = Envelope::from_bytes(&msg.payload) {
                    if tx.send(AppEvent::Envelope(env)).await.is_err() {
                        break;
                    }
                }
            }
        });
    }

    let client = bus.client.clone();
    let tx_hb = tx.clone();
    tokio::spawn(async move {
        let mut sub = match client.subscribe(AGENT_REGISTRY_HEARTBEAT.to_string()).await {
            Ok(s) => s,
            Err(_) => return,
        };
        while let Some(msg) = sub.next().await {
            if let Ok(m) = serde_json::from_slice::<AgentManifest>(&msg.payload) {
                if tx_hb.send(AppEvent::Heartbeat(m)).await.is_err() {
                    break;
                }
            }
        }
    });

    let client = bus.client.clone();
    tokio::spawn(async move {
        let mut sub = match client.subscribe(AGENT_TELEMETRY_LOGS.to_string()).await {
            Ok(s) => s,
            Err(_) => return,
        };
        while let Some(msg) = sub.next().await {
            if let Ok(entry) = serde_json::from_slice::<TelemetryEntry>(&msg.payload) {
                if tx.send(AppEvent::Telemetry(entry)).await.is_err() {
                    break;
                }
            }
        }
    });
}
