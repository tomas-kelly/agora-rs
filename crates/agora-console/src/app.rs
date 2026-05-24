use agora_core::{
    bus::Bus,
    envelope::Envelope,
    manifest::AgentManifest,
    tokens::mint_actor_token,
    topics::{direct_inbox_topic, SESSION_DELETED, SESSION_NAMED, WORKSPACE_IDEA_SUBMITTED},
};
use anyhow::Result;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

const MAX_EVENTS: usize = 500;
const MAX_TELEMETRY: usize = 2000;

#[derive(Debug, Clone, Deserialize)]
pub struct TelemetryEntry {
    pub timestamp: String,
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub agent: String,
    #[serde(default)]
    pub level: String,
    pub action: String,
    #[serde(default)]
    pub telemetry: serde_json::Value,
}

pub enum AppEvent {
    Envelope(Envelope),
    Heartbeat(AgentManifest),
    Telemetry(TelemetryEntry),
    Disconnect(String),
}

/// Side effects the App wants the main loop to perform after handling
/// input/events — used for actions that require suspending the TUI
/// (editor, pager) or terminating the loop.
pub enum PendingAction {
    Exit,
    OpenEditor { initial: String },
    OpenPager { content: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMode {
    Normal,
    NamingNew,
    Renaming,
}

#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub session_id: String,
    pub started_at: String,
    pub last_topic: String,
    pub event_count: usize,
    pub deleted: bool,
    pub deleted_at: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Panel {
    Sessions,
    Events,
    Agents,
    Detail,
}

impl Panel {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "session" | "sessions" => Some(Self::Sessions),
            "event" | "events" => Some(Self::Events),
            "agent" | "agents" => Some(Self::Agents),
            "detail" | "details" | "latest" => Some(Self::Detail),
            _ => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Sessions => "sessions",
            Self::Events => "events",
            Self::Agents => "agents",
            Self::Detail => "detail",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PanelVisibility {
    pub sessions: bool,
    pub events: bool,
    pub agents: bool,
    pub detail: bool,
}

impl Default for PanelVisibility {
    fn default() -> Self {
        Self {
            sessions: true,
            events: true,
            agents: true,
            detail: true,
        }
    }
}

impl PanelVisibility {
    pub fn is_visible(&self, panel: Panel) -> bool {
        match panel {
            Panel::Sessions => self.sessions,
            Panel::Events => self.events,
            Panel::Agents => self.agents,
            Panel::Detail => self.detail,
        }
    }

    pub fn set(&mut self, panel: Panel, visible: bool) {
        match panel {
            Panel::Sessions => self.sessions = visible,
            Panel::Events => self.events = visible,
            Panel::Agents => self.agents = visible,
            Panel::Detail => self.detail = visible,
        }
    }

    pub fn set_all(&mut self, visible: bool) {
        self.sessions = visible;
        self.events = visible;
        self.agents = visible;
        self.detail = visible;
    }
}

pub struct App {
    pub input: String,
    pub stashed_input: String,
    pub mode: InputMode,
    pub rename_target: Option<String>,

    pub events: Vec<Envelope>,
    pub telemetry: Vec<TelemetryEntry>,
    pub sessions: BTreeMap<String, SessionInfo>,
    pub session_names: HashMap<String, String>,
    pub active_session: Option<String>,
    pub agents: BTreeMap<String, AgentManifest>,

    pub bus_url: String,
    pub bus: Arc<Bus>,
    pub signing_key: Vec<u8>,
    pub status_msg: Option<String>,
    pub command_output: Option<String>,
    pub output_scroll: u16,

    pub input_scroll: u16,
    pub input_view_height: u16,
    pub input_view_width: u16,
    pub input_visual_lines: u16,
    pub input_area_top: u16,
    pub input_area_bottom: u16,

    pub events_scroll: u16,
    /// Last-known height of the events pane (lines visible at once). Updated
    /// by the renderer on every frame so `scroll_up` can cap correctly even
    /// after a terminal resize.
    pub events_view_height: u16,
    pub auto_scroll: bool,
    pub panels: PanelVisibility,

    pub pending_action: Option<PendingAction>,
}

impl App {
    pub fn new(bus: Arc<Bus>, signing_key: Vec<u8>, bus_url: String) -> Self {
        Self {
            input: String::new(),
            stashed_input: String::new(),
            mode: InputMode::Normal,
            rename_target: None,
            events: Vec::new(),
            telemetry: Vec::new(),
            sessions: BTreeMap::new(),
            session_names: HashMap::new(),
            active_session: None,
            agents: BTreeMap::new(),
            bus_url,
            bus,
            signing_key,
            status_msg: Some("Type !help for commands · Enter to submit · Esc to quit".into()),
            command_output: None,
            output_scroll: 0,
            input_scroll: 0,
            input_view_height: 1,
            input_view_width: 1,
            input_visual_lines: 1,
            input_area_top: 0,
            input_area_bottom: 0,
            events_scroll: 0,
            events_view_height: 1,
            auto_scroll: true,
            panels: PanelVisibility::default(),
            pending_action: None,
        }
    }

    /// Maximum scroll offset such that the events pane stays full of content.
    /// Anything beyond this would leave blank space at the top.
    pub fn max_events_scroll(&self) -> u16 {
        let len = self.events.len() as u16;
        len.saturating_sub(self.events_view_height)
    }

    pub fn take_pending_action(&mut self) -> Option<PendingAction> {
        self.pending_action.take()
    }

    pub fn scroll_output(&mut self, delta: i32) {
        if self.command_output.is_none() {
            return;
        }
        if delta < 0 {
            self.output_scroll = self.output_scroll.saturating_sub((-delta) as u16);
        } else {
            self.output_scroll = self.output_scroll.saturating_add(delta as u16);
        }
    }

    pub fn set_input_viewport(&mut self, top: u16, height: u16, width: u16, visual_lines: u16) {
        self.input_area_top = top;
        self.input_area_bottom = top.saturating_add(height);
        self.input_view_height = height.max(1);
        self.input_view_width = width.max(1);
        self.input_visual_lines = visual_lines.max(1);
        self.clamp_input_scroll();
    }

    pub fn input_overflows(&self) -> bool {
        self.input_visual_lines > self.input_view_height
    }

    pub fn mouse_over_input(&self, row: u16) -> bool {
        row >= self.input_area_top && row < self.input_area_bottom
    }

    pub fn scroll_input(&mut self, delta: i32) {
        if delta < 0 {
            self.input_scroll = self.input_scroll.saturating_add((-delta) as u16);
        } else {
            self.input_scroll = self.input_scroll.saturating_sub(delta as u16);
        }
        self.clamp_input_scroll();
    }

    pub fn reset_input_scroll(&mut self) {
        self.input_scroll = 0;
    }

    pub fn push_input_char(&mut self, ch: char) {
        self.input.push(ch);
        self.reset_input_scroll();
    }

    pub fn push_input_newline(&mut self) {
        self.input.push('\n');
        self.reset_input_scroll();
    }

    pub fn pop_input_char(&mut self) {
        self.input.pop();
        self.reset_input_scroll();
    }

    pub fn replace_input(&mut self, input: String) {
        self.input = input;
        self.reset_input_scroll();
    }

    pub fn dismiss_command_output(&mut self) -> bool {
        if self.command_output.is_some() {
            self.command_output = None;
            true
        } else {
            false
        }
    }

    fn clamp_input_scroll(&mut self) {
        let max_scroll = self
            .input_visual_lines
            .saturating_sub(self.input_view_height);
        self.input_scroll = self.input_scroll.min(max_scroll);
    }

    pub fn display_name(&self, session_id: &str) -> String {
        self.session_names
            .get(session_id)
            .cloned()
            .unwrap_or_else(|| short_id(session_id, 18))
    }

    pub fn resolve_session(&self, value: &str) -> Option<String> {
        if self.sessions.contains_key(value) {
            return Some(value.to_string());
        }
        self.session_names
            .iter()
            .find(|(_, name)| name.as_str() == value)
            .map(|(session_id, _)| session_id.clone())
    }

    pub fn handle_app_event(&mut self, ev: AppEvent) {
        match ev {
            AppEvent::Envelope(env) => self.handle_envelope(env),
            AppEvent::Heartbeat(m) => {
                self.agents.insert(m.agent_name.clone(), m);
            }
            AppEvent::Telemetry(entry) => {
                self.telemetry.push(entry);
                if self.telemetry.len() > MAX_TELEMETRY {
                    let excess = self.telemetry.len() - MAX_TELEMETRY;
                    self.telemetry.drain(0..excess);
                }
            }
            AppEvent::Disconnect(reason) => {
                self.status_msg = Some(format!("Disconnected: {reason}"));
            }
        }
    }

    pub fn handle_envelope(&mut self, env: Envelope) {
        // `session.named` is metadata — update the name map and ensure a
        // SessionInfo exists, but don't push it into the events list.
        if env.topic == SESSION_NAMED {
            if let Some(name) = env.data.get("name").and_then(|v| v.as_str()) {
                let sid = env.context.session_id.clone();
                self.session_names.insert(sid.clone(), name.to_string());
                self.sessions
                    .entry(sid.clone())
                    .or_insert_with(|| SessionInfo {
                        session_id: sid,
                        started_at: env.timestamp.clone(),
                        last_topic: String::new(),
                        event_count: 0,
                        deleted: false,
                        deleted_at: None,
                    });
            }
            return;
        }

        if env.topic == SESSION_DELETED {
            let sid = env.context.session_id.clone();
            let session = self
                .sessions
                .entry(sid.clone())
                .or_insert_with(|| SessionInfo {
                    session_id: sid.clone(),
                    started_at: env.timestamp.clone(),
                    last_topic: String::new(),
                    event_count: 0,
                    deleted: false,
                    deleted_at: None,
                });
            session.deleted = true;
            session.deleted_at = Some(env.timestamp.clone());
            if self.active_session.as_deref() == Some(sid.as_str()) {
                self.active_session = None;
            }
            return;
        }

        let sid = env.context.session_id.clone();
        let topic = env.topic.clone();
        let session = self
            .sessions
            .entry(sid.clone())
            .or_insert_with(|| SessionInfo {
                session_id: sid.clone(),
                started_at: env.timestamp.clone(),
                last_topic: topic.clone(),
                event_count: 0,
                deleted: false,
                deleted_at: None,
            });
        session.last_topic = topic;
        session.event_count += 1;

        self.events.push(env);
        if self.events.len() > MAX_EVENTS {
            let excess = self.events.len() - MAX_EVENTS;
            self.events.drain(0..excess);
        }
    }

    // -------------------------------------------- mode transitions

    pub fn enter_naming_new(&mut self) {
        if self.mode != InputMode::Normal {
            return;
        }
        self.stashed_input = std::mem::take(&mut self.input);
        self.reset_input_scroll();
        self.mode = InputMode::NamingNew;
    }

    pub fn enter_renaming(&mut self) {
        if self.mode != InputMode::Normal {
            return;
        }
        let Some(target) = self.active_session.clone() else {
            self.status_msg =
                Some("No active session to rename. Press Ctrl-N to create one.".into());
            return;
        };
        self.stashed_input = std::mem::take(&mut self.input);
        // Preload with existing name (if any) so user can edit
        self.replace_input(self.session_names.get(&target).cloned().unwrap_or_default());
        self.rename_target = Some(target);
        self.mode = InputMode::Renaming;
    }

    pub fn cancel_modal(&mut self) {
        if self.mode == InputMode::Normal {
            return;
        }
        let restored = std::mem::take(&mut self.stashed_input);
        self.replace_input(restored);
        self.mode = InputMode::Normal;
        self.rename_target = None;
    }

    pub async fn confirm_modal(&mut self) -> Result<()> {
        match self.mode {
            InputMode::NamingNew => {
                let name = std::mem::take(&mut self.input).trim().to_string();
                if name.is_empty() {
                    self.cancel_modal();
                    return Ok(());
                }
                self.create_session(&name).await;
                let restored = std::mem::take(&mut self.stashed_input);
                self.replace_input(restored);
                self.mode = InputMode::Normal;
            }
            InputMode::Renaming => {
                let new_name = std::mem::take(&mut self.input).trim().to_string();
                if let Some(sid) = self.rename_target.take() {
                    self.rename_session(&sid, &new_name).await;
                }
                let restored = std::mem::take(&mut self.stashed_input);
                self.replace_input(restored);
                self.mode = InputMode::Normal;
            }
            InputMode::Normal => {}
        }
        Ok(())
    }

    async fn create_session(&mut self, name: &str) {
        let session_id = format!("sess_{}", ulid::Ulid::new());
        let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        self.sessions.insert(
            session_id.clone(),
            SessionInfo {
                session_id: session_id.clone(),
                started_at: now,
                last_topic: String::new(),
                event_count: 0,
                deleted: false,
                deleted_at: None,
            },
        );
        self.session_names
            .insert(session_id.clone(), name.to_string());
        self.active_session = Some(session_id.clone());

        if let Err(e) = self.publish_session_named(&session_id, name).await {
            self.status_msg = Some(format!("Created locally; broadcast failed: {e}"));
        } else {
            self.status_msg = Some(format!("Created session: {name}"));
        }
    }

    async fn rename_session(&mut self, sid: &str, name: &str) {
        if name.is_empty() {
            self.session_names.remove(sid);
        } else {
            self.session_names.insert(sid.to_string(), name.to_string());
        }
        if let Err(e) = self.publish_session_named(sid, name).await {
            self.status_msg = Some(format!("Renamed locally; broadcast failed: {e}"));
        } else {
            self.status_msg = Some(format!(
                "Renamed: {}",
                if name.is_empty() { "(cleared)" } else { name }
            ));
        }
    }

    async fn delete_session(&mut self, sid: &str) {
        let label = self.display_name(sid);
        if let Some(session) = self.sessions.get_mut(sid) {
            session.deleted = true;
            session.deleted_at = Some(chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string());
        }
        if self.active_session.as_deref() == Some(sid) {
            self.active_session = None;
        }
        if let Err(e) = self.publish_session_deleted(sid).await {
            self.status_msg = Some(format!("Deleted locally; broadcast failed: {e}"));
        } else {
            self.status_msg = Some(format!("Deleted session {label} (history retained)"));
        }
    }

    // -------------------------------------------- session switching

    pub fn cycle_session(&mut self, forward: bool) {
        let mut ids: Vec<String> = self
            .sessions
            .iter()
            .filter(|(_, session)| !session.deleted)
            .map(|(id, _)| id.clone())
            .collect();
        ids.sort_by(|a, b| {
            self.sessions[a]
                .started_at
                .cmp(&self.sessions[b].started_at)
        });
        if ids.is_empty() {
            self.active_session = None;
            return;
        }

        let next = match &self.active_session {
            Some(current) => match ids.iter().position(|s| s == current) {
                Some(p) => {
                    let n = ids.len();
                    let np = if forward {
                        (p + 1) % n
                    } else {
                        (p + n - 1) % n
                    };
                    ids[np].clone()
                }
                None => ids[0].clone(),
            },
            None => {
                if forward {
                    ids[0].clone()
                } else {
                    ids[ids.len() - 1].clone()
                }
            }
        };
        self.active_session = Some(next.clone());
        self.status_msg = Some(format!("Active: {}", self.display_name(&next)));
    }

    pub fn clear_active(&mut self) {
        if self.active_session.take().is_some() {
            self.status_msg = Some("Cleared active session".into());
        }
    }

    pub fn scroll_up(&mut self) {
        let max = self.max_events_scroll();
        if self.events_scroll >= max {
            // Already showing the oldest event at the top — nothing further
            // back to scroll to. Keep auto_scroll false (user is reviewing
            // history) but don't push the view off the top.
            self.events_scroll = max;
            self.auto_scroll = max == 0;
            return;
        }
        self.auto_scroll = false;
        self.events_scroll = (self.events_scroll + 1).min(max);
    }

    pub fn scroll_down(&mut self) {
        self.events_scroll = self.events_scroll.saturating_sub(1);
        if self.events_scroll == 0 {
            self.auto_scroll = true;
        }
    }

    pub fn end_scroll(&mut self) {
        self.events_scroll = 0;
        self.auto_scroll = true;
    }

    pub fn toggle_panel(&mut self, panel: Panel) {
        let visible = !self.panels.is_visible(panel);
        self.panels.set(panel, visible);
        self.status_msg = Some(format!(
            "{} panel {}",
            panel.label(),
            if visible { "shown" } else { "hidden" }
        ));
    }

    // -------------------------------------------- publishing

    pub async fn submit(&mut self, text: String) -> Result<()> {
        let text = text.trim().to_string();
        if text.is_empty() {
            return Ok(());
        }

        if let Some(rest) = text.strip_prefix('!') {
            return self.run_command(rest).await;
        }
        if let Some(rest) = text.strip_prefix('@') {
            return self.submit_direct(rest, "steering").await;
        }
        if let Some(rest) = text.strip_prefix("/steer ") {
            return self
                .submit_direct(rest.trim_start_matches('@'), "steering")
                .await;
        }
        if let Some(rest) = text.strip_prefix("/queue ") {
            return self
                .submit_direct(rest.trim_start_matches('@'), "queue")
                .await;
        }

        self.submit_idea(text).await
    }

    // -------------------------------------------- commands

    async fn run_command(&mut self, line: &str) -> Result<()> {
        let line = line.trim();
        let (cmd, rest) = match line.split_once(char::is_whitespace) {
            Some((c, r)) => (c, r.trim()),
            None => (line, ""),
        };

        match cmd {
            "help" | "h" | "?" => self.cmd_help(),
            "new" => self.cmd_new(rest).await,
            "rename" => self.cmd_rename(rest).await,
            "delete" | "del" => self.cmd_delete(rest).await,
            "agents" => self.cmd_agents(),
            "status" => self.cmd_status(rest),
            "history" => self.cmd_history(rest),
            "panel" | "panels" | "toggle" => self.cmd_panel(rest),
            "clear" => {
                self.command_output = None;
                self.output_scroll = 0;
            }
            "reset" => self.cmd_reset(),
            "copy" => self.cmd_copy(),
            "editor" => self.cmd_editor(),
            "page" => self.cmd_page(),
            "exit" | "quit" => {
                self.pending_action = Some(PendingAction::Exit);
            }
            "" => self.status_msg = Some("Empty command. Try !help".into()),
            other => self.status_msg = Some(format!("Unknown command: !{other} (try !help)")),
        }
        Ok(())
    }

    fn cmd_reset(&mut self) {
        let n_events = self.events.len();
        let n_tel = self.telemetry.len();
        self.events.clear();
        self.telemetry.clear();
        self.sessions.clear();
        // Keep session_names + agents — they're persistent metadata.
        self.events_scroll = 0;
        self.auto_scroll = true;
        self.command_output = None;
        self.output_scroll = 0;
        self.status_msg = Some(format!(
            "Reset view ({n_events} events, {n_tel} telemetry cleared)"
        ));
    }

    fn cmd_copy(&mut self) {
        let text = self
            .command_output
            .clone()
            .or_else(|| self.events.last().map(format_event_for_clipboard));
        let Some(text) = text else {
            self.status_msg = Some("Nothing to copy.".into());
            return;
        };
        match arboard::Clipboard::new().and_then(|mut c| c.set_text(text)) {
            Ok(()) => self.status_msg = Some("Copied to clipboard".into()),
            Err(e) => self.status_msg = Some(format!("Clipboard failed: {e}")),
        }
    }

    fn cmd_editor(&mut self) {
        let initial = std::mem::take(&mut self.input);
        self.reset_input_scroll();
        self.pending_action = Some(PendingAction::OpenEditor { initial });
    }

    fn cmd_page(&mut self) {
        let Some(content) = self.command_output.clone() else {
            self.status_msg = Some("Nothing to page. Run !help or !history <agent> first.".into());
            return;
        };
        self.pending_action = Some(PendingAction::OpenPager { content });
    }

    fn set_output(&mut self, text: String) {
        self.panels.detail = true;
        self.command_output = Some(text);
        self.output_scroll = 0;
    }

    fn cmd_help(&mut self) {
        let text = "COMMANDS (type, Enter):\n\
             \x20\x20!help                  This panel\n\
             \x20\x20!new [name]            Create new session (prompts if name omitted)\n\
             \x20\x20!rename [name]         Rename active session\n\
             \x20\x20!delete [session]      Hide a session from lists; history is retained\n\
             \x20\x20!agents                Agents seen in active session\n\
             \x20\x20!status <agent>        Agent's manifest + activity in active session\n\
             \x20\x20!history <agent>       Full conversation: received · prompts · responses · published\n\
             \x20\x20!panel <name>          Toggle sessions, events, agents, detail, or all\n\
             \x20\x20!copy                  Copy command output (or latest event) to clipboard\n\
             \x20\x20!editor                Compose input in $EDITOR (for long ideas / multi-line)\n\
             \x20\x20!page                  Open command output in $PAGER (for long !history)\n\
             \x20\x20!reset                 Clear local events/telemetry/sessions view\n\
             \x20\x20!clear                 Dismiss this panel\n\
             \x20\x20!exit | !quit          Quit (same as Esc)\n\
             \n\
             DIRECT MESSAGES:\n\
             \x20\x20@agent <msg>           Steering message  (Tab completes the agent name)\n\
             \x20\x20/steer @agent <msg>    Same as @\n\
             \x20\x20/queue @agent <msg>    Queue behind agent's current event\n\
             \n\
             Plain text is published as workspace.idea.submitted.\n\
             \n\
             KEYS:  Ctrl-N/R/X · F1/F2/F3/F4 panels · Tab/Shift-Tab · ↑/↓/End for events\n\
             \x20\x20\x20\x20\x20\x20Shift+Enter or Alt+Enter inserts a newline\n\
             \x20\x20\x20\x20\x20\x20PgUp/PgDn scroll long drafts or this panel · wheel over the composer scrolls it · Esc to dismiss"
            .to_string();
        self.set_output(text);
    }

    fn cmd_panel(&mut self, rest: &str) {
        let arg = rest.trim().to_ascii_lowercase();
        match arg.as_str() {
            "" => {
                self.status_msg =
                    Some("Panel toggles: !panel sessions|events|agents|detail|all".into());
            }
            "all" | "show" | "show-all" => {
                self.panels.set_all(true);
                self.status_msg = Some("All panels shown".into());
            }
            "none" | "hide-all" => {
                self.panels.set_all(false);
                self.status_msg = Some("Context panels hidden; composer remains available".into());
            }
            other => {
                if let Some(panel) = Panel::parse(other) {
                    self.toggle_panel(panel);
                } else {
                    self.status_msg = Some(format!(
                        "Unknown panel `{other}`. Use sessions, events, agents, detail, or all."
                    ));
                }
            }
        }
    }

    async fn cmd_new(&mut self, name: &str) {
        if name.is_empty() {
            self.enter_naming_new();
            return;
        }
        self.create_session(name).await;
    }

    async fn cmd_rename(&mut self, name: &str) {
        let Some(sid) = self.active_session.clone() else {
            self.status_msg = Some("No active session. Use !new <name> first.".into());
            return;
        };
        if name.is_empty() {
            self.enter_renaming();
            return;
        }
        self.rename_session(&sid, name).await;
    }

    async fn cmd_delete(&mut self, session: &str) {
        let sid = if session.is_empty() {
            match self.active_session.clone() {
                Some(sid) => sid,
                None => {
                    self.status_msg =
                        Some("No active session. Use !delete <session-id-or-name>.".into());
                    return;
                }
            }
        } else {
            match self.resolve_session(session) {
                Some(sid) => sid,
                None => {
                    self.status_msg = Some(format!("Session not found: {session}"));
                    return;
                }
            }
        };
        self.delete_session(&sid).await;
    }

    fn cmd_agents(&mut self) {
        let Some(sid) = self.active_session.clone() else {
            self.status_msg = Some("No active session.".into());
            return;
        };

        let mut counts: BTreeMap<String, usize> = BTreeMap::new();
        for e in &self.events {
            if e.context.session_id == sid {
                *counts.entry(e.sender.agent_name.clone()).or_insert(0) += 1;
            }
        }

        let label = self.display_name(&sid);
        let mut out = format!("Agents in session \"{label}\":\n\n");
        if counts.is_empty() {
            out.push_str("  (no events yet — submit an idea to begin)\n");
        } else {
            for (name, count) in &counts {
                let status = self
                    .agents
                    .get(name)
                    .map(|a| format!("{:?}", a.observed_status()).to_lowercase())
                    .unwrap_or_else(|| "—".into());
                out.push_str(&format!(
                    "  {name:30}  {status:8}  {count} event{}\n",
                    if *count == 1 { "" } else { "s" }
                ));
            }
        }
        self.set_output(out);
    }

    fn cmd_status(&mut self, agent: &str) {
        if agent.is_empty() {
            self.status_msg = Some("Usage: !status <agent-name>".into());
            return;
        }
        let Some(sid) = self.active_session.clone() else {
            self.status_msg = Some("No active session.".into());
            return;
        };

        let manifest = self.agents.get(agent);
        let session_events: Vec<&Envelope> = self
            .events
            .iter()
            .filter(|e| e.context.session_id == sid && e.sender.agent_name == agent)
            .collect();

        let label = self.display_name(&sid);
        let mut out = format!("{agent}\n");
        match manifest {
            Some(m) => {
                out.push_str(&format!("  status:        {:?}\n", m.status));
                let observed = m.observed_status();
                if observed != m.status {
                    out.push_str(&format!("  observed:      {:?}\n", observed));
                }
                out.push_str(&format!("  port:          {}\n", m.port));
                out.push_str(&format!("  last seen:     {}\n", m.last_seen));
                out.push_str(&format!("  capabilities:  {}\n", m.capabilities.join(", ")));
            }
            None => {
                out.push_str("  (no heartbeat received from this agent yet)\n");
            }
        }
        out.push_str(&format!("\n  In session \"{label}\":\n"));
        if session_events.is_empty() {
            out.push_str("    (no events from this agent in this session)\n");
        } else {
            for e in session_events {
                let t = if e.timestamp.len() >= 19 {
                    &e.timestamp[11..19]
                } else {
                    &e.timestamp
                };
                out.push_str(&format!("    {t}  {}\n", e.topic));
            }
        }
        self.set_output(out);
    }

    fn cmd_history(&mut self, agent: &str) {
        if agent.is_empty() {
            self.status_msg = Some("Usage: !history <agent-name>".into());
            return;
        }
        let Some(sid) = self.active_session.clone() else {
            self.status_msg = Some("No active session.".into());
            return;
        };

        // Topics this agent subscribes to — used to flag inbound events
        let subscribed: Vec<String> = self
            .agents
            .get(agent)
            .map(|m| m.subscribes_to.clone())
            .unwrap_or_default();

        #[derive(Clone)]
        enum Item {
            Received(Envelope),
            Prompt(String, Option<String>),   // text, trigger_event_id
            Response(String, Option<String>), // text, trigger_event_id
            Published(Envelope),
            Other(TelemetryEntry),
        }

        let mut items: Vec<(String, Item)> = Vec::new();

        for e in &self.events {
            if e.context.session_id != sid {
                continue;
            }
            if e.sender.agent_name == agent {
                items.push((e.timestamp.clone(), Item::Published(e.clone())));
            } else if subscribed.iter().any(|pat| topic_matches(pat, &e.topic)) {
                items.push((e.timestamp.clone(), Item::Received(e.clone())));
            }
        }

        for t in &self.telemetry {
            if t.session_id != sid || t.agent != agent {
                continue;
            }
            let trigger = t
                .telemetry
                .get("triggerEventId")
                .and_then(|v| v.as_str())
                .map(String::from);
            match t.action.as_str() {
                "prompt_sent" | "prompt" => {
                    let text = t
                        .telemetry
                        .get("prompt")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    items.push((t.timestamp.clone(), Item::Prompt(text, trigger)));
                }
                "response_received" | "response" => {
                    let text = t
                        .telemetry
                        .get("response")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    items.push((t.timestamp.clone(), Item::Response(text, trigger)));
                }
                _ => items.push((t.timestamp.clone(), Item::Other(t.clone()))),
            }
        }

        items.sort_by(|a, b| a.0.cmp(&b.0));

        let label = self.display_name(&sid);
        let mut out = format!("History: {agent} in \"{label}\"\n");
        out.push_str(&format!(
            "  {} item(s) · PgUp/PgDn or mouse wheel to scroll\n\n",
            items.len()
        ));

        if items.is_empty() {
            out.push_str("  (no activity yet — start an idea, or wait for the agent to react)\n");
        }

        for (_, item) in items {
            match item {
                Item::Received(e) => {
                    let t = time_only(&e.timestamp);
                    out.push_str(&format!(
                        "{t}  ← received  {}  [{}]\n",
                        e.topic,
                        short(&e.event_id, 18)
                    ));
                    push_indented(
                        &mut out,
                        &serde_json::to_string_pretty(&e.data).unwrap_or_default(),
                        "         ",
                    );
                    out.push('\n');
                }
                Item::Prompt(text, _trigger) => {
                    out.push_str("·····  prompt to ACP  ·····\n");
                    push_indented(&mut out, &text, "  ");
                    out.push('\n');
                }
                Item::Response(text, _trigger) => {
                    out.push_str("·····  response from ACP  ·····\n");
                    push_indented(&mut out, &text, "  ");
                    out.push('\n');
                }
                Item::Published(e) => {
                    let t = time_only(&e.timestamp);
                    out.push_str(&format!(
                        "{t}  → published {}  [{}]\n",
                        e.topic,
                        short(&e.event_id, 18)
                    ));
                    push_indented(
                        &mut out,
                        &serde_json::to_string_pretty(&e.data).unwrap_or_default(),
                        "         ",
                    );
                    out.push('\n');
                }
                Item::Other(t) => {
                    let ts = time_only(&t.timestamp);
                    out.push_str(&format!("{ts}  · {}  {}\n", t.level, t.action));
                    push_indented(&mut out, &t.telemetry.to_string(), "         ");
                    out.push('\n');
                }
            }
        }

        self.set_output(out);
    }

    async fn submit_idea(&mut self, idea: String) -> Result<()> {
        let session_id = self
            .active_session
            .clone()
            .unwrap_or_else(|| format!("sess_{}", ulid::Ulid::new()));
        let token = mint_actor_token(
            "console-user",
            &["workspace:read", "workspace:write"],
            &session_id,
            &self.signing_key,
            900,
        )?;
        let env = Envelope::build(
            WORKSPACE_IDEA_SUBMITTED,
            "agora-console",
            0,
            token,
            session_id.clone(),
            serde_json::json!({ "idea": idea }),
            None,
            vec![],
        );
        match self.bus.publish(&env).await {
            Ok(()) => {
                let label = self.display_name(&session_id);
                self.status_msg = Some(format!("Submitted idea to {label}"));
            }
            Err(e) => self.status_msg = Some(format!("Submit failed: {e}")),
        }
        Ok(())
    }

    async fn submit_direct(&mut self, text: &str, msg_type: &str) -> Result<()> {
        let text = text.trim_start_matches('@');
        let mut parts = text.splitn(2, char::is_whitespace);
        let agent = parts.next().unwrap_or("").trim();
        let message = parts.next().unwrap_or("").trim();
        if agent.is_empty() || message.is_empty() {
            self.status_msg = Some("Direct message format: @agent <message>".into());
            return Ok(());
        }

        let session_id = self
            .active_session
            .clone()
            .unwrap_or_else(|| format!("sess_{}", ulid::Ulid::new()));
        let token = mint_actor_token(
            "console-user",
            &["agent:message"],
            &session_id,
            &self.signing_key,
            900,
        )?;
        let env = Envelope::build(
            direct_inbox_topic(agent),
            "agora-console",
            0,
            token,
            session_id,
            serde_json::json!({
                "messageType": msg_type,
                "recipient": agent,
                "message": message,
            }),
            None,
            vec![],
        );
        match self.bus.publish(&env).await {
            Ok(()) => self.status_msg = Some(format!("[{msg_type}] @{agent}: {message}")),
            Err(e) => self.status_msg = Some(format!("Direct send failed: {e}")),
        }
        Ok(())
    }

    async fn publish_session_named(&self, session_id: &str, name: &str) -> Result<()> {
        let token = mint_actor_token(
            "console-user",
            &["workspace:read"],
            session_id,
            &self.signing_key,
            900,
        )?;
        let env = Envelope::build(
            SESSION_NAMED,
            "agora-console",
            0,
            token,
            session_id,
            serde_json::json!({ "name": name }),
            None,
            vec![],
        );
        self.bus.publish(&env).await
    }

    async fn publish_session_deleted(&self, session_id: &str) -> Result<()> {
        let token = mint_actor_token(
            "console-user",
            &["workspace:read", "workspace:write"],
            session_id,
            &self.signing_key,
            900,
        )?;
        let env = Envelope::build(
            SESSION_DELETED,
            "agora-console",
            0,
            token,
            session_id,
            serde_json::json!({ "sessionId": session_id, "deletedBy": "agora-console" }),
            None,
            vec![],
        );
        self.bus.publish(&env).await
    }
}

fn short_id(id: &str, n: usize) -> String {
    if id.len() <= n {
        id.to_string()
    } else {
        format!("{}…", &id[..n])
    }
}

fn format_event_for_clipboard(e: &Envelope) -> String {
    let data = serde_json::to_string_pretty(&e.data).unwrap_or_else(|_| e.data.to_string());
    format!(
        "topic:   {}\nevent:   {}\nsession: {}\ntime:    {}\nfrom:    {}\ndata:\n{}",
        e.topic, e.event_id, e.context.session_id, e.timestamp, e.sender.agent_name, data
    )
}

/// Longest common prefix among matching agent names; returns the completed
/// `@name` form (with trailing space if it's a unique match) or `None` if
/// nothing matches.
pub fn autocomplete_agent(input: &str, agents: &BTreeMap<String, AgentManifest>) -> Option<String> {
    let stripped = input.strip_prefix('@')?;
    if stripped.contains(char::is_whitespace) {
        return None;
    }
    let matches: Vec<&String> = agents.keys().filter(|n| n.starts_with(stripped)).collect();
    match matches.len() {
        0 => None,
        1 => Some(format!("@{} ", matches[0])),
        _ => {
            let lcp = longest_common_prefix(&matches);
            if lcp.len() > stripped.len() {
                Some(format!("@{lcp}"))
            } else {
                None // ambiguous; keep input as-is (caller may show a list)
            }
        }
    }
}

fn longest_common_prefix(strs: &[&String]) -> String {
    if strs.is_empty() {
        return String::new();
    }
    let first = strs[0].as_bytes();
    let mut len = first.len();
    for s in &strs[1..] {
        let bytes = s.as_bytes();
        len = len.min(bytes.len());
        while len > 0 && bytes[..len] != first[..len] {
            len -= 1;
        }
        if len == 0 {
            return String::new();
        }
    }
    String::from_utf8_lossy(&first[..len]).into_owned()
}

fn short(s: &str, n: usize) -> String {
    short_id(s, n)
}

fn time_only(ts: &str) -> &str {
    if ts.len() >= 19 {
        &ts[11..19]
    } else {
        ts
    }
}

fn push_indented(out: &mut String, text: &str, indent: &str) {
    for line in text.lines() {
        out.push_str(indent);
        out.push_str(line);
        out.push('\n');
    }
}

/// NATS-style subject matching: `*` matches one token, `>` matches all
/// remaining tokens (must be last).
fn topic_matches(pattern: &str, topic: &str) -> bool {
    let pp: Vec<&str> = pattern.split('.').collect();
    let tp: Vec<&str> = topic.split('.').collect();
    let mut i = 0;
    let mut j = 0;
    while i < pp.len() && j < tp.len() {
        match pp[i] {
            ">" => return true,
            "*" => {
                i += 1;
                j += 1;
            }
            tok if tok == tp[j] => {
                i += 1;
                j += 1;
            }
            _ => return false,
        }
    }
    if i < pp.len() && pp[i] == ">" {
        return true;
    }
    i == pp.len() && j == tp.len()
}

#[cfg(test)]
mod tests {
    use super::topic_matches;

    #[test]
    fn nats_wildcard_matching() {
        assert!(topic_matches(
            "workspace.idea.submitted",
            "workspace.idea.submitted"
        ));
        assert!(topic_matches("workspace.>", "workspace.idea.submitted"));
        assert!(topic_matches("workspace.>", "workspace.design.finalized"));
        assert!(topic_matches("*.changed", "code.changed"));
        assert!(!topic_matches("*.changed", "code.changed.again"));
        assert!(!topic_matches("workspace.idea.submitted", "code.changed"));
        assert!(topic_matches(">", "anything.goes.here"));
    }
}
