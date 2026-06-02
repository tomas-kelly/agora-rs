use agora_core::{
    bookmark::validate_bookmark_label,
    bus::Bus,
    command::split_command_line,
    envelope::Envelope,
    event_data_from_input,
    manifest::AgentManifest,
    tokens::mint_actor_token,
    topics::{
        direct_inbox_topic, AGENT_TELEMETRY_LOGS, EVENT_BOOKMARKED, EVENT_UNBOOKMARKED,
        HUMAN_INTERACTION_REQUEST, HUMAN_INTERACTION_RESPONSE, SESSION_DELETED, SESSION_NAMED,
    },
};
use anyhow::Result;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
};

const MAX_EVENTS: usize = 500;
const MAX_TELEMETRY: usize = 2000;
const MAX_INPUT_HISTORY: usize = 200;

// Local constants until agora-core exposes these (backend task B3)
const SESSION_TAGGED: &str = "session.tagged";
const SESSION_UNTAGGED: &str = "session.untagged";
const MAX_TAGS_PER_SESSION: usize = 10;

fn validate_tag(input: &str) -> Option<String> {
    let tag = input.trim().to_lowercase();
    if tag.is_empty() || tag.len() > 64 {
        return None;
    }
    let bytes = tag.as_bytes();
    if !bytes[0].is_ascii_alphanumeric() {
        return None;
    }
    if tag
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
    {
        Some(tag)
    } else {
        None
    }
}

fn command_value(input: &str) -> Result<Option<String>> {
    let value = input.trim();
    if value.is_empty() {
        return Ok(None);
    }

    if value.starts_with('"') || value.starts_with('\'') {
        let parts = split_command_line(value)?;
        if parts.len() != 1 {
            anyhow::bail!("expected one quoted argument");
        }
        Ok(parts.into_iter().next())
    } else {
        Ok(Some(value.to_string()))
    }
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FocusTarget {
    Sessions,
    Events,
    Agents,
    AgentOutput,
    Composer,
}

impl FocusTarget {
    pub fn label(self) -> &'static str {
        match self {
            Self::Sessions => "Sessions",
            Self::Events => "Events",
            Self::Agents => "Agents",
            Self::AgentOutput => "Agent Output",
            Self::Composer => "Composer",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionItem {
    pub value: String,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub session_id: String,
    pub started_at: String,
    pub last_topic: String,
    pub event_count: usize,
    pub deleted: bool,
    pub deleted_at: Option<String>,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingInteraction {
    pub event_id: String,
    pub session_id: String,
    pub agent_name: String,
    pub kind: String,
    pub question: String,
    pub timestamp: String,
}

impl PendingInteraction {
    pub fn kind_label(&self) -> &'static str {
        if self.kind == "tool_approval" {
            "tool approval"
        } else {
            "input"
        }
    }
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
    pub selected_event: Option<usize>,
    pub inspected_event: Option<usize>,
    pub telemetry: Vec<TelemetryEntry>,
    pub sessions: BTreeMap<String, SessionInfo>,
    pub session_names: HashMap<String, String>,
    pub active_session: Option<String>,
    pub agents: BTreeMap<String, AgentManifest>,
    pub selected_agent: Option<String>,
    pub focus: FocusTarget,

    pub bus_url: String,
    pub bus: Arc<Bus>,
    pub signing_key: Vec<u8>,
    pub submit_topic: String,
    pub submit_field: String,
    pub status_msg: Option<String>,
    pub command_output: Option<String>,
    pub agent_tail: Option<String>,
    pub agent_tail_output: Option<String>,
    pub output_scroll: u16,
    pub tail_scroll: u16,

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

    pub tag_filter: Option<String>,
    pub bookmarks: HashMap<String, Option<String>>,
    /// Topology-derived view of which topics are valid + what scopes the
    /// human's actor token must carry to publish each one.
    pub topology: agora_core::TopicCatalog,
    pub pending_action: Option<PendingAction>,
    pub command_palette_open: bool,
    pub completion_selected: usize,
    pub completion_selection_active: bool,
    history_path: Option<PathBuf>,
    input_history: Vec<String>,
    input_history_cursor: Option<usize>,
    input_history_draft: String,
}

impl App {
    pub fn new(
        bus: Arc<Bus>,
        signing_key: Vec<u8>,
        bus_url: String,
        submit_topic: String,
        submit_field: String,
        topology: agora_core::TopicCatalog,
        history_path: Option<PathBuf>,
    ) -> Self {
        Self {
            input: String::new(),
            stashed_input: String::new(),
            mode: InputMode::Normal,
            rename_target: None,
            events: Vec::new(),
            selected_event: None,
            inspected_event: None,
            telemetry: Vec::new(),
            sessions: BTreeMap::new(),
            session_names: HashMap::new(),
            active_session: None,
            agents: BTreeMap::new(),
            selected_agent: None,
            focus: FocusTarget::Composer,
            bus_url,
            bus,
            signing_key,
            submit_topic,
            submit_field,
            status_msg: Some("Type !help for commands · Enter to submit · Esc to quit".into()),
            command_output: None,
            agent_tail: None,
            agent_tail_output: None,
            output_scroll: 0,
            tail_scroll: 0,
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
            tag_filter: None,
            bookmarks: HashMap::new(),
            topology,
            pending_action: None,
            command_palette_open: false,
            completion_selected: 0,
            completion_selection_active: false,
            history_path,
            input_history: Vec::new(),
            input_history_cursor: None,
            input_history_draft: String::new(),
        }
    }

    pub fn load_input_history(&mut self) -> Result<()> {
        let Some(path) = self.history_path.as_deref() else {
            return Ok(());
        };
        if !path.exists() {
            return Ok(());
        }
        self.input_history = load_history_entries(path)?;
        if self.input_history.len() > MAX_INPUT_HISTORY {
            let excess = self.input_history.len() - MAX_INPUT_HISTORY;
            self.input_history.drain(0..excess);
        }
        Ok(())
    }

    fn persist_input_history(&self) -> Result<()> {
        let Some(path) = self.history_path.as_deref() else {
            return Ok(());
        };
        write_history_entries(path, &self.input_history)
    }

    /// Maximum scroll offset such that the events pane stays full of content.
    /// Anything beyond this would leave blank space at the top.
    pub fn max_events_scroll(&self) -> u16 {
        let len = self.active_session_event_count() as u16;
        len.saturating_sub(self.events_view_height)
    }

    pub fn active_session_event_indices(&self) -> Vec<usize> {
        let Some(session_id) = self.active_session.as_deref() else {
            return Vec::new();
        };
        self.events
            .iter()
            .enumerate()
            .filter_map(|(idx, event)| (event.context.session_id == session_id).then_some(idx))
            .collect()
    }

    pub fn active_session_event_count(&self) -> usize {
        self.active_session_event_indices().len()
    }

    pub fn sync_events_viewport(&mut self, height: u16) {
        self.events_view_height = height.max(1);
        self.clamp_event_selection();
        self.ensure_selected_event_visible();
    }

    pub fn selected_event_index(&self) -> Option<usize> {
        self.selected_event
            .filter(|idx| self.event_belongs_to_active_session(*idx))
    }

    pub fn selected_event_with_index(&self) -> Option<(usize, &Envelope)> {
        let idx = self.selected_event_index()?;
        self.events.get(idx).map(|event| (idx, event))
    }

    pub fn selected_event_position(&self) -> Option<usize> {
        let selected = self.selected_event_index()?;
        self.active_session_event_indices()
            .iter()
            .position(|idx| *idx == selected)
    }

    pub fn inspected_event_index(&self) -> Option<usize> {
        self.inspected_event
            .filter(|idx| self.event_belongs_to_active_session(*idx))
    }

    pub fn inspected_event_with_index(&self) -> Option<(usize, &Envelope)> {
        let idx = self.inspected_event_index()?;
        self.events.get(idx).map(|event| (idx, event))
    }

    pub fn inspected_event_position(&self) -> Option<usize> {
        let inspected = self.inspected_event_index()?;
        self.active_session_event_indices()
            .iter()
            .position(|idx| *idx == inspected)
    }

    pub fn event_details_open(&self) -> bool {
        self.inspected_event_index().is_some()
    }

    pub fn event_details_visible(&self) -> bool {
        self.event_details_open() && self.command_output.is_none()
    }

    fn event_belongs_to_active_session(&self, idx: usize) -> bool {
        let Some(session_id) = self.active_session.as_deref() else {
            return false;
        };
        self.events
            .get(idx)
            .is_some_and(|event| event.context.session_id == session_id)
    }

    /// Returns (event_id, session_id, question) if the inspected event is a
    /// pending human interaction request the user can respond to.
    pub fn active_interaction_request(&self) -> Option<(String, String, String)> {
        let (_, event) = self.inspected_event_with_index()?;
        if event.topic != HUMAN_INTERACTION_REQUEST {
            return None;
        }
        if !self.is_pending_interaction(&event.event_id) {
            return None;
        }
        let question = event
            .data
            .get("question")
            .and_then(|v| v.as_str())
            .unwrap_or("?")
            .to_string();
        Some((
            event.event_id.clone(),
            event.context.session_id.clone(),
            question,
        ))
    }

    pub fn pending_interactions(&self) -> Vec<PendingInteraction> {
        pending_interactions_from_events(&self.events, &self.sessions)
    }

    pub fn pending_interaction_count(&self) -> usize {
        self.pending_interactions().len()
    }

    pub fn first_pending_interaction(&self) -> Option<PendingInteraction> {
        let pending = self.pending_interactions();
        if let Some(active_session) = self.active_session.as_deref() {
            if let Some(item) = pending
                .iter()
                .find(|item| item.session_id == active_session)
                .cloned()
            {
                return Some(item);
            }
        }
        pending.into_iter().next()
    }

    pub fn pending_count_for_session(&self, session_id: &str) -> usize {
        self.pending_interactions()
            .iter()
            .filter(|item| item.session_id == session_id)
            .count()
    }

    pub fn pending_count_for_agent(&self, agent_name: &str) -> usize {
        self.pending_interactions()
            .iter()
            .filter(|item| item.agent_name == agent_name)
            .count()
    }

    pub fn agent_names(&self) -> Vec<String> {
        self.agents.keys().cloned().collect()
    }

    pub fn selected_agent_name(&self) -> Option<&str> {
        self.selected_agent
            .as_deref()
            .filter(|agent| self.agents.contains_key(*agent))
    }

    pub fn ensure_selected_agent(&mut self) {
        if self
            .selected_agent
            .as_deref()
            .is_some_and(|agent| self.agents.contains_key(agent))
        {
            return;
        }
        self.selected_agent = self.agents.keys().next().cloned();
    }

    pub fn move_agent_selection(&mut self, delta: i32) {
        let names = self.agent_names();
        if names.is_empty() {
            self.selected_agent = None;
            self.status_msg = Some("No agents registered yet.".into());
            return;
        }
        let len = names.len();
        let current = self
            .selected_agent_name()
            .and_then(|agent| names.iter().position(|name| name == agent))
            .unwrap_or(0);
        let next = if delta < 0 {
            current.saturating_sub((-delta) as usize)
        } else {
            current.saturating_add(delta as usize).min(len - 1)
        };
        self.selected_agent = Some(names[next].clone());
        self.focus = FocusTarget::Agents;
        self.status_msg = Some(format!(
            "Agent {}/{}: {} · Enter tails · h history · m message",
            next + 1,
            len,
            names[next]
        ));
    }

    pub fn selected_agent_tail(&mut self) {
        self.ensure_selected_agent();
        let Some(agent) = self.selected_agent.clone() else {
            self.status_msg = Some("No agent selected.".into());
            return;
        };
        self.cmd_tail(&agent);
    }

    pub fn selected_agent_history(&mut self) {
        self.ensure_selected_agent();
        let Some(agent) = self.selected_agent.clone() else {
            self.status_msg = Some("No agent selected.".into());
            return;
        };
        self.cmd_history(&agent);
    }

    pub fn selected_agent_status(&mut self) {
        self.ensure_selected_agent();
        let Some(agent) = self.selected_agent.clone() else {
            self.status_msg = Some("No agent selected.".into());
            return;
        };
        self.cmd_status(&agent);
    }

    pub fn compose_message_to_selected_agent(&mut self, queued: bool) {
        self.ensure_selected_agent();
        let Some(agent) = self.selected_agent.clone() else {
            self.status_msg = Some("No agent selected.".into());
            return;
        };
        let prefix = if queued { "/queue @" } else { "@" };
        self.replace_input(format!("{prefix}{agent} "));
        self.status_msg = Some(format!(
            "Composing {}message to {agent} in current session",
            if queued { "queued " } else { "" }
        ));
    }

    pub fn is_pending_interaction(&self, event_id: &str) -> bool {
        self.pending_interactions()
            .iter()
            .any(|item| item.event_id == event_id)
    }

    pub fn jump_to_pending_interaction(&mut self) -> bool {
        let Some(target) = self.first_pending_interaction() else {
            self.status_msg = Some("No pending human input or tool approvals.".into());
            return false;
        };
        let Some(idx) = self
            .events
            .iter()
            .position(|event| event.event_id == target.event_id)
        else {
            self.status_msg =
                Some("Pending request is no longer in the local event buffer.".into());
            return false;
        };

        self.active_session = Some(target.session_id.clone());
        self.selected_event = Some(idx);
        self.inspected_event = Some(idx);
        self.command_output = None;
        self.output_scroll = 0;
        self.auto_scroll = false;
        self.panels.sessions = true;
        self.panels.events = true;
        self.panels.detail = true;
        self.ensure_selected_event_visible();
        self.status_msg = Some(format!(
            "Responding to {} from {} in {}",
            target.kind_label(),
            target.agent_name,
            self.display_name(&target.session_id)
        ));
        true
    }

    pub fn take_pending_action(&mut self) -> Option<PendingAction> {
        self.pending_action.take()
    }

    pub fn scroll_output(&mut self, delta: i32) {
        if self.focus == FocusTarget::AgentOutput && self.agent_tail_output.is_some() {
            self.scroll_agent_tail(delta);
            return;
        }
        if self.command_output.is_none() && !self.event_details_visible() {
            return;
        }
        if delta < 0 {
            self.output_scroll = self.output_scroll.saturating_sub((-delta) as u16);
        } else {
            self.output_scroll = self.output_scroll.saturating_add(delta as u16);
        }
    }

    pub fn scroll_agent_tail(&mut self, delta: i32) {
        if self.agent_tail_output.is_none() {
            return;
        }
        if delta < 0 {
            self.tail_scroll = self.tail_scroll.saturating_sub((-delta) as u16);
        } else {
            self.tail_scroll = self.tail_scroll.saturating_add(delta as u16);
        }
        self.focus = FocusTarget::AgentOutput;
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
        self.focus = FocusTarget::Composer;
        self.input_history_cursor = None;
        self.sync_completion_after_input_change();
        self.reset_input_scroll();
    }

    pub fn push_input_newline(&mut self) {
        self.input.push('\n');
        self.focus = FocusTarget::Composer;
        self.input_history_cursor = None;
        self.sync_completion_after_input_change();
        self.reset_input_scroll();
    }

    pub fn pop_input_char(&mut self) {
        self.input.pop();
        self.focus = FocusTarget::Composer;
        self.input_history_cursor = None;
        self.sync_completion_after_input_change();
        self.reset_input_scroll();
    }

    pub fn replace_input(&mut self, input: String) {
        self.input = input;
        self.focus = FocusTarget::Composer;
        self.command_palette_open = false;
        self.reset_completion_selection();
        self.reset_input_scroll();
    }

    pub fn complete_input(&mut self) -> bool {
        let Some(completed) = complete_input(
            &self.input,
            &self.agents,
            &self.topology,
            &self.sessions,
            &self.session_names,
            &self.session_tags(),
            &self.bookmark_labels(),
        ) else {
            let suggestions = self.completion_suggestions();
            if suggestions.is_empty() {
                self.status_msg = Some("No completion available.".into());
            } else {
                self.status_msg = Some(format!("Matches: {}", suggestions.join(", ")));
            }
            return false;
        };
        self.replace_input(completed);
        true
    }

    pub fn completion_suggestions(&self) -> Vec<String> {
        self.completion_items()
            .into_iter()
            .map(|item| item.value)
            .collect()
    }

    pub fn completion_items(&self) -> Vec<CompletionItem> {
        completion_items(
            &self.input,
            &self.agents,
            &self.topology,
            &self.sessions,
            &self.session_names,
            &self.session_tags(),
            &self.bookmark_labels(),
        )
    }

    pub fn open_command_palette(&mut self) {
        if self.input.trim().is_empty() {
            self.replace_input("!".into());
            self.command_palette_open = true;
            self.status_msg = Some("Command palette · type to filter, Tab completes".into());
        } else if self.input.trim_start().starts_with('!') {
            self.focus = FocusTarget::Composer;
            self.command_palette_open = true;
            self.reset_completion_selection();
            self.status_msg = Some("Command palette · type to filter, Tab completes".into());
        } else {
            self.status_msg = Some(
                "Command palette opens from an empty composer; finish or clear draft first.".into(),
            );
        }
    }

    pub fn completion_menu_active(&self) -> bool {
        self.mode == InputMode::Normal && !self.completion_items().is_empty()
    }

    pub fn move_completion_selection(&mut self, delta: i32) -> bool {
        let len = self.completion_items().len();
        if len == 0 {
            return false;
        }
        let current = self.completion_selected.min(len - 1);
        let next = if delta < 0 {
            current.saturating_sub((-delta) as usize)
        } else {
            current.saturating_add(delta as usize).min(len - 1)
        };
        self.completion_selected = next;
        self.completion_selection_active = true;
        self.focus = FocusTarget::Composer;
        self.status_msg = Some(format!("Completion {}/{}", next + 1, len));
        true
    }

    pub fn accept_selected_completion(&mut self) -> bool {
        let items = self.completion_items();
        let Some(item) = items.get(self.completion_selected.min(items.len().saturating_sub(1)))
        else {
            return false;
        };
        let Some(completed) = apply_completion_value(&self.input, &item.value) else {
            return false;
        };
        self.replace_input(completed);
        self.status_msg = Some(format!("Selected {}", item.value));
        true
    }

    pub fn close_command_palette(&mut self) -> bool {
        if !self.command_palette_open && !self.completion_selection_active {
            return false;
        }
        self.command_palette_open = false;
        self.reset_completion_selection();
        self.status_msg = Some("Completion menu closed".into());
        true
    }

    fn reset_completion_selection(&mut self) {
        self.completion_selected = 0;
        self.completion_selection_active = false;
    }

    fn sync_completion_after_input_change(&mut self) {
        self.reset_completion_selection();
        if !self.input.trim_start().starts_with('!') {
            self.command_palette_open = false;
        }
    }

    pub fn history_previous(&mut self) -> bool {
        if self.input_history.is_empty() {
            self.status_msg = Some("No composer history yet.".into());
            return false;
        }
        let next_cursor = match self.input_history_cursor {
            Some(0) => 0,
            Some(idx) => idx - 1,
            None => {
                self.input_history_draft = self.input.clone();
                self.input_history.len() - 1
            }
        };
        self.input_history_cursor = Some(next_cursor);
        self.input = self.input_history[next_cursor].clone();
        self.focus = FocusTarget::Composer;
        self.reset_input_scroll();
        self.status_msg = Some(format!(
            "History {}/{}",
            next_cursor + 1,
            self.input_history.len()
        ));
        true
    }

    pub fn history_next(&mut self) -> bool {
        let Some(cursor) = self.input_history_cursor else {
            self.status_msg = Some("Already at newest composer entry.".into());
            return false;
        };
        if cursor + 1 >= self.input_history.len() {
            self.input_history_cursor = None;
            self.input = std::mem::take(&mut self.input_history_draft);
            self.status_msg = Some("Returned to current draft.".into());
        } else {
            let next = cursor + 1;
            self.input_history_cursor = Some(next);
            self.input = self.input_history[next].clone();
            self.status_msg = Some(format!("History {}/{}", next + 1, self.input_history.len()));
        }
        self.focus = FocusTarget::Composer;
        self.reset_input_scroll();
        true
    }

    pub fn history_next_or_enter_naming_new(&mut self) {
        if !self.history_next() {
            self.enter_naming_new();
        }
    }

    fn remember_input_history(&mut self, text: &str) {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return;
        }
        if self
            .input_history
            .last()
            .is_some_and(|previous| previous == trimmed)
        {
            self.input_history_cursor = None;
            self.input_history_draft.clear();
            return;
        }
        self.input_history.push(trimmed.to_string());
        if self.input_history.len() > MAX_INPUT_HISTORY {
            let excess = self.input_history.len() - MAX_INPUT_HISTORY;
            self.input_history.drain(0..excess);
        }
        self.input_history_cursor = None;
        self.input_history_draft.clear();
        if let Err(e) = self.persist_input_history() {
            self.status_msg = Some(format!("History saved in memory only: {e}"));
        }
    }

    pub fn dismiss_command_output(&mut self) -> bool {
        if self.close_command_palette() {
            true
        } else if self.command_output.is_some() {
            self.command_output = None;
            true
        } else if self.agent_tail_output.is_some() {
            self.agent_tail = None;
            self.agent_tail_output = None;
            self.tail_scroll = 0;
            if self.focus == FocusTarget::AgentOutput {
                self.cycle_focus(true);
            }
            true
        } else if self.inspected_event.is_some() {
            self.inspected_event = None;
            self.output_scroll = 0;
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

    fn clamp_event_selection(&mut self) {
        if self.active_session_event_count() == 0 {
            self.selected_event = None;
            self.inspected_event = None;
            self.events_scroll = 0;
            self.auto_scroll = true;
            return;
        }

        if self
            .selected_event
            .is_some_and(|idx| !self.event_belongs_to_active_session(idx))
        {
            self.selected_event = None;
        }
        if self
            .inspected_event
            .is_some_and(|idx| !self.event_belongs_to_active_session(idx))
        {
            self.inspected_event = None;
        }
        self.events_scroll = self.events_scroll.min(self.max_events_scroll());
    }

    fn ensure_selected_event_visible(&mut self) {
        self.clamp_event_selection();
        let Some(pos) = self.selected_event_position() else {
            return;
        };

        let len = self.active_session_event_count();
        let height = self.events_view_height.max(1) as usize;
        let max_scroll = len.saturating_sub(height);
        let scroll = (self.events_scroll as usize).min(max_scroll);
        let end = len.saturating_sub(scroll);
        let start = end.saturating_sub(height);

        if pos < start {
            let desired_end = (pos + height).min(len);
            self.events_scroll = len.saturating_sub(desired_end) as u16;
        } else if pos >= end {
            self.events_scroll = len.saturating_sub(pos + 1) as u16;
        } else {
            self.events_scroll = scroll as u16;
        }

        self.events_scroll = self.events_scroll.min(self.max_events_scroll());
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

    fn session_tags(&self) -> Vec<String> {
        let mut tags: Vec<String> = self
            .sessions
            .values()
            .flat_map(|session| session.tags.iter().cloned())
            .collect();
        tags.sort();
        tags.dedup();
        tags
    }

    fn bookmark_labels(&self) -> Vec<String> {
        let mut labels: Vec<String> = self
            .bookmarks
            .values()
            .filter_map(|label| label.clone())
            .collect();
        labels.sort();
        labels.dedup();
        labels
    }

    pub fn handle_app_event(&mut self, ev: AppEvent) {
        match ev {
            AppEvent::Envelope(env) => self.handle_envelope(env),
            AppEvent::Heartbeat(m) => {
                self.agents.insert(m.agent_name.clone(), m);
                self.ensure_selected_agent();
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
        self.refresh_agent_tail();
    }

    pub fn visible_focus_targets(&self) -> Vec<FocusTarget> {
        let mut targets = Vec::new();
        if self.panels.sessions {
            targets.push(FocusTarget::Sessions);
        }
        if self.panels.events {
            targets.push(FocusTarget::Events);
        }
        if self.panels.agents {
            targets.push(FocusTarget::Agents);
        }
        if self.agent_tail_output.is_some() {
            targets.push(FocusTarget::AgentOutput);
        }
        targets.push(FocusTarget::Composer);
        targets
    }

    pub fn cycle_focus(&mut self, forward: bool) {
        let targets = self.visible_focus_targets();
        if targets.is_empty() {
            self.focus = FocusTarget::Composer;
            return;
        }
        let pos = targets
            .iter()
            .position(|target| *target == self.focus)
            .unwrap_or(targets.len() - 1);
        let next = if forward {
            targets[(pos + 1) % targets.len()]
        } else {
            targets[(pos + targets.len() - 1) % targets.len()]
        };
        self.focus = next;
        if next == FocusTarget::Agents {
            self.ensure_selected_agent();
        }
        self.status_msg = Some(format!("Focus: {}", next.label()));
    }

    pub fn focus_hint(&self) -> String {
        match self.focus {
            FocusTarget::Sessions => "Sessions · Up/Down switch · Ctrl-N new · Ctrl-R rename · Tab focus".into(),
            FocusTarget::Events => {
                "Events · Up/Down select · Enter details · Esc closes · End live · Tab focus"
                    .into()
            }
            FocusTarget::Agents => {
                "Agents · Up/Down select · Enter tail · h history · m/M message/queue · s status · Tab focus"
                    .into()
            }
            FocusTarget::AgentOutput => {
                "Agent Output · PgUp/PgDn scroll · Esc closes tail · Tab focus".into()
            }
            FocusTarget::Composer => {
                "Composer · Up/Down history · Tab complete · Ctrl-K commands · Enter send"
                    .into()
            }
        }
    }

    pub fn focus_panel(&mut self, panel: Panel) {
        self.focus = match panel {
            Panel::Sessions => FocusTarget::Sessions,
            Panel::Events => FocusTarget::Events,
            Panel::Agents => {
                self.ensure_selected_agent();
                FocusTarget::Agents
            }
            Panel::Detail => FocusTarget::Events,
        };
    }

    pub fn handle_envelope(&mut self, env: Envelope) {
        // Telemetry envelopes carry the same TelemetryEntry shape inside
        // `data`. Route them to the telemetry collector so JetStream replay
        // on startup populates `!history` with past prompts/responses,
        // without polluting the events pane.
        if env.topic == AGENT_TELEMETRY_LOGS {
            if let Ok(entry) = serde_json::from_value::<TelemetryEntry>(env.data) {
                self.telemetry.push(entry);
                if self.telemetry.len() > MAX_TELEMETRY {
                    let excess = self.telemetry.len() - MAX_TELEMETRY;
                    self.telemetry.drain(0..excess);
                }
            }
            return;
        }

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
                        tags: Vec::new(),
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
                    tags: Vec::new(),
                });
            session.deleted = true;
            session.deleted_at = Some(env.timestamp.clone());
            if self.active_session.as_deref() == Some(sid.as_str()) {
                self.active_session = None;
                self.selected_event = None;
                self.inspected_event = None;
                self.events_scroll = 0;
                self.auto_scroll = true;
            }
            return;
        }

        if env.topic == SESSION_TAGGED {
            if let Some(tag) = env.data.get("tag").and_then(|v| v.as_str()) {
                if let Some(tag) = validate_tag(tag) {
                    let sid = env.context.session_id.clone();
                    let session = self
                        .sessions
                        .entry(sid.clone())
                        .or_insert_with(|| SessionInfo {
                            session_id: sid,
                            started_at: env.timestamp.clone(),
                            last_topic: String::new(),
                            event_count: 0,
                            deleted: false,
                            deleted_at: None,
                            tags: Vec::new(),
                        });
                    if !session.tags.contains(&tag) && session.tags.len() < MAX_TAGS_PER_SESSION {
                        session.tags.push(tag);
                    }
                }
            }
            return;
        }

        if env.topic == SESSION_UNTAGGED {
            if let Some(tag) = env.data.get("tag").and_then(|v| v.as_str()) {
                if let Some(tag) = validate_tag(tag) {
                    let sid = env.context.session_id.clone();
                    if let Some(session) = self.sessions.get_mut(&sid) {
                        session.tags.retain(|t| t != &tag);
                    }
                }
            }
            return;
        }

        if env.topic == EVENT_BOOKMARKED {
            if let Some(target) = env.data.get("targetEventId").and_then(|v| v.as_str()) {
                let label = env
                    .data
                    .get("label")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                self.bookmarks.insert(target.to_string(), label);
            }
            return;
        }

        if env.topic == EVENT_UNBOOKMARKED {
            if let Some(target) = env.data.get("targetEventId").and_then(|v| v.as_str()) {
                self.bookmarks.remove(target);
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
                tags: Vec::new(),
            });
        session.last_topic = topic;
        session.event_count += 1;

        let was_following = self.auto_scroll || self.selected_event.is_none();
        let event_matches_active_session = self.active_session.as_deref() == Some(sid.as_str());
        self.events.push(env);
        if !was_following && event_matches_active_session {
            self.events_scroll = self.events_scroll.saturating_add(1);
        }

        if self.events.len() > MAX_EVENTS {
            let excess = self.events.len() - MAX_EVENTS;
            self.events.drain(0..excess);
            if let Some(idx) = self.selected_event {
                self.selected_event = idx.checked_sub(excess);
            }
            if let Some(idx) = self.inspected_event {
                self.inspected_event = idx.checked_sub(excess);
            }
        }

        if was_following {
            self.selected_event = None;
            self.events_scroll = 0;
            self.auto_scroll = true;
        } else {
            self.clamp_event_selection();
            self.ensure_selected_event_visible();
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
                tags: Vec::new(),
            },
        );
        self.session_names
            .insert(session_id.clone(), name.to_string());
        self.active_session = Some(session_id.clone());
        self.selected_event = None;
        self.inspected_event = None;
        self.events_scroll = 0;
        self.auto_scroll = true;

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
            self.selected_event = None;
            self.inspected_event = None;
            self.events_scroll = 0;
            self.auto_scroll = true;
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
        self.focus = FocusTarget::Sessions;
        self.selected_event = None;
        self.inspected_event = None;
        self.events_scroll = 0;
        self.auto_scroll = true;
        self.status_msg = Some(format!("Active: {}", self.display_name(&next)));
    }

    pub fn clear_active(&mut self) {
        if self.active_session.take().is_some() {
            self.selected_event = None;
            self.inspected_event = None;
            self.events_scroll = 0;
            self.auto_scroll = true;
            self.status_msg = Some("Cleared active session".into());
        }
    }

    pub fn select_default_session(&mut self) {
        if self.active_session.as_deref().is_some_and(|sid| {
            self.sessions
                .get(sid)
                .is_some_and(|session| !session.deleted)
        }) {
            return;
        }
        let Some((sid, _)) = self
            .sessions
            .iter()
            .filter(|(_, session)| !session.deleted)
            .max_by(|a, b| a.1.started_at.cmp(&b.1.started_at))
        else {
            return;
        };
        self.active_session = Some(sid.clone());
        self.selected_event = None;
        self.inspected_event = None;
        self.events_scroll = 0;
        self.auto_scroll = true;
    }

    pub fn scroll_up(&mut self) {
        self.focus = FocusTarget::Events;
        self.select_event_delta(-1);
    }

    pub fn scroll_down(&mut self) {
        self.focus = FocusTarget::Events;
        self.select_event_delta(1);
    }

    pub fn page_events_up(&mut self) {
        self.focus = FocusTarget::Events;
        self.select_event_delta(-5);
    }

    pub fn page_events_down(&mut self) {
        self.focus = FocusTarget::Events;
        self.select_event_delta(5);
    }

    fn select_event_delta(&mut self, delta: i32) {
        let indices = self.active_session_event_indices();
        if indices.is_empty() {
            self.status_msg = Some(match &self.active_session {
                Some(session_id) => {
                    format!("No events in {}", self.display_name(session_id))
                }
                None => "No active session selected.".into(),
            });
            return;
        }
        self.clamp_event_selection();
        let len = indices.len();
        let pos = match self
            .selected_event_index()
            .and_then(|idx| indices.iter().position(|candidate| *candidate == idx))
        {
            Some(pos) => pos,
            None if delta < 0 => len,
            None => len - 1,
        };
        let next_pos = if delta < 0 {
            pos.saturating_sub((-delta) as usize)
        } else {
            pos.saturating_add(delta as usize).min(len - 1)
        };
        let next = indices[next_pos];
        self.selected_event = Some(next);
        self.auto_scroll = false;
        self.ensure_selected_event_visible();
        self.status_msg = Some(format!(
            "Selected event {}/{} · empty Enter opens details",
            next_pos + 1,
            len
        ));
    }

    pub fn end_scroll(&mut self) {
        self.selected_event = None;
        self.inspected_event = None;
        self.focus = FocusTarget::Events;
        self.output_scroll = 0;
        self.events_scroll = 0;
        self.auto_scroll = true;
        let count = self.active_session_event_count();
        if count == 0 {
            self.status_msg = Some("Following live tail".into());
        } else {
            self.status_msg = Some(format!("Following live tail after event {count}"));
        }
    }

    pub fn open_selected_event(&mut self) {
        let Some((idx, _event)) = self.selected_event_with_index() else {
            self.status_msg = Some("No event selected.".into());
            return;
        };
        let pos = self.selected_event_position().unwrap_or(0);
        let count = self.active_session_event_count();
        self.command_output = None;
        self.output_scroll = 0;
        self.focus = FocusTarget::Events;
        self.inspected_event = Some(idx);
        self.panels.detail = true;
        self.status_msg = Some(format!(
            "Viewing event details {}/{} · Esc closes",
            pos + 1,
            count,
        ));
    }

    pub fn toggle_panel(&mut self, panel: Panel) {
        let visible = !self.panels.is_visible(panel);
        self.panels.set(panel, visible);
        if visible {
            self.focus_panel(panel);
        } else if self.focus_matches_panel(panel) {
            self.cycle_focus(true);
        }
        self.status_msg = Some(format!(
            "{} panel {}",
            panel.label(),
            if visible { "shown" } else { "hidden" }
        ));
    }

    fn focus_matches_panel(&self, panel: Panel) -> bool {
        matches!(
            (self.focus, panel),
            (FocusTarget::Sessions, Panel::Sessions)
                | (FocusTarget::Events, Panel::Events)
                | (FocusTarget::Agents, Panel::Agents)
        )
    }

    // -------------------------------------------- publishing

    pub async fn submit(&mut self, text: String) -> Result<()> {
        let text = text.trim().to_string();
        if text.is_empty() {
            return Ok(());
        }
        self.remember_input_history(&text);

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

        if let Some((correlation_id, session_id, _)) = self.active_interaction_request() {
            return self
                .submit_human_response(text, correlation_id, session_id)
                .await;
        }

        let topic = self.submit_topic.clone();
        if let Err(e) = self.submit_event(&topic, text).await {
            self.status_msg = Some(format!("Submit failed: {e}"));
        }
        Ok(())
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
            "tag" => self.cmd_tag(rest).await,
            "untag" => self.cmd_untag(rest).await,
            "filter" => self.cmd_filter(rest),
            "bookmark" | "bm" => self.cmd_bookmark(rest).await,
            "unbookmark" | "ubm" => self.cmd_unbookmark(rest).await,
            "bookmarks" => self.cmd_bookmarks(),
            "agents" => self.cmd_agents(),
            "status" => self.cmd_status(rest),
            "history" => self.cmd_history(rest),
            "tail" | "follow" => self.cmd_tail(rest),
            "pending" => self.cmd_pending(rest),
            "panel" | "panels" | "toggle" => self.cmd_panel(rest),
            "submit" => self.cmd_submit(rest).await,
            "clear" => self.cmd_clear(),
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

    fn cmd_clear(&mut self) {
        let n_events = self.events.len();
        let n_tel = self.telemetry.len();
        self.events.clear();
        self.selected_event = None;
        self.inspected_event = None;
        self.telemetry.clear();
        // Keep sessions, session_names, active session, and agents. Clear is a
        // local event/telemetry clear, not a metadata wipe.
        self.events_scroll = 0;
        self.auto_scroll = true;
        self.command_output = None;
        self.agent_tail = None;
        self.agent_tail_output = None;
        self.output_scroll = 0;
        self.tail_scroll = 0;
        self.status_msg = Some(format!(
            "Cleared view ({n_events} events, {n_tel} telemetry cleared; sessions kept)"
        ));
    }

    fn cmd_copy(&mut self) {
        let text = self
            .command_output
            .clone()
            .or_else(|| self.agent_tail_output.clone())
            .or_else(|| {
                self.inspected_event_with_index()
                    .or_else(|| self.selected_event_with_index())
                    .map(|(_, event)| format_event_for_clipboard(event))
            });
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
        let Some(content) = self
            .command_output
            .clone()
            .or_else(|| self.agent_tail_output.clone())
        else {
            self.status_msg =
                Some("Nothing to page. Run !help, !history, or !tail <agent> first.".into());
            return;
        };
        self.pending_action = Some(PendingAction::OpenPager { content });
    }

    fn set_output(&mut self, text: String) {
        self.panels.detail = true;
        self.inspected_event = None;
        self.command_output = Some(text);
        self.output_scroll = 0;
    }

    fn set_tail_output(&mut self, agent: &str, text: String) {
        self.agent_tail = Some(agent.to_string());
        self.agent_tail_output = Some(text);
        self.tail_scroll = u16::MAX;
        self.focus = FocusTarget::AgentOutput;
    }

    fn refresh_agent_tail(&mut self) {
        let Some(agent) = self.agent_tail.clone() else {
            return;
        };
        if let Ok(text) = self.agent_timeline_output(&agent, true) {
            self.agent_tail_output = Some(text);
            self.tail_scroll = u16::MAX;
        }
    }

    fn cmd_help(&mut self) {
        let text = "COMMANDS (type, Enter):\n\
             \x20\x20!help                  This panel\n\
             \x20\x20!new [name]            Create new session (prompts if name omitted)\n\
             \x20\x20!rename [name]         Rename active session\n\
             \x20\x20!delete [session]      Hide a session from lists; history is retained\n\
             \x20\x20!tag <label>           Tag active session (max 10 tags)\n\
             \x20\x20!untag <label>         Remove tag from active session\n\
             \x20\x20!filter [tag]          Filter sessions pane by tag (no arg clears)\n\
             \x20\x20!agents                Agents seen in active session\n\
             \x20\x20!status <agent>        Agent's manifest + activity in active session\n\
             \x20\x20!history <agent>       Full conversation: received · prompts · responses · published\n\
             \x20\x20!tail <agent>          Live ACP output for an agent in the active session\n\
             \x20\x20!pending [next]        Show pending input/tool approvals, or jump to the next one\n\
             \x20\x20!panel <name>          Toggle sessions, events, agents, detail, or all\n\
             \x20\x20!submit <topic> <data> Publish an arbitrary event (JSON or text)\n\
             \x20\x20!copy                  Copy command output (or selected event) to clipboard\n\
             \x20\x20!editor                Compose input in $EDITOR (for long events / multi-line)\n\
             \x20\x20!page                  Open command output in $PAGER (for long !history)\n\
             \x20\x20!clear                 Clear local events/telemetry; keep sessions\n\
             \x20\x20!exit | !quit          Quit (same as Esc)\n\
             \n\
             DIRECT MESSAGES:\n\
             \x20\x20@agent <msg>           Steering message in the active session\n\
             \x20\x20/steer @agent <msg>    Same as @ (completion includes agent names)\n\
             \x20\x20/queue @agent <msg>    Queue in the active session\n\
             \n\
             Plain text is published as the configured submit topic.\n\
             \n\
             KEYS:  Ctrl-N/R/X · Ctrl-K commands · F1/F2/F3/F4 panels · Tab/Shift-Tab focus or complete\n\
             \x20\x20\x20\x20\x20\x20Agents focus: ↑/↓ select · Enter tail · h history · m/M message/queue · s status\n\
             \x20\x20\x20\x20\x20\x20Events focus: ↑/↓ select · empty Enter opens details · Esc closes\n\
             \x20\x20\x20\x20\x20\x20Completions: ↑/↓ select · Enter accepts · Tab common-prefix completes\n\
             \x20\x20\x20\x20\x20\x20Composer focus: ↑/↓ history · Shift+Enter or Alt+Enter inserts a newline\n\
             \x20\x20\x20\x20\x20\x20PgUp/PgDn scroll long drafts, output, or tail · wheel over the composer scrolls it · Esc dismisses"
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

    async fn cmd_submit(&mut self, rest: &str) {
        let mut parts = rest.splitn(2, char::is_whitespace);
        let topic = parts.next().unwrap_or("").trim();
        let data = parts.next().unwrap_or("").trim();
        if topic.is_empty() || data.is_empty() {
            self.status_msg = Some("Usage: !submit <topic> <json-or-text>".into());
            return;
        }
        if let Err(e) = self.submit_event(topic, data.to_string()).await {
            self.status_msg = Some(format!("Submit failed: {e}"));
        }
    }

    async fn cmd_new(&mut self, name: &str) {
        let name = match command_value(name) {
            Ok(Some(name)) => name,
            Ok(None) => {
                self.enter_naming_new();
                return;
            }
            Err(e) => {
                self.status_msg = Some(format!("Invalid session name: {e}"));
                return;
            }
        };
        if name.is_empty() {
            self.enter_naming_new();
            return;
        }
        self.create_session(&name).await;
    }

    async fn cmd_rename(&mut self, name: &str) {
        let Some(sid) = self.active_session.clone() else {
            self.status_msg = Some("No active session. Use !new <name> first.".into());
            return;
        };
        let name = match command_value(name) {
            Ok(Some(name)) => name,
            Ok(None) => {
                self.enter_renaming();
                return;
            }
            Err(e) => {
                self.status_msg = Some(format!("Invalid session name: {e}"));
                return;
            }
        };
        if name.is_empty() {
            self.enter_renaming();
            return;
        }
        self.rename_session(&sid, &name).await;
    }

    async fn cmd_delete(&mut self, session: &str) {
        let session = match command_value(session) {
            Ok(value) => value,
            Err(e) => {
                self.status_msg = Some(format!("Invalid session: {e}"));
                return;
            }
        };

        let sid = if let Some(session) = session {
            match self.resolve_session(&session) {
                Some(sid) => sid,
                None => {
                    self.status_msg = Some(format!("Session not found: {session}"));
                    return;
                }
            }
        } else {
            match self.active_session.clone() {
                Some(sid) => sid,
                None => {
                    self.status_msg =
                        Some("No active session. Use !delete <session-id-or-name>.".into());
                    return;
                }
            }
        };
        self.delete_session(&sid).await;
    }

    async fn cmd_tag(&mut self, rest: &str) {
        let label = rest.trim();
        if label.is_empty() {
            self.status_msg = Some("Usage: !tag <label>".into());
            return;
        }
        let Some(tag) = validate_tag(label) else {
            self.status_msg = Some(format!(
                "Invalid tag: {label} (lowercase alphanumeric, dashes, underscores, max 64)"
            ));
            return;
        };
        let Some(sid) = self.active_session.clone() else {
            self.status_msg = Some("No active session. Use !new first.".into());
            return;
        };
        if let Err(e) = self.publish_session_tag(&sid, &tag).await {
            self.status_msg = Some(format!("Tag broadcast failed: {e}"));
        } else {
            self.status_msg = Some(format!("Tagged session with [{tag}]"));
        }
    }

    async fn cmd_untag(&mut self, rest: &str) {
        let label = rest.trim();
        if label.is_empty() {
            self.status_msg = Some("Usage: !untag <label>".into());
            return;
        }
        let Some(tag) = validate_tag(label) else {
            self.status_msg = Some(format!("Invalid tag: {label}"));
            return;
        };
        let Some(sid) = self.active_session.clone() else {
            self.status_msg = Some("No active session.".into());
            return;
        };
        if let Err(e) = self.publish_session_untag(&sid, &tag).await {
            self.status_msg = Some(format!("Untag broadcast failed: {e}"));
        } else {
            self.status_msg = Some(format!("Removed tag [{tag}]"));
        }
    }

    fn cmd_filter(&mut self, rest: &str) {
        let label = rest.trim();
        if label.is_empty() {
            self.tag_filter = None;
            self.status_msg = Some("Tag filter cleared — showing all sessions".into());
            return;
        }
        match validate_tag(label) {
            Some(tag) => {
                self.tag_filter = Some(tag.clone());
                self.status_msg = Some(format!("Filtering sessions by tag [{tag}]"));
            }
            None => {
                self.status_msg = Some(format!("Invalid tag: {label}"));
            }
        }
    }

    async fn cmd_bookmark(&mut self, rest: &str) {
        let Some(sid) = self.active_session.clone() else {
            self.status_msg = Some("No active session.".into());
            return;
        };
        let Some((idx, event)) = self.selected_event_with_index() else {
            self.status_msg = Some("Select an event first (↑/↓).".into());
            return;
        };
        let label = if rest.is_empty() {
            None
        } else {
            match validate_bookmark_label(rest) {
                Ok(l) => Some(l),
                Err(e) => {
                    self.status_msg = Some(format!("Invalid label: {e}"));
                    return;
                }
            }
        };
        let target_id = event.event_id.clone();
        let token = match mint_actor_token(
            "console-user",
            &["workspace:read", "workspace:write"],
            &sid,
            &self.signing_key,
            900,
        ) {
            Ok(t) => t,
            Err(e) => {
                self.status_msg = Some(format!("Token error: {e}"));
                return;
            }
        };
        let env = Envelope::build(
            EVENT_BOOKMARKED,
            "agora-console",
            0,
            token,
            sid,
            serde_json::json!({
                "targetEventId": target_id,
                "label": label,
                "actor": "agora-console",
            }),
            None,
            vec![],
        );
        match self.bus.publish(&env).await {
            Ok(()) => self.status_msg = Some(format!("Bookmarked event {}", idx + 1)),
            Err(e) => self.status_msg = Some(format!("Bookmark failed: {e}")),
        }
    }

    async fn cmd_unbookmark(&mut self, _rest: &str) {
        let Some((idx, event)) = self.selected_event_with_index() else {
            self.status_msg = Some("Select an event first (↑/↓).".into());
            return;
        };
        let sid = self
            .active_session
            .clone()
            .unwrap_or_else(|| event.context.session_id.clone());
        let target_id = event.event_id.clone();
        let token = match mint_actor_token(
            "console-user",
            &["workspace:read", "workspace:write"],
            &sid,
            &self.signing_key,
            900,
        ) {
            Ok(t) => t,
            Err(e) => {
                self.status_msg = Some(format!("Token error: {e}"));
                return;
            }
        };
        let env = Envelope::build(
            EVENT_UNBOOKMARKED,
            "agora-console",
            0,
            token,
            sid,
            serde_json::json!({
                "targetEventId": target_id,
                "actor": "agora-console",
            }),
            None,
            vec![],
        );
        match self.bus.publish(&env).await {
            Ok(()) => self.status_msg = Some(format!("Unbookmarked event {}", idx + 1)),
            Err(e) => self.status_msg = Some(format!("Unbookmark failed: {e}")),
        }
    }

    fn cmd_bookmarks(&mut self) {
        if self.bookmarks.is_empty() {
            self.status_msg = Some("No bookmarks yet.".into());
            return;
        }
        let mut out = format!("Bookmarks ({}):\n\n", self.bookmarks.len());
        for (event_id, label) in &self.bookmarks {
            let label_str = label.as_deref().unwrap_or("(no label)");
            out.push_str(&format!("  {} · {}\n", short_id(event_id, 24), label_str));
        }
        self.set_output(out);
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
            out.push_str("  (no events yet — submit an event to begin)\n");
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
        match self.agent_timeline_output(agent, false) {
            Ok(out) => self.set_output(out),
            Err(msg) => self.status_msg = Some(msg),
        }
    }

    fn cmd_tail(&mut self, agent: &str) {
        if agent.is_empty() {
            self.status_msg = Some("Usage: !tail <agent-name>".into());
            return;
        }
        match self.agent_timeline_output(agent, true) {
            Ok(out) => {
                self.set_tail_output(agent, out);
                self.status_msg = Some(format!(
                    "Tailing {agent} in the active session · PgUp/PgDn scroll · !history freezes"
                ));
            }
            Err(msg) => self.status_msg = Some(msg),
        }
    }

    fn agent_timeline_output(
        &self,
        agent: &str,
        live: bool,
    ) -> std::result::Result<String, String> {
        let Some(sid) = self.active_session.clone() else {
            return Err("No active session.".into());
        };

        let subscribed: Vec<String> = self
            .agents
            .get(agent)
            .map(|m| m.subscribes_to.clone())
            .unwrap_or_default();

        #[derive(Clone)]
        enum Item {
            Received(Envelope),
            Prompt(String),
            ResponseStream(String),
            Response(String),
            Published(Envelope),
            Other(TelemetryEntry),
        }

        struct StreamAccumulator {
            timestamp: String,
            sequence: u64,
            text: String,
        }

        let mut items: Vec<(String, u64, Item)> = Vec::new();

        for e in &self.events {
            if e.context.session_id != sid {
                continue;
            }
            if e.sender.agent_name == agent {
                items.push((e.timestamp.clone(), 0, Item::Published(e.clone())));
            } else if subscribed.iter().any(|pat| topic_matches(pat, &e.topic)) {
                items.push((e.timestamp.clone(), 0, Item::Received(e.clone())));
            }
        }

        let mut chunked_triggers = HashSet::new();
        let mut streams: BTreeMap<String, StreamAccumulator> = BTreeMap::new();

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
                    items.push((t.timestamp.clone(), 0, Item::Prompt(text)));
                }
                "response_chunk" => {
                    let chunk = t
                        .telemetry
                        .get("chunk")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    if chunk.is_empty() {
                        continue;
                    }
                    let key = trigger
                        .clone()
                        .unwrap_or_else(|| format!("{}:{}", t.timestamp, streams.len()));
                    chunked_triggers.insert(key.clone());
                    let entry = streams.entry(key).or_insert_with(|| StreamAccumulator {
                        timestamp: t.timestamp.clone(),
                        sequence: 0,
                        text: String::new(),
                    });
                    entry.sequence = t
                        .telemetry
                        .get("sequence")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(entry.sequence.saturating_add(1));
                    entry.text.push_str(chunk);
                }
                "response_received" | "response" => {
                    if trigger
                        .as_ref()
                        .is_some_and(|trigger| chunked_triggers.contains(trigger))
                    {
                        continue;
                    }
                    let text = t
                        .telemetry
                        .get("response")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    items.push((t.timestamp.clone(), 0, Item::Response(text)));
                }
                _ => items.push((t.timestamp.clone(), 0, Item::Other(t.clone()))),
            }
        }

        for stream in streams.into_values() {
            items.push((
                stream.timestamp,
                stream.sequence,
                Item::ResponseStream(stream.text),
            ));
        }

        items.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        let label = self.display_name(&sid);
        let mut out = if live {
            format!("Tail: {agent} in \"{label}\"\n")
        } else {
            format!("History: {agent} in \"{label}\"\n")
        };
        let mode = if live {
            "live ACP output · updates as chunks arrive"
        } else {
            "conversation timeline"
        };
        out.push_str(&format!(
            "  {} item(s) · {mode} · PgUp/PgDn or mouse wheel to scroll\n\n",
            items.len()
        ));

        if items.is_empty() {
            out.push_str("  (no activity yet — submit an event, or wait for the agent to react)\n");
        }

        for (_, _, item) in items {
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
                Item::Prompt(text) => {
                    out.push_str("·····  prompt to ACP  ·····\n");
                    push_indented(&mut out, &text, "  ");
                    out.push('\n');
                }
                Item::ResponseStream(text) => {
                    out.push_str("·····  response from ACP (streaming)  ·····\n");
                    push_indented(&mut out, &text, "  ");
                    out.push('\n');
                }
                Item::Response(text) => {
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

        Ok(out)
    }

    fn cmd_pending(&mut self, rest: &str) {
        let arg = rest.trim();
        if arg == "next" || arg == "jump" {
            self.jump_to_pending_interaction();
            return;
        }

        let pending = self.pending_interactions();
        if pending.is_empty() {
            self.status_msg = Some("No pending human input or tool approvals.".into());
            return;
        }

        let mut out = format!(
            "Pending human input / tool approvals ({})\n\n",
            pending.len()
        );
        for item in pending {
            out.push_str(&format!(
                "{}  {}  {} in {}\n",
                time_only(&item.timestamp),
                item.kind_label(),
                item.agent_name,
                self.display_name(&item.session_id)
            ));
            push_indented(&mut out, &item.question, "  ");
            out.push('\n');
        }
        out.push_str("Run !pending next to jump to the first pending request.\n");
        self.set_output(out);
    }

    async fn submit_event(&mut self, topic: &str, input: String) -> Result<()> {
        let topic = topic.trim();
        if topic.is_empty() {
            self.status_msg = Some("Submit topic cannot be empty".into());
            return Ok(());
        }

        // Topology-aware validation. If the catalog is non-empty and the
        // topic isn't in it, refuse — almost certainly a typo. (An empty
        // catalog means we couldn't load the topology at startup; fall
        // through to legacy behavior so we don't block the user.)
        if !self.topology.known.is_empty() && !self.topology.knows(topic) {
            let hint = match self.topology.matching(topic).first() {
                Some(near) => format!(" Did you mean `{near}`?"),
                None => String::new(),
            };
            self.status_msg = Some(format!(
                "Unknown topic `{topic}` — not declared in topology.{hint}"
            ));
            return Ok(());
        }

        let data = event_data_from_input(&input, &self.submit_field)?;
        let session_id = self
            .active_session
            .clone()
            .unwrap_or_else(|| format!("sess_{}", ulid::Ulid::new()));

        // Derive scopes from the topology so the token satisfies whatever
        // subscribers declared as `required_scopes`. Fall back to the
        // legacy read/write pair when the topology gave us nothing
        // (e.g. publishing a topic with no subscribers).
        let catalog_scopes: Vec<&str> = self
            .topology
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
        let token = mint_actor_token("console-user", scopes, &session_id, &self.signing_key, 900)?;
        let env = Envelope::build(
            topic,
            "agora-console",
            0,
            token,
            session_id.clone(),
            data,
            None,
            vec![],
        );
        match self.bus.publish(&env).await {
            Ok(()) => {
                self.active_session = Some(session_id.clone());
                let label = self.display_name(&session_id);
                self.status_msg = Some(format!("Submitted event {topic} to {label}"));
            }
            Err(e) => self.status_msg = Some(format!("Submit failed: {e}")),
        }
        Ok(())
    }

    async fn submit_human_response(
        &mut self,
        answer: String,
        correlation_id: String,
        session_id: String,
    ) -> Result<()> {
        let token = mint_actor_token(
            "console-user",
            &["workspace:read", "workspace:write"],
            &session_id,
            &self.signing_key,
            900,
        )?;
        let env = Envelope::build(
            HUMAN_INTERACTION_RESPONSE,
            "agora-console",
            0,
            token,
            session_id,
            serde_json::json!({
                "correlationId": correlation_id,
                "answer": answer,
                "respondedBy": "agora-console",
            }),
            None,
            vec![],
        );
        match self.bus.publish(&env).await {
            Ok(()) => self.status_msg = Some("Response sent".into()),
            Err(e) => self.status_msg = Some(format!("Response failed: {e}")),
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

        let Some(session_id) = self.active_session.clone() else {
            self.status_msg = Some(
                "Direct messages require an active session. Create/select a session first.".into(),
            );
            return Ok(());
        };
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
            session_id.clone(),
            serde_json::json!({
                "messageType": msg_type,
                "recipient": agent,
                "sessionId": session_id,
                "message": message,
            }),
            None,
            vec![],
        );
        match self.bus.publish(&env).await {
            Ok(()) => {
                self.active_session = Some(session_id);
                self.status_msg = Some(format!("[{msg_type}] @{agent}: {message}"));
            }
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

    async fn publish_session_tag(&self, session_id: &str, tag: &str) -> Result<()> {
        let token = mint_actor_token(
            "console-user",
            &["workspace:read", "workspace:write"],
            session_id,
            &self.signing_key,
            900,
        )?;
        let env = Envelope::build(
            SESSION_TAGGED,
            "agora-console",
            0,
            token,
            session_id,
            serde_json::json!({ "tag": tag, "actor": "agora-console" }),
            None,
            vec![],
        );
        self.bus.publish(&env).await
    }

    async fn publish_session_untag(&self, session_id: &str, tag: &str) -> Result<()> {
        let token = mint_actor_token(
            "console-user",
            &["workspace:read", "workspace:write"],
            session_id,
            &self.signing_key,
            900,
        )?;
        let env = Envelope::build(
            SESSION_UNTAGGED,
            "agora-console",
            0,
            token,
            session_id,
            serde_json::json!({ "tag": tag, "actor": "agora-console" }),
            None,
            vec![],
        );
        self.bus.publish(&env).await
    }
}

fn response_correlation_id(event: &Envelope) -> Option<&str> {
    event
        .data
        .get("correlationId")
        .or_else(|| event.data.get("correlation_id"))
        .and_then(|v| v.as_str())
}

fn pending_interactions_from_events(
    events: &[Envelope],
    sessions: &BTreeMap<String, SessionInfo>,
) -> Vec<PendingInteraction> {
    let mut pending = Vec::new();
    let mut resolved = HashSet::new();

    for event in events {
        if event.topic == HUMAN_INTERACTION_RESPONSE {
            if let Some(correlation_id) = response_correlation_id(event) {
                resolved.insert(correlation_id.to_string());
                pending.retain(|item: &PendingInteraction| item.event_id != correlation_id);
            }
            continue;
        }

        if event.topic != HUMAN_INTERACTION_REQUEST {
            continue;
        }
        if resolved.contains(&event.event_id) {
            continue;
        }
        if sessions
            .get(&event.context.session_id)
            .is_some_and(|session| session.deleted)
        {
            continue;
        }

        pending.push(PendingInteraction {
            event_id: event.event_id.clone(),
            session_id: event.context.session_id.clone(),
            agent_name: event.sender.agent_name.clone(),
            kind: event
                .data
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("input")
                .to_string(),
            question: event_question(event),
            timestamp: event.timestamp.clone(),
        });
    }

    pending
}

fn event_question(event: &Envelope) -> String {
    event
        .data
        .get("question")
        .and_then(|v| v.as_str())
        .or_else(|| {
            event
                .data
                .pointer("/details/toolCall/title")
                .and_then(|v| v.as_str())
        })
        .unwrap_or("?")
        .to_string()
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

fn load_history_entries(path: &Path) -> Result<Vec<String>> {
    let content = std::fs::read_to_string(path)?;
    let entries: Vec<String> = serde_json::from_str(&content)?;
    Ok(entries
        .into_iter()
        .map(|entry| entry.trim().to_string())
        .filter(|entry| !entry.is_empty())
        .collect())
}

fn write_history_entries(path: &Path, entries: &[String]) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let payload = serde_json::to_string_pretty(entries)?;
    std::fs::write(path, payload)?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct CommandSpec {
    name: &'static str,
    detail: &'static str,
}

const COMMANDS: &[CommandSpec] = &[
    CommandSpec {
        name: "help",
        detail: "show command reference",
    },
    CommandSpec {
        name: "new",
        detail: "create a session",
    },
    CommandSpec {
        name: "rename",
        detail: "rename active session",
    },
    CommandSpec {
        name: "delete",
        detail: "hide a session",
    },
    CommandSpec {
        name: "tag",
        detail: "tag active session",
    },
    CommandSpec {
        name: "untag",
        detail: "remove a session tag",
    },
    CommandSpec {
        name: "filter",
        detail: "filter sessions by tag",
    },
    CommandSpec {
        name: "bookmark",
        detail: "bookmark selected event",
    },
    CommandSpec {
        name: "unbookmark",
        detail: "remove selected event bookmark",
    },
    CommandSpec {
        name: "bookmarks",
        detail: "list bookmarks",
    },
    CommandSpec {
        name: "agents",
        detail: "list agents in active session",
    },
    CommandSpec {
        name: "status",
        detail: "show agent status",
    },
    CommandSpec {
        name: "history",
        detail: "agent timeline snapshot",
    },
    CommandSpec {
        name: "tail",
        detail: "open live agent output",
    },
    CommandSpec {
        name: "follow",
        detail: "alias for tail",
    },
    CommandSpec {
        name: "pending",
        detail: "show pending human requests",
    },
    CommandSpec {
        name: "panel",
        detail: "toggle panels",
    },
    CommandSpec {
        name: "submit",
        detail: "publish explicit event topic",
    },
    CommandSpec {
        name: "clear",
        detail: "clear local view",
    },
    CommandSpec {
        name: "copy",
        detail: "copy visible output",
    },
    CommandSpec {
        name: "editor",
        detail: "edit composer in $EDITOR",
    },
    CommandSpec {
        name: "page",
        detail: "open output in $PAGER",
    },
    CommandSpec {
        name: "exit",
        detail: "quit console",
    },
    CommandSpec {
        name: "quit",
        detail: "quit console",
    },
];

#[cfg(test)]
fn completion_suggestions(
    input: &str,
    agents: &BTreeMap<String, AgentManifest>,
    topology: &agora_core::TopicCatalog,
    sessions: &BTreeMap<String, SessionInfo>,
    session_names: &HashMap<String, String>,
    tags: &[String],
    bookmark_labels: &[String],
) -> Vec<String> {
    completion_items(
        input,
        agents,
        topology,
        sessions,
        session_names,
        tags,
        bookmark_labels,
    )
    .into_iter()
    .map(|item| item.value)
    .take(6)
    .collect()
}

fn complete_input(
    input: &str,
    agents: &BTreeMap<String, AgentManifest>,
    topology: &agora_core::TopicCatalog,
    sessions: &BTreeMap<String, SessionInfo>,
    session_names: &HashMap<String, String>,
    tags: &[String],
    bookmark_labels: &[String],
) -> Option<String> {
    let trimmed = input.trim_start();
    let candidates = completion_items(
        trimmed,
        agents,
        topology,
        sessions,
        session_names,
        tags,
        bookmark_labels,
    )
    .into_iter()
    .map(|item| item.value)
    .collect::<Vec<_>>();
    if candidates.is_empty() {
        return None;
    }

    if let Some(stripped) = trimmed.strip_prefix('@') {
        if stripped.contains(char::is_whitespace) {
            return None;
        }
        return complete_token("@", stripped, candidates, true);
    }

    for prefix in ["/steer @", "/queue @"] {
        if let Some(partial) = trimmed.strip_prefix(prefix) {
            if partial.contains(char::is_whitespace) {
                return None;
            }
            return complete_token(prefix, partial, candidates, true);
        }
    }

    let command = trimmed.strip_prefix('!')?;
    if !command.contains(char::is_whitespace) {
        return complete_token("!", command, candidates, true);
    }

    let (cmd, rest) = command.split_once(char::is_whitespace)?;
    let rest = rest.trim_start();
    match cmd {
        "tail" | "follow" | "history" | "status" => {
            complete_token(&format!("!{cmd} "), rest, candidates, true)
        }
        "submit" => {
            if rest.contains(char::is_whitespace) {
                None
            } else {
                complete_token("!submit ", rest, candidates, true)
            }
        }
        "panel" | "panels" | "toggle" => {
            complete_token(&format!("!{cmd} "), rest, candidates, false)
        }
        "delete" | "del" => complete_token(&format!("!{cmd} "), rest, candidates, false),
        "tag" | "untag" | "filter" | "bookmark" => {
            complete_token(&format!("!{cmd} "), rest, candidates, false)
        }
        _ => None,
    }
}

fn completion_items(
    input: &str,
    agents: &BTreeMap<String, AgentManifest>,
    topology: &agora_core::TopicCatalog,
    sessions: &BTreeMap<String, SessionInfo>,
    session_names: &HashMap<String, String>,
    tags: &[String],
    bookmark_labels: &[String],
) -> Vec<CompletionItem> {
    let trimmed = input.trim_start();
    if let Some(stripped) = trimmed.strip_prefix('@') {
        if stripped.contains(char::is_whitespace) {
            return Vec::new();
        }
        return ranked_items(
            stripped,
            agents.keys().map(|name| CompletionItem::new(name, "agent")),
        );
    }

    for prefix in ["/steer @", "/queue @"] {
        if let Some(partial) = trimmed.strip_prefix(prefix) {
            if partial.contains(char::is_whitespace) {
                return Vec::new();
            }
            return ranked_items(
                partial,
                agents.keys().map(|name| CompletionItem::new(name, "agent")),
            );
        }
    }

    let Some(command) = trimmed.strip_prefix('!') else {
        return Vec::new();
    };
    if !command.contains(char::is_whitespace) {
        return ranked_items(
            command,
            COMMANDS
                .iter()
                .map(|spec| CompletionItem::new(spec.name, spec.detail)),
        );
    }

    let Some((cmd, rest)) = command.split_once(char::is_whitespace) else {
        return Vec::new();
    };
    let rest = rest.trim_start();
    match cmd {
        "tail" | "follow" | "history" | "status" => ranked_items(
            rest,
            agents.keys().map(|name| CompletionItem::new(name, "agent")),
        ),
        "submit" if !rest.contains(char::is_whitespace) => ranked_items(
            rest,
            topology
                .known
                .iter()
                .map(|topic| CompletionItem::new(topic, "topic")),
        ),
        "panel" | "panels" | "toggle" => ranked_items(
            rest,
            ["sessions", "events", "agents", "detail", "all"]
                .into_iter()
                .map(|panel| CompletionItem::new(panel, "panel")),
        ),
        "delete" | "del" => {
            let mut values: Vec<CompletionItem> = Vec::new();
            for session in sessions.values().filter(|session| !session.deleted) {
                values.push(CompletionItem::new(&session.session_id, "session id"));
                if let Some(name) = session_names.get(&session.session_id) {
                    values.push(CompletionItem::new(name, "session name"));
                }
            }
            ranked_items(rest, values.into_iter())
        }
        "tag" | "untag" | "filter" => {
            ranked_items(rest, tags.iter().map(|tag| CompletionItem::new(tag, "tag")))
        }
        "bookmark" => ranked_items(
            rest,
            bookmark_labels
                .iter()
                .map(|label| CompletionItem::new(label, "bookmark label")),
        ),
        _ => Vec::new(),
    }
}

impl CompletionItem {
    fn new(value: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            detail: detail.into(),
        }
    }
}

fn ranked_items(query: &str, values: impl Iterator<Item = CompletionItem>) -> Vec<CompletionItem> {
    let query_empty = query.is_empty();
    let mut ranked: Vec<(u8, usize, CompletionItem)> = values
        .enumerate()
        .filter_map(|(idx, item)| {
            completion_score(query, &item.value).map(|score| (score, idx, item))
        })
        .collect();
    ranked.sort_by(|(a_score, a_idx, a), (b_score, b_idx, b)| {
        let base = a_score.cmp(b_score);
        if query_empty {
            base.then_with(|| a_idx.cmp(b_idx))
        } else {
            base.then_with(|| a.value.to_lowercase().cmp(&b.value.to_lowercase()))
        }
    });
    ranked.dedup_by(|(_, _, a), (_, _, b)| a.value == b.value);
    ranked
        .into_iter()
        .map(|(_, _, item)| item)
        .take(8)
        .collect()
}

fn completion_score(query: &str, value: &str) -> Option<u8> {
    if query.is_empty() {
        return Some(0);
    }
    let query = query.to_lowercase();
    let value = value.to_lowercase();
    if value == query {
        Some(0)
    } else if value.starts_with(&query) {
        Some(1)
    } else if value.contains(&query) {
        Some(2)
    } else if fuzzy_subsequence(&query, &value) {
        Some(3)
    } else {
        None
    }
}

fn fuzzy_subsequence(query: &str, value: &str) -> bool {
    let mut chars = value.chars();
    query.chars().all(|needle| chars.any(|c| c == needle))
}

fn apply_completion_value(input: &str, value: &str) -> Option<String> {
    let trimmed = input.trim_start();
    let leading = &input[..input.len() - trimmed.len()];
    if let Some(stripped) = trimmed.strip_prefix('@') {
        if stripped.contains(char::is_whitespace) {
            return None;
        }
        return Some(format!("{leading}@{value} "));
    }

    for prefix in ["/steer @", "/queue @"] {
        if let Some(partial) = trimmed.strip_prefix(prefix) {
            if partial.contains(char::is_whitespace) {
                return None;
            }
            return Some(format!("{leading}{prefix}{value} "));
        }
    }

    let command = trimmed.strip_prefix('!')?;
    if !command.contains(char::is_whitespace) {
        return Some(format!("{leading}!{value} "));
    }

    let (cmd, rest) = command.split_once(char::is_whitespace)?;
    let rest = rest.trim_start();
    if rest.contains(char::is_whitespace) {
        return None;
    }
    let suffix = match cmd {
        "submit" | "tail" | "follow" | "history" | "status" => " ",
        _ => "",
    };
    Some(format!("{leading}!{cmd} {value}{suffix}"))
}

fn complete_token(
    before_token: &str,
    partial: &str,
    candidates: Vec<String>,
    trailing_space_on_unique: bool,
) -> Option<String> {
    match candidates.len() {
        0 => None,
        1 => {
            let suffix = if trailing_space_on_unique { " " } else { "" };
            Some(format!("{}{}{}", before_token, candidates[0], suffix))
        }
        _ => {
            let lcp = longest_common_prefix_strings(&candidates);
            if lcp.len() > partial.len() {
                Some(format!("{before_token}{lcp}"))
            } else {
                None
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

fn longest_common_prefix_strings(strs: &[String]) -> String {
    if strs.is_empty() {
        return String::new();
    }
    let refs: Vec<&String> = strs.iter().collect();
    longest_common_prefix(&refs)
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
    use super::{
        command_value, complete_input, completion_suggestions, load_history_entries,
        pending_interactions_from_events, topic_matches, write_history_entries, SessionInfo,
    };
    use agora_core::{topics::*, AgentManifest, Envelope, TopicCatalog};
    use std::collections::{BTreeMap, HashMap};

    struct CompletionFixture {
        agents: BTreeMap<String, AgentManifest>,
        topology: TopicCatalog,
        sessions: BTreeMap<String, SessionInfo>,
        session_names: HashMap<String, String>,
        tags: Vec<String>,
        bookmark_labels: Vec<String>,
    }

    #[test]
    fn nats_wildcard_matching() {
        assert!(topic_matches(
            "workspace.event.submitted",
            "workspace.event.submitted"
        ));
        assert!(topic_matches("workspace.>", "workspace.event.submitted"));
        assert!(topic_matches("workspace.>", "workspace.design.finalized"));
        assert!(topic_matches("*.changed", "code.changed"));
        assert!(!topic_matches("*.changed", "code.changed.again"));
        assert!(!topic_matches("workspace.event.submitted", "code.changed"));
        assert!(topic_matches(">", "anything.goes.here"));
    }

    #[test]
    fn command_value_accepts_quoted_session_names() {
        assert_eq!(
            command_value(r#""AWS redelivery fix smoke""#)
                .unwrap()
                .as_deref(),
            Some("AWS redelivery fix smoke")
        );
        assert_eq!(
            command_value("AWS redelivery fix smoke")
                .unwrap()
                .as_deref(),
            Some("AWS redelivery fix smoke")
        );
        assert!(command_value(r#""AWS redelivery fix smoke" extra"#).is_err());
        assert!(command_value("").unwrap().is_none());
    }

    #[test]
    fn completion_suggestions_cover_commands_agents_topics_and_sessions() {
        let fixture = completion_fixture();

        assert_eq!(
            completion_suggestions(
                "!tai",
                &fixture.agents,
                &fixture.topology,
                &fixture.sessions,
                &fixture.session_names,
                &fixture.tags,
                &fixture.bookmark_labels
            ),
            vec!["tail"]
        );
        assert_eq!(
            completion_suggestions(
                "!submit workspace.e",
                &fixture.agents,
                &fixture.topology,
                &fixture.sessions,
                &fixture.session_names,
                &fixture.tags,
                &fixture.bookmark_labels
            ),
            vec!["workspace.event.submitted"]
        );
        assert_eq!(
            completion_suggestions(
                "!delete AWS",
                &fixture.agents,
                &fixture.topology,
                &fixture.sessions,
                &fixture.session_names,
                &fixture.tags,
                &fixture.bookmark_labels
            ),
            vec!["AWS redelivery fix smoke", "sess_aws"]
        );
        assert_eq!(
            completion_suggestions(
                "!delete sess",
                &fixture.agents,
                &fixture.topology,
                &fixture.sessions,
                &fixture.session_names,
                &fixture.tags,
                &fixture.bookmark_labels
            ),
            vec!["sess_aws"]
        );
        assert_eq!(
            completion_suggestions(
                "/steer @back",
                &fixture.agents,
                &fixture.topology,
                &fixture.sessions,
                &fixture.session_names,
                &fixture.tags,
                &fixture.bookmark_labels
            ),
            vec!["backend-coder"]
        );
        assert_eq!(
            completion_suggestions(
                "!filter prod",
                &fixture.agents,
                &fixture.topology,
                &fixture.sessions,
                &fixture.session_names,
                &fixture.tags,
                &fixture.bookmark_labels
            ),
            vec!["production"]
        );
        assert_eq!(
            completion_suggestions(
                "!bookmark smoke",
                &fixture.agents,
                &fixture.topology,
                &fixture.sessions,
                &fixture.session_names,
                &fixture.tags,
                &fixture.bookmark_labels
            ),
            vec!["smoke-test"]
        );
    }

    #[test]
    fn complete_input_expands_unique_matches_and_common_prefixes() {
        let fixture = completion_fixture();

        assert_eq!(
            complete_input(
                "!tai",
                &fixture.agents,
                &fixture.topology,
                &fixture.sessions,
                &fixture.session_names,
                &fixture.tags,
                &fixture.bookmark_labels
            ),
            Some("!tail ".into())
        );
        assert_eq!(
            complete_input(
                "@back",
                &fixture.agents,
                &fixture.topology,
                &fixture.sessions,
                &fixture.session_names,
                &fixture.tags,
                &fixture.bookmark_labels
            ),
            Some("@backend-coder ".into())
        );
        assert_eq!(
            complete_input(
                "!panel ag",
                &fixture.agents,
                &fixture.topology,
                &fixture.sessions,
                &fixture.session_names,
                &fixture.tags,
                &fixture.bookmark_labels
            ),
            Some("!panel agents".into())
        );

        let mut ambiguous_agents = fixture.agents.clone();
        ambiguous_agents.insert(
            "backend-reviewer".into(),
            AgentManifest::new("backend-reviewer", 4010, vec![], vec![], vec![]),
        );
        assert_eq!(
            complete_input(
                "@back",
                &ambiguous_agents,
                &fixture.topology,
                &fixture.sessions,
                &fixture.session_names,
                &fixture.tags,
                &fixture.bookmark_labels
            ),
            Some("@backend-".into())
        );
    }

    #[test]
    fn apply_completion_value_accepts_selected_items() {
        assert_eq!(
            super::apply_completion_value("!delete AWS", "AWS redelivery fix smoke"),
            Some("!delete AWS redelivery fix smoke".into())
        );
        assert_eq!(
            super::apply_completion_value("/queue @back", "backend-coder"),
            Some("/queue @backend-coder ".into())
        );
    }

    #[test]
    fn history_entries_round_trip_trimmed_non_empty_values() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("history.json");
        write_history_entries(
            &path,
            &[
                "  !help  ".to_string(),
                String::new(),
                "workspace event".to_string(),
            ],
        )
        .unwrap();

        assert_eq!(
            load_history_entries(&path).unwrap(),
            vec!["!help", "workspace event"]
        );
    }

    #[test]
    fn pending_interactions_track_unanswered_requests() {
        let request = envelope(
            HUMAN_INTERACTION_REQUEST,
            "backend-coder",
            "sess_1",
            serde_json::json!({
                "kind": "tool_approval",
                "question": "Approve cargo test?",
            }),
        );
        let pending =
            pending_interactions_from_events(std::slice::from_ref(&request), &BTreeMap::new());

        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].agent_name, "backend-coder");
        assert_eq!(pending[0].session_id, "sess_1");
        assert_eq!(pending[0].kind_label(), "tool approval");

        let response = envelope(
            HUMAN_INTERACTION_RESPONSE,
            "agora-console",
            "sess_1",
            serde_json::json!({
                "correlationId": request.event_id,
                "answer": "allow-once",
            }),
        );
        let pending = pending_interactions_from_events(&[request, response], &BTreeMap::new());

        assert!(pending.is_empty());
    }

    #[test]
    fn pending_interactions_ignore_deleted_sessions() {
        let request = envelope(
            HUMAN_INTERACTION_REQUEST,
            "quality-assurance",
            "sess_deleted",
            serde_json::json!({ "question": "Need input" }),
        );
        let mut sessions = BTreeMap::new();
        sessions.insert(
            "sess_deleted".into(),
            SessionInfo {
                session_id: "sess_deleted".into(),
                started_at: "2026-05-30T00:00:00Z".into(),
                last_topic: String::new(),
                event_count: 0,
                deleted: true,
                deleted_at: Some("2026-05-30T00:01:00Z".into()),
                tags: vec![],
            },
        );

        let pending = pending_interactions_from_events(&[request], &sessions);

        assert!(pending.is_empty());
    }

    fn envelope(topic: &str, sender: &str, session_id: &str, data: serde_json::Value) -> Envelope {
        Envelope::build(topic, sender, 0, "tok", session_id, data, None, vec![])
    }

    fn completion_fixture() -> CompletionFixture {
        let mut agents = BTreeMap::new();
        agents.insert(
            "backend-coder".into(),
            AgentManifest::new("backend-coder", 4001, vec![], vec![], vec![]),
        );
        agents.insert(
            "quality-assurance".into(),
            AgentManifest::new("quality-assurance", 4002, vec![], vec![], vec![]),
        );

        let mut topology = TopicCatalog::default();
        topology.known.insert(WORKSPACE_EVENT_SUBMITTED.into());

        let mut sessions = BTreeMap::new();
        sessions.insert(
            "sess_aws".into(),
            SessionInfo {
                session_id: "sess_aws".into(),
                started_at: "2026-05-30T00:00:00Z".into(),
                last_topic: WORKSPACE_EVENT_SUBMITTED.into(),
                event_count: 1,
                deleted: false,
                deleted_at: None,
                tags: vec!["production".into()],
            },
        );

        let mut session_names = HashMap::new();
        session_names.insert("sess_aws".into(), "AWS redelivery fix smoke".into());

        CompletionFixture {
            agents,
            topology,
            sessions,
            session_names,
            tags: vec!["production".into()],
            bookmark_labels: vec!["smoke-test".into()],
        }
    }
}
