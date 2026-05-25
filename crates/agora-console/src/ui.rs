use agora_core::manifest::AgentStatus;
use ratatui::{
    prelude::*,
    widgets::{Block, Borders, List, ListItem, Paragraph, Wrap},
};

use crate::app::{App, InputMode};

const COMPOSER_MIN_HEIGHT: u16 = 7;
const COMPOSER_MAX_HEIGHT: u16 = 18;
const EVENT_LIST_WITH_DETAIL_WIDTH: u16 = 46;
const EVENT_LIST_WITH_DETAIL_MIN_WIDTH: u16 = 32;
const EVENT_DETAIL_MIN_WIDTH: u16 = 48;

pub fn render(f: &mut Frame, app: &mut App) {
    let area = f.area();

    let main_visible = app.panels.sessions || app.panels.events || app.panels.agents;
    let output_height = if app.panels.detail && app.command_output.is_some() {
        (area.height / 3).clamp(10, 16)
    } else {
        0
    };
    let content_width = area.width.saturating_sub(4).max(1);
    let input_lines = visual_line_count(&app.input, content_width);
    let max_for_terminal = ((area.height * 2) / 5).clamp(COMPOSER_MIN_HEIGHT, COMPOSER_MAX_HEIGHT);
    let input_height = (input_lines + 3)
        .clamp(COMPOSER_MIN_HEIGHT, COMPOSER_MAX_HEIGHT)
        .min(max_for_terminal);
    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // status bar
            if main_visible {
                Constraint::Min(6)
            } else {
                Constraint::Length(0)
            },
            Constraint::Length(output_height), // command output
            Constraint::Length(input_height),  // input (grows for multi-line)
        ])
        .split(area);

    render_status(f, app, outer[0]);

    render_main_panels(f, app, outer[1]);

    if app.panels.detail && app.command_output.is_some() {
        render_command_output(f, app, outer[2]);
    }
    render_input(f, app, outer[3]);
}

fn render_status(f: &mut Frame, app: &App, area: Rect) {
    let left = format!(
        " agora console │ {} │ events:{} telemetry:{} sessions:{} agents:{} ",
        app.bus_url,
        app.events.len(),
        app.telemetry.len(),
        app.sessions.len(),
        app.agents.len(),
    );
    let right = app
        .status_msg
        .clone()
        .unwrap_or_else(|| "Esc to quit".into());

    let line = Line::from(vec![
        Span::styled(
            left,
            Style::default()
                .fg(Color::White)
                .bg(Color::Blue)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("  {right}"),
            Style::default().fg(Color::Gray).bg(Color::Black),
        ),
    ]);
    let para = Paragraph::new(line).style(Style::default().bg(Color::Black));
    f.render_widget(para, area);
}

fn render_main_panels(f: &mut Frame, app: &mut App, area: Rect) {
    if area.height == 0 || area.width == 0 {
        app.events_view_height = 1;
        return;
    }

    let sidebar_visible = app.panels.sessions || app.panels.agents;
    let events_visible = app.panels.events;
    let details_visible = app.panels.detail && app.event_details_open();

    match (sidebar_visible, events_visible, details_visible) {
        (false, false, false) => {
            app.events_view_height = 1;
        }
        (false, true, false) => {
            render_events(f, app, area);
        }
        (true, false, false) => {
            app.events_view_height = 1;
            render_sidebar(f, app, area);
        }
        (false, false, true) => render_event_detail(f, app, area),
        (true, false, true) => {
            app.events_view_height = 1;
            let cols = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Length(34), Constraint::Min(40)])
                .split(area);
            render_sidebar(f, app, cols[0]);
            render_event_detail(f, app, cols[1]);
        }
        (false, true, true) => render_events_and_detail(f, app, area),
        (true, true, true) => {
            let cols = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Length(34), Constraint::Min(40)])
                .split(area);
            render_sidebar(f, app, cols[0]);
            render_events_and_detail(f, app, cols[1]);
        }
        (true, true, false) => {
            let cols = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Length(34), Constraint::Min(40)])
                .split(area);
            render_sidebar(f, app, cols[0]);
            render_events(f, app, cols[1]);
        }
    }
}

fn render_events_and_detail(f: &mut Frame, app: &mut App, area: Rect) {
    let event_width = event_list_width_with_detail(area.width);
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(event_width),
            Constraint::Min(EVENT_DETAIL_MIN_WIDTH),
        ])
        .split(area);
    render_events(f, app, cols[0]);
    render_event_detail(f, app, cols[1]);
}

fn event_list_width_with_detail(width: u16) -> u16 {
    let max_for_events = width.saturating_sub(EVENT_DETAIL_MIN_WIDTH);
    if max_for_events >= EVENT_LIST_WITH_DETAIL_MIN_WIDTH {
        EVENT_LIST_WITH_DETAIL_WIDTH.min(max_for_events)
    } else {
        EVENT_LIST_WITH_DETAIL_MIN_WIDTH.min(width)
    }
}

fn render_sidebar(f: &mut Frame, app: &App, area: Rect) {
    let sessions_visible = app.panels.sessions;
    let agents_visible = app.panels.agents;

    match (sessions_visible, agents_visible) {
        (true, true) => {
            let rows = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)])
                .split(area);
            render_sessions(f, app, rows[0]);
            render_agents(f, app, rows[1]);
        }
        (true, false) => render_sessions(f, app, area),
        (false, true) => render_agents(f, app, area),
        (false, false) => {}
    }
}

fn render_sessions(f: &mut Frame, app: &App, area: Rect) {
    let mut sessions: Vec<&crate::app::SessionInfo> = app
        .sessions
        .values()
        .filter(|session| !session.deleted)
        .filter(|session| match app.tag_filter.as_deref() {
            Some(tag) => session.tags.iter().any(|session_tag| session_tag == tag),
            None => true,
        })
        .collect();
    sessions.sort_by(|a, b| b.started_at.cmp(&a.started_at));

    let block = Block::default()
        .borders(Borders::ALL)
        .title(match app.tag_filter.as_deref() {
            Some(tag) => format!(" Sessions ({}) · tag:{tag} ", sessions.len()),
            None => format!(" Sessions ({}) ", sessions.len()),
        })
        .border_style(Style::default().fg(Color::DarkGray));
    let inner = block.inner(area);
    f.render_widget(block, area);

    if inner.height == 0 || inner.width == 0 {
        return;
    }

    if sessions.is_empty() {
        let message = match app.tag_filter.as_deref() {
            Some(tag) => format!(" No sessions tagged {tag} "),
            None => " No sessions yet ".to_string(),
        };
        let para = Paragraph::new(message).style(Style::default().fg(Color::DarkGray));
        f.render_widget(para, inner);
        return;
    }

    let items: Vec<ListItem> = sessions
        .iter()
        .take(session_item_limit(inner.height))
        .map(|s| {
            let is_active = app.active_session.as_deref() == Some(s.session_id.as_str());
            let display = app.display_name(&s.session_id);
            let marker = if is_active { "▶ " } else { "  " };
            let style = if is_active {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            };
            ListItem::new(Line::from(vec![
                Span::raw(marker),
                Span::styled(display, style),
            ]))
        })
        .collect();

    let list = List::new(items);
    f.render_widget(list, inner);
}

fn session_item_limit(inner_height: u16) -> usize {
    inner_height as usize
}

fn render_events(f: &mut Frame, app: &mut App, area: Rect) {
    let inner = Block::default().borders(Borders::ALL).inner(area);

    // Record viewport height so selection movement can keep the highlighted
    // event visible after terminal resizes.
    app.sync_events_viewport(inner.height);
    let indices = app.active_session_event_indices();
    let total = indices.len();

    let title = match (app.active_session.as_deref(), app.selected_event_position()) {
        (Some(session_id), Some(pos)) => {
            format!(
                " Events: {} ({}/{}) ",
                app.display_name(session_id),
                pos + 1,
                total
            )
        }
        (Some(session_id), None) => {
            format!(" Events: {} ({}) ", app.display_name(session_id), total)
        }
        (None, _) => " Events: no active session ".to_string(),
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .border_style(Style::default().fg(Color::DarkGray));
    f.render_widget(block, area);

    let height = inner.height as usize;
    // Clamp scroll so the pane never goes blank when scrolled past the
    // oldest event. Belt + braces against any path that bumped
    // events_scroll without going through scroll_up.
    let max_scroll = total.saturating_sub(height);
    if (app.events_scroll as usize) > max_scroll {
        app.events_scroll = max_scroll as u16;
    }
    let scroll = app.events_scroll as usize;

    // The newest events sit at the end; auto-scroll keeps the bottom in view.
    let end = total.saturating_sub(scroll);
    let start = end.saturating_sub(height);
    let selected_idx = app.selected_event_index();

    if indices.is_empty() {
        let message = if app.active_session.is_some() {
            " No events in selected session "
        } else {
            " Select a session with Tab or create one with Ctrl-N "
        };
        let para = Paragraph::new(message).style(Style::default().fg(Color::DarkGray));
        f.render_widget(para, inner);
        return;
    }

    let items: Vec<ListItem> = indices[start..end]
        .iter()
        .filter_map(|idx| {
            let e = app.events.get(*idx)?;
            let selected = selected_idx == Some(*idx);
            let bookmarked = app.bookmarks.contains_key(&e.event_id);
            let item = ListItem::new(format_event_line(e, selected, bookmarked));
            Some(if selected {
                item.style(Style::default().add_modifier(Modifier::REVERSED))
            } else {
                item
            })
        })
        .collect();

    let list = List::new(items);
    f.render_widget(list, inner);
}

fn format_event_line(
    e: &agora_core::envelope::Envelope,
    selected: bool,
    bookmarked: bool,
) -> Line<'static> {
    let ts = if e.timestamp.len() >= 19 {
        e.timestamp[11..19].to_string()
    } else {
        e.timestamp.clone()
    };
    let color = topic_color(&e.topic);
    let marker = if selected { "› " } else { "  " };

    let mut spans = vec![
        Span::styled(marker, Style::default().fg(Color::Yellow)),
        Span::styled(ts, Style::default().fg(Color::DarkGray)),
        Span::raw("  "),
        Span::styled(format!("{:30}", e.topic), Style::default().fg(color)),
    ];
    if bookmarked {
        spans.push(Span::styled(" ★", Style::default().fg(Color::Yellow)));
    }
    Line::from(spans)
}

fn topic_color(topic: &str) -> Color {
    match topic.split('.').next().unwrap_or("") {
        "workspace" => Color::Cyan,
        "code" => Color::Yellow,
        "test" => {
            if topic.ends_with("failed") {
                Color::Red
            } else {
                Color::Green
            }
        }
        "security" => {
            if topic.contains("alert") {
                Color::Red
            } else {
                Color::Green
            }
        }
        "human" => Color::Magenta,
        "agent" => Color::Blue,
        "product" => Color::LightBlue,
        "event" => Color::Red,
        _ => Color::White,
    }
}

fn render_agents(f: &mut Frame, app: &App, area: Rect) {
    let items: Vec<ListItem> = app
        .agents
        .values()
        .map(|a| {
            let (icon, color) = match a.observed_status() {
                AgentStatus::Ready => ("●", Color::Green),
                AgentStatus::Busy => ("◐", Color::Yellow),
                AgentStatus::Stale => ("◌", Color::Yellow),
                AgentStatus::Starting => ("◌", Color::Gray),
                AgentStatus::Draining => ("◑", Color::Yellow),
                AgentStatus::Down => ("○", Color::Red),
            };
            ListItem::new(vec![
                Line::from(vec![
                    Span::styled(format!("{icon} "), Style::default().fg(color)),
                    Span::styled(
                        a.agent_name.clone(),
                        Style::default().add_modifier(Modifier::BOLD),
                    ),
                ]),
                Line::from(format!("   :{} · {}", a.port, a.capabilities.join(","))),
            ])
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(" Agents ({}) ", app.agents.len()))
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    f.render_widget(list, area);
}

fn render_command_output(f: &mut Frame, app: &App, area: Rect) {
    if let Some(out) = &app.command_output {
        let total_lines = out.lines().count() as u16;
        let visible = area.height.saturating_sub(2);
        let max_scroll = total_lines.saturating_sub(visible);
        let scroll = app.output_scroll.min(max_scroll);
        let title = if total_lines > visible {
            format!(
                " Command output  [line {}/{}, PgUp/PgDn or wheel · Esc to dismiss] ",
                scroll + 1,
                total_lines
            )
        } else {
            " Command output (Esc to dismiss) ".to_string()
        };
        let block = Block::default()
            .borders(Borders::ALL)
            .title(title)
            .border_style(Style::default().fg(Color::Cyan));
        let para = Paragraph::new(out.clone())
            .block(block)
            .wrap(Wrap { trim: false })
            .scroll((scroll, 0));
        f.render_widget(para, area);
    }
}

fn render_event_detail(f: &mut Frame, app: &App, area: Rect) {
    let (_idx, event) = match app.inspected_event_with_index() {
        Some(event) => event,
        None => return,
    };
    let pos = app.inspected_event_position().unwrap_or(0);
    let total = app.active_session_event_count();

    if app.full_event_details_open() {
        let text = redacted_event_json(event);
        let total_lines = text.lines().count() as u16;
        let visible = area.height.saturating_sub(2);
        let max_scroll = total_lines.saturating_sub(visible);
        let scroll = app.output_scroll.min(max_scroll);
        let title = if total_lines > visible {
            format!(
                " Full event details ({}/{})  [line {}/{}, PgUp/PgDn or wheel · Left collapses · Esc closes] ",
                pos + 1,
                total,
                scroll + 1,
                total_lines
            )
        } else {
            format!(
                " Full event details ({}/{}) · Left collapses · Esc closes ",
                pos + 1,
                total
            )
        };
        let block = Block::default()
            .borders(Borders::ALL)
            .title(title)
            .border_style(Style::default().fg(Color::Cyan));
        let para = Paragraph::new(text)
            .block(block)
            .wrap(Wrap { trim: false })
            .scroll((scroll, 0));
        f.render_widget(para, area);
        return;
    }

    let (title, text) = {
        let e = event;
        let data_pretty =
            serde_json::to_string_pretty(&e.data).unwrap_or_else(|_| e.data.to_string());
        let session_label = app.display_name(&e.context.session_id);
        (
            format!(
                " Event inspector ({}/{}) · Right expands · Esc closes ",
                pos + 1,
                total
            ),
            format!(
                "topic:   {}\nevent:   {}\ntime:    {}\nsession: {}  ({})\nfrom:    {}\ndata:    {}",
                e.topic,
                e.event_id,
                e.timestamp,
                session_label,
                e.context.session_id,
                e.sender.agent_name,
                data_pretty
            ),
        )
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .border_style(Style::default().fg(Color::DarkGray));
    let para = Paragraph::new(text).block(block).wrap(Wrap { trim: false });
    f.render_widget(para, area);
}

fn redacted_event_json(e: &agora_core::envelope::Envelope) -> String {
    let mut value = serde_json::to_value(e).unwrap_or_else(|_| serde_json::json!({}));
    if let Some(token) = value
        .get_mut("security")
        .and_then(|security| security.get_mut("actorToken"))
    {
        *token = serde_json::Value::String("<redacted>".into());
    }
    serde_json::to_string_pretty(&value).unwrap_or_else(|_| e.data.to_string())
}

fn render_input(f: &mut Frame, app: &mut App, area: Rect) {
    let (base_title, prompt_char, border) = match app.mode {
        InputMode::Normal => {
            if let Some((_, _, question)) = app.active_interaction_request() {
                let preview = if question.len() > 60 {
                    format!("{}…", &question[..60])
                } else {
                    question
                };
                (
                    format!(" Respond: {preview} · Enter sends · select another event to cancel "),
                    "›",
                    Color::Cyan,
                )
            } else {
                let scope = match &app.active_session {
                    Some(sid) => format!("active: {}", app.display_name(sid)),
                    None => "no active session — submit creates one".into(),
                };
                (format!(" Compose · {scope} "), "›", Color::Yellow)
            }
        }
        InputMode::NamingNew => (
            " Name new session (Enter to create · Esc to cancel) ".into(),
            "✎",
            Color::Green,
        ),
        InputMode::Renaming => {
            let target = app
                .rename_target
                .as_ref()
                .map(|s| app.display_name(s))
                .unwrap_or_default();
            (
                format!(" Rename {target} (Enter to save · empty to clear · Esc to cancel) "),
                "✎",
                Color::Magenta,
            )
        }
    };

    let inner = Rect {
        x: area.x.saturating_add(1),
        y: area.y.saturating_add(1),
        width: area.width.saturating_sub(2),
        height: area.height.saturating_sub(2).max(1),
    };
    let content_width = inner.width.saturating_sub(2).max(1);
    let total_lines = visual_line_count(&app.input, content_width);
    app.set_input_viewport(inner.y, inner.height, content_width, total_lines);
    let max_scroll = total_lines.saturating_sub(inner.height);
    let bottom_offset = app.input_scroll.min(max_scroll);
    let scroll = max_scroll.saturating_sub(bottom_offset);
    let title = if total_lines > inner.height {
        format!(
            "{base_title}[line {}/{total_lines} · PgUp/PgDn or wheel] ",
            scroll + 1
        )
    } else {
        base_title
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .border_style(Style::default().fg(border));
    f.render_widget(block, area);

    // Render as multi-line, with the prompt prefix on line 0 only.
    let mut lines: Vec<Line> = Vec::new();
    let prompt_span = Span::styled(
        format!("{prompt_char} "),
        Style::default().fg(border).add_modifier(Modifier::BOLD),
    );
    let cont_span = Span::styled("  ", Style::default());
    let mut input_lines = app.input.split('\n');
    if let Some(first) = input_lines.next() {
        lines.push(Line::from(vec![prompt_span, Span::raw(first.to_string())]));
        for rest in input_lines {
            lines.push(Line::from(vec![
                cont_span.clone(),
                Span::raw(rest.to_string()),
            ]));
        }
    } else {
        lines.push(Line::from(vec![prompt_span]));
    }
    let para = Paragraph::new(lines)
        .wrap(Wrap { trim: false })
        .scroll((scroll, 0));
    f.render_widget(para, inner);

    // The cursor follows the append point. When the draft is scrolled up,
    // keep the cursor out of the way so the user can read the older text.
    if app.input_scroll == 0 {
        let (cursor_row, cursor_col) = input_end_position(&app.input, content_width);
        let cursor_y = inner.y + cursor_row.saturating_sub(scroll);
        if cursor_y < inner.y.saturating_add(inner.height) {
            let cursor_x = inner.x + cursor_col.min(inner.width.saturating_sub(1));
            f.set_cursor_position((cursor_x, cursor_y));
        }
    }
}

fn visual_line_count(input: &str, content_width: u16) -> u16 {
    let width = content_width.max(1) as usize;
    input
        .split('\n')
        .map(|line| {
            let chars = line.chars().count();
            chars.max(1).div_ceil(width) as u16
        })
        .sum::<u16>()
        .max(1)
}

fn input_end_position(input: &str, content_width: u16) -> (u16, u16) {
    let width = content_width.max(1) as usize;
    let mut row = 0u16;
    let mut col = 2u16;
    let lines: Vec<&str> = input.split('\n').collect();
    for (idx, line) in lines.iter().enumerate() {
        let effective_width = width.max(1);
        let chars = line.chars().count();
        let wrapped_rows = chars / effective_width;
        let wrapped_col = chars % effective_width;
        if idx + 1 == lines.len() {
            row = row.saturating_add(wrapped_rows as u16);
            col = 2 + wrapped_col as u16;
        } else {
            row = row.saturating_add(wrapped_rows as u16 + 1);
        }
    }
    (row, col)
}

#[cfg(test)]
mod tests {
    use super::{event_list_width_with_detail, session_item_limit};

    #[test]
    fn session_item_limit_uses_one_row_per_session() {
        assert_eq!(session_item_limit(0), 0);
        assert_eq!(session_item_limit(1), 1);
        assert_eq!(session_item_limit(2), 2);
        assert_eq!(session_item_limit(3), 3);
        assert_eq!(session_item_limit(4), 4);
    }

    #[test]
    fn event_list_stays_compact_when_detail_is_open() {
        assert_eq!(event_list_width_with_detail(160), 46);
        assert_eq!(event_list_width_with_detail(94), 46);
        assert_eq!(event_list_width_with_detail(80), 32);
    }
}
