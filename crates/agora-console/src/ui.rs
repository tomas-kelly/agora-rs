use agora_core::manifest::AgentStatus;
use ratatui::{
    prelude::*,
    widgets::{Block, Borders, List, ListItem, Paragraph, Wrap},
};

use crate::app::{App, InputMode, Panel};

const COMPOSER_MIN_HEIGHT: u16 = 7;
const COMPOSER_MAX_HEIGHT: u16 = 18;

pub fn render(f: &mut Frame, app: &mut App) {
    let area = f.area();

    let main_visible = app.panels.sessions || app.panels.events || app.panels.agents;
    let detail_height = if !app.panels.detail {
        0
    } else if app.command_output.is_some() {
        (area.height / 3).clamp(10, 16)
    } else {
        8
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
            Constraint::Length(detail_height), // detail / command output
            Constraint::Length(input_height),  // input (grows for multi-line)
        ])
        .split(area);

    render_status(f, app, outer[0]);

    render_main_panels(f, app, outer[1]);

    if app.panels.detail {
        render_detail(f, app, outer[2]);
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
    let mut panels = Vec::new();
    if app.panels.sessions {
        panels.push(Panel::Sessions);
    }
    if app.panels.events {
        panels.push(Panel::Events);
    }
    if app.panels.agents {
        panels.push(Panel::Agents);
    }

    if panels.is_empty() || area.height == 0 || area.width == 0 {
        app.events_view_height = 1;
        return;
    }

    let constraints = main_panel_constraints(&panels);
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(constraints)
        .split(area);

    for (idx, panel) in panels.into_iter().enumerate() {
        match panel {
            Panel::Sessions => render_sessions(f, app, cols[idx]),
            Panel::Events => render_events(f, app, cols[idx]),
            Panel::Agents => render_agents(f, app, cols[idx]),
            Panel::Detail => {}
        }
    }
}

fn main_panel_constraints(panels: &[Panel]) -> Vec<Constraint> {
    if panels.len() == 1 {
        return vec![Constraint::Min(1)];
    }

    let events_visible = panels.contains(&Panel::Events);
    panels
        .iter()
        .map(|panel| match (panel, events_visible) {
            (Panel::Events, _) => Constraint::Min(40),
            (Panel::Sessions | Panel::Agents, true) => Constraint::Length(34),
            (Panel::Sessions | Panel::Agents, false) => Constraint::Ratio(1, panels.len() as u32),
            (Panel::Detail, _) => Constraint::Length(0),
        })
        .collect()
}

fn render_sessions(f: &mut Frame, app: &App, area: Rect) {
    let mut sessions: Vec<&crate::app::SessionInfo> = app
        .sessions
        .values()
        .filter(|session| !session.deleted)
        .collect();
    sessions.sort_by(|a, b| b.started_at.cmp(&a.started_at));

    let items: Vec<ListItem> = sessions
        .iter()
        .take(area.height.saturating_sub(2) as usize / 2)
        .map(|s| {
            let is_active = app.active_session.as_deref() == Some(s.session_id.as_str());
            let display = app.display_name(&s.session_id);
            let marker = if is_active { "▶ " } else { "  " };
            let title_style = if is_active {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            };
            let secondary = if s.event_count == 0 && s.last_topic.is_empty() {
                "  (empty)".to_string()
            } else {
                format!("  {} · {} events", &s.last_topic, s.event_count)
            };
            ListItem::new(vec![
                Line::from(vec![Span::raw(marker), Span::styled(display, title_style)]),
                Line::from(Span::styled(
                    secondary,
                    Style::default().fg(Color::DarkGray),
                )),
            ])
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(" Sessions ({}) ", sessions.len()))
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    f.render_widget(list, area);
}

fn render_events(f: &mut Frame, app: &mut App, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Events ")
        .border_style(Style::default().fg(Color::DarkGray));
    let inner = block.inner(area);
    f.render_widget(block, area);

    // Record viewport height so `scroll_up` in app.rs can cap correctly
    // (terminal resize may have changed it since last frame).
    app.events_view_height = inner.height;

    let height = inner.height as usize;
    // Clamp scroll so the pane never goes blank when scrolled past the
    // oldest event. Belt + braces against any path that bumped
    // events_scroll without going through scroll_up.
    let max_scroll = app.events.len().saturating_sub(height);
    if (app.events_scroll as usize) > max_scroll {
        app.events_scroll = max_scroll as u16;
    }
    let scroll = app.events_scroll as usize;

    // The newest events sit at the end; auto-scroll keeps the bottom in view.
    let end = app.events.len().saturating_sub(scroll);
    let start = end.saturating_sub(height);

    let items: Vec<ListItem> = app.events[start..end]
        .iter()
        .map(|e| ListItem::new(format_event_line(e)))
        .collect();

    let list = List::new(items);
    f.render_widget(list, inner);
}

fn format_event_line(e: &agora_core::envelope::Envelope) -> Line<'static> {
    let ts = if e.timestamp.len() >= 19 {
        e.timestamp[11..19].to_string()
    } else {
        e.timestamp.clone()
    };
    let session = short_id(&e.context.session_id, 12);
    let color = topic_color(&e.topic);

    Line::from(vec![
        Span::styled(ts, Style::default().fg(Color::DarkGray)),
        Span::raw("  "),
        Span::styled(format!("{:32}", e.topic), Style::default().fg(color)),
        Span::raw("  "),
        Span::styled(session, Style::default().fg(Color::DarkGray)),
    ])
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

fn render_detail(f: &mut Frame, app: &App, area: Rect) {
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
            " Command output (Esc to dismiss · !clear) ".to_string()
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
        return;
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Latest event ")
        .border_style(Style::default().fg(Color::DarkGray));
    let text = match app.events.last() {
        Some(e) => {
            let data_pretty =
                serde_json::to_string_pretty(&e.data).unwrap_or_else(|_| e.data.to_string());
            let session_label = app.display_name(&e.context.session_id);
            format!(
                "topic:   {}\nevent:   {}\nsession: {}  ({})\nfrom:    {}\ndata:    {}",
                e.topic,
                e.event_id,
                session_label,
                e.context.session_id,
                e.sender.agent_name,
                data_pretty
            )
        }
        None => "(no events yet — submit an idea below, or type !help)".into(),
    };
    let para = Paragraph::new(text).block(block).wrap(Wrap { trim: false });
    f.render_widget(para, area);
}

fn render_input(f: &mut Frame, app: &mut App, area: Rect) {
    let (base_title, prompt_char, border) = match app.mode {
        InputMode::Normal => {
            let scope = match &app.active_session {
                Some(sid) => format!("active: {}", app.display_name(sid)),
                None => "no active session — submit creates one".into(),
            };
            (format!(" Compose · {scope} "), "›", Color::Yellow)
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

fn short_id(id: &str, n: usize) -> String {
    if id.len() <= n {
        id.to_string()
    } else {
        format!("{}…", &id[..n])
    }
}
