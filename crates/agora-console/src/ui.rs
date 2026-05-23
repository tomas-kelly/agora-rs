use ratatui::{
    prelude::*,
    widgets::{Block, Borders, List, ListItem, Paragraph, Wrap},
};
use swarm_core::manifest::AgentStatus;

use crate::app::{App, InputMode};

pub fn render(f: &mut Frame, app: &App) {
    let area = f.area();

    let detail_height = if app.command_output.is_some() { 16 } else { 8 };
    let input_lines = 1 + app.input.chars().filter(|c| *c == '\n').count();
    let input_height = (input_lines as u16 + 2).clamp(3, 10);
    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),             // status bar
            Constraint::Min(6),                // main 3-column area
            Constraint::Length(detail_height), // detail / command output
            Constraint::Length(input_height),  // input (grows for multi-line)
        ])
        .split(area);

    render_status(f, app, outer[0]);

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(34),
            Constraint::Min(40),
            Constraint::Length(34),
        ])
        .split(outer[1]);

    render_sessions(f, app, cols[0]);
    render_events(f, app, cols[1]);
    render_agents(f, app, cols[2]);

    render_detail(f, app, outer[2]);
    render_input(f, app, outer[3]);
}

fn render_status(f: &mut Frame, app: &App, area: Rect) {
    let left = format!(
        " agora-console │ {} │ events:{} telemetry:{} sessions:{} agents:{} ",
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

fn render_sessions(f: &mut Frame, app: &App, area: Rect) {
    let mut sessions: Vec<&crate::app::SessionInfo> = app.sessions.values().collect();
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
            .title(format!(" Sessions ({}) ", app.sessions.len()))
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    f.render_widget(list, area);
}

fn render_events(f: &mut Frame, app: &App, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Events ")
        .border_style(Style::default().fg(Color::DarkGray));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let height = inner.height as usize;
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

fn format_event_line(e: &swarm_core::envelope::Envelope) -> Line<'static> {
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
            let (icon, color) = match a.status {
                AgentStatus::Ready => ("●", Color::Green),
                AgentStatus::Busy => ("◐", Color::Yellow),
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

fn render_input(f: &mut Frame, app: &App, area: Rect) {
    let (title, prompt_char, border) = match app.mode {
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

    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .border_style(Style::default().fg(border));
    let inner = block.inner(area);
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
    let para = Paragraph::new(lines).wrap(Wrap { trim: false });
    f.render_widget(para, inner);

    // Cursor lands at the end of the last logical line.
    let last_line = app.input.rsplit('\n').next().unwrap_or("");
    let line_idx = app.input.chars().filter(|c| *c == '\n').count() as u16;
    let cursor_x = inner.x + 2 + last_line.chars().count() as u16;
    let cursor_y = inner.y + line_idx;
    f.set_cursor_position((cursor_x, cursor_y));
}

fn short_id(id: &str, n: usize) -> String {
    if id.len() <= n {
        id.to_string()
    } else {
        format!("{}…", &id[..n])
    }
}
