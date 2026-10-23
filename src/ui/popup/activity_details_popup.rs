use chrono::Local;
use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Margin, Rect},
    prelude::{Line, StatefulWidget, Text, Widget},
    text::Span,
    widgets::{Block, BorderType, Borders, List, ListState, Paragraph, Wrap},
};
use tachyonfx::RefRect;

use crate::{
    domain::{ActivityKind, ProjectActivityEvent},
    event::GlimEvent,
    theme::theme,
    ui::{format_relative_24h, fx::popup_window, popup::utility::CenteredShrink},
};

pub struct ActivityDetailsPopup {}

pub struct ActivityDetailsPopupState {
    pub event: ProjectActivityEvent,
    pub actions: Vec<(String, GlimEvent)>,
    pub list_state: ListState,
    pub popup_area: RefRect,
}

impl ActivityDetailsPopup {
    pub fn new() -> Self {
        Self {}
    }
}

impl ActivityDetailsPopupState {
    pub fn new(event: ProjectActivityEvent, popup_area: RefRect) -> Self {
        let actions = Self::build_actions(&event);

        Self {
            event,
            actions,
            list_state: ListState::default().with_selected(Some(0)),
            popup_area,
        }
    }

    fn build_actions(event: &ProjectActivityEvent) -> Vec<(String, GlimEvent)> {
        let mut actions = Vec::new();
        if let Some(iid) = event.mr_iid {
            actions.push((
                format!("browse to merge request (!{iid})"),
                GlimEvent::MergeRequestOpenUrl(event.project_id, iid),
            ));
        }
        if let Some(ref sha) = event.commit_sha {
            if !sha.is_empty() {
                let short_sha = &sha[..sha.len().min(8)];
                actions.push((
                    format!("browse to commit ({short_sha})"),
                    GlimEvent::CommitOpenUrl(event.project_id, sha.clone()),
                ));
            }
        }
        actions.push((
            format!("browse to project ({})", event.project_name),
            GlimEvent::ProjectOpenUrl(event.project_id),
        ));
        actions.push((
            format!("inspect project pipelines ({})", event.project_name),
            GlimEvent::ProjectDetailsOpen(event.project_id),
        ));
        let copy_text = event
            .body
            .clone()
            .unwrap_or_else(|| event.summary.clone());
        actions.push((
            "copy event text to clipboard".to_string(),
            GlimEvent::CopyToClipboard(copy_text),
        ));
        actions
    }

    pub fn update_commit(&mut self, commit: &crate::domain::CommitDto) {
        if !commit.title.is_empty() {
            self.event.summary = commit.title.clone();
        }
        if !commit.message.is_empty() {
            self.event.body = Some(commit.message.trim().into());
        }
        self.actions = Self::build_actions(&self.event);
    }

    pub fn selected_action_event(&self) -> Option<GlimEvent> {
        self.list_state
            .selected()
            .and_then(|idx| self.actions.get(idx))
            .map(|(_, ev)| ev.clone())
    }

    pub fn body_text(&self) -> &str {
        self.event
            .body
            .as_deref()
            .unwrap_or(self.event.summary.as_str())
    }

    pub fn wrapped_body_height(&self, inner_width: u16) -> u16 {
        let usable_w = inner_width.saturating_sub(2).max(20) as usize;
        let lines: usize = self
            .body_text()
            .lines()
            .map(|l| {
                let chars = l.chars().count();
                if chars == 0 {
                    1
                } else {
                    chars.div_ceil(usable_w)
                }
            })
            .sum();
        (lines.max(1) as u16).min(14)
    }

    pub fn update_popup_area(&self, screen: Rect) -> Rect {
        let popup_w = screen.width.saturating_sub(12).clamp(50, 96);
        let body_h = self.wrapped_body_height(popup_w.saturating_sub(4));
        let header_h = 3;
        let actions_h = self.actions.len() as u16 + 1;
        let total_h = (2 + header_h + body_h + 2 + actions_h).min(screen.height.saturating_sub(2));

        let area = screen.inner_centered(popup_w, total_h);
        self.popup_area.set(area);
        area
    }
}

impl StatefulWidget for ActivityDetailsPopup {
    type State = ActivityDetailsPopupState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        let area = state.update_popup_area(area);

        popup_window(
            "Activity Event Details",
            Some(vec![
                ("ESC", "close"),
                ("↑ ↓", "selection"),
                ("↵", "apply"),
                ("w", "open web"),
            ]),
        )
        .render(area, buf);

        let content_area = area.inner(Margin::new(2, 1));
        let actions_h = state.actions.len() as u16;
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(3),
                Constraint::Length(actions_h),
            ])
            .split(content_area);

        let ev = &state.event;
        let action_style = match ev.kind {
            ActivityKind::Mention | ActivityKind::MergeTrainRemoved => theme().mr_conflict,
            ActivityKind::ReviewRequested | ActivityKind::Assigned => theme().mr_draft,
            _ => theme().activity_action,
        };

        let local_ts = ev.created_at.with_timezone(&Local);
        let full_ts = match format_relative_24h(local_ts, Local::now()) {
            Some(rel_ts) => format!(
                "{} ({} ago)",
                local_ts.format("%Y-%m-%d %H:%M:%S"),
                rel_ts.trim()
            ),
            None => local_ts.format("%Y-%m-%d %H:%M:%S").to_string(),
        };

        let mut header_line_1 = vec![
            Span::from(format!("{} ", ev.kind.icon())),
            Span::from(format!("[{}] ", ev.project_name)).style(theme().project_name),
            Span::from(format!("@{} ", ev.author)).style(theme().activity_author),
            Span::from(ev.kind.action_label()).style(action_style),
        ];
        if let Some(iid) = ev.mr_iid {
            header_line_1.push(Span::from(format!(" !{iid}")).style(theme().mr_iid));
        }
        if let Some(branch) = &ev.branch {
            header_line_1.push(Span::from(format!(" ({branch})")).style(theme().pipeline_branch));
        }

        let mut header_line_2 = vec![Span::from(full_ts).style(theme().date)];
        if let Some(ref sha) = ev.commit_sha {
            if !sha.is_empty() {
                let short_sha = &sha[..sha.len().min(8)];
                header_line_2.push(Span::from("  •  commit ").style(theme().date));
                header_line_2.push(Span::from(short_sha).style(theme().mr_iid));
            }
        }
        if let Some(detail) = &ev.detail {
            header_line_2.push(Span::from("  •  ").style(theme().date));
            header_line_2.push(Span::from(detail.as_str()).style(theme().mr_target));
        }

        Paragraph::new(Text::from(vec![
            Line::from(header_line_1),
            Line::from(header_line_2),
        ]))
        .render(chunks[0], buf);

        let body_block = Block::new()
            .borders(Borders::TOP | Borders::BOTTOM)
            .border_style(theme().table_border)
            .border_type(BorderType::Plain);
        let body_inner = body_block.inner(chunks[1]);
        body_block.render(chunks[1], buf);

        let body_lines: Vec<Line<'_>> = state
            .body_text()
            .lines()
            .map(|l| Line::from(Span::from(l).style(theme().commit_title)))
            .collect();
        Paragraph::new(Text::from(body_lines))
            .wrap(Wrap { trim: false })
            .render(body_inner, buf);

        let action_lines: Vec<Line<'static>> = state
            .actions
            .iter()
            .map(|(label, _)| Line::from(label.clone()).style(theme().pipeline_action))
            .collect();

        let actions_list = List::new(action_lines)
            .style(theme().table_row_b)
            .highlight_style(theme().pipeline_action_selected);

        StatefulWidget::render(actions_list, chunks[2], buf, &mut state.list_state);
    }
}
