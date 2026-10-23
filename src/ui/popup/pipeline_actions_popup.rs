use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Margin, Rect},
    prelude::{Line, StatefulWidget, Text, Widget},
    text::Span,
    widgets::{Block, BorderType, Borders, List, ListState, Paragraph, Wrap},
};
use tachyonfx::RefRect;

use crate::{
    event::GlimEvent,
    theme::theme,
    ui::{fx::popup_window, popup::utility::CenteredShrink},
};

/// pipeline actions popup
pub struct PipelineActionsPopup {}

/// state of the pipeline actions popup
pub struct PipelineActionsPopupState {
    pub actions: Vec<GlimEvent>,
    pub commit_message: Option<String>,
    pub list_state: ListState,
    pub popup_area: RefRect,
}

impl PipelineActionsPopupState {
    pub fn new(
        actions: Vec<GlimEvent>,
        commit_message: Option<String>,
        popup_area: RefRect,
    ) -> Self {
        Self {
            actions,
            commit_message,
            list_state: ListState::default().with_selected(Some(0)),
            popup_area,
        }
    }

    pub fn copy_selected_action(&self, selected_action: usize) -> GlimEvent {
        self.actions[selected_action].clone()
    }

    fn actions_as_lines(&self) -> Vec<Line<'static>> {
        self.actions
            .iter()
            .map(|action| {
                let action: String = match action {
                    GlimEvent::MergeRequestOpenUrl(_, mr_iid) => {
                        format!("browse to merge request (!{mr_iid})")
                    },
                    GlimEvent::CommitOpenUrl(_, sha) => {
                        let short_sha = &sha[..sha.len().min(8)];
                        format!("browse to commit ({short_sha})")
                    },
                    GlimEvent::JobOpenUrl(_, _, _) => "browse to failed job".into(),
                    GlimEvent::PipelineOpenUrl(_, _) => "browse to pipeline".into(),
                    GlimEvent::ProjectOpenUrl(_) => "browse to project".into(),
                    GlimEvent::JobLogFetch(_, _) => "download failed job log to clipboard".into(),
                    GlimEvent::CopyToClipboard(_) => "copy commit message to clipboard".into(),
                    _ => panic!("unsupported action"),
                };
                Line::from(action).style(theme().pipeline_action)
            })
            .collect()
    }

    pub fn wrapped_message_height(&self, inner_width: u16) -> u16 {
        let Some(ref msg) = self.commit_message else {
            return 0;
        };
        let usable_w = inner_width.saturating_sub(2).max(20) as usize;
        let lines: usize = msg
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
        (lines.max(1) as u16).min(12) + 1
    }

    pub fn update_popup_area(&self, screen: Rect) -> Rect {
        let width = if self.commit_message.is_some() {
            screen.width.saturating_sub(16).clamp(48, 88)
        } else {
            44
        };
        let msg_h = self.wrapped_message_height(width.saturating_sub(4));
        let total_h = (2 + msg_h + self.actions.len() as u16).min(screen.height.saturating_sub(2));
        let area = screen.inner_centered(width, total_h);
        self.popup_area.set(area);
        area
    }
}

impl PipelineActionsPopup {
    pub fn new() -> PipelineActionsPopup {
        Self {}
    }
}

impl StatefulWidget for PipelineActionsPopup {
    type State = PipelineActionsPopupState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        let area = state.update_popup_area(area);

        popup_window(
            "Pipeline Actions",
            Some(vec![("ESC", "close"), ("↑ ↓", "selection"), ("↵", "apply")]),
        )
        .render(area, buf);

        let inner_area = area.inner(Margin::new(2, 1));
        let actions_h = state.actions.len() as u16;
        let actions_area = if let Some(ref msg) = state.commit_message {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(2), Constraint::Length(actions_h)])
                .split(inner_area);

            let msg_block = Block::new()
                .borders(Borders::BOTTOM)
                .border_style(theme().table_border)
                .border_type(BorderType::Plain);
            let msg_inner = msg_block.inner(chunks[0]);
            msg_block.render(chunks[0], buf);

            let msg_lines: Vec<Line<'_>> = msg
                .lines()
                .map(|l| Line::from(Span::from(l).style(theme().commit_title)))
                .collect();
            Paragraph::new(Text::from(msg_lines))
                .wrap(Wrap { trim: false })
                .render(msg_inner, buf);

            chunks[1]
        } else {
            inner_area
        };

        let actions = state.actions_as_lines();
        let actions_list = List::new(actions)
            .style(theme().table_row_b)
            .highlight_style(theme().pipeline_action_selected);

        StatefulWidget::render(actions_list, actions_area, buf, &mut state.list_state);
    }
}
