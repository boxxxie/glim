use chrono::Local;
use ratatui::{
    buffer::Buffer,
    layout::{Margin, Rect},
    text::{Line, Span, Text},
    widgets::{Block, BorderType, Borders, Clear, Paragraph, Widget},
};

use crate::{
    domain::{ActivityKind, ProjectActivityEvent},
    theme::theme,
    ui::widget::Shortcuts,
};

pub struct ActivityFeed<'a> {
    events: &'a [ProjectActivityEvent],
    scroll_offset: usize,
    focused: bool,
    selected_index: Option<usize>,
    loading_more: bool,
}

impl<'a> ActivityFeed<'a> {
    pub fn new(
        events: &'a [ProjectActivityEvent],
        scroll_offset: usize,
        focused: bool,
        selected_index: Option<usize>,
        loading_more: bool,
    ) -> Self {
        Self {
            events,
            scroll_offset,
            focused,
            selected_index,
            loading_more,
        }
    }
}

impl Widget for ActivityFeed<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if area.height < 3 {
            return;
        }

        Clear.render(area, buf);

        let total = self.events.len();
        let loading_suffix = if self.loading_more {
            " • loading older..."
        } else {
            ""
        };
        let title = if self.scroll_offset > 0 {
            format!(
                " mr & repo activity ({total} total, +{} scrolled{loading_suffix}) ",
                self.scroll_offset
            )
        } else {
            format!(" mr & repo activity ({total}{loading_suffix}) ")
        };

        let shortcuts = Shortcuts::from(vec![
            ("tab", "switch pane"),
            ("↑↓", "selection"),
            ("↵", "inspect event"),
            ("click", "inspect"),
            ("m", "hide"),
        ]);

        let mut block = Block::new()
            .title(title)
            .title_style(theme().border_title)
            .borders(Borders::ALL)
            .border_style(theme().table_border)
            .style(theme().background)
            .border_type(BorderType::Plain);

        if self.focused {
            block = block.title_bottom(shortcuts.as_line());
        }
        block.render(area, buf);

        let content_area = area.inner(Margin::new(2, 1));
        let max_lines = content_area.height as usize;

        if self.events.is_empty() {
            let empty_line = Line::from(vec![Span::from(
                "Waiting for MR and repository events...",
            )
            .style(theme().commit_title)]);
            Paragraph::new(empty_line).render(content_area, buf);
            return;
        }

        let lines: Vec<Line<'_>> = self
            .events
            .iter()
            .enumerate()
            .skip(self.scroll_offset)
            .take(max_lines)
            .map(|(abs_idx, ev)| {
                let local_ts = ev.created_at.with_timezone(&Local);
                let mut spans = crate::ui::format_timestamp_spans(local_ts);
                let action_style = match ev.kind {
                    ActivityKind::Mention | ActivityKind::MergeTrainRemoved => theme().mr_conflict,
                    ActivityKind::ReviewRequested | ActivityKind::Assigned => theme().mr_draft,
                    _ => theme().activity_action,
                };
                spans.extend([
                    Span::from(" "),
                    Span::from(ev.kind.icon()),
                    Span::from(" "),
                    Span::from(format!("[{}]", ev.project_name)).style(theme().project_parents),
                    Span::from(" "),
                    Span::from(format!("@{}", ev.author)).style(theme().activity_author),
                    Span::from(" "),
                    Span::from(ev.kind.action_label()).style(action_style),
                ]);

                if let Some(mr_iid) = ev.mr_iid {
                    spans.push(Span::from(format!(" !{mr_iid}")).style(theme().mr_iid));
                }

                if let Some(branch) = &ev.branch {
                    spans.push(Span::from(format!(" {}", branch)).style(theme().pipeline_branch));
                }

                if !ev.summary.is_empty() {
                    spans.push(Span::from(" — ").style(theme().date));
                    spans.push(Span::from(ev.summary.as_str()).style(theme().commit_title));
                }

                if let Some(detail) = &ev.detail {
                    if !detail.is_empty() && detail != &ev.summary {
                        spans.push(Span::from(format!(" ({detail})")).style(theme().mr_target));
                    }
                }

                let mut line = Line::from(spans).style(theme().table_row(abs_idx));
                if self.focused && self.selected_index == Some(abs_idx) {
                    line = line.style(theme().highlight_symbol);
                }
                line
            })
            .collect();

        Paragraph::new(Text::from(lines)).render(content_area, buf);

        if self.focused {
            if let Some(sel_idx) = self.selected_index {
                if sel_idx >= self.scroll_offset && sel_idx < self.scroll_offset + max_lines {
                    let row_y = content_area.y + (sel_idx - self.scroll_offset) as u16;
                    buf.set_style(
                        Rect::new(content_area.x, row_y, content_area.width, 1),
                        theme().highlight_symbol,
                    );
                }
            }
        }
    }
}
