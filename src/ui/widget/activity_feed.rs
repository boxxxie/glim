use chrono::Local;
use compact_str::ToCompactString;
use ratatui::{
    buffer::Buffer,
    layout::{Margin, Rect},
    text::{Line, Span, Text},
    widgets::{Block, BorderType, Borders, Clear, Paragraph, Widget},
};

use crate::{domain::ProjectActivityEvent, theme::theme};

pub struct ActivityFeed<'a> {
    events: &'a [ProjectActivityEvent],
    scroll_offset: usize,
}

impl<'a> ActivityFeed<'a> {
    pub fn new(events: &'a [ProjectActivityEvent], scroll_offset: usize) -> Self {
        Self { events, scroll_offset }
    }
}

impl Widget for ActivityFeed<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        if area.height < 3 {
            return;
        }

        Clear.render(area, buf);

        let title = if self.scroll_offset > 0 {
            format!(" mr & repo activity (+{} scrolled) ", self.scroll_offset)
        } else {
            " mr & repo activity ".to_string()
        };

        Block::new()
            .title(title)
            .title_style(theme().border_title)
            .borders(Borders::ALL)
            .border_style(theme().table_border)
            .style(theme().background)
            .border_type(BorderType::Plain)
            .render(area, buf);

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
            .skip(self.scroll_offset)
            .take(max_lines)
            .map(|ev| {
                let local_ts = ev.created_at.with_timezone(&Local);
                let mut spans = vec![
                    Span::from(local_ts.format("%d %b ").to_compact_string()).style(theme().date),
                    Span::from(local_ts.format("%H:%M:%S").to_compact_string()).style(theme().time),
                    Span::from(" "),
                    Span::from(ev.kind.icon()),
                    Span::from(" "),
                    Span::from(format!("[{}]", ev.project_name)).style(theme().project_parents),
                    Span::from(" "),
                    Span::from(format!("@{}", ev.author)).style(theme().activity_author),
                    Span::from(" "),
                    Span::from(ev.kind.action_label()).style(theme().activity_action),
                ];

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
                    spans.push(Span::from(format!(" ({detail})")).style(theme().mr_target));
                }

                Line::from(spans)
            })
            .collect();

        Paragraph::new(Text::from(lines)).render(content_area, buf);
    }
}
