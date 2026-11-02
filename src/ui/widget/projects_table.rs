use std::collections::HashMap;

use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Margin, Rect},
    prelude::StatefulWidget,
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Clear, Paragraph, Row, Table, TableState, Widget},
};

use crate::{
    domain::{parse_flat_ci_row, parse_ranked_row, parse_row, Project},
    glim_app::{CiViewMode, RankingMode},
    id::ProjectId,
    relevance::{RankedCiProcessEntry, RelevanceContext},
    theme::theme,
    ui::widget::Shortcuts,
};

/// gitlab pipelines widget
pub struct ProjectsTable<'a> {
    rows: Vec<Row<'a>>,
    filter_active: bool,
    filter_text: &'a str,
    focused: bool,
    compact_mode: bool,
    view_mode: CiViewMode,
    ranking_mode: RankingMode,
}

impl<'a> ProjectsTable<'a> {
    #[allow(dead_code)]
    pub fn new(
        projects: &'a [Project],
        filter_active: bool,
        filter_text: &'a str,
        max_pipelines_per_project: usize,
        include_mr_pipelines: bool,
        focused: bool,
    ) -> Self {
        Self {
            rows: projects
                .iter()
                .map(|proj| parse_row(proj, max_pipelines_per_project, include_mr_pipelines))
                .enumerate()
                .map(|(idx, r)| r.style(theme().table_row(idx)))
                .collect(),
            filter_active,
            filter_text,
            focused,
            compact_mode: false,
            view_mode: CiViewMode::Grouped,
            ranking_mode: RankingMode::Hybrid,
        }
    }

    pub fn new_dynamic(
        projects: &'a [Project],
        ctx: &RelevanceContext<'_>,
        row_budgets: &HashMap<ProjectId, usize>,
        filter_active: bool,
        filter_text: &'a str,
        compact_mode: bool,
        focused: bool,
    ) -> Self {
        let default_max_pipelines = ctx.config.max_pipelines_per_project;
        Self {
            rows: projects
                .iter()
                .map(|proj| {
                    let budget = row_budgets
                        .get(&proj.id)
                        .copied()
                        .unwrap_or(default_max_pipelines);
                    parse_ranked_row(proj, ctx, budget, compact_mode)
                })
                .enumerate()
                .map(|(idx, r)| r.style(theme().table_row(idx)))
                .collect(),
            filter_active,
            filter_text,
            focused,
            compact_mode,
            view_mode: CiViewMode::Grouped,
            ranking_mode: ctx.ranking_mode,
        }
    }

    pub fn new_flat(
        entries: &[RankedCiProcessEntry<'a>],
        filter_active: bool,
        filter_text: &'a str,
        compact_mode: bool,
        focused: bool,
        ranking_mode: RankingMode,
    ) -> Self {
        Self {
            rows: entries
                .iter()
                .map(parse_flat_ci_row)
                .enumerate()
                .map(|(idx, r)| r.style(theme().table_row(idx)))
                .collect(),
            filter_active,
            filter_text,
            focused,
            compact_mode,
            view_mode: CiViewMode::Flat,
            ranking_mode,
        }
    }
}

impl StatefulWidget for ProjectsTable<'_> {
    type State = TableState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        Clear.render(area, buf);

        let view_toggle_label = match self.view_mode {
            CiViewMode::Grouped => "flat view",
            CiViewMode::Flat => "repo view",
        };

        let shortcuts = if self.filter_active {
            Shortcuts::from(vec![("↵", "finish filter"), ("esc", "cancel")])
        } else {
            Shortcuts::from(vec![
                ("q", "quit"),
                ("tab", "switch pane"),
                ("v", view_toggle_label),
                ("s", "rank mode"),
                ("S", "reroll 🎲"),
                ("w", "open web"),
                ("c", "config"),
                ("m", "mr activity"),
                ("a", "last notification"),
                ("f/", "filter"),
                ("r", "refresh"),
                ("p", "pipeline refresh"),
                ("↑↓", "selection"),
                ("↵", "details"),
            ])
        };

        // Split area into main table and filter input if active
        let (table_area, filter_area) = if self.filter_active {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Min(3),    // Main table
                    Constraint::Length(3), // Filter input
                ])
                .split(area);
            (chunks[0], Some(chunks[1]))
        } else {
            (area, None)
        };

        let row_count = self.rows.len();
        let mode_badge = match self.ranking_mode {
            RankingMode::Hybrid => "",
            RankingMode::Stochastic => " • 🎲 100% stochastic",
            RankingMode::Deterministic => " • deterministic",
        };
        let title = match (self.view_mode, self.compact_mode) {
            (CiViewMode::Grouped, true) => {
                format!(" gitlab pipelines (by repo{mode_badge} • summary • tab to expand, v: flat) ")
            },
            (CiViewMode::Grouped, false) => {
                format!(" gitlab pipelines (by repo{mode_badge} • v: flat, s: mode) ")
            },
            (CiViewMode::Flat, true) => {
                format!(
                    " gitlab ci processes ({row_count}{mode_badge} • summary • tab to expand, v: by repo) "
                )
            },
            (CiViewMode::Flat, false) => {
                format!(" gitlab ci processes ({row_count}{mode_badge} • v: by repo, s: mode) ")
            },
        };

        // Render main table block
        let mut block = Block::new()
            .title(title)
            .title_style(theme().border_title)
            .borders(if self.filter_active {
                Borders::TOP | Borders::LEFT | Borders::RIGHT
            } else {
                Borders::ALL
            })
            .border_style(theme().table_border)
            .style(theme().background)
            .border_type(BorderType::Plain);

        if self.focused || !self.compact_mode {
            block = block.title_bottom(shortcuts.as_line());
        }
        block.render(table_area, buf);

        let content_area = table_area.inner(Margin::new(2, 1));
        let mut table = match self.view_mode {
            CiViewMode::Grouped => {
                Table::new(self.rows, PROJECT_COLUMN_CONSTRAINTS).column_spacing(1)
            },
            CiViewMode::Flat => Table::new(self.rows, FLAT_CI_COLUMN_CONSTRAINTS).column_spacing(1),
        };
        if self.focused {
            table = table.row_highlight_style(theme().highlight_symbol);
        }

        StatefulWidget::render(table, content_area, buf, state);

        // Render filter input if active
        if let Some(filter_area) = filter_area {
            let filter_content = Line::from(vec![
                Span::from("Filter: "),
                Span::from(self.filter_text).style(theme().highlight_symbol),
                Span::from("█"), // Cursor
            ]);

            let filter_block = Block::new()
                .borders(Borders::ALL)
                .border_style(theme().table_border)
                .style(theme().background);

            Paragraph::new(filter_content)
                .block(filter_block)
                .render(filter_area, buf);
        }
    }
}

const PROJECT_COLUMN_CONSTRAINTS: [Constraint; 3] = [
    Constraint::Length(16),      // date and time
    Constraint::Min(40),         // project name
    Constraint::Percentage(100), // pipelines
];

const FLAT_CI_COLUMN_CONSTRAINTS: [Constraint; 1] = [Constraint::Percentage(100)];
