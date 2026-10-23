use ratatui::{
    layout::{Constraint, Layout, Margin},
    prelude::{Direction, Rect},
    Frame,
};
use tachyonfx::Duration;
use tracing::debug;

use crate::{
    dispatcher::Dispatcher,
    domain::{Project, RowEntryTarget},
    effect_registry::EffectRegistry,
    event::GlimEvent,
    glim_app::GlimApp,
    theme::theme,
    ui::{
        popup::{ConfigPopup, PipelineActionsPopup, ProjectDetailsPopup},
        widget::{ActivityFeed, Notification, ProjectsTable},
        ClickAction, ClickRegion, StatefulWidgets,
    },
};

pub fn render_main_ui(
    f: &mut Frame,
    app: &GlimApp,
    widget_states: &mut StatefulWidgets,
    effects: &mut EffectRegistry,
) {
    effects.update_screen_area(f.area());

    let last_tick = widget_states.last_frame;
    let frame_area = f.area();
    let layout =
        Layout::new(Direction::Horizontal, [Constraint::Percentage(100)]).split(frame_area);

    render_projects_table(f, app, widget_states, layout[0]);
    render_popups(f, widget_states, layout[0], last_tick);
    render_effects(f, effects, last_tick, frame_area);

    // Handle screen capture if requested
    if widget_states.capture_screen_requested {
        handle_screen_capture(f, app, widget_states);
    }
}

fn render_projects_table(
    f: &mut Frame,
    app: &GlimApp,
    widget_states: &mut StatefulWidgets,
    area: Rect,
) {
    let config = app.config();
    let effective_filter = widget_states.effective_filter(&config.search_filter);
    let (filtered_projects, filtered_indices) = app.filtered_projects(&effective_filter);
    widget_states.update_filtered_indices(filtered_indices);

    let show_feed = widget_states.show_activity_feed && config.activity_feed_lines > 0;
    let (projects_area, feed_area) = if show_feed && area.height > config.activity_feed_lines + 8 {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(8),
                Constraint::Length(config.activity_feed_lines + 2),
            ])
            .split(area);
        (chunks[0], Some(chunks[1]))
    } else {
        (area, None)
    };

    let projects = ProjectsTable::new(
        &filtered_projects,
        widget_states.filter_input_active,
        &widget_states.filter_input_text,
        config.max_pipelines_per_project,
        config.include_mr_pipelines,
    );
    f.render_stateful_widget(
        projects,
        projects_area,
        &mut widget_states.project_table_state,
    );

    if let Some(feed_area) = feed_area {
        let activity = ActivityFeed::new(app.recent_activity());
        f.render_widget(activity, feed_area);
    }

    update_main_click_regions(
        app,
        widget_states,
        &filtered_projects,
        projects_area,
        feed_area,
    );
}

fn render_popups(
    f: &mut Frame,
    widget_states: &mut StatefulWidgets,
    area: Rect,
    last_tick: Duration,
) {
    if let Some(project_details) = widget_states.project_details.as_mut() {
        let popup = ProjectDetailsPopup::new();
        let popup_area = area.inner(Margin::new(6, 2));
        f.render_stateful_widget(popup, popup_area, project_details);
    }

    if let Some(pipeline_actions) = widget_states.pipeline_actions.as_mut() {
        let popup = PipelineActionsPopup::new();
        f.render_stateful_widget(popup, area, pipeline_actions);
    }

    if let Some(config_popup) = &mut widget_states.config_popup_state {
        render_config_popup(f, config_popup, last_tick, area);
    }

    if let Some(notification) = &mut widget_states.notice {
        f.render_stateful_widget(Notification::new(), area, notification);
    }

    update_popup_click_regions(widget_states);
}

fn update_main_click_regions(
    app: &GlimApp,
    widget_states: &mut StatefulWidgets,
    filtered_projects: &[Project],
    projects_area: Rect,
    feed_area: Option<Rect>,
) {
    widget_states.click_regions.clear();
    let config = app.config();

    let table_area = if widget_states.filter_input_active {
        Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(3), Constraint::Length(3)])
            .split(projects_area)[0]
    } else {
        projects_area
    };

    let content_area = table_area.inner(Margin::new(2, 1));
    let offset = widget_states.project_table_state.offset();
    let mut row_y = content_area.y;

    let pipeline_col_x = (content_area.x + 36).min(content_area.right());
    let pipeline_col_w = content_area.right().saturating_sub(pipeline_col_x);
    let project_col_w = pipeline_col_x.saturating_sub(content_area.x);

    for (table_row, proj) in filtered_projects.iter().enumerate().skip(offset) {
        if row_y >= content_area.bottom() {
            break;
        }
        let (targets, row_height) = proj.row_targets_and_height(
            config.max_pipelines_per_project,
            config.include_mr_pipelines,
        );

        for (line_idx, target) in targets.iter().enumerate() {
            let line_y = row_y + line_idx as u16;
            if line_y >= content_area.bottom() {
                break;
            }
            let event = match target {
                RowEntryTarget::MergeRequest(iid) => GlimEvent::MergeRequestOpenUrl(proj.id, *iid),
                RowEntryTarget::Pipeline(pid) => GlimEvent::PipelineOpenUrl(proj.id, *pid),
            };
            if pipeline_col_w > 0 {
                widget_states.click_regions.push(ClickRegion {
                    area: Rect::new(pipeline_col_x, line_y, pipeline_col_w, 1),
                    action: ClickAction::SelectProjectAndDispatch {
                        table_row,
                        project_id: proj.id,
                        event: Some(event),
                    },
                });
            }
        }

        let visible_h = row_height.min(content_area.bottom().saturating_sub(row_y));
        if project_col_w > 0 && visible_h > 0 {
            widget_states.click_regions.push(ClickRegion {
                area: Rect::new(content_area.x, row_y, project_col_w, visible_h),
                action: ClickAction::SelectProjectAndDispatch {
                    table_row,
                    project_id: proj.id,
                    event: Some(GlimEvent::ProjectDetailsOpen(proj.id)),
                },
            });
        }

        row_y += row_height;
    }

    if let Some(feed_rect) = feed_area {
        let feed_inner = feed_rect.inner(Margin::new(2, 1));
        for (idx, ev) in app
            .recent_activity()
            .iter()
            .take(feed_inner.height as usize)
            .enumerate()
        {
            let line_y = feed_inner.y + idx as u16;
            let event = if let Some(iid) = ev.mr_iid {
                GlimEvent::MergeRequestOpenUrl(ev.project_id, iid)
            } else {
                GlimEvent::ProjectOpenUrl(ev.project_id)
            };
            widget_states.click_regions.push(ClickRegion {
                area: Rect::new(feed_inner.x, line_y, feed_inner.width, 1),
                action: ClickAction::Dispatch(vec![event]),
            });
        }
    }
}

fn update_popup_click_regions(widget_states: &mut StatefulWidgets) {
    if widget_states.config_popup_state.is_some() {
        widget_states.click_regions.clear();
        return;
    }

    if let Some(pa) = &widget_states.pipeline_actions {
        widget_states.click_regions.clear();
        let popup_rect = pa.popup_area.get();
        let inner = popup_rect.inner(Margin::new(2, 1));
        for (idx, action) in pa.actions.iter().enumerate() {
            let row_y = inner.y + idx as u16;
            if row_y < inner.bottom() {
                widget_states.click_regions.push(ClickRegion {
                    area: Rect::new(inner.x, row_y, inner.width, 1),
                    action: ClickAction::Dispatch(vec![
                        GlimEvent::PipelineActionsClose,
                        action.clone(),
                    ]),
                });
            }
        }
        return;
    }

    if let Some(pd) = &widget_states.project_details {
        widget_states.click_regions.clear();
        let popup_rect = pd.popup_area.get();
        let content_area = popup_rect.inner(Margin::new(2, 1));
        let header_h = 4.min(content_area.height);
        if header_h > 0 {
            widget_states.click_regions.push(ClickRegion {
                area: Rect::new(content_area.x, content_area.y, content_area.width, header_h),
                action: ClickAction::Dispatch(vec![GlimEvent::ProjectOpenUrl(pd.project.id)]),
            });
        }

        let table_y = content_area.y + 4;
        let pipelines = pd
            .project
            .recent_pipelines(pd.max_recent_pipelines, pd.include_mr_pipelines);
        let offset = pd.pipelines_table_state.offset();

        for (vis_idx, (table_row, p)) in pipelines.iter().enumerate().skip(offset).enumerate() {
            let row_y = table_y + (vis_idx as u16) * 2;
            if row_y >= content_area.bottom() {
                break;
            }
            let row_h = 2.min(content_area.bottom().saturating_sub(row_y));
            let event = if let Some(mr) = pd.project.merge_request_for_branch(&p.branch) {
                GlimEvent::MergeRequestOpenUrl(pd.project.id, mr.iid)
            } else {
                GlimEvent::PipelineOpenUrl(pd.project.id, p.id)
            };
            widget_states.click_regions.push(ClickRegion {
                area: Rect::new(content_area.x, row_y, content_area.width, row_h),
                action: ClickAction::SelectPipelineAndDispatch {
                    table_row,
                    pipeline_id: p.id,
                    event,
                },
            });
        }
    }
}

fn render_effects(
    f: &mut Frame,
    effects: &mut EffectRegistry,
    last_tick: Duration,
    frame_area: Rect,
) {
    effects.process_effects(last_tick, f.buffer_mut(), frame_area);
}

fn render_config_popup(
    f: &mut Frame,
    config_popup: &mut crate::ui::popup::ConfigPopupState,
    _last_tick: Duration,
    layout: Rect,
) {
    let popup = ConfigPopup::new();
    f.render_stateful_widget(popup, layout, config_popup);

    let cursor = config_popup.cursor_position;
    f.buffer_mut()
        .set_style(Rect::new(cursor.x, cursor.y, 1, 1), theme().input_selected);
    f.set_cursor_position(cursor);
}

fn handle_screen_capture(f: &mut Frame, app: &GlimApp, widget_states: &mut StatefulWidgets) {
    widget_states.capture_screen_requested = false;

    debug!("Converting screen buffer to ANSI string using tachyonfx");

    // Use tachyonfx's built-in function to convert buffer to ANSI string
    let ansi_output = tachyonfx::buffer_to_ansi_string(f.buffer_mut(), false);

    // Dispatch event to copy to clipboard since we can't access app's clipboard here
    app.dispatch(crate::event::GlimEvent::ScreenCaptureToClipboard(
        ansi_output,
    ));
}
