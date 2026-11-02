use ratatui::{
    layout::{Constraint, Layout, Margin},
    prelude::{Direction, Rect},
    style::Modifier,
    Frame,
};
use tachyonfx::Duration;
use tracing::debug;

use crate::{
    dispatcher::Dispatcher,
    domain::{Project, RowEntryTarget},
    effect_registry::EffectRegistry,
    event::GlimEvent,
    glim_app::{CiViewMode, GlimApp},
    theme::theme,
    ui::{
        popup::{ActivityDetailsPopup, ConfigPopup, PipelineActionsPopup, ProjectDetailsPopup},
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
    render_hover_highlight(f, widget_states);
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
    let ranking_mode = widget_states.effective_ranking_mode(config);
    let stochastic_nonce = widget_states.stochastic_nonce;
    let (filtered_projects, filtered_indices) = app.filtered_projects_with_mode(
        &effective_filter,
        ranking_mode,
        stochastic_nonce,
    );
    widget_states.update_filtered_indices(filtered_indices);

    let now = chrono::Utc::now();
    let events = app.recent_activity();
    let interactions = app.user_interactions();
    let username = app.current_username();
    let ctx = crate::relevance::RelevanceContext::new(config, events, interactions, username, now)
        .with_mode(Some(ranking_mode), stochastic_nonce);
    let compact_ci_mode =
        config.dynamic_split && widget_states.activity_feed_focused && widget_states.show_activity_feed;
    let view_mode = widget_states.effective_ci_view_mode(config);

    let (projects_area, feed_area, row_budgets) = match view_mode {
        CiViewMode::Grouped => {
            widget_states.current_flat_targets.clear();
            widget_states.selected_flat_target = None;

            if filtered_projects.is_empty() {
                widget_states.project_table_state.select(None);
            } else if let Some(sel_id) = widget_states.selected_project_id {
                if let Some(idx) = filtered_projects.iter().position(|p| p.id == sel_id) {
                    widget_states.project_table_state.select(Some(idx));
                } else {
                    let clamped = widget_states
                        .project_table_state
                        .selected()
                        .unwrap_or(0)
                        .min(filtered_projects.len() - 1);
                    widget_states.project_table_state.select(Some(clamped));
                    let new_id = filtered_projects[clamped].id;
                    widget_states.selected_project_id = Some(new_id);
                    widget_states.sender.dispatch(GlimEvent::ProjectSelected(new_id));
                }
            } else {
                let idx = widget_states
                    .project_table_state
                    .selected()
                    .unwrap_or(0)
                    .min(filtered_projects.len() - 1);
                widget_states.project_table_state.select(Some(idx));
                let new_id = filtered_projects[idx].id;
                widget_states.selected_project_id = Some(new_id);
                widget_states.sender.dispatch(GlimEvent::ProjectSelected(new_id));
            }

            let (projects_h, feed_h_opt, budgets) =
                crate::relevance::compute_dynamic_split_and_budgets(
                    &filtered_projects,
                    widget_states.selected_project_id,
                    area.height,
                    widget_states.show_activity_feed,
                    widget_states.activity_feed_focused,
                    widget_states.filter_input_active,
                    &ctx,
                );

            let (p_area, f_area) = if let Some(feed_h) = feed_h_opt {
                let chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([Constraint::Length(projects_h), Constraint::Min(feed_h)])
                    .split(area);
                (chunks[0], Some(chunks[1]))
            } else {
                (area, None)
            };

            let projects = ProjectsTable::new_dynamic(
                &filtered_projects,
                &ctx,
                &budgets,
                widget_states.filter_input_active,
                &widget_states.filter_input_text,
                compact_ci_mode,
                !widget_states.activity_feed_focused,
            );
            f.render_stateful_widget(
                projects,
                p_area,
                &mut widget_states.project_table_state,
            );

            (p_area, f_area, budgets)
        },
        CiViewMode::Flat => {
            let flat_entries =
                crate::relevance::rank_global_ci_runs(&filtered_projects, &ctx, 40);
            widget_states.current_flat_targets = flat_entries
                .iter()
                .map(|item| {
                    (
                        item.project.id,
                        item.entry.target.clone(),
                        item.entry.pipeline.map(|p| p.id),
                    )
                })
                .collect();

            if widget_states.current_flat_targets.is_empty() {
                widget_states.project_table_state.select(None);
                widget_states.selected_flat_target = None;
            } else {
                let matched_idx = widget_states
                    .selected_flat_target
                    .as_ref()
                    .and_then(|(sel_proj, sel_target, _)| {
                        widget_states
                            .current_flat_targets
                            .iter()
                            .position(|(pid, t, _)| pid == sel_proj && t == sel_target)
                    })
                    .or_else(|| {
                        widget_states.selected_project_id.and_then(|sel_proj| {
                            widget_states
                                .current_flat_targets
                                .iter()
                                .position(|(pid, _, _)| *pid == sel_proj)
                        })
                    })
                    .unwrap_or_else(|| {
                        widget_states
                            .project_table_state
                            .selected()
                            .unwrap_or(0)
                            .min(widget_states.current_flat_targets.len() - 1)
                    });

                widget_states.project_table_state.select(Some(matched_idx));
                let chosen = widget_states.current_flat_targets[matched_idx].clone();
                if widget_states.selected_project_id != Some(chosen.0) {
                    widget_states.selected_project_id = Some(chosen.0);
                    widget_states
                        .sender
                        .dispatch(GlimEvent::ProjectSelected(chosen.0));
                }
                widget_states.selected_flat_target = Some(chosen);
            }

            let (projects_h, feed_h_opt) = crate::relevance::compute_flat_ci_split(
                flat_entries.len(),
                area.height,
                widget_states.show_activity_feed,
                widget_states.activity_feed_focused,
                widget_states.filter_input_active,
                config,
                events,
                now,
            );

            let (p_area, f_area) = if let Some(feed_h) = feed_h_opt {
                let chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([Constraint::Length(projects_h), Constraint::Min(feed_h)])
                    .split(area);
                (chunks[0], Some(chunks[1]))
            } else {
                (area, None)
            };

            let flat_table = ProjectsTable::new_flat(
                &flat_entries,
                widget_states.filter_input_active,
                &widget_states.filter_input_text,
                compact_ci_mode,
                !widget_states.activity_feed_focused,
                ranking_mode,
            );
            f.render_stateful_widget(
                flat_table,
                p_area,
                &mut widget_states.project_table_state,
            );

            (p_area, f_area, std::collections::HashMap::new())
        },
    };
    widget_states.last_feed_area = feed_area;

    if let Some(feed_area) = feed_area {
        if events.is_empty() {
            widget_states.activity_selected_index = None;
            widget_states.selected_activity_id = None;
        } else if let Some(sel_id) = widget_states.selected_activity_id {
            if let Some(idx) = events.iter().position(|e| e.id == sel_id) {
                widget_states.activity_selected_index = Some(idx);
            } else {
                let clamped = widget_states
                    .activity_selected_index
                    .unwrap_or(0)
                    .min(events.len() - 1);
                widget_states.activity_selected_index = Some(clamped);
                widget_states.selected_activity_id = Some(events[clamped].id);
            }
        } else if widget_states.activity_feed_focused {
            let idx = widget_states.activity_feed_scroll.min(events.len() - 1);
            widget_states.activity_selected_index = Some(idx);
            widget_states.selected_activity_id = Some(events[idx].id);
        }

        if widget_states.activity_feed_focused {
            if let Some(sel_idx) = widget_states.activity_selected_index {
                let visible_rows = feed_area.height.saturating_sub(2).max(1) as usize;
                if sel_idx < widget_states.activity_feed_scroll {
                    widget_states.activity_feed_scroll = sel_idx;
                } else if sel_idx >= widget_states.activity_feed_scroll + visible_rows {
                    widget_states.activity_feed_scroll = sel_idx + 1 - visible_rows;
                }
            }
        } else if !events.is_empty() {
            let visible_rows = feed_area.height.saturating_sub(2).max(1) as usize;
            let max_scroll = events.len().saturating_sub(visible_rows);
            widget_states.activity_feed_scroll = widget_states.activity_feed_scroll.min(max_scroll);
        }

        let activity = ActivityFeed::new(
            events,
            widget_states.activity_feed_scroll,
            widget_states.activity_feed_focused,
            widget_states.activity_selected_index,
            app.is_loading_more_activity(),
        );
        f.render_widget(activity, feed_area);
    }

    update_main_click_regions(
        app,
        widget_states,
        &filtered_projects,
        &row_budgets,
        &ctx,
        compact_ci_mode,
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

    if let Some(activity_details) = widget_states.activity_details.as_mut() {
        let popup = ActivityDetailsPopup::new();
        f.render_stateful_widget(popup, area, activity_details);
    }

    if let Some(config_popup) = &mut widget_states.config_popup_state {
        render_config_popup(f, config_popup, last_tick, area);
    }

    if let Some(notification) = &mut widget_states.notice {
        f.render_stateful_widget(Notification::new(), area, notification);
    }

    update_popup_click_regions(widget_states);
}

#[allow(clippy::too_many_arguments)]
fn update_main_click_regions(
    app: &GlimApp,
    widget_states: &mut StatefulWidgets,
    filtered_projects: &[Project],
    row_budgets: &std::collections::HashMap<crate::id::ProjectId, usize>,
    ctx: &crate::relevance::RelevanceContext<'_>,
    compact_ci_mode: bool,
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

    if widget_states.effective_ci_view_mode(config) == CiViewMode::Flat {
        for (table_row, (proj_id, target, _)) in widget_states
            .current_flat_targets
            .clone()
            .into_iter()
            .enumerate()
            .skip(offset)
        {
            if row_y >= content_area.bottom() {
                break;
            }
            let event = match target {
                RowEntryTarget::MergeRequest(iid) => GlimEvent::MergeRequestOpenUrl(proj_id, iid),
                RowEntryTarget::Pipeline(pid) => GlimEvent::PipelineOpenUrl(proj_id, pid),
            };
            widget_states.click_regions.push(ClickRegion {
                area: Rect::new(content_area.x, row_y, content_area.width, 1),
                hover_area: None,
                action: ClickAction::SelectProjectAndDispatch {
                    table_row,
                    project_id: proj_id,
                    event: Some(event),
                },
            });
            row_y += 1;
        }
    } else {
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(16),
                Constraint::Min(40),
                Constraint::Percentage(100),
            ])
            .spacing(1)
            .split(content_area);

        let project_col_x = cols[1].x;
        let project_col_w = cols[1].width;
        let pipeline_col_x = cols[2].x;
        let pipeline_col_w = cols[2].width;

        for (table_row, proj) in filtered_projects.iter().enumerate().skip(offset) {
            if row_y >= content_area.bottom() {
                break;
            }
            let budget = row_budgets
                .get(&proj.id)
                .copied()
                .unwrap_or(config.max_pipelines_per_project);
            let (targets, row_height) =
                proj.ranked_targets_and_height(ctx, budget, compact_ci_mode);

            for (line_idx, target) in targets.iter().enumerate() {
                let line_y = row_y + line_idx as u16;
                if line_y >= content_area.bottom() {
                    break;
                }
                let event = match target {
                    RowEntryTarget::MergeRequest(iid) => {
                        GlimEvent::MergeRequestOpenUrl(proj.id, *iid)
                    },
                    RowEntryTarget::Pipeline(pid) => GlimEvent::PipelineOpenUrl(proj.id, *pid),
                };
                if pipeline_col_w > 0 {
                    widget_states.click_regions.push(ClickRegion {
                        area: Rect::new(pipeline_col_x, line_y, pipeline_col_w, 1),
                        hover_area: None,
                        action: ClickAction::SelectProjectAndDispatch {
                            table_row,
                            project_id: proj.id,
                            event: Some(event),
                        },
                    });
                }
            }

            let visible_h = row_height.min(content_area.bottom().saturating_sub(row_y));
            if visible_h > 0 {
                widget_states.click_regions.push(ClickRegion {
                    area: Rect::new(
                        content_area.x,
                        row_y,
                        cols[0].width + 1 + project_col_w,
                        visible_h,
                    ),
                    hover_area: Some(Rect::new(project_col_x, row_y, project_col_w, visible_h)),
                    action: ClickAction::SelectProjectAndDispatch {
                        table_row,
                        project_id: proj.id,
                        event: Some(GlimEvent::ProjectDetailsOpen(proj.id)),
                    },
                });
            }

            row_y += row_height;
        }
    }

    if let Some(feed_rect) = feed_area {
        let feed_inner = feed_rect.inner(Margin::new(2, 1));
        for (vis_idx, (abs_idx, ev)) in app
            .recent_activity()
            .iter()
            .enumerate()
            .skip(widget_states.activity_feed_scroll)
            .take(feed_inner.height as usize)
            .enumerate()
        {
            let line_y = feed_inner.y + vis_idx as u16;
            widget_states.click_regions.push(ClickRegion {
                area: Rect::new(feed_inner.x, line_y, feed_inner.width, 1),
                hover_area: None,
                action: ClickAction::SelectActivityAndDispatch {
                    index: abs_idx,
                    activity_id: ev.id,
                    event: GlimEvent::ActivityDetailsOpen(ev.id),
                },
            });
        }
    }
}

fn update_popup_click_regions(widget_states: &mut StatefulWidgets) {
    if widget_states.config_popup_state.is_some() {
        widget_states.click_regions.clear();
        return;
    }

    if let Some(ad) = &widget_states.activity_details {
        widget_states.click_regions.clear();
        let popup_rect = ad.popup_area.get();
        let content_area = popup_rect.inner(Margin::new(2, 1));
        let actions_h = ad.actions.len() as u16;
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(3),
                Constraint::Length(actions_h),
            ])
            .split(content_area);
        let actions_area = chunks[2];
        for (idx, (_, action)) in ad.actions.iter().enumerate() {
            let row_y = actions_area.y + idx as u16;
            if row_y < actions_area.bottom() {
                widget_states.click_regions.push(ClickRegion {
                    area: Rect::new(actions_area.x, row_y, actions_area.width, 1),
                    hover_area: None,
                    action: ClickAction::Dispatch(vec![
                        GlimEvent::ActivityDetailsClose,
                        action.clone(),
                    ]),
                });
            }
        }
        return;
    }

    if let Some(pa) = &widget_states.pipeline_actions {
        widget_states.click_regions.clear();
        let popup_rect = pa.popup_area.get();
        let inner = popup_rect.inner(Margin::new(2, 1));
        let actions_area = if pa.commit_message.is_some() {
            Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Min(2),
                    Constraint::Length(pa.actions.len() as u16),
                ])
                .split(inner)[1]
        } else {
            inner
        };
        for (idx, action) in pa.actions.iter().enumerate() {
            let row_y = actions_area.y + idx as u16;
            if row_y < actions_area.bottom() {
                widget_states.click_regions.push(ClickRegion {
                    area: Rect::new(actions_area.x, row_y, actions_area.width, 1),
                    hover_area: None,
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
                hover_area: Some(Rect::new(
                    content_area.x,
                    content_area.y,
                    content_area.width.saturating_sub(22),
                    header_h,
                )),
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
                hover_area: None,
                action: ClickAction::SelectPipelineAndDispatch {
                    table_row,
                    pipeline_id: p.id,
                    event,
                },
            });
        }
    }
}

fn render_hover_highlight(f: &mut Frame, widget_states: &StatefulWidgets) {
    let Some(pos) = widget_states.mouse_position else {
        return;
    };
    let Some(region) = widget_states
        .click_regions
        .iter()
        .find(|r| r.area.contains(pos))
    else {
        return;
    };

    let buf = f.buffer_mut();
    let target_rect = region.hover_area.unwrap_or(region.area);
    let area = target_rect.intersection(buf.area);
    for y in area.top()..area.bottom() {
        let mut first_non_space = None;
        let mut last_non_space = None;
        for x in area.left()..area.right() {
            let sym = buf[(x, y)].symbol();
            if !sym.trim().is_empty() {
                if first_non_space.is_none() {
                    first_non_space = Some(x);
                }
                last_non_space = Some(x);
            }
        }
        if let (Some(start_x), Some(end_x)) = (first_non_space, last_non_space) {
            for x in start_x..=end_x {
                let cell = &mut buf[(x, y)];
                cell.set_style(cell.style().add_modifier(Modifier::UNDERLINED));
            }
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
