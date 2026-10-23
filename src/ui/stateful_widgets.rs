use std::sync::mpsc::Sender;

use compact_str::CompactString;
use crossterm::event::{MouseButton, MouseEvent, MouseEventKind};
use ratatui::{
    layout::{Position, Rect},
    widgets::TableState,
};
use tachyonfx::{Duration, RefRect};

use crate::{
    dispatcher::Dispatcher,
    domain::Project,
    effect_registry::EffectRegistry,
    event::GlimEvent,
    glim_app::{GlimApp, GlimConfig, Modulo},
    id::{PipelineId, ProjectId},
    ui::{
        popup::{
            ActivityDetailsPopupState, ConfigPopupState, PipelineActionsPopupState,
            ProjectDetailsPopupState,
        },
        widget::NotificationState,
    },
};

#[derive(Clone, Debug)]
pub enum ClickAction {
    SelectProjectAndDispatch {
        table_row: usize,
        project_id: ProjectId,
        event: Option<GlimEvent>,
    },
    SelectPipelineAndDispatch {
        table_row: usize,
        pipeline_id: PipelineId,
        event: GlimEvent,
    },
    SelectActivityAndDispatch {
        index: usize,
        activity_id: u64,
        event: GlimEvent,
    },
    Dispatch(Vec<GlimEvent>),
}

#[derive(Clone, Debug)]
pub struct ClickRegion {
    pub area: Rect,
    pub action: ClickAction,
}

pub struct StatefulWidgets {
    pub last_frame: Duration,
    pub sender: Sender<GlimEvent>,
    pub project_table_state: TableState,
    pub selected_project_id: Option<ProjectId>,
    pub config_popup_state: Option<ConfigPopupState>,
    pub project_details: Option<ProjectDetailsPopupState>,
    pub pipeline_actions: Option<PipelineActionsPopupState>,
    pub activity_details: Option<ActivityDetailsPopupState>,
    pub notice: Option<NotificationState>,
    pub filter_input_active: bool,
    pub filter_input_text: CompactString,
    pub temporary_filter: Option<CompactString>,
    pub capture_screen_requested: bool,
    pub show_activity_feed: bool,
    pub activity_feed_focused: bool,
    pub activity_selected_index: Option<usize>,
    pub selected_activity_id: Option<u64>,
    pub activity_feed_scroll: usize,
    pub last_feed_area: Option<Rect>,
    pub click_regions: Vec<ClickRegion>,
    current_filtered_indices: Vec<usize>,
}

impl StatefulWidgets {
    pub fn new(sender: Sender<GlimEvent>) -> Self {
        Self {
            last_frame: Duration::default(),
            sender,
            project_table_state: TableState::default().with_selected(0),
            selected_project_id: None,
            config_popup_state: None,
            project_details: None,
            pipeline_actions: None,
            activity_details: None,
            notice: None,
            filter_input_active: false,
            filter_input_text: CompactString::default(),
            temporary_filter: None,
            capture_screen_requested: false,
            show_activity_feed: true,
            activity_feed_focused: false,
            activity_selected_index: Some(0),
            selected_activity_id: None,
            activity_feed_scroll: 0,
            last_feed_area: None,
            click_regions: Vec::new(),
            current_filtered_indices: Vec::new(),
        }
    }

    pub fn apply(&mut self, app: &GlimApp, effects: &mut EffectRegistry, event: &GlimEvent) {
        match event {
            GlimEvent::InputMouse(mouse) => self.handle_mouse_event(*mouse, app),
            GlimEvent::ProjectNext => self.handle_project_selection(1, app),
            GlimEvent::ProjectPrevious => self.handle_project_selection(-1, app),
            GlimEvent::ProjectPageDown => self.handle_project_selection(5, app),
            GlimEvent::ProjectPageUp => self.handle_project_selection(-5, app),
            GlimEvent::ProjectFirst => self.handle_project_first(app),
            GlimEvent::ProjectLast => self.handle_project_last(app),
            GlimEvent::ActivityNext => self.handle_activity_selection(1, app),
            GlimEvent::ActivityPrevious => self.handle_activity_selection(-1, app),
            GlimEvent::ActivityPageDown => {
                let page = self.activity_page_size();
                self.handle_activity_selection(page, app);
            },
            GlimEvent::ActivityPageUp => {
                let page = self.activity_page_size();
                self.handle_activity_selection(-page, app);
            },
            GlimEvent::ActivityFirst => self.handle_activity_first(app),
            GlimEvent::ActivityLast => self.handle_activity_last(app),
            GlimEvent::FocusToggle => {
                if self.show_activity_feed {
                    self.activity_feed_focused = !self.activity_feed_focused;
                    if self.activity_feed_focused && self.selected_activity_id.is_none() {
                        if let Some(first) = app.recent_activity().first() {
                            self.activity_selected_index = Some(0);
                            self.selected_activity_id = Some(first.id);
                        }
                    }
                } else {
                    self.activity_feed_focused = false;
                }
            },
            GlimEvent::ActivityDetailsOpen(id) => {
                if let Some(ev) = app.activity_event(*id).cloned() {
                    let popup_area = RefRect::default();
                    effects.register_project_details(popup_area.clone());
                    self.activity_details = Some(ActivityDetailsPopupState::new(ev, popup_area));
                }
            },
            GlimEvent::ActivityDetailsClose => self.activity_details = None,
            GlimEvent::CommitLoaded(_, commit) => {
                if let Some(ad) = self.activity_details.as_mut() {
                    if ad.event.commit_sha.as_deref() == Some(commit.id.as_str()) {
                        ad.update_commit(commit);
                    }
                }
            },
            GlimEvent::ProjectsLoaded(_) => {
                let exclude_area_from_fx = self.current_popup_area();
                effects.register_projects_table_new_data(exclude_area_from_fx)
            },
            GlimEvent::ProjectDetailsOpen(id) => {
                let popup_area = RefRect::default();
                effects.register_project_details(popup_area.clone());
                self.open_project_details(
                    app.project(*id).clone(),
                    popup_area,
                    app.sender(),
                    app.config().max_recent_pipelines,
                    app.config().include_mr_pipelines,
                )
            },
            GlimEvent::ProjectDetailsClose => self.project_details = None,
            GlimEvent::ProjectUpdated(p) => self.refresh_project_details(p),

            GlimEvent::PipelineActionsClose => self.close_pipeline_actions(),
            GlimEvent::PipelineActionsOpen(project_id, pipeline_id) => {
                let popup_area = RefRect::default();
                effects.register_pipeline_actions(popup_area.clone());
                let project = app.project(*project_id);
                self.open_pipeline_actions(project, *pipeline_id, popup_area);
            },

            GlimEvent::ConfigOpen => {
                let popup_area = RefRect::default();
                effects.register_config_popup(popup_area.clone());
                self.open_config(app.load_config().unwrap_or_default(), popup_area);
            },
            GlimEvent::ConfigClose => self.config_popup_state = None,

            GlimEvent::FilterMenuShow => self.show_filter_input(),
            GlimEvent::FilterMenuClose => self.close_filter_input(),
            GlimEvent::FilterInputChar(c) => self.add_filter_char(c),
            GlimEvent::FilterInputBackspace => self.remove_filter_char(),
            GlimEvent::FilterClear => self.clear_filter(),
            GlimEvent::ApplyTemporaryFilter(filter) => self.apply_temporary_filter(filter.clone()),
            GlimEvent::ToggleActivityFeed => {
                self.show_activity_feed = !self.show_activity_feed;
                if !self.show_activity_feed {
                    self.activity_feed_focused = false;
                }
            },

            _ => (),
        }
    }

    fn current_popup_area(&self) -> Option<RefRect> {
        if let Some(pd) = &self.project_details {
            Some(pd.popup_area.clone())
        } else if let Some(ad) = &self.activity_details {
            Some(ad.popup_area.clone())
        } else if let Some(cp) = &self.config_popup_state {
            Some(cp.popup_area.clone())
        } else {
            self.pipeline_actions
                .as_ref()
                .map(|pa| pa.popup_area.clone())
        }
    }

    fn refresh_project_details(&mut self, project: &Project) {
        let requires_refresh = self
            .project_details
            .as_ref()
            .is_some_and(|pd| pd.project.id == project.id);

        if requires_refresh {
            let existing = self.project_details.take().unwrap();
            let prev_pipeline_id = existing.selected_pipeline_id;
            let next = existing.with_project(project.clone());
            if next.selected_pipeline_id != prev_pipeline_id {
                if let Some(pid) = next.selected_pipeline_id {
                    self.sender.dispatch(GlimEvent::PipelineSelected(pid));
                }
            }
            self.project_details = Some(next);
        }
    }

    fn open_project_details(
        &mut self,
        project: Project,
        area_tracker: RefRect,
        sender: Sender<GlimEvent>,
        max_recent_pipelines: usize,
        include_mr_pipelines: bool,
    ) {
        project
            .recent_pipelines(max_recent_pipelines, include_mr_pipelines)
            .first()
            .map(|p| sender.dispatch(GlimEvent::PipelineSelected(p.id)))
            .unwrap_or(());

        self.project_details = Some(ProjectDetailsPopupState::new(
            project,
            area_tracker,
            max_recent_pipelines,
            include_mr_pipelines,
        ));
    }

    fn open_config(&mut self, config: GlimConfig, popup_area: RefRect) {
        self.config_popup_state = Some(ConfigPopupState::new(config, popup_area));
    }

    fn open_pipeline_actions(
        &mut self,
        project: &Project,
        pipeline_id: PipelineId,
        popup_area: RefRect,
    ) {
        let pipeline = project.pipeline(pipeline_id);
        let failed_job = pipeline.and_then(|p| p.failed_job());
        let mr = pipeline.and_then(|p| project.merge_request_for_branch(&p.branch));
        let mr_iid = mr.map(|m| m.iid);
        let commit_sha = pipeline
            .and_then(|p| p.commit.as_ref())
            .map(|c| c.id.clone())
            .filter(|id| !id.is_empty());
        let commit_message = pipeline
            .and_then(|p| p.commit.as_ref())
            .map(|c| {
                if !c.message.is_empty() {
                    c.message.trim().to_string()
                } else {
                    c.title.to_string()
                }
            })
            .filter(|m| !m.is_empty())
            .or_else(|| mr.map(|m| m.title.to_string()));

        let mut actions = Vec::new();
        if let Some(iid) = mr_iid {
            actions.push(GlimEvent::MergeRequestOpenUrl(project.id, iid));
        }
        if let Some(ref sha) = commit_sha {
            actions.push(GlimEvent::CommitOpenUrl(project.id, sha.clone()));
        }
        if let Some(job) = failed_job {
            actions.push(GlimEvent::JobOpenUrl(project.id, pipeline_id, job.id));
            actions.push(GlimEvent::PipelineOpenUrl(project.id, pipeline_id));
            actions.push(GlimEvent::ProjectOpenUrl(project.id));
            actions.push(GlimEvent::JobLogFetch(project.id, pipeline_id));
        } else {
            actions.push(GlimEvent::PipelineOpenUrl(project.id, pipeline_id));
            actions.push(GlimEvent::ProjectOpenUrl(project.id));
        }
        if let Some(ref msg) = commit_message {
            actions.push(GlimEvent::CopyToClipboard(msg.as_str().into()));
        }

        self.pipeline_actions = Some(PipelineActionsPopupState::new(
            actions,
            commit_message,
            popup_area,
        ));
    }

    fn close_pipeline_actions(&mut self) {
        self.pipeline_actions = None;
    }

    fn handle_project_selection(&mut self, direction: i32, app: &GlimApp) {
        let all_projects = app.projects();
        let filtered_count = if self.current_filtered_indices.is_empty() {
            all_projects.len()
        } else {
            self.current_filtered_indices.len()
        };

        if filtered_count == 0 {
            return;
        }

        if let Some(current) = self.project_table_state.selected() {
            let new_index = current
                .saturating_add_signed(direction as isize)
                .min(filtered_count.saturating_sub(1));

            self.project_table_state.select(Some(new_index));

            // Get the actual project from the filtered list
            let project_index = if self.current_filtered_indices.is_empty() {
                new_index
            } else {
                self.current_filtered_indices[new_index]
            };

            let project = &all_projects[project_index];
            self.selected_project_id = Some(project.id);
            app.dispatch(GlimEvent::ProjectSelected(project.id));
        } else {
            self.project_table_state.select(Some(0));
        }
    }

    fn handle_project_first(&mut self, app: &GlimApp) {
        let all_projects = app.projects();
        let filtered_count = if self.current_filtered_indices.is_empty() {
            all_projects.len()
        } else {
            self.current_filtered_indices.len()
        };
        if filtered_count == 0 {
            return;
        }
        *self.project_table_state.offset_mut() = 0;
        self.project_table_state.select(Some(0));
        let project_index = if self.current_filtered_indices.is_empty() {
            0
        } else {
            self.current_filtered_indices[0]
        };
        let project = &all_projects[project_index];
        self.selected_project_id = Some(project.id);
        app.dispatch(GlimEvent::ProjectSelected(project.id));
    }

    fn handle_project_last(&mut self, app: &GlimApp) {
        let all_projects = app.projects();
        let filtered_count = if self.current_filtered_indices.is_empty() {
            all_projects.len()
        } else {
            self.current_filtered_indices.len()
        };
        if filtered_count == 0 {
            return;
        }
        let last_idx = filtered_count - 1;
        self.project_table_state.select(Some(last_idx));
        let project_index = if self.current_filtered_indices.is_empty() {
            last_idx
        } else {
            self.current_filtered_indices[last_idx]
        };
        let project = &all_projects[project_index];
        self.selected_project_id = Some(project.id);
        app.dispatch(GlimEvent::ProjectSelected(project.id));
    }

    pub fn handle_pipeline_selection(&mut self, direction: i32) {
        if self.project_details.is_none() {
            return;
        }
        let pd = self.project_details.as_mut().unwrap();

        if let Some(current) = pd.pipelines_table_state.selected() {
            let pipelines = pd
                .project
                .recent_pipelines(pd.max_recent_pipelines, pd.include_mr_pipelines);

            let new_index = (current as i32 + direction).modulo(pipelines.len() as i32) as usize;

            if pipelines.is_empty() {
                pd.pipelines_table_state.select(None);
                pd.selected_pipeline_id = None;
            } else {
                pd.pipelines_table_state.select(Some(new_index));
                let pipeline = &pipelines[new_index];
                pd.selected_pipeline_id = Some(pipeline.id);
                self.sender
                    .dispatch(GlimEvent::PipelineSelected(pipeline.id));
            }
        }
    }

    pub fn handle_pipeline_page(&mut self, delta: i32) {
        let Some(pd) = self.project_details.as_mut() else {
            return;
        };
        let pipelines = pd
            .project
            .recent_pipelines(pd.max_recent_pipelines, pd.include_mr_pipelines);
        if pipelines.is_empty() {
            pd.pipelines_table_state.select(None);
            pd.selected_pipeline_id = None;
            return;
        }
        let current = pd.pipelines_table_state.selected().unwrap_or(0);
        let new_index = current
            .saturating_add_signed(delta as isize)
            .min(pipelines.len() - 1);
        pd.pipelines_table_state.select(Some(new_index));
        let pipeline = &pipelines[new_index];
        pd.selected_pipeline_id = Some(pipeline.id);
        self.sender
            .dispatch(GlimEvent::PipelineSelected(pipeline.id));
    }

    pub fn handle_pipeline_home(&mut self) {
        let Some(pd) = self.project_details.as_mut() else {
            return;
        };
        let pipelines = pd
            .project
            .recent_pipelines(pd.max_recent_pipelines, pd.include_mr_pipelines);
        if pipelines.is_empty() {
            pd.pipelines_table_state.select(None);
            pd.selected_pipeline_id = None;
            return;
        }
        *pd.pipelines_table_state.offset_mut() = 0;
        pd.pipelines_table_state.select(Some(0));
        let pipeline = &pipelines[0];
        pd.selected_pipeline_id = Some(pipeline.id);
        self.sender
            .dispatch(GlimEvent::PipelineSelected(pipeline.id));
    }

    pub fn handle_pipeline_end(&mut self) {
        let Some(pd) = self.project_details.as_mut() else {
            return;
        };
        let pipelines = pd
            .project
            .recent_pipelines(pd.max_recent_pipelines, pd.include_mr_pipelines);
        if pipelines.is_empty() {
            pd.pipelines_table_state.select(None);
            pd.selected_pipeline_id = None;
            return;
        }
        let last_idx = pipelines.len() - 1;
        pd.pipelines_table_state.select(Some(last_idx));
        let pipeline = &pipelines[last_idx];
        pd.selected_pipeline_id = Some(pipeline.id);
        self.sender
            .dispatch(GlimEvent::PipelineSelected(pipeline.id));
    }

    pub fn handle_pipeline_action_selection(&mut self, direction: i32) {
        if self.pipeline_actions.is_none() {
            return;
        }

        let pipelines = self.pipeline_actions.as_mut().unwrap();
        if let Some(current) = pipelines.list_state.selected() {
            let new_index = (current as i32 + direction).modulo(pipelines.actions.len() as i32);

            pipelines
                .list_state
                .select(Some(new_index as usize));
        }
    }

    fn activity_page_size(&self) -> i32 {
        self.last_feed_area
            .map(|r| r.height.saturating_sub(2).max(1) as i32)
            .unwrap_or(8)
    }

    pub fn handle_activity_first(&mut self, app: &GlimApp) {
        let events = app.recent_activity();
        if events.is_empty() {
            self.activity_selected_index = None;
            self.selected_activity_id = None;
            return;
        }
        self.activity_selected_index = Some(0);
        self.selected_activity_id = Some(events[0].id);
        self.activity_feed_scroll = 0;
        if let Some(ad) = self.activity_details.as_mut() {
            let popup_area = ad.popup_area.clone();
            *ad = ActivityDetailsPopupState::new(events[0].clone(), popup_area);
        }
    }

    pub fn handle_activity_last(&mut self, app: &GlimApp) {
        let events = app.recent_activity();
        if events.is_empty() {
            self.activity_selected_index = None;
            self.selected_activity_id = None;
            return;
        }
        let last_idx = events.len() - 1;
        self.activity_selected_index = Some(last_idx);
        self.selected_activity_id = Some(events[last_idx].id);
        if let Some(feed_rect) = self.last_feed_area {
            let visible_rows = feed_rect.height.saturating_sub(2).max(1) as usize;
            self.activity_feed_scroll = (last_idx + 1).saturating_sub(visible_rows);
        }
        if let Some(ad) = self.activity_details.as_mut() {
            let popup_area = ad.popup_area.clone();
            *ad = ActivityDetailsPopupState::new(events[last_idx].clone(), popup_area);
        }
        self.sender.dispatch(GlimEvent::ActivityLoadMore);
    }

    pub fn handle_activity_selection(&mut self, direction: i32, app: &GlimApp) {
        let events = app.recent_activity();
        if events.is_empty() {
            self.activity_selected_index = None;
            self.selected_activity_id = None;
            return;
        }

        let current = self
            .selected_activity_id
            .and_then(|id| events.iter().position(|e| e.id == id))
            .or(self.activity_selected_index)
            .unwrap_or(0);
        let new_index = current
            .saturating_add_signed(direction as isize)
            .min(events.len().saturating_sub(1));

        self.activity_selected_index = Some(new_index);
        self.selected_activity_id = Some(events[new_index].id);

        if let Some(ad) = self.activity_details.as_mut() {
            let popup_area = ad.popup_area.clone();
            *ad = ActivityDetailsPopupState::new(events[new_index].clone(), popup_area);
        }

        if let Some(feed_rect) = self.last_feed_area {
            let visible_rows = feed_rect.height.saturating_sub(2).max(1) as usize;
            if new_index < self.activity_feed_scroll {
                self.activity_feed_scroll = new_index;
            } else if new_index >= self.activity_feed_scroll + visible_rows {
                self.activity_feed_scroll = new_index + 1 - visible_rows;
            }
        }

        if direction > 0 && new_index + 5 >= events.len() {
            self.sender.dispatch(GlimEvent::ActivityLoadMore);
        }
    }

    pub fn handle_activity_action_selection(&mut self, direction: i32) {
        if let Some(ad) = self.activity_details.as_mut() {
            if let Some(current) = ad.list_state.selected() {
                let new_index = (current as i32 + direction).modulo(ad.actions.len() as i32);
                ad.list_state.select(Some(new_index as usize));
            }
        }
    }

    fn show_filter_input(&mut self) {
        self.filter_input_active = true;
        // Start with the current temporary filter or empty string
        self.filter_input_text = self.temporary_filter.clone().unwrap_or_default();
    }

    fn close_filter_input(&mut self) {
        self.filter_input_active = false;
    }

    fn add_filter_char(&mut self, c: &str) {
        if self.filter_input_active {
            self.filter_input_text.push_str(c);
            // Apply filter immediately as user types
            let filter = if self.filter_input_text.is_empty() {
                None
            } else {
                Some(self.filter_input_text.clone())
            };
            self.temporary_filter = filter;
            self.selected_project_id = None;
            self.project_table_state.select(Some(0)); // Reset selection to first item
        }
    }

    fn remove_filter_char(&mut self) {
        if self.filter_input_active {
            self.filter_input_text.pop();
            // Apply filter immediately as user deletes characters
            let filter = if self.filter_input_text.is_empty() {
                None
            } else {
                Some(self.filter_input_text.clone())
            };
            self.temporary_filter = filter;
            self.selected_project_id = None;
            self.project_table_state.select(Some(0)); // Reset selection to first item
        }
    }

    fn apply_temporary_filter(&mut self, filter: Option<CompactString>) {
        self.temporary_filter = filter;
        self.filter_input_active = false;
        self.selected_project_id = None;
        // Reset table selection when filter changes
        self.project_table_state.select(Some(0));
    }

    fn clear_filter(&mut self) {
        self.filter_input_text.clear();
        self.filter_input_active = false;
        self.temporary_filter = None;
        self.sender
            .dispatch(GlimEvent::ApplyTemporaryFilter(None));
    }

    pub fn effective_filter(&self, config_filter: &Option<CompactString>) -> Option<CompactString> {
        // Temporary filter takes precedence over config filter
        self.temporary_filter
            .clone()
            .or_else(|| config_filter.clone())
    }

    pub fn update_filtered_indices(&mut self, indices: Vec<usize>) {
        self.current_filtered_indices = indices;
    }

    fn handle_mouse_event(&mut self, mouse: MouseEvent, app: &GlimApp) {
        let pos = Position::new(mouse.column, mouse.row);
        match mouse.kind {
            MouseEventKind::ScrollUp => {
                if self.activity_details.is_some() {
                    self.handle_activity_action_selection(-1);
                } else if self.pipeline_actions.is_some() {
                    self.handle_pipeline_action_selection(-1);
                } else if self.project_details.is_some() {
                    self.handle_pipeline_selection(-1);
                } else if self.config_popup_state.is_none() {
                    if self.last_feed_area.is_some_and(|r| r.contains(pos)) {
                        if self.activity_feed_focused {
                            self.handle_activity_selection(-1, app);
                        } else {
                            self.activity_feed_scroll = self.activity_feed_scroll.saturating_sub(1);
                        }
                    } else {
                        self.handle_project_selection(-1, app);
                    }
                }
            },
            MouseEventKind::ScrollDown => {
                if self.activity_details.is_some() {
                    self.handle_activity_action_selection(1);
                } else if self.pipeline_actions.is_some() {
                    self.handle_pipeline_action_selection(1);
                } else if self.project_details.is_some() {
                    self.handle_pipeline_selection(1);
                } else if self.config_popup_state.is_none() {
                    if self.last_feed_area.is_some_and(|r| r.contains(pos)) {
                        if self.activity_feed_focused {
                            self.handle_activity_selection(1, app);
                        } else {
                            let total = app.recent_activity().len();
                            let max_scroll = total.saturating_sub(1);
                            self.activity_feed_scroll =
                                (self.activity_feed_scroll + 1).min(max_scroll);
                            let visible_rows = self
                                .last_feed_area
                                .map(|r| r.height.saturating_sub(2).max(1) as usize)
                                .unwrap_or(8);
                            if self.activity_feed_scroll + visible_rows + 5 >= total {
                                self.sender.dispatch(GlimEvent::ActivityLoadMore);
                            }
                        }
                    } else {
                        self.handle_project_selection(1, app);
                    }
                }
            },
            MouseEventKind::Down(MouseButton::Left) => {
                let matched = self
                    .click_regions
                    .iter()
                    .find(|region| region.area.contains(pos))
                    .map(|r| r.action.clone());

                if let Some(action) = matched {
                    match action {
                        ClickAction::SelectProjectAndDispatch {
                            table_row,
                            project_id,
                            event,
                        } => {
                            self.activity_feed_focused = false;
                            self.project_table_state.select(Some(table_row));
                            self.selected_project_id = Some(project_id);
                            self.sender.dispatch(GlimEvent::ProjectSelected(project_id));
                            if let Some(ev) = event {
                                self.sender.dispatch(ev);
                            }
                        },
                        ClickAction::SelectActivityAndDispatch {
                            index,
                            activity_id,
                            event,
                        } => {
                            self.activity_feed_focused = true;
                            self.activity_selected_index = Some(index);
                            self.selected_activity_id = Some(activity_id);
                            self.sender.dispatch(event);
                        },
                        ClickAction::SelectPipelineAndDispatch {
                            table_row,
                            pipeline_id,
                            event,
                        } => {
                            if let Some(pd) = self.project_details.as_mut() {
                                pd.pipelines_table_state.select(Some(table_row));
                                pd.selected_pipeline_id = Some(pipeline_id);
                            }
                            self.sender
                                .dispatch(GlimEvent::PipelineSelected(pipeline_id));
                            self.sender.dispatch(event);
                        },
                        ClickAction::Dispatch(events) => {
                            for ev in events {
                                self.sender.dispatch(ev);
                            }
                        },
                    }
                }
            },
            _ => {},
        }
    }
}
