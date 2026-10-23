use std::{path::PathBuf, sync::mpsc::Sender};

use compact_str::CompactString;
use ratatui::layout::Rect;
use serde::{Deserialize, Serialize};
use tachyonfx::{Duration, RefRect};
use tracing::{debug, info, instrument, warn};

use crate::{
    client::{ClientConfig, GitlabService},
    config::save_config,
    dispatcher::Dispatcher,
    domain::Project,
    effect_registry::EffectRegistry,
    event::GlimEvent,
    id::ProjectId,
    input::{processor::NormalModeProcessor, InputMultiplexer},
    logging::LoggingReloadHandle,
    notice_service::{Notice, NoticeLevel, NoticeService},
    result::GlimError,
    stores::{log_event, ProjectStore},
    ui::{widget::NotificationState, StatefulWidgets},
};

pub struct GlimApp {
    running: bool,
    config_path: PathBuf,
    config: GlimConfig,
    gitlab: GitlabService,
    last_tick: std::time::Instant,
    sender: Sender<GlimEvent>,
    project_store: ProjectStore,
    notices: NoticeService,
    input: InputMultiplexer,
    clipboard: arboard::Clipboard,
    log_reload_handle: LoggingReloadHandle,
    current_log_level: tracing::Level,
}

pub const fn default_max_pipelines_per_project() -> usize {
    6
}

pub const fn default_max_recent_pipelines() -> usize {
    12
}

pub const fn default_true() -> bool {
    true
}

pub const fn default_projects_poll_seconds() -> u64 {
    15
}

pub const fn default_jobs_poll_seconds() -> u64 {
    10
}

pub const fn default_activity_feed_lines() -> u16 {
    6
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GlimConfig {
    /// The URL of the GitLab instance
    pub gitlab_url: CompactString,
    /// The Personal Access Token to authenticate with GitLab
    pub gitlab_token: CompactString,
    /// Filter applied to the projects list (supports comma-separated terms)
    pub search_filter: Option<CompactString>,
    /// Logging level: Off, Error, Warn, Info, Debug, Trace
    pub log_level: Option<CompactString>,
    /// Enable animations (default: true)
    #[serde(default = "default_true")]
    pub animations: bool,
    /// Enable random character glitch effect (default: true)
    #[serde(default = "default_true")]
    pub glitch_effect: bool,
    /// Enable full-table sweep/fade-in effect on project refresh (default: true)
    #[serde(default = "default_true")]
    pub table_sweep_effect: bool,
    /// Enable popup open/close animations and background dimming (default: true)
    #[serde(default = "default_true")]
    pub popup_effects: bool,
    /// Enable notification banner scramble/border/pulse animations (default: true)
    #[serde(default = "default_true")]
    pub notification_effects: bool,
    /// Maximum number of pipelines (branches/MRs) displayed per project in the main table
    #[serde(default = "default_max_pipelines_per_project")]
    pub max_pipelines_per_project: usize,
    /// Maximum number of pipelines displayed in the project details popup
    #[serde(default = "default_max_recent_pipelines")]
    pub max_recent_pipelines: usize,
    /// Hide projects with no activity in the last N days
    #[serde(default)]
    pub max_age_days: Option<u32>,
    /// Include merge_request_event pipelines (default: true)
    #[serde(default = "default_true")]
    pub include_mr_pipelines: bool,
    /// Optional explicit allowlist of project paths or substrings
    #[serde(default)]
    pub include_projects: Option<Vec<CompactString>>,
    /// Optional explicit blocklist of project paths or substrings
    #[serde(default)]
    pub exclude_projects: Option<Vec<CompactString>>,
    /// Send desktop OS notifications via notify-send on pipeline/MR updates (default: true)
    #[serde(default = "default_true")]
    pub os_notifications: bool,
    /// Ignore automated bot / linear-linkback comments in notifications & feed (default: true)
    #[serde(default = "default_true")]
    pub ignore_bot_comments: bool,
    /// Polling interval in seconds for projects, pipelines, MRs, and events (default: 15)
    #[serde(default = "default_projects_poll_seconds")]
    pub projects_poll_seconds: u64,
    /// Polling interval in seconds for active jobs (default: 10)
    #[serde(default = "default_jobs_poll_seconds")]
    pub jobs_poll_seconds: u64,
    /// Number of lines in the bottom MR & repo activity feed panel (0 to hide, default: 6)
    #[serde(default = "default_activity_feed_lines")]
    pub activity_feed_lines: u16,
}

impl Default for GlimConfig {
    fn default() -> Self {
        Self {
            gitlab_url: "https://".into(),
            gitlab_token: "".into(),
            search_filter: None,
            log_level: Some("Error".into()),
            animations: true,
            glitch_effect: true,
            table_sweep_effect: true,
            popup_effects: true,
            notification_effects: true,
            max_pipelines_per_project: default_max_pipelines_per_project(),
            max_recent_pipelines: default_max_recent_pipelines(),
            max_age_days: None,
            include_mr_pipelines: true,
            include_projects: None,
            exclude_projects: None,
            os_notifications: true,
            ignore_bot_comments: true,
            projects_poll_seconds: default_projects_poll_seconds(),
            jobs_poll_seconds: default_jobs_poll_seconds(),
            activity_feed_lines: default_activity_feed_lines(),
        }
    }
}

impl GlimApp {
    pub fn new(
        sender: Sender<GlimEvent>,
        config_path: PathBuf,
        gitlab: GitlabService,
        log_reload_handle: LoggingReloadHandle,
        config: &GlimConfig,
    ) -> Self {
        let mut input = InputMultiplexer::new(sender.clone());
        input.push(Box::new(NormalModeProcessor::new(sender.clone())));

        // Parse initial log level from config, defaulting to ERROR
        let current_log_level = config
            .log_level
            .as_ref()
            .and_then(|level_str| level_str.parse().ok())
            .unwrap_or(tracing::Level::ERROR);

        Self {
            running: true,
            config_path,
            config: config.clone(),
            gitlab,
            last_tick: std::time::Instant::now(),
            sender: sender.clone(),
            project_store: ProjectStore::new(sender, config),
            notices: NoticeService::new(),
            input,
            clipboard: arboard::Clipboard::new().expect("failed to create clipboard"),
            log_reload_handle,
            current_log_level,
        }
    }

    #[instrument(skip(self, event, ui, effects), fields(event_type = %event.variant_name()))]
    pub fn apply(
        &mut self,
        event: GlimEvent,
        ui: &mut StatefulWidgets,
        effects: &mut EffectRegistry,
    ) {
        self.input.apply(&event, ui);
        log_event(&event);
        effects.apply(&event);
        self.notices.apply(&event);
        self.project_store.apply(&event);

        match event {
            GlimEvent::AppExit => self.running = false,

            // www
            GlimEvent::ProjectOpenUrl(id) => {
                debug!(project_id = %id, "Opening project in browser");
                let _ = open::that(&self.project(id).url);
            },
            GlimEvent::PipelineOpenUrl(project_id, pipeline_id) => {
                debug!(project_id = %project_id, pipeline_id = %pipeline_id, "Opening pipeline in browser");
                let project = self.project(project_id);
                if let Some(pipeline) = project.pipeline(pipeline_id) {
                    let _ = open::that(&pipeline.url);
                }
            },
            GlimEvent::JobOpenUrl(project_id, pipeline_id, job_id) => {
                debug!(project_id = %project_id, pipeline_id = %pipeline_id, job_id = %job_id, "Opening job in browser");
                let project = self.project(project_id);
                if let Some(job_url) = project
                    .pipeline(pipeline_id)
                    .and_then(|p| p.job(job_id))
                    .map(|job| &job.url)
                {
                    let _ = open::that(job_url);
                }
            },
            GlimEvent::MergeRequestOpenUrl(project_id, mr_iid) => {
                debug!(project_id = %project_id, mr_iid = %mr_iid, "Opening merge request in browser");
                let project = self.project(project_id);
                let url = project
                    .merge_request(mr_iid)
                    .map(|mr| mr.url.to_string())
                    .unwrap_or_else(|| format!("{}/-/merge_requests/{}", project.url, mr_iid));
                let _ = open::that(&url);
            },

            GlimEvent::JobLogFetch(project_id, pipeline_id) => {
                debug!(project_id = %project_id, pipeline_id = %pipeline_id, "Downloading error log");
                let project = self.project(project_id);
                let pipeline = project
                    .pipeline(pipeline_id)
                    .expect("pipeline not found");

                let job = pipeline
                    .failed_job()
                    .expect("no failed job found");

                self.gitlab
                    .spawn_download_job_log(project_id, job.id);
            },
            GlimEvent::JobLogDownloaded(project_id, job_id, trace) => {
                info!(project_id = %project_id, job_id = %job_id, trace_length = trace.len(), "Job log downloaded and copied to clipboard");
                self.clipboard.set_text(trace).unwrap();
            },
            GlimEvent::CopyToClipboard(text) => {
                let _ = self.clipboard.set_text(text.to_string());
            },

            GlimEvent::JobsActiveFetch => {
                debug!("Requesting active jobs for all projects");
                self.project_store
                    .sorted_projects()
                    .iter()
                    .flat_map(|p| p.pipelines.iter())
                    .flatten()
                    .filter(|p| p.status.is_active() || p.has_active_jobs())
                    .for_each(|p| self.gitlab.spawn_fetch_jobs(p.project_id, p.id));
            },
            GlimEvent::PipelinesFetch(id) => {
                debug!(project_id = %id, "Requesting pipelines, MRs, and events for project");
                self.gitlab.spawn_fetch_pipelines(id, None);
                self.gitlab.spawn_fetch_merge_requests(id);
                self.gitlab.spawn_fetch_project_events(id);
            },
            GlimEvent::MergeRequestsFetch(id) => {
                debug!(project_id = %id, "Requesting open merge requests for project");
                self.gitlab.spawn_fetch_merge_requests(id);
            },
            GlimEvent::ProjectEventsFetch(id) => {
                debug!(project_id = %id, "Requesting recent events for project");
                self.gitlab.spawn_fetch_project_events(id);
            },
            GlimEvent::ProjectsFetch => {
                self.gitlab.spawn_fetch_projects(None)
            },
            GlimEvent::JobsFetch(project_id, pipeline_id) => {
                debug!(project_id = %project_id, pipeline_id = %pipeline_id, "Requesting jobs for pipeline");
                self.gitlab
                    .spawn_fetch_jobs(project_id, pipeline_id)
            },

            // configuration
            GlimEvent::ConfigUpdate(config) => {
                self.config = config.clone();
                let client_config = ClientConfig::from(config.clone())
                    .with_debug_logging(self.gitlab.config().debug.log_responses);
                let _ = self.gitlab.update_config(client_config);

                // Update logging level
                if let Some(ref log_level_str) = config.log_level {
                    self.update_logging_level(log_level_str);
                }
            },
            GlimEvent::LogLevelChanged(level) => {
                info!("Log level changed to: {:?}", level);
                // Event is primarily for user confirmation - actual level change is
                // handled in update_logging_level
            },
            GlimEvent::ConfigApply => {
                if let Some(config_popup) = ui.config_popup_state.as_ref() {
                    let config = config_popup.to_config();
                    let client_config = ClientConfig::from(config.clone())
                        .with_debug_logging(self.gitlab.config().debug.log_responses);

                    // Pre-validate configuration before attempting to connect
                    if let Err(validation_error) = client_config.validate() {
                        let glim_error = GlimError::from(&validation_error);
                        self.dispatch(GlimEvent::AppError(glim_error));
                        return;
                    }

                    // Create a temporary service for connection validation
                    match self.gitlab.update_config(client_config) {
                        Ok(_) => {
                            save_config(&self.config_path, config.clone())
                                .expect("failed to save config");
                            self.dispatch(GlimEvent::ConfigUpdate(config));
                            self.dispatch(GlimEvent::ConfigClose);
                            self.dispatch(GlimEvent::ProjectsFetch);
                        },
                        Err(e) => {
                            let glim_error = GlimError::config_connection_error(e.to_string());
                            self.dispatch(GlimEvent::AppError(glim_error));
                        },
                    }
                }
            },

            GlimEvent::NotificationLast => {
                self.project_store.notify_latest_activity();
                if let Some(notice) = self.notices.last_notification() {
                    let content_area = RefRect::new(Rect::default());
                    effects.register_notification_effect(content_area.clone());
                    ui.notice = Some(NotificationState::new(
                        notice.clone(),
                        &self.project_store,
                        content_area,
                    ));
                }
            },

            GlimEvent::NotificationDismiss => {
                ui.notice = None;
            },

            GlimEvent::FilterMenuShow => {
                // Initialize filter input with the current temporary filter
                // The show_filter_input method will handle initialization
                ui.filter_input_active = true;
            },

            GlimEvent::ScreenCapture => {
                debug!("Screen capture requested");
                // The actual screen capture will be handled in the rendering loop
                // where we have access to the frame buffer
                ui.capture_screen_requested = true;
            },

            GlimEvent::ScreenCaptureToClipboard(ansi_string) => {
                debug!("Copying screen capture to clipboard");
                match self.clipboard.set_text(ansi_string) {
                    Ok(_) => {
                        info!("Screen buffer captured and copied to clipboard");
                    },
                    Err(e) => {
                        warn!(error = %e, "Failed to copy screen capture to clipboard");
                    },
                }
            },

            _ => {},
        }

        // if there are any error notifications, and the current notification is an info notice,
        // dismiss it
        if self.notices.has_error()
            && ui
                .notice
                .as_ref()
                .map(|n| n.notice.level == NoticeLevel::Info)
                .unwrap_or(false)
        {
            ui.notice = None;
        }

        if ui.notice.is_none() {
            // if there's a notice waiting, update fetch it
            if let Some(notice) = self.pop_notice() {
                let content_area = RefRect::new(Rect::default());
                effects.register_notification_effect(content_area.clone());
                ui.notice = Some(NotificationState::new(
                    notice,
                    &self.project_store,
                    content_area,
                ));
            }
        }
    }

    pub fn config(&self) -> &GlimConfig {
        &self.config
    }

    pub fn load_config(&self) -> Result<GlimConfig, GlimError> {
        let config_file = &self.config_path;
        if config_file.exists() {
            let config: GlimConfig = confy::load_path(config_file)
                .map_err(|e| GlimError::config_load_error(config_file.clone(), e))?;

            Ok(config)
        } else {
            Err(GlimError::config_file_not_found(config_file.clone()))
        }
    }

    pub fn process_timers(&mut self) -> Duration {
        let now = std::time::Instant::now();
        let elapsed = now - self.last_tick;
        self.last_tick = now;

        // do nothing with elapsed time for now;
        // and consider moving to UiState

        Duration::from_millis(elapsed.as_millis() as u32)
    }

    pub fn project(&self, id: ProjectId) -> &Project {
        self.project_store
            .find(id)
            .expect("project not found")
    }

    pub fn projects(&self) -> &[Project] {
        self.project_store.sorted_projects()
    }

    pub fn recent_activity(&self) -> &[crate::domain::ProjectActivityEvent] {
        self.project_store.recent_activity()
    }

    pub fn activity_event(&self, id: u64) -> Option<&crate::domain::ProjectActivityEvent> {
        self.project_store.find_activity(id)
    }

    pub fn filtered_projects(
        &self,
        temporary_filter: &Option<CompactString>,
    ) -> (Vec<Project>, Vec<usize>) {
        let all_projects = self.project_store.sorted_projects();
        let now = chrono::Utc::now();

        let search_terms: Vec<String> = temporary_filter
            .as_ref()
            .map(|f| {
                f.split(',')
                    .map(|s| s.trim().to_lowercase())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        let include_terms: Vec<String> = self
            .config
            .include_projects
            .as_ref()
            .map(|list| {
                list.iter()
                    .map(|s| s.trim().to_lowercase())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        let exclude_terms: Vec<String> = self
            .config
            .exclude_projects
            .as_ref()
            .map(|list| {
                list.iter()
                    .map(|s| s.trim().to_lowercase())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        let mut filtered_projects = Vec::new();
        let mut filtered_indices = Vec::new();

        for (index, project) in all_projects.iter().enumerate() {
            // 1. Check max_age_days cutoff
            if let Some(max_age) = self.config.max_age_days {
                if max_age > 0 {
                    let age_days = now
                        .signed_duration_since(project.last_activity_at)
                        .num_days();
                    if age_days > max_age as i64 {
                        continue;
                    }
                }
            }

            let path_lower = project.path.to_lowercase();
            let desc_lower = project
                .description
                .as_ref()
                .map(|d| d.to_lowercase());

            // 2. Check exclude_projects blocklist
            if !exclude_terms.is_empty()
                && exclude_terms
                    .iter()
                    .any(|term| path_lower.contains(term.as_str()))
            {
                continue;
            }

            // 3. Check include_projects allowlist
            if !include_terms.is_empty()
                && !include_terms
                    .iter()
                    .any(|term| path_lower.contains(term.as_str()))
            {
                continue;
            }

            // 4. Check search_filter / temporary_filter (supports comma-separated terms)
            if !search_terms.is_empty() {
                let matches_search = search_terms.iter().any(|term| {
                    path_lower.contains(term.as_str())
                        || desc_lower
                            .as_ref()
                            .is_some_and(|d| d.contains(term.as_str()))
                });
                if !matches_search {
                    continue;
                }
            }

            filtered_projects.push(project.clone());
            filtered_indices.push(index);
        }

        (filtered_projects, filtered_indices)
    }

    pub fn sender(&self) -> Sender<GlimEvent> {
        self.sender.clone()
    }

    pub fn is_running(&self) -> bool {
        self.running
    }

    pub fn pop_notice(&mut self) -> Option<Notice> {
        self.notices.pop_notice()
    }

    /// Update the logging level at runtime
    fn update_logging_level(&mut self, log_level_str: &str) {
        let level = match log_level_str.to_lowercase().as_str() {
            "error" => tracing::Level::ERROR,
            "warn" => tracing::Level::WARN,
            "info" => tracing::Level::INFO,
            "debug" => tracing::Level::DEBUG,
            "trace" => tracing::Level::TRACE,
            _ => {
                warn!(
                    "Invalid log level: {}, keeping current level",
                    log_level_str
                );
                return;
            },
        };

        // Only update and dispatch if the level actually changed
        if level != self.current_log_level {
            info!(
                "Updating log level from {:?} to {:?}",
                self.current_log_level, level
            );
            self.log_reload_handle.update_levels(level, level);
            self.current_log_level = level;

            // Dispatch confirmation event to user
            self.dispatch(GlimEvent::LogLevelChanged(level));
        }
    }
}

impl Dispatcher for GlimApp {
    fn dispatch(&self, event: GlimEvent) {
        self.sender.send(event).unwrap_or(());
    }
}

#[allow(unused)]
pub fn modulo(a: u32, b: u32) -> u32 {
    if b == 0 {
        return 0;
    }

    let a = a as i32;
    let b = b as i32;
    ((a % b) + b) as u32 % b as u32
}

pub trait Modulo {
    fn modulo(self, b: Self) -> Self;
}

impl Modulo for i32 {
    fn modulo(self, b: i32) -> i32 {
        if b == 0 {
            return 0;
        }

        ((self % b) + b) % b
    }
}

impl Modulo for u32 {
    fn modulo(self, b: u32) -> u32 {
        if b == 0 {
            return 0;
        }

        (self as i32).modulo(b as i32) as u32
    }
}

impl Modulo for isize {
    fn modulo(self, b: isize) -> isize {
        if b == 0 {
            return 0;
        }

        ((self % b) + b) % b
    }
}

impl Modulo for usize {
    fn modulo(self, b: usize) -> usize {
        if b == 0 {
            return 0;
        }

        (self as isize).modulo(b as isize) as usize
    }
}
