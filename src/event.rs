use std::{
    fmt::Debug,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread,
};

use compact_str::CompactString;
use crossterm::event::{
    self, Event as CrosstermEvent, KeyEvent, KeyEventKind, MouseButton, MouseEvent, MouseEventKind,
};
use tracing::Level;

use crate::{
    dispatcher::Dispatcher,
    domain::{
        CommitDto, GitlabEventDto, GitlabTodoDto, JobDto, MergeRequestDto, PipelineDto, Project,
        ProjectDto,
    },
    glim_app::GlimConfig,
    id::{JobId, PipelineId, ProjectId},
    result,
};

#[derive(Debug, Clone)]
pub enum GlimEvent {
    ActivityDetailsClose,
    ActivityDetailsOpen(u64),
    ActivityFirst,
    ActivityLast,
    ActivityLoadMore,
    ActivityNext,
    ActivityNotice(CompactString),
    ActivityPageDown,
    ActivityPageUp,
    ActivityPrevious,
    AppError(result::GlimError),
    AppExit,
    AppTick,
    ApplyTemporaryFilter(Option<CompactString>),
    CommitFetch(ProjectId, CompactString),
    CommitLoaded(ProjectId, CommitDto),
    CommitOpenUrl(ProjectId, CompactString),
    ConfigApply,
    ConfigClose,
    ConfigOpen,
    ConfigUpdate(GlimConfig),
    CopyToClipboard(CompactString),
    CurrentUserLoaded(CompactString),
    FilterClear,
    FilterInputBackspace,
    FilterInputChar(CompactString),
    FilterMenuClose,
    FilterMenuShow,
    FocusToggle,
    #[allow(dead_code)]
    GlitchOverride(GlitchState),
    InputKey(KeyEvent),
    InputMouse(MouseEvent),
    JobLogDownloaded(ProjectId, JobId, CompactString),
    JobLogFetch(ProjectId, PipelineId),
    JobOpenUrl(ProjectId, PipelineId, JobId),
    JobsActiveFetch,
    JobsFetch(ProjectId, PipelineId),
    JobsLoaded(ProjectId, PipelineId, Vec<JobDto>),
    LogEntry(CompactString),
    LogLevelChanged(Level),
    MergeRequestOpenUrl(ProjectId, u64),
    MergeRequestsFetch(ProjectId),
    MergeRequestsLoaded(ProjectId, Vec<MergeRequestDto>),
    NotificationDismiss,
    NotificationLast,
    PipelineActionsClose,
    PipelineActionsOpen(ProjectId, PipelineId),
    PipelineOpenUrl(ProjectId, PipelineId),
    PipelineSelected(PipelineId),
    PipelinesFetch(ProjectId),
    PipelinesLoaded(Vec<PipelineDto>),
    ProjectDetailsClose,
    ProjectDetailsOpen(ProjectId),
    ProjectEventsFetch(ProjectId),
    ProjectEventsLoaded(ProjectId, Vec<GitlabEventDto>),
    ProjectEventsPageFetch(ProjectId, u32),
    ProjectEventsPageLoaded(ProjectId, u32, Vec<GitlabEventDto>),
    #[allow(dead_code)]
    ProjectFetch(ProjectId),
    ProjectFirst,
    ProjectLast,
    ProjectNext,
    ProjectOpenUrl(ProjectId),
    ProjectPageDown,
    ProjectPageUp,
    ProjectPrevious,
    ProjectSelected(ProjectId),
    ProjectUpdated(Box<Project>),
    ProjectsFetch,
    ProjectsLoaded(Vec<ProjectDto>),
    ScreenCapture,
    ScreenCaptureToClipboard(String),
    TodosLoaded(Vec<GitlabTodoDto>),
    ToggleActivityFeed,
}

impl GlimEvent {
    /// Get the variant name as a string slice (without "GlimEvent::" prefix)
    pub fn variant_name(&self) -> &'static str {
        match self {
            GlimEvent::ActivityDetailsClose => "ActivityDetailsClose",
            GlimEvent::ActivityDetailsOpen(_) => "ActivityDetailsOpen",
            GlimEvent::ActivityFirst => "ActivityFirst",
            GlimEvent::ActivityLast => "ActivityLast",
            GlimEvent::ActivityLoadMore => "ActivityLoadMore",
            GlimEvent::ActivityNext => "ActivityNext",
            GlimEvent::ActivityNotice(_) => "ActivityNotice",
            GlimEvent::ActivityPageDown => "ActivityPageDown",
            GlimEvent::ActivityPageUp => "ActivityPageUp",
            GlimEvent::ActivityPrevious => "ActivityPrevious",
            GlimEvent::AppError(_) => "AppError",
            GlimEvent::AppExit => "AppExit",
            GlimEvent::AppTick => "AppTick",
            GlimEvent::ApplyTemporaryFilter(_) => "ApplyTemporaryFilter",
            GlimEvent::CommitFetch(_, _) => "CommitFetch",
            GlimEvent::CommitLoaded(_, _) => "CommitLoaded",
            GlimEvent::CommitOpenUrl(_, _) => "CommitOpenUrl",
            GlimEvent::ConfigApply => "ConfigApply",
            GlimEvent::ConfigClose => "ConfigClose",
            GlimEvent::ConfigOpen => "ConfigOpen",
            GlimEvent::ConfigUpdate(_) => "ConfigUpdate",
            GlimEvent::CopyToClipboard(_) => "CopyToClipboard",
            GlimEvent::CurrentUserLoaded(_) => "CurrentUserLoaded",
            GlimEvent::FilterClear => "FilterClear",
            GlimEvent::FilterInputBackspace => "FilterInputBackspace",
            GlimEvent::FilterInputChar(_) => "FilterInputChar",
            GlimEvent::FilterMenuClose => "FilterMenuClose",
            GlimEvent::FilterMenuShow => "FilterMenuShow",
            GlimEvent::FocusToggle => "FocusToggle",
            GlimEvent::GlitchOverride(_) => "GlitchOverride",
            GlimEvent::InputKey(_) => "InputKey",
            GlimEvent::InputMouse(_) => "InputMouse",
            GlimEvent::JobLogDownloaded(_, _, _) => "JobLogDownloaded",
            GlimEvent::JobLogFetch(_, _) => "JobLogFetch",
            GlimEvent::JobOpenUrl(_, _, _) => "JobOpenUrl",
            GlimEvent::JobsActiveFetch => "JobsActiveFetch",
            GlimEvent::JobsFetch(_, _) => "JobsFetch",
            GlimEvent::JobsLoaded(_, _, _) => "JobsLoaded",
            GlimEvent::LogEntry(_) => "LogEntry",
            GlimEvent::LogLevelChanged(_) => "LogLevelChanged",
            GlimEvent::MergeRequestOpenUrl(_, _) => "MergeRequestOpenUrl",
            GlimEvent::MergeRequestsFetch(_) => "MergeRequestsFetch",
            GlimEvent::MergeRequestsLoaded(_, _) => "MergeRequestsLoaded",
            GlimEvent::NotificationDismiss => "NotificationDismiss",
            GlimEvent::NotificationLast => "NotificationLast",
            GlimEvent::PipelineActionsClose => "PipelineActionsClose",
            GlimEvent::PipelineActionsOpen(_, _) => "PipelineActionsOpen",
            GlimEvent::PipelineOpenUrl(_, _) => "PipelineOpenUrl",
            GlimEvent::PipelineSelected(_) => "PipelineSelected",
            GlimEvent::PipelinesFetch(_) => "PipelinesFetch",
            GlimEvent::PipelinesLoaded(_) => "PipelinesLoaded",
            GlimEvent::ProjectDetailsClose => "ProjectDetailsClose",
            GlimEvent::ProjectDetailsOpen(_) => "ProjectDetailsOpen",
            GlimEvent::ProjectEventsFetch(_) => "ProjectEventsFetch",
            GlimEvent::ProjectEventsLoaded(_, _) => "ProjectEventsLoaded",
            GlimEvent::ProjectEventsPageFetch(_, _) => "ProjectEventsPageFetch",
            GlimEvent::ProjectEventsPageLoaded(_, _, _) => "ProjectEventsPageLoaded",
            GlimEvent::ProjectFetch(_) => "ProjectFetch",
            GlimEvent::ProjectFirst => "ProjectFirst",
            GlimEvent::ProjectLast => "ProjectLast",
            GlimEvent::ProjectNext => "ProjectNext",
            GlimEvent::ProjectOpenUrl(_) => "ProjectOpenUrl",
            GlimEvent::ProjectPageDown => "ProjectPageDown",
            GlimEvent::ProjectPageUp => "ProjectPageUp",
            GlimEvent::ProjectPrevious => "ProjectPrevious",
            GlimEvent::ProjectSelected(_) => "ProjectSelected",
            GlimEvent::ProjectUpdated(_) => "ProjectUpdated",
            GlimEvent::ProjectsFetch => "ProjectsFetch",
            GlimEvent::ProjectsLoaded(_) => "ProjectsLoaded",
            GlimEvent::ScreenCapture => "ScreenCapture",
            GlimEvent::ScreenCaptureToClipboard(_) => "ScreenCaptureToClipboard",
            GlimEvent::TodosLoaded(_) => "TodosLoaded",
            GlimEvent::ToggleActivityFeed => "ToggleActivityFeed",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum GlitchState {
    #[allow(dead_code)]
    RampedUp,
    #[allow(dead_code)]
    Normal,
}

#[derive(Debug)]
pub struct EventHandler {
    sender: mpsc::Sender<GlimEvent>,
    receiver: mpsc::Receiver<GlimEvent>,
    fast_tick: Arc<AtomicBool>,
    _handler: thread::JoinHandle<()>,
}

pub trait IntoGlimEvent {
    fn into_glim_event(self) -> GlimEvent;
}

impl EventHandler {
    pub fn new(tick_rate: std::time::Duration) -> Self {
        let (sender, receiver) = mpsc::channel();
        let fast_tick = Arc::new(AtomicBool::new(false));

        let handler = {
            let sender = sender.clone();
            let fast_tick = Arc::clone(&fast_tick);
            thread::spawn(move || {
                let mut last_tick = std::time::Instant::now();
                loop {
                    let current_rate = if fast_tick.load(Ordering::Relaxed) {
                        tick_rate
                    } else {
                        std::time::Duration::from_secs(15)
                    };
                    let timeout = current_rate
                        .checked_sub(last_tick.elapsed())
                        .unwrap_or(std::time::Duration::ZERO)
                        .min(std::time::Duration::from_millis(50));

                    if event::poll(timeout).expect("unable to poll for events") {
                        Self::apply_event(&sender);
                    }

                    if last_tick.elapsed() >= current_rate {
                        sender.dispatch(GlimEvent::AppTick);
                        last_tick = std::time::Instant::now();
                    }
                }
            })
        };

        Self { sender, receiver, fast_tick, _handler: handler }
    }

    pub fn set_fast_tick(&self, enabled: bool) {
        self.fast_tick.store(enabled, Ordering::Relaxed);
    }

    pub fn sender(&self) -> mpsc::Sender<GlimEvent> {
        self.sender.clone()
    }

    pub fn next(&self) -> Result<GlimEvent, mpsc::RecvError> {
        self.receiver.recv()
    }

    pub fn try_next(&self) -> Option<GlimEvent> {
        self.receiver.try_recv().ok()
    }

    fn apply_event(sender: &mpsc::Sender<GlimEvent>) {
        match event::read().expect("unable to read event") {
            CrosstermEvent::Key(e) if e.kind == KeyEventKind::Press => {
                sender.send(GlimEvent::InputKey(e))
            },
            CrosstermEvent::Mouse(e)
                if matches!(
                    e.kind,
                    MouseEventKind::Down(MouseButton::Left)
                        | MouseEventKind::Moved
                        | MouseEventKind::ScrollUp
                        | MouseEventKind::ScrollDown
                ) =>
            {
                sender.send(GlimEvent::InputMouse(e))
            },

            _ => Ok(()),
        }
        .expect("failed to send event")
    }
}

impl From<Vec<ProjectDto>> for GlimEvent {
    fn from(projects: Vec<ProjectDto>) -> Self {
        GlimEvent::ProjectsLoaded(projects)
    }
}

impl From<Vec<PipelineDto>> for GlimEvent {
    fn from(pipelines: Vec<PipelineDto>) -> Self {
        GlimEvent::PipelinesLoaded(pipelines)
    }
}

impl From<(ProjectId, PipelineId, Vec<JobDto>)> for GlimEvent {
    fn from(value: (ProjectId, PipelineId, Vec<JobDto>)) -> Self {
        let (project_id, pipeline_id, jobs) = value;
        GlimEvent::JobsLoaded(project_id, pipeline_id, jobs)
    }
}

impl IntoGlimEvent for Vec<ProjectDto> {
    fn into_glim_event(self) -> GlimEvent {
        GlimEvent::ProjectsLoaded(self)
    }
}

impl IntoGlimEvent for Vec<PipelineDto> {
    fn into_glim_event(self) -> GlimEvent {
        GlimEvent::PipelinesLoaded(self)
    }
}

impl IntoGlimEvent for (ProjectId, PipelineId, Vec<JobDto>) {
    fn into_glim_event(self) -> GlimEvent {
        let (project_id, pipeline_id, jobs) = self;
        GlimEvent::JobsLoaded(project_id, pipeline_id, jobs)
    }
}
