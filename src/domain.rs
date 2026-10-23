// GitLab API Documentation: https://docs.gitlab.com/ee/api/api_resources.html
use chrono::{DateTime, Duration, Local, Utc};
use compact_str::{CompactString, ToCompactString};
use itertools::Itertools;
use ratatui::{
    text::{Line, Span, Text},
    widgets::Row,
};
use serde::{Deserialize, Serialize};

use crate::{
    id::{JobId, PipelineId, ProjectId},
    theme::theme,
    ui::{format_duration, widget::text_from},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Project {
    pub id: ProjectId,
    pub path: CompactString,
    pub description: Option<CompactString>,
    pub default_branch: CompactString,
    pub ssh_git_url: CompactString,
    pub url: CompactString,
    pub last_activity_at: DateTime<Utc>,
    pub pipelines: Option<Vec<Pipeline>>,
    pub merge_requests: Vec<MergeRequest>,
    pub commit_count: u32,
    pub repo_size_kb: u64,
    pub artifacts_size_kb: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct MergeRequest {
    pub id: u64,
    pub iid: u64,
    pub project_id: ProjectId,
    pub title: CompactString,
    pub description: Option<CompactString>,
    pub state: CompactString,
    pub source_branch: CompactString,
    pub target_branch: CompactString,
    pub draft: bool,
    pub detailed_merge_status: CompactString,
    pub has_conflicts: bool,
    pub blocking_discussions_resolved: bool,
    pub user_notes_count: u32,
    pub upvotes: u32,
    pub author_username: CompactString,
    pub updated_at: DateTime<Utc>,
    pub url: CompactString,
}

impl MergeRequest {
    pub fn is_conflicted(&self) -> bool {
        self.has_conflicts || self.detailed_merge_status == "conflict"
    }

    pub fn is_mergeable(&self) -> bool {
        !self.is_conflicted()
            && !self.draft
            && self.blocking_discussions_resolved
            && (self.detailed_merge_status == "mergeable"
                || self.detailed_merge_status == "can_be_merged")
    }

    pub fn has_unresolved_threads(&self) -> bool {
        !self.blocking_discussions_resolved
            || self.detailed_merge_status == "discussions_not_resolved"
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[allow(dead_code)]
pub enum ActivityKind {
    Mention,
    ReviewRequested,
    Assigned,
    MergeTrainRemoved,
    MrOpened,
    MrMerged,
    MrClosed,
    MrApproved,
    MrComment,
    BranchPush,
    PipelineStatus,
}

impl ActivityKind {
    pub fn icon(&self) -> &'static str {
        match self {
            ActivityKind::Mention => "📣",
            ActivityKind::ReviewRequested => "👀",
            ActivityKind::Assigned => "📌",
            ActivityKind::MergeTrainRemoved => "🚂",
            ActivityKind::MrOpened => "🟣",
            ActivityKind::MrMerged => "🟢",
            ActivityKind::MrClosed => "🚫",
            ActivityKind::MrApproved => "👍",
            ActivityKind::MrComment => "💬",
            ActivityKind::BranchPush => "⬆️",
            ActivityKind::PipelineStatus => "🔔",
        }
    }

    pub fn action_label(&self) -> &'static str {
        match self {
            ActivityKind::Mention => "mentioned you",
            ActivityKind::ReviewRequested => "requested review",
            ActivityKind::Assigned => "assigned you",
            ActivityKind::MergeTrainRemoved => "removed from merge train",
            ActivityKind::MrOpened => "opened",
            ActivityKind::MrMerged => "merged",
            ActivityKind::MrClosed => "closed",
            ActivityKind::MrApproved => "approved",
            ActivityKind::MrComment => "commented",
            ActivityKind::BranchPush => "pushed",
            ActivityKind::PipelineStatus => "pipeline",
        }
    }

    pub fn is_direct_attention(&self) -> bool {
        matches!(
            self,
            ActivityKind::Mention
                | ActivityKind::ReviewRequested
                | ActivityKind::Assigned
                | ActivityKind::MergeTrainRemoved
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct ProjectActivityEvent {
    pub id: u64,
    pub project_id: ProjectId,
    pub project_name: CompactString,
    pub kind: ActivityKind,
    pub author: CompactString,
    pub mr_iid: Option<u64>,
    pub branch: Option<CompactString>,
    #[serde(default)]
    pub commit_sha: Option<CompactString>,
    pub summary: CompactString,
    pub detail: Option<CompactString>,
    #[serde(default)]
    pub body: Option<CompactString>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Pipeline {
    pub id: PipelineId,
    pub project_id: ProjectId,
    pub status: PipelineStatus,
    pub source: PipelineSource,
    pub branch: CompactString,
    pub url: CompactString,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub jobs: Option<Vec<Job>>,
    pub commit: Option<Commit>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Commit {
    #[serde(default)]
    pub id: CompactString,
    pub title: CompactString,
    #[serde(default)]
    pub message: CompactString,
    #[allow(dead_code)]
    pub author_name: CompactString,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Job {
    pub id: JobId,
    pub name: CompactString,
    pub status: PipelineStatus,
    #[allow(dead_code)]
    pub stage: CompactString,
    #[allow(dead_code)]
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub url: CompactString,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ProjectDto {
    pub id: ProjectId,
    pub path_with_namespace: CompactString,
    pub description: Option<CompactString>,
    pub default_branch: CompactString,
    pub ssh_url_to_repo: CompactString,
    pub web_url: CompactString,
    pub last_activity_at: DateTime<Utc>,
    pub statistics: StatisticsDto,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct StatisticsDto {
    pub commit_count: u32,
    pub job_artifacts_size: u64,
    pub repository_size: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CommitDto {
    #[serde(default)]
    pub id: CompactString,
    #[serde(default)]
    pub title: CompactString,
    #[serde(default)]
    pub message: CompactString,
    #[serde(default)]
    pub author_name: CompactString,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct JobDto {
    pub id: JobId,
    pub name: CompactString,
    pub stage: CompactString,
    pub commit: CommitDto,
    pub status: PipelineStatus,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub web_url: CompactString,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PipelineDto {
    pub id: PipelineId,
    pub project_id: ProjectId,
    pub status: PipelineStatus,
    pub source: PipelineSource,
    #[serde(rename = "ref")]
    pub branch: CompactString,
    pub web_url: CompactString,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

const fn default_true_bool() -> bool {
    true
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct MergeRequestAuthorDto {
    #[serde(default)]
    pub username: CompactString,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct MergeRequestDto {
    pub id: u64,
    pub iid: u64,
    pub project_id: ProjectId,
    pub title: CompactString,
    #[serde(default)]
    pub description: Option<CompactString>,
    pub state: CompactString,
    pub source_branch: CompactString,
    pub target_branch: CompactString,
    #[serde(default)]
    pub draft: bool,
    #[serde(default)]
    pub detailed_merge_status: Option<CompactString>,
    #[serde(default)]
    pub merge_status: Option<CompactString>,
    #[serde(default)]
    pub has_conflicts: bool,
    #[serde(default = "default_true_bool")]
    pub blocking_discussions_resolved: bool,
    #[serde(default)]
    pub user_notes_count: u32,
    #[serde(default)]
    pub upvotes: u32,
    #[serde(default)]
    pub author: MergeRequestAuthorDto,
    pub updated_at: DateTime<Utc>,
    pub web_url: CompactString,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[allow(dead_code)]
pub struct GitlabEventNoteDto {
    pub id: u64,
    pub body: CompactString,
    pub noteable_type: Option<CompactString>,
    pub noteable_iid: Option<u64>,
    #[serde(default)]
    pub system: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[allow(dead_code)]
pub struct GitlabEventPushDataDto {
    #[serde(default)]
    pub commit_count: u32,
    #[serde(default)]
    pub action: CompactString,
    #[serde(default)]
    pub ref_type: CompactString,
    #[serde(rename = "ref")]
    pub branch: Option<CompactString>,
    pub commit_to: Option<CompactString>,
    pub commit_title: Option<CompactString>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[allow(dead_code)]
pub struct GitlabEventDto {
    pub id: u64,
    pub project_id: ProjectId,
    pub action_name: CompactString,
    pub target_type: Option<CompactString>,
    pub target_iid: Option<u64>,
    pub target_title: Option<CompactString>,
    #[serde(default)]
    pub author_username: CompactString,
    pub created_at: DateTime<Utc>,
    pub note: Option<GitlabEventNoteDto>,
    pub push_data: Option<GitlabEventPushDataDto>,
}

impl GitlabEventDto {
    /// Returns true if this event was generated by an automated project bot or Linear linkback comment.
    pub fn is_bot_event(&self) -> bool {
        let user = self.author_username.as_str();
        if user.starts_with("project_") && user.contains("_bot") {
            return true;
        }
        if let Some(note) = &self.note {
            if note.system || note.body.contains("<!-- linear-linkback -->") {
                return true;
            }
        }
        false
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[allow(dead_code)]
pub struct GitlabUserDto {
    pub id: u64,
    #[serde(default)]
    pub username: CompactString,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[allow(dead_code)]
pub struct TodoProjectDto {
    pub id: ProjectId,
    #[serde(default)]
    pub path_with_namespace: CompactString,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[allow(dead_code)]
pub struct TodoTargetDto {
    pub iid: Option<u64>,
    pub title: Option<CompactString>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[allow(dead_code)]
pub struct GitlabTodoDto {
    pub id: u64,
    #[serde(default)]
    pub action_name: CompactString,
    #[serde(default)]
    pub target_type: CompactString,
    #[serde(default)]
    pub target_url: CompactString,
    #[serde(default)]
    pub body: CompactString,
    #[serde(default)]
    pub author: MergeRequestAuthorDto,
    pub project: Option<TodoProjectDto>,
    pub target: Option<TodoTargetDto>,
    pub created_at: DateTime<Utc>,
}

/// Returns true if `text` contains `@username` (or `@all`) with a word boundary.
pub fn contains_mention(text: &str, username: &str) -> bool {
    if username.is_empty() {
        return false;
    }
    let lower = text.to_lowercase();
    let user_lower = username.to_lowercase();
    for needle in [format!("@{user_lower}"), "@all".to_string()] {
        let mut search_from = 0;
        while let Some(pos) = lower[search_from..].find(&needle) {
            let abs_pos = search_from + pos;
            let after_pos = abs_pos + needle.len();
            let valid_before = abs_pos == 0
                || !lower[..abs_pos]
                    .chars()
                    .next_back()
                    .is_some_and(|c| c.is_alphanumeric() || c == '_');
            let valid_after = after_pos >= lower.len()
                || !lower[after_pos..]
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_alphanumeric() || c == '_' || c == '-');
            if valid_before && valid_after {
                return true;
            }
            search_from = after_pos;
        }
    }
    false
}

/// Strip HTML tags while preserving line/paragraph breaks for multi-line inspection popups.
pub fn clean_multiline_body(raw: &str) -> CompactString {
    let with_breaks = raw
        .replace("<br>", "\n")
        .replace("<br/>", "\n")
        .replace("<br />", "\n")
        .replace("</p>", "\n")
        .replace("</li>", "\n")
        .replace("</div>", "\n");
    let mut out = String::with_capacity(with_breaks.len());
    let mut in_tag = false;
    for ch in with_breaks.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {},
        }
    }
    let lines: Vec<String> = out
        .lines()
        .map(|l| l.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|l| !l.is_empty())
        .collect();
    lines.join("\n").into()
}
pub fn clean_note_body(raw: &str, max_chars: usize) -> CompactString {
    let mut out = String::with_capacity(raw.len().min(max_chars + 16));
    let mut in_tag = false;
    for ch in raw.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => {
                in_tag = false;
                out.push(' ');
            },
            _ if !in_tag => out.push(ch),
            _ => {},
        }
    }
    let collapsed = out.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() > max_chars {
        let truncated: String = collapsed.chars().take(max_chars).collect();
        format!("{truncated}…").into()
    } else {
        collapsed.into()
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PipelineStatus {
    #[default]
    Created,
    WaitingForResource,
    Preparing,
    Pending,
    Running,
    Success,
    Failed,
    Canceling,
    Canceled,
    Skipped,
    Manual,
    Scheduled,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PipelineSource {
    #[default]
    Api,
    Chat,
    External,
    ExternalPullRequestEvent,
    MergeRequestEvent,
    OndemandDastScan,
    OndemandDastValidation,
    ParentPipeline,
    Pipeline,
    Push,
    Schedule,
    SecurityOrchestrationPolicy,
    Trigger,
    Web,
    Webide,
}

impl PipelineSource {
    pub fn to_string(&self) -> CompactString {
        match self {
            PipelineSource::Api => "api",
            PipelineSource::Chat => "chat",
            PipelineSource::External => "external",
            PipelineSource::ExternalPullRequestEvent => "pull request",
            PipelineSource::MergeRequestEvent => "merge request",
            PipelineSource::OndemandDastScan => "dast scan",
            PipelineSource::OndemandDastValidation => "dast validation",
            PipelineSource::ParentPipeline => "parent pipeline",
            PipelineSource::Pipeline => "pipeline",
            PipelineSource::Push => "push",
            PipelineSource::Schedule => "schedule",
            PipelineSource::SecurityOrchestrationPolicy => "sec policy",
            PipelineSource::Trigger => "trigger",
            PipelineSource::Web => "web",
            PipelineSource::Webide => "web ide",
        }
        .into()
    }
}

impl PipelineStatus {
    pub(crate) fn is_active(&self) -> bool {
        self < &PipelineStatus::Success
    }
}

impl PipelineSource {
    pub(crate) fn is_interesting(&self, include_mr_pipelines: bool) -> bool {
        if include_mr_pipelines
            && matches!(
                self,
                PipelineSource::MergeRequestEvent | PipelineSource::ExternalPullRequestEvent
            )
        {
            return true;
        }
        matches!(
            self,
            PipelineSource::Api
                | PipelineSource::Chat
                | PipelineSource::ParentPipeline
                | PipelineSource::Push
                | PipelineSource::Schedule
                | PipelineSource::Trigger
                | PipelineSource::Web
                | PipelineSource::Webide
        )
    }
}

impl Project {
    #[allow(dead_code)]
    pub fn row(&self) -> Row<'_> {
        Row::new(vec![
            Span::from(self.last_activity_at.to_compact_string()),
            Span::from(self.path.as_str()),
            Span::from(self.default_branch.as_str()),
        ])
    }

    pub fn last_activity(&self) -> DateTime<Utc> {
        self.last_activity_at
    }

    pub fn title(&self) -> CompactString {
        match self.path.rfind('/') {
            Some(i) => self.path[i + 1..].into(),
            None => self.path.clone(),
        }
    }

    pub fn first_pipeline_per_branch(
        &self,
        count: usize,
        include_mr_pipelines: bool,
        predicate: impl Fn(&Pipeline) -> bool,
    ) -> Vec<&Pipeline> {
        if let Some(pipelines) = self.pipelines.as_ref() {
            pipelines
                .iter()
                .filter(|p| p.source.is_interesting(include_mr_pipelines) || predicate(p))
                .unique_by(|p| &p.branch)
                .take(count)
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn recent_pipelines(&self, max_count: usize, include_mr_pipelines: bool) -> Vec<&Pipeline> {
        if let Some(pipelines) = self.pipelines.as_ref() {
            pipelines
                .iter()
                .filter(|p| p.source.is_interesting(include_mr_pipelines))
                .take(max_count)
                .collect()
        } else {
            Vec::new()
        }
    }

    #[allow(dead_code)]
    pub fn has_active_pipelines(&self) -> bool {
        self.pipelines.as_ref().is_some_and(|ps| {
            ps.iter()
                .any(|p| p.status.is_active() || p.has_active_jobs())
        })
    }

    pub fn path_and_name(&self) -> (&str, &str) {
        match self.path.rfind('/') {
            Some(i) => (&self.path[0..=i], &self.path[i + 1..]),
            None => ("", self.path.as_str()),
        }
    }

    pub fn pipeline(&self, id: PipelineId) -> Option<&Pipeline> {
        self.pipelines
            .as_ref()
            .and_then(|ps| ps.iter().find(|p| p.id == id))
    }

    pub fn merge_request_for_branch(&self, branch: &str) -> Option<&MergeRequest> {
        self.merge_requests
            .iter()
            .find(|mr| mr.source_branch == branch)
    }

    pub fn merge_request(&self, iid: u64) -> Option<&MergeRequest> {
        self.merge_requests.iter().find(|mr| mr.iid == iid)
    }
}

impl From<ProjectDto> for Project {
    fn from(p: ProjectDto) -> Self {
        Self {
            id: p.id,
            description: p.description,
            path: p.path_with_namespace,
            default_branch: p.default_branch,
            ssh_git_url: p.ssh_url_to_repo,
            url: p.web_url,
            last_activity_at: p.last_activity_at,
            pipelines: None,
            merge_requests: Vec::new(),
            commit_count: p.statistics.commit_count,
            repo_size_kb: p.statistics.repository_size / 1024,
            artifacts_size_kb: p.statistics.job_artifacts_size / 1024,
        }
    }
}

impl From<MergeRequestDto> for MergeRequest {
    fn from(dto: MergeRequestDto) -> Self {
        let detailed_merge_status = dto
            .detailed_merge_status
            .or(dto.merge_status)
            .unwrap_or_else(|| "unknown".into());
        Self {
            id: dto.id,
            iid: dto.iid,
            project_id: dto.project_id,
            title: dto.title,
            description: dto.description,
            state: dto.state,
            source_branch: dto.source_branch,
            target_branch: dto.target_branch,
            draft: dto.draft,
            detailed_merge_status,
            has_conflicts: dto.has_conflicts,
            blocking_discussions_resolved: dto.blocking_discussions_resolved,
            user_notes_count: dto.user_notes_count,
            upvotes: dto.upvotes,
            author_username: dto.author.username,
            updated_at: dto.updated_at,
            url: dto.web_url,
        }
    }
}

impl Job {
    pub fn duration(&self) -> Duration {
        match (&self.started_at, &self.finished_at) {
            (Some(begin), Some(end)) => end.signed_duration_since(begin),
            (Some(begin), None) => Utc::now().signed_duration_since(begin),
            _ => Duration::zero(),
        }
    }
}

impl Project {
    pub fn update_pipelines(&mut self, pipelines: Vec<Pipeline>) {
        self.pipelines = Some(
            pipelines
                .iter()
                .map(|p| {
                    if let Some(existing) = self
                        .pipelines
                        .as_ref()
                        .and_then(|ps| ps.iter().find(|ep| ep.id == p.id))
                    {
                        let mut new = p.clone();
                        new.jobs.clone_from(&existing.jobs);
                        new.commit.clone_from(&existing.commit);
                        new
                    } else {
                        p.clone()
                    }
                })
                .sorted_by(|a, b| b.updated_at.cmp(&a.updated_at))
                .collect(),
        );
    }

    pub fn update_project(&mut self, project: Project) {
        self.id = project.id;
        self.path = project.path;
        self.description = project.description;
        self.default_branch = project.default_branch;
        self.ssh_git_url = project.ssh_git_url;
        self.url = project.url;
        self.last_activity_at = project.last_activity_at;
        self.commit_count = project.commit_count;
        self.repo_size_kb = project.repo_size_kb;
        self.artifacts_size_kb = project.artifacts_size_kb;
    }

    pub fn update_jobs(&mut self, pipeline_id: PipelineId, jobs: Vec<Job>) {
        if let Some(pipelines) = self.pipelines.as_mut() {
            if let Some(pipeline) = pipelines.iter_mut().find(|p| p.id == pipeline_id) {
                pipeline.jobs = Some(jobs);
            }
        }
    }

    pub fn update_commit(&mut self, pipeline_id: PipelineId, commit: Commit) {
        if let Some(pipelines) = self.pipelines.as_mut() {
            if let Some(pipeline) = pipelines.iter_mut().find(|p| p.id == pipeline_id) {
                pipeline.commit = Some(commit);
            }
        }
    }

    pub fn update_merge_requests(&mut self, merge_requests: Vec<MergeRequest>) {
        self.merge_requests = merge_requests
            .into_iter()
            .sorted_by(|a, b| b.updated_at.cmp(&a.updated_at))
            .collect();
    }
}

impl From<PipelineDto> for Pipeline {
    fn from(p: PipelineDto) -> Self {
        Self {
            id: p.id,
            project_id: p.project_id,
            status: p.status,
            source: p.source,
            branch: p.branch,
            url: p.web_url,
            created_at: p.created_at,
            updated_at: p.updated_at,
            jobs: None,
            commit: None,
        }
    }
}

impl From<JobDto> for Job {
    fn from(j: JobDto) -> Self {
        Self {
            id: j.id,
            name: j.name,
            stage: j.stage,
            status: j.status,
            created_at: j.created_at,
            started_at: j.started_at,
            finished_at: j.finished_at,
            url: j.web_url,
        }
    }
}

impl From<CommitDto> for Commit {
    fn from(c: CommitDto) -> Self {
        Self {
            id: c.id,
            title: c.title,
            message: c.message,
            author_name: c.author_name,
        }
    }
}

impl Pipeline {
    pub fn has_active_jobs(&self) -> bool {
        self.jobs
            .as_ref()
            .is_some_and(|jobs| jobs.iter().any(|j| j.status.is_active()))
    }

    pub fn active_job(&self) -> Option<&Job> {
        self.jobs
            .as_ref()
            .and_then(|jobs| jobs.iter().find(|j| j.status.is_active()))
    }

    pub fn failed_job(&self) -> Option<&Job> {
        self.jobs.as_ref().and_then(|jobs| {
            jobs.iter()
                .find(|j| j.status == PipelineStatus::Failed)
        })
    }

    pub fn active_job_name(&self) -> CompactString {
        self.active_job()
            .map_or("".into(), |j| j.name.clone())
    }

    #[allow(dead_code)]
    pub fn has_failed_jobs(&self) -> bool {
        self.failed_job().is_some()
    }

    pub fn failing_job_name(&self) -> Option<CompactString> {
        self.failed_job().map(|j| j.name.clone())
    }

    pub fn job(&self, id: JobId) -> Option<&Job> {
        self.jobs
            .as_ref()
            .and_then(|jobs| jobs.iter().find(|j| j.id == id))
    }

    /// Returns the duration of the pipeline, measured from the time it was started
    /// to the time it was finished. If the pipeline is still running, the duration
    /// is measured from the time it was started to the current time.
    pub fn duration(&self) -> Duration {
        match (&self.created_at, &self.finished_at()) {
            (begin, Some(end)) => end.signed_duration_since(begin),
            (begin, None) => Utc::now().signed_duration_since(begin),
        }
    }

    fn finished_at(&self) -> Option<DateTime<Utc>> {
        match () {
            _ if self.status.is_active() => None,
            _ => self
                .jobs
                .as_ref()
                .and_then(|jobs| jobs.iter().map(|j| j.finished_at).max().unwrap()),
        }
    }
}

fn append_mr_spans<'a>(
    spans: &mut Vec<Span<'a>>,
    mr: &'a MergeRequest,
    default_branch: &str,
) {
    spans.push(Span::from(" "));
    spans.push(Span::from(format!("!{}", mr.iid)).style(theme().mr_iid));

    if mr.is_conflicted() {
        spans.push(Span::from(" ⚡conflict").style(theme().mr_conflict));
    } else if mr.draft {
        spans.push(Span::from(" 📝draft").style(theme().mr_draft));
    } else if mr.has_unresolved_threads() {
        spans.push(Span::from(" 🔒threads").style(theme().mr_draft));
    } else if mr.is_mergeable() {
        spans.push(Span::from(" ✓").style(theme().mr_mergeable));
    }

    if mr.upvotes > 0 {
        spans.push(Span::from(format!(" 👍{}", mr.upvotes)).style(theme().mr_mergeable));
    }

    if mr.user_notes_count > 0 {
        spans.push(Span::from(format!(" 💬{}", mr.user_notes_count)).style(theme().mr_comments));
    }

    if !mr.target_branch.is_empty() && mr.target_branch != default_branch {
        spans.push(Span::from(format!(" → {}", mr.target_branch)).style(theme().mr_target));
    }
}

pub fn parse_row<'a>(
    project: &'a Project,
    max_pipelines: usize,
    include_mr_pipelines: bool,
) -> Row<'a> {
    let max_rows = max_pipelines.max(1);
    let distinct_by_branch = project.first_pipeline_per_branch(
        max_rows,
        include_mr_pipelines,
        |p| p.status.is_active(),
    );

    let pipeline_to_span = |p: &'a Pipeline| -> Line<'a> {
        let icon = p.status.icon();
        let branch = p.branch.as_str();

        let updated_at = p.updated_at.with_timezone(&Local);
        let mut spans = crate::ui::format_timestamp_spans(updated_at);
        spans.push(Span::from(" "));

        match () {
            _ if p.has_active_jobs() => {
                spans.extend([
                    Span::from(p.jobs.as_ref().unwrap().icon()),
                    Span::from(" "),
                    Span::from(branch).style(theme().pipeline_branch),
                    Span::from(" "),
                    Span::from(p.active_job_name()).style(theme().pipeline_job),
                    Span::from(" "),
                    Span::from(format_duration(p.duration())).style(theme().time),
                ]);
            },
            _ if p.status.is_active() => {
                spans.extend([
                    Span::from(icon),
                    Span::from(" "),
                    Span::from(branch).style(theme().pipeline_branch),
                    Span::from(" "),
                    Span::from(format_duration(p.duration())).style(theme().time),
                ]);
            },
            _ => {
                spans.extend([
                    Span::from(icon),
                    Span::from(" "),
                    Span::from(branch).style(theme().pipeline_branch),
                ]);
            },
        }

        if let Some(mr) = project.merge_request_for_branch(branch) {
            append_mr_spans(&mut spans, mr, &project.default_branch);
        }

        Line::from(spans)
    };

    let mut pipeline_spans: Vec<Line<'a>> = distinct_by_branch
        .iter()
        .map(|p| pipeline_to_span(p))
        .collect();

    // If there are open MRs whose source_branch did not appear in the recent pipelines list,
    // fill remaining slots up to `max_rows` so open MRs are always visible.
    if pipeline_spans.len() < max_rows && !project.merge_requests.is_empty() {
        for mr in &project.merge_requests {
            if pipeline_spans.len() >= max_rows {
                break;
            }
            let already_shown = distinct_by_branch
                .iter()
                .any(|p| p.branch == mr.source_branch);
            if !already_shown {
                let updated_at = mr.updated_at.with_timezone(&Local);
                let mut spans = crate::ui::format_timestamp_spans(updated_at);
                spans.push(Span::from(" 🟣 "));
                spans.push(Span::from(mr.source_branch.as_str()).style(theme().pipeline_branch));
                append_mr_spans(&mut spans, mr, &project.default_branch);
                pipeline_spans.push(Line::from(spans));
            }
        }
    }

    let last_activity = project.last_activity_at.with_timezone(&Local);

    let open_mr_line = if !project.merge_requests.is_empty() {
        let conflicts = project
            .merge_requests
            .iter()
            .filter(|mr| mr.is_conflicted())
            .count();
        if conflicts > 0 {
            Some(Line::from(vec![
                Span::from(format!("{} open MRs ", project.merge_requests.len()))
                    .style(theme().mr_iid),
                Span::from(format!("({conflicts} ⚡conflict)")).style(theme().mr_conflict),
            ]))
        } else {
            Some(
                Line::from(format!("{} open MRs", project.merge_requests.len()))
                    .style(theme().mr_iid),
            )
        }
    } else {
        None
    };

    let project_path = match project.path.rfind('/') {
        Some(i) => {
            let mut lines = vec![
                Line::from(&project.path[i + 1..]).style(theme().project_name),
                Line::from(&project.path[0..=i]).style(theme().project_parents),
            ];
            if let Some(mr_line) = open_mr_line {
                lines.push(mr_line);
            }
            Text::from(lines)
        },
        None => {
            let mut lines = vec![Line::from(Span::from(&project.path)).style(theme().project_name)];
            if let Some(mr_line) = open_mr_line {
                lines.push(mr_line);
            }
            Text::from(lines)
        },
    };

    let row_height = (pipeline_spans.len() as u16).max(3);

    Row::new(vec![
        text_from(last_activity),
        project_path,
        Text::from(pipeline_spans),
    ])
    .height(row_height)
}

#[derive(Clone, Debug)]
pub enum RowEntryTarget {
    MergeRequest(u64),
    Pipeline(PipelineId),
}

impl Project {
    /// Returns the ordered list of clickable targets for each pipeline/MR line rendered by `parse_row`
    /// and the total row height in terminal lines.
    pub fn row_targets_and_height(
        &self,
        max_pipelines: usize,
        include_mr_pipelines: bool,
    ) -> (Vec<RowEntryTarget>, u16) {
        let max_rows = max_pipelines.max(1);
        let distinct_by_branch = self.first_pipeline_per_branch(
            max_rows,
            include_mr_pipelines,
            |p| p.status.is_active(),
        );

        let mut targets: Vec<RowEntryTarget> = distinct_by_branch
            .iter()
            .map(|p| {
                if let Some(mr) = self.merge_request_for_branch(&p.branch) {
                    RowEntryTarget::MergeRequest(mr.iid)
                } else {
                    RowEntryTarget::Pipeline(p.id)
                }
            })
            .collect();

        if targets.len() < max_rows && !self.merge_requests.is_empty() {
            for mr in &self.merge_requests {
                if targets.len() >= max_rows {
                    break;
                }
                let already_shown = distinct_by_branch
                    .iter()
                    .any(|p| p.branch == mr.source_branch);
                if !already_shown {
                    targets.push(RowEntryTarget::MergeRequest(mr.iid));
                }
            }
        }

        let row_height = (targets.len() as u16).max(3);
        (targets, row_height)
    }
}

/// Represents types that can be associated with an icon.
///
/// The icon returned is expected to be a string that may contain
/// special characters or emojis
pub trait IconRepresentable {
    fn icon(&self) -> CompactString;
}

impl IconRepresentable for PipelineStatus {
    fn icon(&self) -> CompactString {
        match self {
            PipelineStatus::Created => "⚪",
            PipelineStatus::WaitingForResource => "⏳",
            PipelineStatus::Preparing => "🟡",
            PipelineStatus::Pending => "🕒",
            PipelineStatus::Running => "🔵",
            PipelineStatus::Success => "🟢",
            PipelineStatus::Failed => "🔴",
            PipelineStatus::Canceled => "🚫",
            PipelineStatus::Canceling => "🚫",
            PipelineStatus::Skipped => "⚫",
            PipelineStatus::Manual => "🟣",
            PipelineStatus::Scheduled => "📅",
            PipelineStatus::Unknown => "❓",
        }
        .into()
    }
}

impl IconRepresentable for &Vec<Job> {
    fn icon(&self) -> CompactString {
        self.iter()
            .map(|j| j.status.icon())
            .collect::<CompactString>()
    }
}

impl IconRepresentable for Pipeline {
    fn icon(&self) -> CompactString {
        self.jobs
            .as_ref()
            .map(|jobs| jobs.icon())
            .unwrap_or(self.status.icon())
    }
}
