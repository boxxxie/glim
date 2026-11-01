use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc::{self, Sender},
    thread,
};

const MAX_IN_MEMORY_ACTIVITY_EVENTS: usize = 10_000;
const MAX_IN_MEMORY_COMMITS: usize = 5_000;
const DEFAULT_HYDRATE_MAX_PROJECTS: usize = 200;

const DUCKDB_LOW_MEMORY_SQL: &str = "\
SET threads=1;
SET external_threads=1;
SET async_threads=1;
SET preserve_insertion_order=false;
SET memory_limit='8MB';
SET allocator_flush_threshold='512KB';
SET allocator_bulk_deallocation_flush_threshold='512KB';
SET wal_autocheckpoint='256KB';
";

const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS projects (
    id BIGINT PRIMARY KEY,
    path VARCHAR,
    default_branch VARCHAR,
    url VARCHAR,
    last_activity_at VARCHAR,
    pipeline_count BIGINT,
    open_mr_count BIGINT,
    payload VARCHAR
);
CREATE TABLE IF NOT EXISTS pipelines (
    id BIGINT PRIMARY KEY,
    project_id BIGINT,
    project_path VARCHAR,
    status VARCHAR,
    source VARCHAR,
    branch VARCHAR,
    url VARCHAR,
    created_at VARCHAR,
    updated_at VARCHAR,
    payload VARCHAR
);
CREATE TABLE IF NOT EXISTS merge_requests (
    id BIGINT PRIMARY KEY,
    iid BIGINT,
    project_id BIGINT,
    project_path VARCHAR,
    title VARCHAR,
    state VARCHAR,
    source_branch VARCHAR,
    target_branch VARCHAR,
    draft BOOLEAN,
    has_conflicts BOOLEAN,
    blocking_discussions_resolved BOOLEAN,
    upvotes BIGINT,
    author_username VARCHAR,
    updated_at VARCHAR,
    url VARCHAR,
    payload VARCHAR
);
CREATE TABLE IF NOT EXISTS activity_events (
    id BIGINT PRIMARY KEY,
    project_id BIGINT,
    project_name VARCHAR,
    kind VARCHAR,
    author VARCHAR,
    mr_iid BIGINT,
    branch VARCHAR,
    commit_sha VARCHAR,
    summary VARCHAR,
    created_at VARCHAR,
    payload VARCHAR
);
CREATE TABLE IF NOT EXISTS commits (
    id VARCHAR PRIMARY KEY,
    title VARCHAR,
    author_name VARCHAR,
    message VARCHAR,
    payload VARCHAR
);
"#;

use chrono::{DateTime, Utc};
use compact_str::CompactString;
use itertools::Itertools;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tracing::{debug, info, instrument, warn};

use crate::{
    config::{default_data_dir, default_quack_token_path},
    dispatcher::Dispatcher,
    domain::{
        clean_multiline_body, clean_note_body, contains_mention, ActivityKind, CommitDto,
        GitlabEventDto, GitlabTodoDto, Job, MergeRequest, Pipeline, PipelineStatus, Project,
        ProjectActivityEvent,
    },
    event::GlimEvent,
    glim_app::GlimConfig,
    id::ProjectId,
    os_notify::{send_os_notification, NotifyUrgency},
};

pub fn resolve_duckdb_binary() -> PathBuf {
    if let Some(base) = directories::BaseDirs::new() {
        let home_duckdb = base.home_dir().join(".local").join("bin").join("duckdb");
        if home_duckdb.exists() {
            return home_duckdb;
        }
    }
    PathBuf::from("duckdb")
}

pub fn load_or_create_quack_token() -> CompactString {
    let token_path = default_quack_token_path();
    if let Ok(existing) = std::fs::read_to_string(&token_path) {
        let trimmed = existing.trim();
        if !trimmed.is_empty() {
            return CompactString::from(trimmed);
        }
    }
    if let Some(parent) = token_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let token = format!(
        "glim-quack-{:x}",
        Utc::now().timestamp_nanos_opt().unwrap_or(9497)
    );
    let _ = std::fs::write(&token_path, &token);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = std::fs::metadata(&token_path) {
            let mut perms = meta.permissions();
            perms.set_mode(0o600);
            let _ = std::fs::set_permissions(&token_path, perms);
        }
    }
    CompactString::from(token)
}

fn sql_escape(s: &str) -> String {
    s.replace('\'', "''").replace(['\r', '\n'], " ")
}

fn sql_opt_str(opt: Option<&str>) -> String {
    match opt {
        Some(s) => format!("'{}'", sql_escape(s)),
        None => "NULL".to_string(),
    }
}

fn sql_opt_u64(opt: Option<u64>) -> String {
    match opt {
        Some(v) => v.to_string(),
        None => "NULL".to_string(),
    }
}

#[derive(Debug)]
enum SqlWorkerMsg {
    Execute(String),
    Shutdown(mpsc::SyncSender<()>),
}

#[derive(Debug)]
pub struct DuckDbHandle {
    sql_tx: Sender<SqlWorkerMsg>,
    pub quack_listen_uri: Option<CompactString>,
    pub quack_token: Option<CompactString>,
}

impl DuckDbHandle {
    pub fn execute_async(&self, sql_batch: String) {
        if !sql_batch.trim().is_empty() {
            let _ = self.sql_tx.send(SqlWorkerMsg::Execute(sql_batch));
        }
    }

    pub fn shutdown(&self) {
        if let Some(ref uri) = self.quack_listen_uri {
            let _ = self.sql_tx.send(SqlWorkerMsg::Execute(format!(
                "CALL quack_stop('{}');",
                sql_escape(uri)
            )));
        }
        let (ack_tx, ack_rx) = mpsc::sync_channel(1);
        if self.sql_tx.send(SqlWorkerMsg::Shutdown(ack_tx)).is_ok() {
            let _ = ack_rx.recv_timeout(std::time::Duration::from_secs(5));
        }
    }
}

pub struct ProjectStore {
    sender: Sender<GlimEvent>,
    cache_dir: Option<PathBuf>,
    db_path: Option<PathBuf>,
    duckdb_handle: Option<DuckDbHandle>,
    projects: Vec<Project>,
    project_id_lookup: HashMap<ProjectId, usize>,
    sorted: Vec<Project>,
    config: GlimConfig,
    auto_fetch_days: i64,
    hydrated_pipelines: HashSet<ProjectId>,
    hydrated_mrs: HashSet<ProjectId>,
    fetched_this_session: HashSet<ProjectId>,
    next_events_page: HashMap<ProjectId, u32>,
    exhausted_events: HashSet<ProjectId>,
    loading_more_events: HashSet<ProjectId>,
    last_seen_event_id: HashMap<ProjectId, u64>,
    last_seen_todo_id: Option<u64>,
    current_username: CompactString,
    recent_activity: Vec<ProjectActivityEvent>,
    commits: HashMap<CompactString, CommitDto>,
    requested_commits: HashSet<CompactString>,
    startup_notified: bool,
}

impl ProjectStore {
    pub fn new(sender: Sender<GlimEvent>, config: &GlimConfig) -> Self {
        #[cfg(test)]
        let cache_dir = None;
        #[cfg(not(test))]
        let cache_dir = Some(default_data_dir());

        Self::with_cache_dir(sender, config, cache_dir)
    }

    pub fn with_cache_dir(
        sender: Sender<GlimEvent>,
        config: &GlimConfig,
        cache_dir: Option<PathBuf>,
    ) -> Self {
        let auto_fetch_days = config.max_age_days.map(|d| d as i64).unwrap_or(14).max(7);
        let db_path = cache_dir.as_ref().map(|d| d.join("glim.duckdb"));
        let mut store = Self {
            sender,
            cache_dir,
            db_path,
            duckdb_handle: None,
            projects: Vec::new(),
            project_id_lookup: HashMap::new(),
            sorted: Vec::new(),
            config: config.clone(),
            auto_fetch_days,
            hydrated_pipelines: HashSet::new(),
            hydrated_mrs: HashSet::new(),
            fetched_this_session: HashSet::new(),
            next_events_page: HashMap::new(),
            exhausted_events: HashSet::new(),
            loading_more_events: HashSet::new(),
            last_seen_event_id: HashMap::new(),
            last_seen_todo_id: None,
            current_username: "paul".into(),
            recent_activity: Vec::new(),
            commits: HashMap::new(),
            requested_commits: HashSet::new(),
            startup_notified: false,
        };
        store.init_duckdb_and_load();
        store
    }

    pub fn shutdown(&mut self) {
        if let Some(handle) = self.duckdb_handle.take() {
            handle.shutdown();
        }
    }

    pub fn recent_activity(&self) -> &[ProjectActivityEvent] {
        &self.recent_activity
    }

    pub fn is_loading_more_activity(&self) -> bool {
        !self.loading_more_events.is_empty()
    }

    pub fn find_activity(&self, id: u64) -> Option<&ProjectActivityEvent> {
        self.recent_activity.iter().find(|e| e.id == id)
    }

    pub fn notify_latest_activity(&self) {
        if let Some(latest) = self.recent_activity.first() {
            let (summary, body, urgency) = format_activity_notification(latest, &self.config);
            let url = self.resolve_activity_url(latest);
            if self.config.os_notifications {
                send_os_notification(
                    &summary,
                    &body,
                    urgency,
                    self.config.os_notification_timeout_ms,
                    url.as_deref(),
                );
            }
        }
    }

    fn resolve_activity_url(&self, ev: &ProjectActivityEvent) -> Option<String> {
        let project = self.find(ev.project_id)?;
        if let Some(iid) = ev.mr_iid {
            if let Some(mr) = project.merge_request(iid) {
                return Some(mr.url.to_string());
            }
            return Some(format!("{}/-/merge_requests/{iid}", project.url));
        }
        if let Some(ref body) = ev.body {
            for line in body.lines() {
                if let Some(url) = line.strip_prefix("URL: ") {
                    let trimmed = url.trim();
                    if !trimmed.is_empty() {
                        return Some(trimmed.to_string());
                    }
                }
            }
        }
        if let Some(ref sha) = ev.commit_sha {
            if !sha.is_empty() {
                return Some(format!("{}/-/commit/{sha}", project.url));
            }
        }
        if let Some(ref branch) = ev.branch {
            if let Some(mr) = project.merge_request_for_branch(branch) {
                return Some(mr.url.to_string());
            }
        }
        Some(project.url.to_string())
    }

    fn emit_notification(
        &self,
        summary: &str,
        body: &str,
        urgency: NotifyUrgency,
        url: Option<&str>,
    ) {
        if self.config.in_app_notifications {
            let banner_msg: CompactString = format!("{summary}: {body}").into();
            self.sender.dispatch(GlimEvent::ActivityNotice(banner_msg));
        }
        if self.config.os_notifications {
            send_os_notification(
                summary,
                body,
                urgency,
                self.config.os_notification_timeout_ms,
                url,
            );
        }
    }

    fn should_notify_activity(&self, kind: &ActivityKind) -> bool {
        match kind {
            ActivityKind::Mention
            | ActivityKind::ReviewRequested
            | ActivityKind::Assigned
            | ActivityKind::MergeTrainRemoved => self.config.notify_mentions_and_todos,
            ActivityKind::MrOpened | ActivityKind::MrMerged | ActivityKind::MrClosed => {
                self.config.notify_mr_events
            },
            ActivityKind::MrApproved => self.config.notify_mr_approved,
            ActivityKind::MrComment => self.config.notify_mr_comments,
            ActivityKind::BranchPush => self.config.notify_branch_push,
            ActivityKind::PipelineStatus => true,
        }
    }

    fn should_paginate_project(&self, project: &Project) -> bool {
        let path_lower = project.path.to_lowercase();

        if let Some(excludes) = &self.config.exclude_projects {
            if excludes
                .iter()
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
                .any(|term| path_lower.contains(&term))
            {
                return false;
            }
        }

        if let Some(includes) = &self.config.include_projects {
            let terms: Vec<String> = includes
                .iter()
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
                .collect();
            if !terms.is_empty() && !terms.iter().any(|term| path_lower.contains(term)) {
                return false;
            }
        }

        true
    }

    fn should_watch_project(&self, project: &Project) -> bool {
        if is_older_than_days(project.last_activity(), self.auto_fetch_days) {
            return false;
        }
        self.should_paginate_project(project)
    }

    #[instrument(skip(self, event), fields(event_type = %event.variant_name()))]
    pub fn apply(&mut self, event: &GlimEvent) {
        match event {
            GlimEvent::ConfigUpdate(config) => {
                self.auto_fetch_days = config.max_age_days.map(|d| d as i64).unwrap_or(14).max(7);
                self.config = config.clone();
            },

            // requests jobs for pipelines that have not been loaded yet, plus fresh MRs & events
            GlimEvent::ProjectDetailsOpen(id) => {
                debug!(project_id = %id, "Opening project details and requesting missing jobs");
                let max_recent = self.config.max_recent_pipelines;
                let include_mr = self.config.include_mr_pipelines;
                let project = self.find(*id).unwrap();
                let project_id = project.id;
                project
                    .recent_pipelines(max_recent, include_mr)
                    .into_iter()
                    .filter(|p| p.jobs.is_none())
                    .for_each(|p| self.dispatch(GlimEvent::JobsFetch(project_id, p.id)));
                self.dispatch(GlimEvent::MergeRequestsFetch(project_id));
                self.dispatch(GlimEvent::ProjectEventsFetch(project_id));
            },

            // updates the projects in the store
            GlimEvent::ProjectsLoaded(projects) => {
                debug!(
                    project_count = projects.len(),
                    "Processing received projects"
                );
                let first_projects = self.sorted.is_empty();
                projects
                    .iter()
                    .map(|p| Project::from(p.clone()))
                    .for_each(|p| {
                        let id = p.id;
                        self.sync_project(p);
                        if let Some(merged) = self.find(id).cloned() {
                            let sender = self.sender.clone();
                            sender.dispatch(GlimEvent::ProjectUpdated(Box::new(merged)));
                        }
                    });

                self.sorted = self.projects_sorted_by_last_activity();
                self.save_projects_cache();
                if first_projects {
                    if let Some(first) = self.sorted.first() {
                        self.dispatch(GlimEvent::ProjectSelected(first.id));
                    }
                }
            },

            // updates the pipelines for a project
            GlimEvent::PipelinesLoaded(pipelines) => {
                let Some(first_pipeline) = pipelines.first() else {
                    return;
                };
                let project_id = first_pipeline.project_id;
                debug!(project_id = %project_id, pipeline_count = pipelines.len(), "Processing received pipelines");
                let sender = self.sender.clone();

                let is_hydrated = self.hydrated_pipelines.contains(&project_id);
                let mut notifications: Vec<(String, String, NotifyUrgency, Option<String>)> =
                    Vec::new();
                let mut pipeline_feed_events: Vec<ProjectActivityEvent> = Vec::new();
                let notify_failed = self.config.notify_pipeline_failed;
                let notify_passed = self.config.notify_pipeline_passed;
                let notify_running = self.config.notify_pipeline_running;
                let urg_failed = self.config.notify_urgency_pipeline_failed;
                let urg_passed = self.config.notify_urgency_pipeline_passed;
                let urg_running = self.config.notify_urgency_pipeline_running;

                if let Some(project) = self.find_mut(project_id) {
                    let project_title = project.title();
                    let new_pipelines: Vec<Pipeline> = pipelines
                        .iter()
                        .map(|p| Pipeline::from(p.clone()))
                        .collect();

                    if is_hydrated {
                        if let Some(old_pipelines) = &project.pipelines {
                            let max_old_id = old_pipelines
                                .iter()
                                .map(|op| op.id.value())
                                .max()
                                .unwrap_or(0);

                            for p in &new_pipelines {
                                let mr = project.merge_request_for_branch(&p.branch);
                                let mr_iid = mr.map(|m| m.iid);
                                let target_url = mr
                                    .map(|m| m.url.to_string())
                                    .unwrap_or_else(|| p.url.to_string());
                                let mr_tag = mr_iid
                                    .map(|iid| format!(" (!{})", iid))
                                    .unwrap_or_default();

                                if let Some(old_p) = old_pipelines.iter().find(|op| op.id == p.id) {
                                    if old_p.status != p.status {
                                        let transition = match p.status {
                                            PipelineStatus::Failed => Some((
                                                format!("🔴 [{}] Pipeline failed", project_title),
                                                format!("{}{}", p.branch, mr_tag),
                                                urg_failed,
                                                "failed",
                                                notify_failed,
                                            )),
                                            PipelineStatus::Success => Some((
                                                format!("🟢 [{}] Pipeline passed", project_title),
                                                format!("{}{}", p.branch, mr_tag),
                                                urg_passed,
                                                "passed",
                                                notify_passed,
                                            )),
                                            _ if !old_p.status.is_active()
                                                && p.status.is_active() =>
                                            {
                                                Some((
                                                    format!(
                                                        "🔵 [{}] Pipeline running",
                                                        project_title
                                                    ),
                                                    format!("{}{}", p.branch, mr_tag),
                                                    urg_running,
                                                    "running",
                                                    notify_running,
                                                ))
                                            },
                                            _ => None,
                                        };
                                        if let Some((sum, body, urg, label, should_notify)) =
                                            transition
                                        {
                                            if should_notify {
                                                notifications.push((
                                                    sum.clone(),
                                                    body.clone(),
                                                    urg,
                                                    Some(target_url.clone()),
                                                ));
                                            }
                                            pipeline_feed_events.push(ProjectActivityEvent {
                                                id: p.id.value().wrapping_mul(1000)
                                                    + (p.updated_at.timestamp() as u64 % 1000),
                                                project_id,
                                                project_name: project_title.clone(),
                                                kind: ActivityKind::PipelineStatus,
                                                author: "ci".into(),
                                                mr_iid,
                                                branch: Some(p.branch.clone()),
                                                commit_sha: None,
                                                summary: format!("Pipeline #{} {}", p.id, label)
                                                    .into(),
                                                detail: None,
                                                body: Some(format!("{sum}\nBranch: {body}\nURL: {}", p.url).into()),
                                                created_at: p.updated_at,
                                            });
                                        }
                                    }
                                } else if p.id.value() > max_old_id {
                                    let transition = match p.status {
                                        PipelineStatus::Failed => Some((
                                            format!("🔴 [{}] Pipeline failed", project_title),
                                            format!("{}{}", p.branch, mr_tag),
                                            urg_failed,
                                            "failed",
                                            notify_failed,
                                        )),
                                        PipelineStatus::Success => Some((
                                            format!("🟢 [{}] Pipeline passed", project_title),
                                            format!("{}{}", p.branch, mr_tag),
                                            urg_passed,
                                            "passed",
                                            notify_passed,
                                        )),
                                        _ if p.status.is_active() => Some((
                                            format!("🔵 [{}] Pipeline started", project_title),
                                            format!("{}{}", p.branch, mr_tag),
                                            urg_running,
                                            "started",
                                            notify_running,
                                        )),
                                        _ => None,
                                    };
                                    if let Some((sum, body, urg, label, should_notify)) = transition
                                    {
                                        if should_notify {
                                            notifications.push((
                                                sum.clone(),
                                                body.clone(),
                                                urg,
                                                Some(target_url.clone()),
                                            ));
                                        }
                                        pipeline_feed_events.push(ProjectActivityEvent {
                                            id: p.id.value().wrapping_mul(1000)
                                                + (p.updated_at.timestamp() as u64 % 1000),
                                            project_id,
                                            project_name: project_title.clone(),
                                            kind: ActivityKind::PipelineStatus,
                                            author: "ci".into(),
                                            mr_iid,
                                            branch: Some(p.branch.clone()),
                                            commit_sha: None,
                                            summary: format!("Pipeline #{} {}", p.id, label).into(),
                                            detail: None,
                                            body: Some(format!("{sum}\nBranch: {body}\nURL: {}", p.url).into()),
                                            created_at: p.updated_at,
                                        });
                                    }
                                }
                            }
                        }
                    }

                    new_pipelines
                        .iter()
                        .filter(|&p| p.status.is_active() || p.has_active_jobs())
                        .for_each(|p| sender.dispatch(GlimEvent::JobsFetch(project_id, p.id)));

                    project.update_pipelines(new_pipelines);
                    sender.dispatch(GlimEvent::ProjectUpdated(Box::new(project.clone())))
                }

                if !pipeline_feed_events.is_empty() {
                    self.append_and_trim_activity(pipeline_feed_events);
                }

                self.hydrated_pipelines.insert(project_id);
                for (summary, body, urgency, url) in notifications {
                    self.emit_notification(&summary, &body, urgency, url.as_deref());
                }

                self.sorted = self.projects_sorted_by_last_activity();
                self.save_projects_cache();
            },

            // updates open merge requests for a project
            GlimEvent::MergeRequestsLoaded(project_id, mr_dtos) => {
                debug!(
                    project_id = %project_id,
                    mr_count = mr_dtos.len(),
                    "Processing received merge requests"
                );
                let sender = self.sender.clone();
                let is_hydrated = self.hydrated_mrs.contains(project_id);
                let mut notifications: Vec<(String, String, NotifyUrgency, Option<String>)> =
                    Vec::new();
                let notify_conflicts = self.config.notify_mr_conflicts;
                let notify_approved = self.config.notify_mr_approved;
                let notify_threads = self.config.notify_mr_threads;
                let urg_conflicts = self.config.notify_urgency_mr_conflicts;
                let urg_default = self.config.notify_urgency_default;

                if let Some(project) = self.find_mut(*project_id) {
                    let project_title = project.title();
                    let new_mrs: Vec<MergeRequest> = mr_dtos
                        .iter()
                        .cloned()
                        .map(MergeRequest::from)
                        .collect();

                    if is_hydrated {
                        for mr in &new_mrs {
                            if let Some(old_mr) =
                                project.merge_requests.iter().find(|omr| omr.iid == mr.iid)
                            {
                                let mr_url = Some(mr.url.to_string());
                                if notify_conflicts && !old_mr.is_conflicted() && mr.is_conflicted()
                                {
                                    notifications.push((
                                        format!("⚡ [{}] !{} has merge conflicts", project_title, mr.iid),
                                        format!("{} ({})", mr.title, mr.source_branch),
                                        urg_conflicts,
                                        mr_url.clone(),
                                    ));
                                }
                                if notify_approved && mr.upvotes > old_mr.upvotes {
                                    notifications.push((
                                        format!("👍 [{}] !{} approved", project_title, mr.iid),
                                        format!("{} (👍{})", mr.title, mr.upvotes),
                                        urg_default,
                                        mr_url.clone(),
                                    ));
                                }
                                if notify_threads
                                    && old_mr.blocking_discussions_resolved
                                    && !mr.blocking_discussions_resolved
                                {
                                    notifications.push((
                                        format!("🔒 [{}] !{} has unresolved threads", project_title, mr.iid),
                                        mr.title.to_string(),
                                        urg_default,
                                        mr_url.clone(),
                                    ));
                                } else if notify_threads
                                    && !old_mr.blocking_discussions_resolved
                                    && mr.blocking_discussions_resolved
                                {
                                    notifications.push((
                                        format!("✅ [{}] !{} threads resolved", project_title, mr.iid),
                                        mr.title.to_string(),
                                        urg_default,
                                        mr_url,
                                    ));
                                }
                            }
                        }
                    }

                    project.update_merge_requests(new_mrs);
                    sender.dispatch(GlimEvent::ProjectUpdated(Box::new(project.clone())));
                }

                self.hydrated_mrs.insert(*project_id);
                for (summary, body, urgency, url) in notifications {
                    self.emit_notification(&summary, &body, urgency, url.as_deref());
                }

                self.sorted = self.projects_sorted_by_last_activity();
                self.save_projects_cache();
            },

            // updates recent project events (MR opens/merges/comments/pushes)
            GlimEvent::ProjectEventsLoaded(project_id, event_dtos) => {
                debug!(
                    project_id = %project_id,
                    event_count = event_dtos.len(),
                    "Processing received project events"
                );
                let prev_max_id = self.last_seen_event_id.get(project_id).copied();
                let max_id = event_dtos
                    .iter()
                    .map(|e| e.id)
                    .max()
                    .unwrap_or(prev_max_id.unwrap_or(0));
                self.last_seen_event_id.insert(*project_id, max_id);
                if event_dtos.len() < 100 {
                    self.exhausted_events.insert(*project_id);
                } else {
                    self.next_events_page.entry(*project_id).or_insert(2);
                }

                let Some(project) = self.find(*project_id).cloned() else {
                    return;
                };

                let mut newly_arrived: Vec<ProjectActivityEvent> = Vec::new();
                let mut newly_cached: Vec<ProjectActivityEvent> = Vec::new();
                let current_username = self.current_username.clone();

                for dto in event_dtos {
                    if self.config.ignore_bot_comments && dto.is_bot_event() {
                        continue;
                    }
                    if let Some(mut activity) =
                        convert_gitlab_event(dto, &project, &current_username)
                    {
                        if let Some(ref sha) = activity.commit_sha {
                            if let Some(commit) = self.commits.get(sha) {
                                if !commit.title.is_empty() {
                                    activity.summary = commit.title.clone();
                                }
                                if !commit.message.is_empty() {
                                    activity.body = Some(commit.message.trim().into());
                                }
                            } else if self.requested_commits.insert(sha.clone()) {
                                self.dispatch(GlimEvent::CommitFetch(*project_id, sha.clone()));
                            }
                        }

                        let already_present = self
                            .recent_activity
                            .iter()
                            .chain(newly_cached.iter())
                            .any(|existing| is_duplicate_activity(existing, &activity));
                        if let Some(prev_max) = prev_max_id {
                            if activity.id > prev_max && !already_present {
                                newly_arrived.push(activity.clone());
                            }
                        }
                        if !already_present {
                            newly_cached.push(activity);
                        }
                    }
                }

                if !newly_cached.is_empty() {
                    self.append_and_trim_activity(newly_cached);
                }

                // Emit notifications for newly arrived events in chronological order
                newly_arrived.sort_by(|a, b| a.created_at.cmp(&b.created_at));
                for ev in newly_arrived {
                    if self.should_notify_activity(&ev.kind) {
                        let (summary, body, urgency) =
                            format_activity_notification(&ev, &self.config);
                        let url = self.resolve_activity_url(&ev);
                        self.emit_notification(&summary, &body, urgency, url.as_deref());
                    }
                }

                // On first startup hydration, emit the single latest activity notification so the user
                // sees immediate confirmation that in-app and OS notifications are active.
                if !self.startup_notified {
                    self.startup_notified = true;
                    if self.config.notify_on_startup {
                        if let Some(latest) = self.recent_activity.first() {
                            if self.should_notify_activity(&latest.kind) {
                                let (summary, body, urgency) =
                                    format_activity_notification(latest, &self.config);
                                let url = self.resolve_activity_url(latest);
                                self.emit_notification(&summary, &body, urgency, url.as_deref());
                            }
                        }
                    }
                }
            },

            GlimEvent::ActivityLoadMore => {
                let candidate_ids: Vec<ProjectId> = self
                    .projects
                    .iter()
                    .filter(|p| self.should_paginate_project(p))
                    .map(|p| p.id)
                    .filter(|id| {
                        !self.exhausted_events.contains(id)
                            && !self.loading_more_events.contains(id)
                    })
                    .collect();

                for project_id in candidate_ids {
                    let page = *self.next_events_page.entry(project_id).or_insert(2);
                    self.loading_more_events.insert(project_id);
                    self.dispatch(GlimEvent::ProjectEventsPageFetch(project_id, page));
                }
            },

            GlimEvent::ProjectEventsPageLoaded(project_id, page, event_dtos) => {
                self.loading_more_events.remove(project_id);
                if event_dtos.len() < 100 {
                    self.exhausted_events.insert(*project_id);
                } else {
                    self.next_events_page.insert(*project_id, page + 1);
                }

                let Some(project) = self.find(*project_id).cloned() else {
                    return;
                };

                let mut newly_cached: Vec<ProjectActivityEvent> = Vec::new();
                let current_username = self.current_username.clone();
                for dto in event_dtos {
                    if self.config.ignore_bot_comments && dto.is_bot_event() {
                        continue;
                    }
                    if let Some(mut activity) =
                        convert_gitlab_event(dto, &project, &current_username)
                    {
                        if let Some(ref sha) = activity.commit_sha {
                            if let Some(commit) = self.commits.get(sha) {
                                if !commit.title.is_empty() {
                                    activity.summary = commit.title.clone();
                                }
                                if !commit.message.is_empty() {
                                    activity.body = Some(commit.message.trim().into());
                                }
                            } else if self.requested_commits.insert(sha.clone()) {
                                self.dispatch(GlimEvent::CommitFetch(*project_id, sha.clone()));
                            }
                        }

                        let already_present = self
                            .recent_activity
                            .iter()
                            .chain(newly_cached.iter())
                            .any(|existing| is_duplicate_activity(existing, &activity));
                        if !already_present {
                            newly_cached.push(activity);
                        }
                    }
                }

                if !newly_cached.is_empty() {
                    self.append_and_trim_activity(newly_cached);
                } else if event_dtos.len() >= 100 && *page < 50 {
                    // All events on this page were already cached locally; advance to the next page
                    let next_page = page + 1;
                    self.loading_more_events.insert(*project_id);
                    self.dispatch(GlimEvent::ProjectEventsPageFetch(*project_id, next_page));
                }
            },

            GlimEvent::CurrentUserLoaded(username) => {
                debug!(username = %username, "Authenticated GitLab user loaded");
                self.current_username = username.clone();
            },

            GlimEvent::TodosLoaded(todos) => {
                debug!(todo_count = todos.len(), "Processing GitLab todos");
                let prev_max_todo_id = self.last_seen_todo_id;
                let max_todo_id = todos
                    .iter()
                    .map(|t| t.id)
                    .max()
                    .unwrap_or(prev_max_todo_id.unwrap_or(0));
                self.last_seen_todo_id = Some(max_todo_id);

                let mut newly_arrived: Vec<(ProjectActivityEvent, Option<String>)> = Vec::new();
                let mut newly_cached: Vec<ProjectActivityEvent> = Vec::new();
                let current_username = self.current_username.clone();
                for todo in todos {
                    if let Some(activity) = convert_gitlab_todo(todo, &current_username) {
                        let already_present = self
                            .recent_activity
                            .iter()
                            .chain(newly_cached.iter())
                            .any(|existing| is_duplicate_activity(existing, &activity));
                        if let Some(prev_max) = prev_max_todo_id {
                            if todo.id > prev_max && !already_present {
                                let todo_url = if !todo.target_url.is_empty() {
                                    Some(todo.target_url.to_string())
                                } else {
                                    self.resolve_activity_url(&activity)
                                };
                                newly_arrived.push((activity.clone(), todo_url));
                            }
                        }
                        if !already_present {
                            newly_cached.push(activity);
                        }
                    }
                }

                if !newly_cached.is_empty() {
                    self.append_and_trim_activity(newly_cached);
                }

                newly_arrived.sort_by(|(a, _), (b, _)| a.created_at.cmp(&b.created_at));
                for (ev, todo_url) in newly_arrived {
                    if self.should_notify_activity(&ev.kind) {
                        let (summary, body, urgency) =
                            format_activity_notification(&ev, &self.config);
                        let url = todo_url.or_else(|| self.resolve_activity_url(&ev));
                        self.emit_notification(&summary, &body, urgency, url.as_deref());
                    }
                }
            },

            GlimEvent::JobsLoaded(project_id, pipeline_id, job_dtos) => {
                debug!(project_id = %project_id, pipeline_id = %pipeline_id, job_count = job_dtos.len(), "Processing received jobs");
                let jobs: Vec<Job> = job_dtos
                    .iter()
                    .map(|j| Job::from(j.clone()))
                    .collect();

                if let Some(commit_dto) = job_dtos.first().map(|j| j.commit.clone()) {
                    if !commit_dto.id.is_empty() && !commit_dto.message.is_empty() {
                        self.apply_commit_details(commit_dto);
                    }
                }

                let sender = self.sender.clone();
                if let Some(project) = self.find_mut(*project_id) {
                    project.update_jobs(*pipeline_id, jobs);
                    if let Some(commit) = job_dtos.first().map(|j| j.commit.clone().into()) {
                        project.update_commit(*pipeline_id, commit);
                    }
                    sender.dispatch(GlimEvent::ProjectUpdated(Box::new(project.clone())))
                }

                self.sorted = self.projects_sorted_by_last_activity();
                self.save_projects_cache();
            },

            GlimEvent::CommitLoaded(_project_id, commit_dto) => {
                if !commit_dto.id.is_empty() {
                    self.apply_commit_details(commit_dto.clone());
                }
            },

            GlimEvent::ActivityDetailsOpen(activity_id) => {
                if let Some((project_id, sha)) = self
                    .find_activity(*activity_id)
                    .and_then(|ev| ev.commit_sha.as_ref().map(|s| (ev.project_id, s.clone())))
                {
                    if !self.commits.contains_key(&sha) {
                        self.requested_commits.insert(sha.clone());
                        self.dispatch(GlimEvent::CommitFetch(project_id, sha));
                    }
                }
            },

            // requests pipelines for a project if they have not been fetched yet this session
            GlimEvent::ProjectSelected(id) => {
                debug!(project_id = %id, "Project selected");
                let mut request_pipelines = false;
                if self.fetched_this_session.insert(*id) {
                    if let Some(project) = self.find_mut(*id) {
                        if project.pipelines.is_none() {
                            project.pipelines = Some(Vec::new());
                        }
                        request_pipelines = true;
                    }
                }

                if request_pipelines {
                    self.dispatch(GlimEvent::PipelinesFetch(*id));
                }
            },
            _ => {},
        }
    }

    fn projects_sorted_by_last_activity(&mut self) -> Vec<Project> {
        self.projects
            .iter()
            .sorted_by(|a, b| b.last_activity().cmp(&a.last_activity()))
            .cloned()
            .collect()
    }

    pub fn find(&self, id: ProjectId) -> Option<&Project> {
        self.project_idx(id)
            .map(|idx| &self.projects[idx])
    }

    pub fn sorted_projects(&self) -> &[Project] {
        &self.sorted
    }

    fn find_mut(&mut self, id: ProjectId) -> Option<&mut Project> {
        self.project_idx(id)
            .map(|idx| &mut self.projects[idx])
    }

    fn project_idx(&self, id: ProjectId) -> Option<usize> {
        self.project_id_lookup.get(&id).copied()
    }

    fn apply_commit_details(&mut self, commit: CommitDto) {
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        for ev in &mut self.recent_activity {
            if ev.commit_sha.as_deref() == Some(commit.id.as_str()) {
                let mut changed = false;
                if !commit.title.is_empty() && ev.summary != commit.title {
                    ev.summary = commit.title.clone();
                    changed = true;
                }
                let trimmed: CompactString = commit.message.trim().into();
                if !trimmed.is_empty() && ev.body.as_ref() != Some(&trimmed) {
                    ev.body = Some(trimmed);
                    changed = true;
                }
                if changed {
                    sql.push_str(&upsert_activity_sql(ev));
                }
            }
        }
        let is_new_commit = !self.commits.contains_key(&commit.id);
        if is_new_commit && self.commits.len() >= MAX_IN_MEMORY_COMMITS {
            if let Some(evict_key) = self.commits.keys().next().cloned() {
                self.commits.remove(&evict_key);
            }
        }
        sql.push_str(&upsert_commit_sql(&commit));
        sql.push_str("COMMIT;\n");
        self.persist_sql(sql);
        self.commits.insert(commit.id.clone(), commit);
    }

    fn append_and_trim_activity(&mut self, new_events: Vec<ProjectActivityEvent>) {
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        for ev in &new_events {
            sql.push_str(&upsert_activity_sql(ev));
        }
        sql.push_str("COMMIT;\n");
        self.persist_sql(sql);

        self.recent_activity.extend(new_events);
        self.recent_activity
            .sort_by(|a, b| b.created_at.cmp(&a.created_at));
        self.recent_activity.truncate(MAX_IN_MEMORY_ACTIVITY_EVENTS);
    }

    #[instrument(skip(self, project), fields(project_id = %project.id, project_path = %project.path))]
    fn sync_project(&mut self, mut project: Project) {
        let sender = self.sender.clone();
        let watch = self.should_watch_project(&project);
        if watch {
            self.fetched_this_session.insert(project.id);
        }
        match self.find_mut(project.id) {
            Some(existing_entry) => {
                existing_entry.update_project(project.clone());
                if watch {
                    sender.dispatch(GlimEvent::PipelinesFetch(project.id));
                }
            },
            None => {
                self.project_id_lookup
                    .insert(project.id, self.projects.len());
                if watch {
                    sender.dispatch(GlimEvent::PipelinesFetch(project.id));
                    project.pipelines = Some(Vec::new());
                }
                self.projects.push(project);
            },
        }
    }

    fn init_duckdb_and_load(&mut self) {
        let Some(ref db_path) = self.db_path.clone() else {
            return;
        };
        if let Some(parent) = db_path.parent() {
            let _ = fs::create_dir_all(parent);
        }

        let duckdb_bin = resolve_duckdb_binary();

        if !db_path.exists() {
            let init_sql = format!("{DUCKDB_LOW_MEMORY_SQL}\n{SCHEMA_SQL}");
            let _ = Command::new(&duckdb_bin)
                .env("MALLOC_ARENA_MAX", "1")
                .arg("-dark-mode")
                .arg("-batch")
                .arg(db_path)
                .arg("-c")
                .arg(init_sql)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }

        self.migrate_legacy_jsonl_if_needed(&duckdb_bin, db_path);
        self.load_existing_rows(&duckdb_bin, db_path);
        self.start_persistent_worker(&duckdb_bin, db_path);
    }

    fn load_existing_rows(&mut self, duckdb_bin: &Path, db_path: &Path) {
        let init_sql = format!("CHECKPOINT;\n{DUCKDB_LOW_MEMORY_SQL}\n{SCHEMA_SQL}");
        let _ = Command::new(duckdb_bin)
            .env("MALLOC_ARENA_MAX", "1")
            .arg("-dark-mode")
            .arg("-batch")
            .arg(db_path)
            .arg("-c")
            .arg(init_sql)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();

        let commits_sql = format!(
            "SELECT payload FROM (SELECT payload FROM commits ORDER BY rowid DESC LIMIT {MAX_IN_MEMORY_COMMITS});"
        );
        for c in query_payloads::<CommitDto>(duckdb_bin, db_path, &commits_sql) {
            if !c.id.is_empty() {
                self.requested_commits.insert(c.id.clone());
                self.commits.insert(c.id.clone(), c);
            }
        }

        let projects_sql = format!(
            "SELECT payload FROM (SELECT payload FROM projects ORDER BY rowid DESC LIMIT {DEFAULT_HYDRATE_MAX_PROJECTS});"
        );
        for p in query_payloads::<Project>(duckdb_bin, db_path, &projects_sql) {
            if !self.project_id_lookup.contains_key(&p.id) {
                self.project_id_lookup.insert(p.id, self.projects.len());
                self.projects.push(p);
            }
        }
        if !self.projects.is_empty() {
            self.sorted = self.projects_sorted_by_last_activity();
            if let Some(first) = self.sorted.first() {
                let _ = self.sender.send(GlimEvent::ProjectSelected(first.id));
            }
        }

        let events_tail_scan = MAX_IN_MEMORY_ACTIVITY_EVENTS.saturating_mul(2);
        let events_sql = format!(
            "SELECT payload FROM (SELECT payload, created_at FROM activity_events ORDER BY rowid DESC LIMIT {events_tail_scan}) ORDER BY created_at DESC LIMIT {MAX_IN_MEMORY_ACTIVITY_EVENTS};"
        );
        let cached_events = query_payloads::<ProjectActivityEvent>(duckdb_bin, db_path, &events_sql);
        self.ingest_cached_activity_events(cached_events);

        info!(
            projects = self.projects.len(),
            commits = self.commits.len(),
            activity_events = self.recent_activity.len(),
            "Loaded cached GitLab state from DuckDB 2.0"
        );
    }

    fn ingest_cached_activity_events(&mut self, cached_events: Vec<ProjectActivityEvent>) {
        let mut cached_event_counts: HashMap<ProjectId, usize> = HashMap::new();
        for mut ev in cached_events {
            if let Some(ref sha) = ev.commit_sha {
                if let Some(commit) = self.commits.get(sha) {
                    if !commit.title.is_empty() {
                        ev.summary = commit.title.clone();
                    }
                    if !commit.message.is_empty() {
                        ev.body = Some(commit.message.trim().into());
                    }
                }
            }
            if ev.kind != ActivityKind::PipelineStatus {
                if ev.id >= 1_000_000_000 && ev.id < 2_000_000_000 {
                    let todo_id = ev.id - 1_000_000_000;
                    self.last_seen_todo_id =
                        Some(self.last_seen_todo_id.unwrap_or(0).max(todo_id));
                } else if ev.id < 1_000_000_000 {
                    let entry = self.last_seen_event_id.entry(ev.project_id).or_insert(0);
                    *entry = (*entry).max(ev.id);
                    *cached_event_counts.entry(ev.project_id).or_insert(0) += 1;
                }
            }
            if !self
                .recent_activity
                .iter()
                .any(|existing| is_duplicate_activity(existing, &ev))
            {
                self.recent_activity.push(ev);
            }
        }
        for (project_id, count) in cached_event_counts {
            let start_page = ((count / 100) as u32).saturating_add(1).max(2);
            self.next_events_page.insert(project_id, start_page);
        }
        if !self.recent_activity.is_empty() {
            self.recent_activity
                .sort_by(|a, b| b.created_at.cmp(&a.created_at));
            self.recent_activity.truncate(MAX_IN_MEMORY_ACTIVITY_EVENTS);
            self.startup_notified = true;
        }
    }

    fn migrate_legacy_jsonl_if_needed(&mut self, duckdb_bin: &Path, db_path: &Path) {
        let Some(ref base_dir) = self.cache_dir.clone() else {
            return;
        };

        for candidate_dir in [base_dir.clone(), base_dir.join("cache")] {
            let commits_path = candidate_dir.join("commits.jsonl");
            let projects_path = candidate_dir.join("projects.jsonl");
            let events_path = candidate_dir.join("activity_events.jsonl");

            if !commits_path.exists() && !projects_path.exists() && !events_path.exists() {
                continue;
            }

            let mut statements: Vec<String> = Vec::new();

            if commits_path.exists() {
                let legacy_commits: Vec<CommitDto> = read_jsonl(&commits_path);
                let mut seen_commits = HashSet::new();
                for c in legacy_commits.into_iter().rev() {
                    if !c.id.is_empty() && seen_commits.insert(c.id.clone()) {
                        statements.push(upsert_commit_sql(&c));
                    }
                }
            }

            if projects_path.exists() {
                let legacy_projects: Vec<Project> = read_jsonl(&projects_path);
                for p in legacy_projects {
                    statements.push(upsert_project_and_children_sql(&p));
                }
            }

            if events_path.exists() {
                let legacy_events: Vec<ProjectActivityEvent> = read_jsonl(&events_path);
                let mut unique_by_id: HashMap<u64, ProjectActivityEvent> = HashMap::new();
                for ev in legacy_events {
                    unique_by_id.insert(ev.id, ev);
                }
                let mut deduped_events: Vec<ProjectActivityEvent> = unique_by_id.into_values().collect();
                // Sort ascending before inserting so chronological tail matches rowid order in DuckDB
                deduped_events.sort_by(|a, b| a.created_at.cmp(&b.created_at));
                for ev in &deduped_events {
                    statements.push(upsert_activity_sql(ev));
                }
            }

            if statements.is_empty() {
                continue;
            }

            let mut full_sql = format!(
                "SET threads=1;\nSET external_threads=1;\nSET async_threads=1;\nSET memory_limit='64MB';\n{SCHEMA_SQL}\n"
            );
            for chunk in statements.chunks(200) {
                full_sql.push_str("BEGIN TRANSACTION;\n");
                for stmt in chunk {
                    full_sql.push_str(stmt);
                }
                full_sql.push_str("COMMIT;\n");
            }
            full_sql.push_str("CHECKPOINT;\n");

            if let Ok(mut child) = Command::new(duckdb_bin)
                .env("MALLOC_ARENA_MAX", "1")
                .arg("-dark-mode")
                .arg("-batch")
                .arg(db_path)
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
            {
                if let Some(mut stdin) = child.stdin.take() {
                    let _ = stdin.write_all(full_sql.as_bytes());
                }
                if let Ok(status) = child.wait() {
                    if status.success() {
                        if commits_path.exists() {
                            let _ = fs::rename(&commits_path, candidate_dir.join("commits.jsonl.migrated"));
                        }
                        if projects_path.exists() {
                            let _ = fs::rename(&projects_path, candidate_dir.join("projects.jsonl.migrated"));
                        }
                        if events_path.exists() {
                            let _ = fs::rename(
                                &events_path,
                                candidate_dir.join("activity_events.jsonl.migrated"),
                            );
                        }
                    }
                }
            }
        }
    }

    fn start_persistent_worker(&mut self, duckdb_bin: &Path, db_path: &Path) {
        let mut child: Child = match Command::new(duckdb_bin)
            .env("MALLOC_ARENA_MAX", "1")
            .arg("-dark-mode")
            .arg("-batch")
            .arg(db_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "Failed to spawn persistent DuckDB 2.0 worker");
                return;
            }
        };

        let mut stdin: ChildStdin = match child.stdin.take() {
            Some(s) => s,
            None => return,
        };
        let stdout = child.stdout.take();

        let _ = writeln!(
            stdin,
            "CHECKPOINT;\n{DUCKDB_LOW_MEMORY_SQL}\n{SCHEMA_SQL}"
        );

        let mut quack_listen_uri = None;
        let mut quack_token = None;

        #[cfg(test)]
        let enable_quack = false;
        #[cfg(not(test))]
        let enable_quack = self.config.quack_server_enabled;

        if enable_quack {
            let uri = format!("quack:127.0.0.1:{}", self.config.quack_port);
            let token = load_or_create_quack_token();
            let init_quack_sql = format!(
                "INSTALL quack;\nLOAD quack;\nCALL quack_serve('{}', token => '{}');\nSELECT 'GLIM_QUACK_DONE';\n",
                sql_escape(&uri),
                sql_escape(&token)
            );
            if writeln!(stdin, "{init_quack_sql}").is_ok() && stdin.flush().is_ok() {
                if let Some(out) = stdout {
                    let mut reader = BufReader::new(out);
                    let mut line = String::new();
                    let mut started_ok = false;
                    while let Ok(n) = reader.read_line(&mut line) {
                        if n == 0 {
                            break;
                        }
                        if line.contains(&uri) {
                            started_ok = true;
                        }
                        if line.contains("GLIM_QUACK_DONE") {
                            break;
                        }
                        line.clear();
                    }
                    thread::spawn(move || {
                        let mut sink = String::new();
                        while let Ok(n) = reader.read_line(&mut sink) {
                            if n == 0 {
                                break;
                            }
                            sink.clear();
                        }
                    });

                    if started_ok {
                        info!(uri = %uri, "Started DuckDB 2.0 Quack protocol server");
                        quack_listen_uri = Some(uri.into());
                        quack_token = Some(token);
                    }
                }
            }
        } else if let Some(out) = stdout {
            thread::spawn(move || {
                let mut reader = BufReader::new(out);
                let mut sink = String::new();
                while let Ok(n) = reader.read_line(&mut sink) {
                    if n == 0 {
                        break;
                    }
                    sink.clear();
                }
            });
        }

        let (sql_tx, sql_rx) = mpsc::channel::<SqlWorkerMsg>();
        thread::spawn(move || {
            let mut ack_on_exit = None;
            while let Ok(msg) = sql_rx.recv() {
                match msg {
                    SqlWorkerMsg::Execute(sql) => {
                        if writeln!(stdin, "{sql}").is_err() {
                            break;
                        }
                        let _ = stdin.flush();
                    },
                    SqlWorkerMsg::Shutdown(ack) => {
                        let _ = writeln!(stdin, "CHECKPOINT;");
                        let _ = stdin.flush();
                        ack_on_exit = Some(ack);
                        break;
                    },
                }
            }
            drop(stdin);
            let _ = child.wait();
            if let Some(ack) = ack_on_exit {
                let _ = ack.send(());
            }
        });

        self.duckdb_handle = Some(DuckDbHandle {
            sql_tx,
            quack_listen_uri,
            quack_token,
        });
    }

    fn persist_sql(&self, sql: String) {
        if let Some(ref handle) = self.duckdb_handle {
            handle.execute_async(sql);
        }
    }

    fn save_projects_cache(&self) {
        if self.duckdb_handle.is_none() {
            return;
        }
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        for p in &self.projects {
            sql.push_str(&upsert_project_and_children_sql(p));
        }
        sql.push_str("COMMIT;\n");
        self.persist_sql(sql);
    }
}

impl Drop for ProjectStore {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn upsert_project_and_children_sql(proj: &Project) -> String {
    let payload = serde_json::to_string(proj).unwrap_or_default();
    let pipeline_count = proj.pipelines.as_ref().map(|v| v.len()).unwrap_or(0);
    let open_mr_count = proj.merge_requests.len();
    let mut sql = format!(
        "INSERT OR REPLACE INTO projects (id, path, default_branch, url, last_activity_at, pipeline_count, open_mr_count, payload) VALUES ({}, '{}', '{}', '{}', '{}', {}, {}, '{}');\n",
        proj.id,
        sql_escape(proj.path.as_str()),
        sql_escape(proj.default_branch.as_str()),
        sql_escape(proj.url.as_str()),
        sql_escape(&proj.last_activity_at.to_rfc3339()),
        pipeline_count,
        open_mr_count,
        sql_escape(&payload)
    );
    if let Some(ref pipelines) = proj.pipelines {
        for p in pipelines {
            let p_payload = serde_json::to_string(p).unwrap_or_default();
            let status_str = serde_json::to_value(&p.status)
                .ok()
                .and_then(|v| v.as_str().map(ToString::to_string))
                .unwrap_or_else(|| "unknown".to_string());
            sql.push_str(&format!(
                "INSERT OR REPLACE INTO pipelines (id, project_id, project_path, status, source, branch, url, created_at, updated_at, payload) VALUES ({}, {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');\n",
                p.id.value(),
                proj.id,
                sql_escape(proj.path.as_str()),
                sql_escape(&status_str),
                sql_escape(p.source.to_string().as_str()),
                sql_escape(p.branch.as_str()),
                sql_escape(p.url.as_str()),
                sql_escape(&p.created_at.to_rfc3339()),
                sql_escape(&p.updated_at.to_rfc3339()),
                sql_escape(&p_payload)
            ));
        }
    }
    for mr in &proj.merge_requests {
        let mr_payload = serde_json::to_string(mr).unwrap_or_default();
        sql.push_str(&format!(
            "INSERT OR REPLACE INTO merge_requests (id, iid, project_id, project_path, title, state, source_branch, target_branch, draft, has_conflicts, blocking_discussions_resolved, upvotes, author_username, updated_at, url, payload) VALUES ({}, {}, {}, '{}', '{}', '{}', '{}', '{}', {}, {}, {}, {}, '{}', '{}', '{}', '{}');\n",
            mr.id,
            mr.iid,
            proj.id,
            sql_escape(proj.path.as_str()),
            sql_escape(mr.title.as_str()),
            sql_escape(mr.state.as_str()),
            sql_escape(mr.source_branch.as_str()),
            sql_escape(mr.target_branch.as_str()),
            if mr.draft { "TRUE" } else { "FALSE" },
            if mr.has_conflicts { "TRUE" } else { "FALSE" },
            if mr.blocking_discussions_resolved { "TRUE" } else { "FALSE" },
            mr.upvotes,
            sql_escape(mr.author_username.as_str()),
            sql_escape(&mr.updated_at.to_rfc3339()),
            sql_escape(mr.url.as_str()),
            sql_escape(&mr_payload)
        ));
    }
    sql
}

fn upsert_activity_sql(ev: &ProjectActivityEvent) -> String {
    let payload = serde_json::to_string(ev).unwrap_or_default();
    format!(
        "INSERT OR REPLACE INTO activity_events (id, project_id, project_name, kind, author, mr_iid, branch, commit_sha, summary, created_at, payload) VALUES ({}, {}, '{}', '{}', '{}', {}, {}, {}, '{}', '{}', '{}');\n",
        ev.id,
        ev.project_id,
        sql_escape(ev.project_name.as_str()),
        sql_escape(ev.kind.action_label()),
        sql_escape(ev.author.as_str()),
        sql_opt_u64(ev.mr_iid),
        sql_opt_str(ev.branch.as_deref()),
        sql_opt_str(ev.commit_sha.as_deref()),
        sql_escape(ev.summary.as_str()),
        sql_escape(&ev.created_at.to_rfc3339()),
        sql_escape(&payload)
    )
}

fn upsert_commit_sql(c: &CommitDto) -> String {
    let payload = serde_json::to_string(c).unwrap_or_default();
    format!(
        "INSERT OR REPLACE INTO commits (id, title, author_name, message, payload) VALUES ('{}', '{}', '{}', '{}', '{}');\n",
        sql_escape(c.id.as_str()),
        sql_escape(c.title.as_str()),
        sql_escape(c.author_name.as_str()),
        sql_escape(c.message.as_str()),
        sql_escape(&payload)
    )
}

fn query_payloads<T: DeserializeOwned>(duckdb_bin: &Path, db_path: &Path, sql: &str) -> Vec<T> {
    let full_sql = format!("{DUCKDB_LOW_MEMORY_SQL}\n{sql}");
    let Ok(out) = Command::new(duckdb_bin)
        .env("MALLOC_ARENA_MAX", "1")
        .arg("-dark-mode")
        .arg("-readonly")
        .arg("-batch")
        .arg("-json")
        .arg(db_path)
        .arg("-c")
        .arg(full_sql)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
    else {
        return Vec::new();
    };

    if !out.status.success() {
        return Vec::new();
    }

    let Ok(rows) = serde_json::from_slice::<Vec<Value>>(&out.stdout) else {
        return Vec::new();
    };

    rows.into_iter()
        .filter_map(|row| {
            let payload_str = row.get("payload")?.as_str()?;
            serde_json::from_str::<T>(payload_str).ok()
        })
        .collect()
}

fn read_jsonl<T: DeserializeOwned>(path: &Path) -> Vec<T> {
    let Ok(file) = File::open(path) else {
        return Vec::new();
    };
    BufReader::new(file)
        .lines()
        .map_while(Result::ok)
        .filter(|l| !l.trim().is_empty())
        .filter_map(|line| serde_json::from_str::<T>(&line).ok())
        .collect()
}

fn mr_full_body(project: &Project, mr_iid: Option<u64>, title: &CompactString) -> CompactString {
    if let Some(desc) = mr_iid
        .and_then(|iid| project.merge_request(iid))
        .and_then(|mr| mr.description.as_deref())
    {
        let cleaned = clean_multiline_body(desc);
        if !cleaned.is_empty() {
            return format!("{title}\n\n{cleaned}").into();
        }
    }
    title.clone()
}

pub fn convert_gitlab_event(
    dto: &GitlabEventDto,
    project: &Project,
    current_username: &str,
) -> Option<ProjectActivityEvent> {
    let project_name = project.title();
    let author = dto.author_username.clone();
    let action = dto.action_name.as_str();
    let target_type = dto.target_type.as_deref().unwrap_or("");

    let is_self = author.eq_ignore_ascii_case(current_username);

    match (action, target_type) {
        ("opened" | "created" | "reopened", "MergeRequest") => {
            let mr_iid = dto.target_iid;
            let title = dto.target_title.clone().unwrap_or_default();
            let kind = if !is_self && contains_mention(&title, current_username) {
                ActivityKind::Mention
            } else {
                ActivityKind::MrOpened
            };
            let body = mr_full_body(project, mr_iid, &title);
            Some(ProjectActivityEvent {
                id: dto.id,
                project_id: project.id,
                project_name,
                kind,
                author,
                mr_iid,
                branch: None,
                commit_sha: None,
                summary: title,
                detail: if action == "reopened" {
                    Some("reopened".into())
                } else {
                    None
                },
                body: Some(body),
                created_at: dto.created_at,
            })
        },
        ("accepted" | "merged", "MergeRequest") => {
            let mr_iid = dto.target_iid;
            let title = dto.target_title.clone().unwrap_or_default();
            let body = mr_full_body(project, mr_iid, &title);
            Some(ProjectActivityEvent {
                id: dto.id,
                project_id: project.id,
                project_name,
                kind: ActivityKind::MrMerged,
                author,
                mr_iid,
                branch: None,
                commit_sha: None,
                summary: title,
                detail: None,
                body: Some(body),
                created_at: dto.created_at,
            })
        },
        ("closed", "MergeRequest") => {
            let mr_iid = dto.target_iid;
            let title = dto.target_title.clone().unwrap_or_default();
            let body = mr_full_body(project, mr_iid, &title);
            Some(ProjectActivityEvent {
                id: dto.id,
                project_id: project.id,
                project_name,
                kind: ActivityKind::MrClosed,
                author,
                mr_iid,
                branch: None,
                commit_sha: None,
                summary: title,
                detail: None,
                body: Some(body),
                created_at: dto.created_at,
            })
        },
        ("approved", "MergeRequest") => {
            let mr_iid = dto.target_iid;
            let title = dto.target_title.clone().unwrap_or_default();
            let body = mr_full_body(project, mr_iid, &title);
            Some(ProjectActivityEvent {
                id: dto.id,
                project_id: project.id,
                project_name,
                kind: ActivityKind::MrApproved,
                author,
                mr_iid,
                branch: None,
                commit_sha: None,
                summary: title,
                detail: None,
                body: Some(body),
                created_at: dto.created_at,
            })
        },
        (a, _) if a.starts_with("commented") => {
            let note = dto.note.as_ref()?;
            let is_mr_note = note.noteable_type.as_deref() == Some("MergeRequest")
                || target_type == "MergeRequest";
            let is_mention = !is_self && contains_mention(&note.body, current_username);
            if !is_mention && !is_mr_note {
                return None;
            }
            let mr_iid = if is_mr_note {
                note.noteable_iid.or(dto.target_iid)
            } else {
                None
            };
            let snippet = clean_note_body(&note.body, 90);
            if snippet.is_empty() {
                return None;
            }
            let full_body = clean_multiline_body(&note.body);
            let detail = dto.target_title.clone().filter(|t| t != &snippet);
            let kind = if is_mention {
                ActivityKind::Mention
            } else {
                ActivityKind::MrComment
            };
            Some(ProjectActivityEvent {
                id: dto.id,
                project_id: project.id,
                project_name,
                kind,
                author,
                mr_iid,
                branch: None,
                commit_sha: None,
                summary: snippet,
                detail,
                body: Some(full_body),
                created_at: dto.created_at,
            })
        },
        (a, _) if a.starts_with("pushed") => {
            let push = dto.push_data.as_ref()?;
            if push.action == "removed" || push.ref_type == "tag" {
                return None;
            }
            let branch = push.branch.clone()?;
            let mr_iid = project
                .merge_request_for_branch(&branch)
                .map(|mr| mr.iid);
            let commit_title = push
                .commit_title
                .clone()
                .unwrap_or_else(|| format!("{} commit(s)", push.commit_count).into());
            Some(ProjectActivityEvent {
                id: dto.id,
                project_id: project.id,
                project_name,
                kind: ActivityKind::BranchPush,
                author,
                mr_iid,
                branch: Some(branch),
                commit_sha: push.commit_to.clone(),
                summary: commit_title.clone(),
                detail: if push.commit_count > 1 {
                    Some(format!("{} commits", push.commit_count).into())
                } else {
                    None
                },
                body: Some(commit_title),
                created_at: dto.created_at,
            })
        },
        (_, "Issue" | "WorkItem" | "Milestone") => {
            let title = dto.target_title.clone().unwrap_or_default();
            if !is_self && contains_mention(&title, current_username) {
                Some(ProjectActivityEvent {
                    id: dto.id,
                    project_id: project.id,
                    project_name,
                    kind: ActivityKind::Mention,
                    author,
                    mr_iid: None,
                    branch: None,
                    commit_sha: None,
                    summary: title.clone(),
                    detail: dto
                        .target_iid
                        .map(|iid| format!("{target_type} #{iid}").into()),
                    body: Some(title),
                    created_at: dto.created_at,
                })
            } else {
                None
            }
        },
        _ => None,
    }
}

pub fn convert_gitlab_todo(
    todo: &GitlabTodoDto,
    current_username: &str,
) -> Option<ProjectActivityEvent> {
    if todo.author.username.eq_ignore_ascii_case(current_username) {
        return None;
    }
    let project = todo.project.as_ref()?;
    let project_name: CompactString = project
        .path_with_namespace
        .rsplit('/')
        .next()
        .unwrap_or(&project.path_with_namespace)
        .into();
    let kind = match todo.action_name.as_str() {
        "mentioned" | "directly_addressed" => ActivityKind::Mention,
        "review_requested" | "approval_required" => ActivityKind::ReviewRequested,
        "assigned" => ActivityKind::Assigned,
        "merge_train_removed" => ActivityKind::MergeTrainRemoved,
        _ => return None,
    };
    let is_mr = todo.target_type == "MergeRequest";
    let mr_iid = if is_mr {
        todo.target.as_ref().and_then(|t| t.iid)
    } else {
        None
    };
    let target_title = todo.target.as_ref().and_then(|t| t.title.clone());
    let snippet = clean_note_body(&todo.body, 90);
    let full_body = clean_multiline_body(&todo.body);
    let summary = if !snippet.is_empty() {
        snippet
    } else {
        target_title
            .clone()
            .unwrap_or_else(|| todo.action_name.clone())
    };
    let detail = target_title.filter(|t| t != &summary);
    Some(ProjectActivityEvent {
        id: todo.id + 1_000_000_000,
        project_id: project.id,
        project_name,
        kind,
        author: todo.author.username.clone(),
        mr_iid,
        branch: None,
        commit_sha: None,
        summary: summary.clone(),
        detail,
        body: Some(if !full_body.is_empty() { full_body } else { summary }),
        created_at: todo.created_at,
    })
}

fn is_duplicate_activity(
    existing: &ProjectActivityEvent,
    candidate: &ProjectActivityEvent,
) -> bool {
    if existing.id == candidate.id {
        return true;
    }
    existing.project_id == candidate.project_id
        && existing.mr_iid == candidate.mr_iid
        && existing.author == candidate.author
        && existing.summary == candidate.summary
        && (existing.created_at - candidate.created_at)
            .num_seconds()
            .abs()
            <= 15
}

fn format_activity_notification(
    ev: &ProjectActivityEvent,
    config: &GlimConfig,
) -> (String, String, NotifyUrgency) {
    let mr_ref = ev
        .mr_iid
        .map(|iid| format!("!{iid}"))
        .or_else(|| ev.branch.as_ref().map(|b| b.to_string()))
        .unwrap_or_default();

    let summary = format!(
        "{} [{}] @{} {} {}",
        ev.kind.icon(),
        ev.project_name,
        ev.author,
        ev.kind.action_label(),
        mr_ref
    );
    let body = ev.summary.to_string();
    let urgency = if ev.kind == ActivityKind::PipelineStatus {
        if ev.summary.ends_with("failed") {
            config.notify_urgency_pipeline_failed
        } else if ev.summary.ends_with("passed") {
            config.notify_urgency_pipeline_passed
        } else {
            config.notify_urgency_pipeline_running
        }
    } else if ev.kind.is_direct_attention() {
        config.notify_urgency_mentions
    } else {
        config.notify_urgency_default
    };
    (summary, body, urgency)
}

fn is_older_than_days(date: DateTime<Utc>, days: i64) -> bool {
    Utc::now().signed_duration_since(date).num_days() > days
}

#[instrument(skip(event))]
pub fn log_event(event: &GlimEvent) {
    match event {
        GlimEvent::ProjectsFetch => info!("Requesting all projects from GitLab"),
        GlimEvent::ProjectsLoaded(projects) => {
            info!(count = projects.len(), "Received projects from GitLab API")
        },
        GlimEvent::ProjectFetch(id) => debug!(project_id = %id, "Refreshing project"),
        GlimEvent::JobsActiveFetch => debug!("Requesting active pipelines for all projects"),
        GlimEvent::PipelinesFetch(id) => {
            debug!(project_id = %id, "Requesting pipelines for project")
        },
        GlimEvent::MergeRequestsFetch(id) => {
            debug!(project_id = %id, "Requesting merge requests for project")
        },
        GlimEvent::MergeRequestsLoaded(project_id, mrs) => {
            debug!(project_id = %project_id, count = mrs.len(), "Received merge requests from GitLab API")
        },
        GlimEvent::ProjectEventsFetch(id) => {
            debug!(project_id = %id, "Requesting recent events for project")
        },
        GlimEvent::ProjectEventsLoaded(project_id, events) => {
            debug!(project_id = %project_id, count = events.len(), "Received project events from GitLab API")
        },
        GlimEvent::JobsFetch(project_id, pipeline_id) => {
            debug!(project_id = %project_id, pipeline_id = %pipeline_id, "Requesting jobs")
        },
        GlimEvent::PipelinesLoaded(pipelines) => {
            let project_id = pipelines.first().map(|p| p.project_id);
            debug!(count = pipelines.len(), project_id = ?project_id, "Received pipelines from GitLab API")
        },
        GlimEvent::JobsLoaded(project_id, pipeline_id, jobs) => {
            debug!(project_id = %project_id, pipeline_id = %pipeline_id, count = jobs.len(), "Received jobs")
        },
        GlimEvent::ProjectDetailsOpen(id) => debug!(project_id = %id, "Opening project details"),
        GlimEvent::ProjectDetailsClose => debug!("Closing project details popup"),
        GlimEvent::PipelineActionsOpen(project_id, pipeline_id) => {
            debug!(project_id = %project_id, pipeline_id = %pipeline_id, "Opening pipeline actions")
        },
        GlimEvent::ProjectSelected(id) => debug!(project_id = %id, "Selected project"),
        GlimEvent::PipelineSelected(id) => debug!(pipeline_id = %id, "Selected pipeline"),
        GlimEvent::ProjectOpenUrl(id) => info!(project_id = %id, "Opening project in browser"),
        GlimEvent::PipelineOpenUrl(project_id, pipeline_id) => {
            info!(project_id = %project_id, pipeline_id = %pipeline_id, "Opening pipeline in browser")
        },
        GlimEvent::MergeRequestOpenUrl(project_id, mr_iid) => {
            info!(project_id = %project_id, mr_iid = %mr_iid, "Opening merge request in browser")
        },
        GlimEvent::JobOpenUrl(project_id, pipeline_id, job_id) => {
            info!(project_id = %project_id, pipeline_id = %pipeline_id, job_id = %job_id, "Opening job in browser")
        },
        GlimEvent::JobLogFetch(project_id, pipeline_id) => {
            info!(project_id = %project_id, pipeline_id = %pipeline_id, "Downloading job error log")
        },
        GlimEvent::JobLogDownloaded(project_id, job_id, log_content) => {
            info!(
                project_id = %project_id,
                job_id = %job_id,
                content_length = log_content.len(),
                "Job log downloaded successfully"
            )
        },
        GlimEvent::ConfigOpen => debug!("Displaying configuration"),
        GlimEvent::ConfigApply => info!("Applying new configuration"),
        GlimEvent::ConfigUpdate(_) => debug!("Updating configuration"),
        GlimEvent::ApplyTemporaryFilter(filter) => {
            debug!(filter = ?filter, "Applying temporary filter")
        },
        GlimEvent::FilterClear => info!("Clearing project filter"),
        GlimEvent::FilterMenuClose => debug!("Closing filter input"),
        GlimEvent::AppExit => info!("Application shutting down"),
        GlimEvent::AppError(err) => {
            warn!(error = %err, error_type = ?std::mem::discriminant(err), "Application error occurred")
        },
        GlimEvent::LogEntry(_msg) => {}, // Don't log LogEntry events to prevent infinite loop
        _ => {},                         // Don't log every event
    }
}

impl Dispatcher for ProjectStore {
    fn dispatch(&self, event: GlimEvent) {
        self.sender.send(event).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{GitlabEventNoteDto, MergeRequestDto, ProjectDto, StatisticsDto};

    fn sample_project() -> Project {
        Project::from(ProjectDto {
            id: ProjectId::new(24),
            path_with_namespace: "caasa/connect".into(),
            description: None,
            default_branch: "master".into(),
            ssh_url_to_repo: "git@gitlab.nvestiv.com:caasa/connect.git".into(),
            web_url: "https://gitlab.nvestiv.com/caasa/connect".into(),
            last_activity_at: Utc::now(),
            statistics: StatisticsDto::default(),
        })
    }

    #[test]
    fn test_bot_event_filtering() {
        let bot_event = GitlabEventDto {
            id: 2887,
            project_id: ProjectId::new(24),
            action_name: "commented on".into(),
            target_type: Some("Note".into()),
            target_iid: Some(3005),
            target_title: Some("Emails 6".into()),
            author_username: "project_24_bot_d365985ed3c305853d6f00fa0f7a979b".into(),
            created_at: Utc::now(),
            note: Some(GitlabEventNoteDto {
                id: 3005,
                body: "<!-- linear-linkback -->\n<details>CAA-322</details>".into(),
                noteable_type: Some("MergeRequest".into()),
                noteable_iid: Some(61),
                system: false,
            }),
            push_data: None,
        };
        assert!(bot_event.is_bot_event());

        let human_comment = GitlabEventDto {
            id: 2890,
            project_id: ProjectId::new(24),
            action_name: "commented on".into(),
            target_type: Some("DiscussionNote".into()),
            target_iid: Some(3010),
            target_title: Some("Emails 6".into()),
            author_username: "saad".into(),
            created_at: Utc::now(),
            note: Some(GitlabEventNoteDto {
                id: 3010,
                body: "<p>Looks great, let's merge after CI passes!</p>".into(),
                noteable_type: Some("MergeRequest".into()),
                noteable_iid: Some(61),
                system: false,
            }),
            push_data: None,
        };
        assert!(!human_comment.is_bot_event());

        let project = sample_project();
        let activity = convert_gitlab_event(&human_comment, &project, "paul").unwrap();
        assert_eq!(activity.kind, ActivityKind::MrComment);
        assert_eq!(activity.mr_iid, Some(61));
        assert_eq!(activity.author, "saad");
        assert_eq!(activity.summary, "Looks great, let's merge after CI passes!");

        let mention_comment = GitlabEventDto {
            id: 2891,
            project_id: ProjectId::new(24),
            action_name: "commented on".into(),
            target_type: Some("DiscussionNote".into()),
            target_iid: Some(3011),
            target_title: Some("Emails 6".into()),
            author_username: "saad".into(),
            created_at: Utc::now(),
            note: Some(GitlabEventNoteDto {
                id: 3011,
                body: "Hey @paul can you take a look at this migration?".into(),
                noteable_type: Some("MergeRequest".into()),
                noteable_iid: Some(61),
                system: false,
            }),
            push_data: None,
        };
        let mention_activity = convert_gitlab_event(&mention_comment, &project, "paul").unwrap();
        assert_eq!(mention_activity.kind, ActivityKind::Mention);
        let default_cfg = GlimConfig::default();
        let (_, _, urgency) = format_activity_notification(&mention_activity, &default_cfg);
        assert!(matches!(urgency, NotifyUrgency::Normal));
        let mut critical_cfg = GlimConfig::default();
        critical_cfg.notify_urgency_mentions = NotifyUrgency::Critical;
        let (_, _, crit_urgency) = format_activity_notification(&mention_activity, &critical_cfg);
        assert!(matches!(crit_urgency, NotifyUrgency::Critical));
        assert!(contains_mention("cc @paul!", "paul"));
        assert!(!contains_mention("cc @paulina", "paul"));
    }

    #[test]
    fn test_merge_request_status_helpers() {
        let dto: MergeRequestDto = serde_json::from_value(serde_json::json!({
            "id": 255,
            "iid": 61,
            "project_id": 24,
            "title": "Emails 6",
            "state": "opened",
            "source_branch": "track/CAA-322-email-template-dsl",
            "target_branch": "track/CAA-322-atdd-email-specs",
            "draft": false,
            "detailed_merge_status": "mergeable",
            "has_conflicts": false,
            "blocking_discussions_resolved": true,
            "user_notes_count": 2,
            "upvotes": 1,
            "author": { "username": "paul" },
            "updated_at": "2026-10-23T06:17:21.689Z",
            "web_url": "https://gitlab.nvestiv.com/caasa/connect/-/merge_requests/61"
        }))
        .unwrap();

        let mr = MergeRequest::from(dto);
        assert!(mr.is_mergeable());
        assert!(!mr.is_conflicted());
        assert!(!mr.has_unresolved_threads());
        assert_eq!(mr.iid, 61);
        assert_eq!(mr.upvotes, 1);
        assert_eq!(mr.user_notes_count, 2);
    }

    #[test]
    fn test_todo_conversion_and_deduplication() {
        let unmergeable_todo: GitlabTodoDto = serde_json::from_value(serde_json::json!({
            "id": 270,
            "action_name": "unmergeable",
            "target_type": "MergeRequest",
            "author": { "username": "paul" },
            "project": { "id": 4, "path_with_namespace": "nvestiv/iris-ex" },
            "target": { "iid": 13, "title": "feat: migration" },
            "body": "feat: migration",
            "created_at": "2026-10-23T08:49:29.483Z"
        }))
        .unwrap();
        assert!(convert_gitlab_todo(&unmergeable_todo, "paul").is_none());

        let review_todo: GitlabTodoDto = serde_json::from_value(serde_json::json!({
            "id": 271,
            "action_name": "review_requested",
            "target_type": "MergeRequest",
            "author": { "username": "saad" },
            "project": { "id": 24, "path_with_namespace": "caasa/connect" },
            "target": { "iid": 61, "title": "Emails 6" },
            "body": "Emails 6",
            "created_at": "2026-10-23T08:50:00.000Z"
        }))
        .unwrap();
        let review_ev = convert_gitlab_todo(&review_todo, "paul").unwrap();
        assert_eq!(review_ev.kind, ActivityKind::ReviewRequested);
        assert_eq!(review_ev.summary, "Emails 6");
        assert_eq!(review_ev.detail, None); // deduplicated when body == target.title
    }

    #[test]
    fn test_disk_cache_roundtrip() {
        let tmp_dir = std::env::temp_dir().join(format!("glim_cache_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp_dir);

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut config = GlimConfig::default();
        config.os_notifications = false;

        let mut store = ProjectStore::with_cache_dir(tx.clone(), &config, Some(tmp_dir.clone()));
        let dto = ProjectDto {
            id: ProjectId::new(24),
            path_with_namespace: "caasa/connect".into(),
            description: Some("Connect API".into()),
            default_branch: "master".into(),
            ssh_url_to_repo: "git@gitlab.nvestiv.com:caasa/connect.git".into(),
            web_url: "https://gitlab.nvestiv.com/caasa/connect".into(),
            last_activity_at: Utc::now(),
            statistics: StatisticsDto::default(),
        };
        store.apply(&GlimEvent::ProjectsLoaded(vec![dto]));

        let comment = GitlabEventDto {
            id: 2890,
            project_id: ProjectId::new(24),
            action_name: "commented on".into(),
            target_type: Some("DiscussionNote".into()),
            target_iid: Some(3010),
            target_title: Some("Emails 6".into()),
            author_username: "saad".into(),
            created_at: Utc::now(),
            note: Some(GitlabEventNoteDto {
                id: 3010,
                body: "<p>Cached comment body</p>".into(),
                noteable_type: Some("MergeRequest".into()),
                noteable_iid: Some(61),
                system: false,
            }),
            push_data: None,
        };
        store.apply(&GlimEvent::ProjectEventsLoaded(
            ProjectId::new(24),
            vec![comment],
        ));
        store.shutdown();

        let reloaded = ProjectStore::with_cache_dir(tx, &config, Some(tmp_dir.clone()));
        assert_eq!(reloaded.sorted_projects().len(), 1);
        assert_eq!(reloaded.sorted_projects()[0].path, "caasa/connect");
        assert_eq!(reloaded.recent_activity().len(), 1);
        assert_eq!(reloaded.recent_activity()[0].summary, "Cached comment body");

        let _ = fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn test_activity_load_more_pagination_and_skip_cached() {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut config = GlimConfig::default();
        config.os_notifications = false;

        let mut store = ProjectStore::with_cache_dir(tx, &config, None);
        let dto = ProjectDto {
            id: ProjectId::new(24),
            path_with_namespace: "caasa/connect".into(),
            description: None,
            default_branch: "master".into(),
            ssh_url_to_repo: "git@gitlab.nvestiv.com:caasa/connect.git".into(),
            web_url: "https://gitlab.nvestiv.com/caasa/connect".into(),
            last_activity_at: Utc::now(),
            statistics: StatisticsDto::default(),
        };
        store.apply(&GlimEvent::ProjectsLoaded(vec![dto]));

        // Simulate 100 events loaded on page 1
        let page1_events: Vec<GitlabEventDto> = (1..=100)
            .map(|i| GitlabEventDto {
                id: 5000 + i,
                project_id: ProjectId::new(24),
                action_name: "opened".into(),
                target_type: Some("MergeRequest".into()),
                target_iid: Some(i),
                target_title: Some(format!("MR {i}").into()),
                author_username: "saad".into(),
                created_at: Utc::now(),
                note: None,
                push_data: None,
            })
            .collect();
        store.apply(&GlimEvent::ProjectEventsLoaded(
            ProjectId::new(24),
            page1_events.clone(),
        ));
        assert_eq!(store.recent_activity().len(), 100);
        while rx.try_recv().is_ok() {}

        // Request loading more events -> should dispatch ProjectEventsPageFetch(24, 2)
        store.apply(&GlimEvent::ActivityLoadMore);
        assert!(store.is_loading_more_activity());
        let mut fetched_pages = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            if let GlimEvent::ProjectEventsPageFetch(pid, page) = ev {
                fetched_pages.push((pid, page));
            }
        }
        assert_eq!(fetched_pages, vec![(ProjectId::new(24), 2)]);

        // If page 2 only has already-cached events (or bot events), it should automatically advance to page 3
        store.apply(&GlimEvent::ProjectEventsPageLoaded(
            ProjectId::new(24),
            2,
            page1_events,
        ));
        assert!(store.is_loading_more_activity());
        let mut next_pages = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            if let GlimEvent::ProjectEventsPageFetch(pid, page) = ev {
                next_pages.push((pid, page));
            }
        }
        assert_eq!(next_pages, vec![(ProjectId::new(24), 3)]);

        // When page 3 returns < 100 events, project is marked exhausted
        let page3_events = vec![GitlabEventDto {
            id: 4001,
            project_id: ProjectId::new(24),
            action_name: "opened".into(),
            target_type: Some("MergeRequest".into()),
            target_iid: Some(999),
            target_title: Some("Older MR".into()),
            author_username: "saad".into(),
            created_at: Utc::now(),
            note: None,
            push_data: None,
        }];
        store.apply(&GlimEvent::ProjectEventsPageLoaded(
            ProjectId::new(24),
            3,
            page3_events,
        ));
        assert!(!store.is_loading_more_activity());
        assert_eq!(store.recent_activity().len(), 101);
    }
}
