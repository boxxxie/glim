use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::mpsc::Sender,
};

use chrono::{DateTime, Utc};
use compact_str::CompactString;
use itertools::Itertools;
use serde::{de::DeserializeOwned, Serialize};
use tracing::{debug, info, instrument, warn};

use crate::{
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

pub struct ProjectStore {
    sender: Sender<GlimEvent>,
    cache_dir: Option<PathBuf>,
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
        let cache_dir = directories::BaseDirs::new()
            .map(|d| d.data_local_dir().join("glim").join("cache"));

        Self::with_cache_dir(sender, config, cache_dir)
    }

    pub fn with_cache_dir(
        sender: Sender<GlimEvent>,
        config: &GlimConfig,
        cache_dir: Option<PathBuf>,
    ) -> Self {
        let auto_fetch_days = config.max_age_days.map(|d| d as i64).unwrap_or(14).max(7);
        let mut store = Self {
            sender,
            cache_dir,
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
        store.load_disk_cache();
        store
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

                let has_pipeline_events = !pipeline_feed_events.is_empty();
                for ev in pipeline_feed_events {
                    self.recent_activity.push(ev);
                }
                if has_pipeline_events {
                    self.recent_activity
                        .sort_by(|a, b| b.created_at.cmp(&a.created_at));
                    self.save_activity_cache();
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
                let mut added_any = false;
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
                            .any(|existing| is_duplicate_activity(existing, &activity));
                        if let Some(prev_max) = prev_max_id {
                            if activity.id > prev_max && !already_present {
                                newly_arrived.push(activity.clone());
                            }
                        }
                        if !already_present {
                            self.recent_activity.push(activity);
                            added_any = true;
                        }
                    }
                }

                if added_any {
                    self.recent_activity
                        .sort_by(|a, b| b.created_at.cmp(&a.created_at));
                    self.save_activity_cache();
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

                let mut added_any = false;
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
                            .any(|existing| is_duplicate_activity(existing, &activity));
                        if !already_present {
                            self.recent_activity.push(activity);
                            added_any = true;
                        }
                    }
                }

                if added_any {
                    self.recent_activity
                        .sort_by(|a, b| b.created_at.cmp(&a.created_at));
                    self.save_activity_cache();
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
                let mut added_any = false;
                let current_username = self.current_username.clone();
                for todo in todos {
                    if let Some(activity) = convert_gitlab_todo(todo, &current_username) {
                        let already_present = self
                            .recent_activity
                            .iter()
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
                            self.recent_activity.push(activity);
                            added_any = true;
                        }
                    }
                }

                if added_any {
                    self.recent_activity
                        .sort_by(|a, b| b.created_at.cmp(&a.created_at));
                    self.save_activity_cache();
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
        let mut updated_activity = false;
        for ev in &mut self.recent_activity {
            if ev.commit_sha.as_deref() == Some(commit.id.as_str()) {
                if !commit.title.is_empty() && ev.summary != commit.title {
                    ev.summary = commit.title.clone();
                    updated_activity = true;
                }
                let trimmed: CompactString = commit.message.trim().into();
                if !trimmed.is_empty() && ev.body.as_ref() != Some(&trimmed) {
                    ev.body = Some(trimmed);
                    updated_activity = true;
                }
            }
        }
        self.commits.insert(commit.id.clone(), commit);
        self.save_commits_cache();
        if updated_activity {
            self.save_activity_cache();
        }
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

    fn load_disk_cache(&mut self) {
        let Some(ref dir) = self.cache_dir.clone() else {
            return;
        };

        let cached_commits: Vec<CommitDto> = read_jsonl(&dir.join("commits.jsonl"));
        for c in cached_commits {
            if !c.id.is_empty() {
                self.requested_commits.insert(c.id.clone());
                self.commits.insert(c.id.clone(), c);
            }
        }

        let cached_projects: Vec<Project> = read_jsonl(&dir.join("projects.jsonl"));
        for p in cached_projects {
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

        let cached_events: Vec<ProjectActivityEvent> =
            read_jsonl(&dir.join("activity_events.jsonl"));
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
            self.startup_notified = true;
        }
    }

    fn save_projects_cache(&self) {
        if let Some(ref dir) = self.cache_dir {
            write_jsonl(&dir.join("projects.jsonl"), self.projects.iter());
        }
    }

    fn save_activity_cache(&self) {
        if let Some(ref dir) = self.cache_dir {
            write_jsonl(
                &dir.join("activity_events.jsonl"),
                self.recent_activity.iter(),
            );
        }
    }

    fn save_commits_cache(&self) {
        if let Some(ref dir) = self.cache_dir {
            write_jsonl(&dir.join("commits.jsonl"), self.commits.values());
        }
    }
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

fn write_jsonl<'a, T: Serialize + 'a>(path: &Path, items: impl IntoIterator<Item = &'a T>) {
    if let Some(parent) = path.parent() {
        if fs::create_dir_all(parent).is_err() {
            return;
        }
    }
    let tmp_path = path.with_extension("jsonl.tmp");
    let Ok(file) = File::create(&tmp_path) else {
        return;
    };
    let mut writer = BufWriter::new(file);
    for item in items {
        if serde_json::to_writer(&mut writer, item).is_err() {
            return;
        }
        if writer.write_all(b"\n").is_err() {
            return;
        }
    }
    if writer.flush().is_ok() {
        let _ = fs::rename(&tmp_path, path);
    }
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
