use std::{
    collections::{HashMap, HashSet},
    sync::mpsc::Sender,
};

use chrono::{DateTime, Utc};
use compact_str::CompactString;
use itertools::Itertools;
use tracing::{debug, info, instrument, warn};

use crate::{
    dispatcher::Dispatcher,
    domain::{
        clean_note_body, ActivityKind, GitlabEventDto, Job, MergeRequest, Pipeline, PipelineStatus,
        Project, ProjectActivityEvent,
    },
    event::GlimEvent,
    glim_app::GlimConfig,
    id::ProjectId,
    os_notify::{send_os_notification, NotifyUrgency},
};

const MAX_ACTIVITY_EVENTS: usize = 100;

pub struct ProjectStore {
    sender: Sender<GlimEvent>,
    projects: Vec<Project>,
    project_id_lookup: HashMap<ProjectId, usize>,
    sorted: Vec<Project>,
    max_recent_pipelines: usize,
    include_mr_pipelines: bool,
    auto_fetch_days: i64,
    os_notifications: bool,
    ignore_bot_comments: bool,
    include_projects: Option<Vec<CompactString>>,
    exclude_projects: Option<Vec<CompactString>>,
    hydrated_pipelines: HashSet<ProjectId>,
    hydrated_mrs: HashSet<ProjectId>,
    last_seen_event_id: HashMap<ProjectId, u64>,
    recent_activity: Vec<ProjectActivityEvent>,
    startup_notified: bool,
}

impl ProjectStore {
    pub fn new(sender: Sender<GlimEvent>, config: &GlimConfig) -> Self {
        let auto_fetch_days = config.max_age_days.map(|d| d as i64).unwrap_or(14).max(7);
        Self {
            sender,
            projects: Vec::new(),
            project_id_lookup: HashMap::new(),
            sorted: Vec::new(),
            max_recent_pipelines: config.max_recent_pipelines,
            include_mr_pipelines: config.include_mr_pipelines,
            auto_fetch_days,
            os_notifications: config.os_notifications,
            ignore_bot_comments: config.ignore_bot_comments,
            include_projects: config.include_projects.clone(),
            exclude_projects: config.exclude_projects.clone(),
            hydrated_pipelines: HashSet::new(),
            hydrated_mrs: HashSet::new(),
            last_seen_event_id: HashMap::new(),
            recent_activity: Vec::new(),
            startup_notified: false,
        }
    }

    pub fn recent_activity(&self) -> &[ProjectActivityEvent] {
        &self.recent_activity
    }

    pub fn notify_latest_activity(&self) {
        if let Some(latest) = self.recent_activity.first() {
            let (summary, body, urgency) = format_activity_notification(latest);
            if self.os_notifications {
                send_os_notification(&summary, &body, urgency);
            }
        }
    }

    fn emit_notification(&self, summary: &str, body: &str, urgency: NotifyUrgency) {
        let banner_msg: CompactString = format!("{summary}: {body}").into();
        self.sender.dispatch(GlimEvent::ActivityNotice(banner_msg));
        if self.os_notifications {
            send_os_notification(summary, body, urgency);
        }
    }

    fn should_watch_project(&self, project: &Project) -> bool {
        if is_older_than_days(project.last_activity(), self.auto_fetch_days) {
            return false;
        }
        let path_lower = project.path.to_lowercase();

        if let Some(excludes) = &self.exclude_projects {
            if excludes
                .iter()
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
                .any(|term| path_lower.contains(&term))
            {
                return false;
            }
        }

        if let Some(includes) = &self.include_projects {
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

    #[instrument(skip(self, event), fields(event_type = %event.variant_name()))]
    pub fn apply(&mut self, event: &GlimEvent) {
        match event {
            GlimEvent::ConfigUpdate(config) => {
                self.max_recent_pipelines = config.max_recent_pipelines;
                self.include_mr_pipelines = config.include_mr_pipelines;
                self.auto_fetch_days = config.max_age_days.map(|d| d as i64).unwrap_or(14).max(7);
                self.os_notifications = config.os_notifications;
                self.ignore_bot_comments = config.ignore_bot_comments;
                self.include_projects = config.include_projects.clone();
                self.exclude_projects = config.exclude_projects.clone();
            },

            // requests jobs for pipelines that have not been loaded yet, plus fresh MRs & events
            GlimEvent::ProjectDetailsOpen(id) => {
                debug!(project_id = %id, "Opening project details and requesting missing jobs");
                let project = self.find(*id).unwrap();
                let project_id = project.id;
                project
                    .recent_pipelines(self.max_recent_pipelines, self.include_mr_pipelines)
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
                        let project = p.clone();
                        self.sync_project(p);
                        let sender = self.sender.clone();
                        sender.dispatch(GlimEvent::ProjectUpdated(Box::new(project)))
                    });

                self.sorted = self.projects_sorted_by_last_activity();
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
                let mut notifications: Vec<(String, String, NotifyUrgency)> = Vec::new();
                let mut pipeline_feed_events: Vec<ProjectActivityEvent> = Vec::new();

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
                                let mr_iid = project
                                    .merge_request_for_branch(&p.branch)
                                    .map(|mr| mr.iid);
                                let mr_tag = mr_iid
                                    .map(|iid| format!(" (!{})", iid))
                                    .unwrap_or_default();

                                if let Some(old_p) = old_pipelines.iter().find(|op| op.id == p.id) {
                                    if old_p.status != p.status {
                                        let transition = match p.status {
                                            PipelineStatus::Failed => Some((
                                                format!("🔴 [{}] Pipeline failed", project_title),
                                                format!("{}{}", p.branch, mr_tag),
                                                NotifyUrgency::Critical,
                                                "failed",
                                            )),
                                            PipelineStatus::Success => Some((
                                                format!("🟢 [{}] Pipeline passed", project_title),
                                                format!("{}{}", p.branch, mr_tag),
                                                NotifyUrgency::Normal,
                                                "passed",
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
                                                    NotifyUrgency::Normal,
                                                    "running",
                                                ))
                                            },
                                            _ => None,
                                        };
                                        if let Some((sum, body, urg, label)) = transition {
                                            notifications.push((sum, body, urg));
                                            pipeline_feed_events.push(ProjectActivityEvent {
                                                id: p.id.value().wrapping_mul(1000)
                                                    + (p.updated_at.timestamp() as u64 % 1000),
                                                project_id,
                                                project_name: project_title.clone(),
                                                kind: ActivityKind::PipelineStatus,
                                                author: "ci".into(),
                                                mr_iid,
                                                branch: Some(p.branch.clone()),
                                                summary: format!("Pipeline #{} {}", p.id, label)
                                                    .into(),
                                                detail: None,
                                                created_at: p.updated_at,
                                            });
                                        }
                                    }
                                } else if p.id.value() > max_old_id {
                                    let transition = match p.status {
                                        PipelineStatus::Failed => Some((
                                            format!("🔴 [{}] Pipeline failed", project_title),
                                            format!("{}{}", p.branch, mr_tag),
                                            NotifyUrgency::Critical,
                                            "failed",
                                        )),
                                        PipelineStatus::Success => Some((
                                            format!("🟢 [{}] Pipeline passed", project_title),
                                            format!("{}{}", p.branch, mr_tag),
                                            NotifyUrgency::Normal,
                                            "passed",
                                        )),
                                        _ if p.status.is_active() => Some((
                                            format!("🔵 [{}] Pipeline started", project_title),
                                            format!("{}{}", p.branch, mr_tag),
                                            NotifyUrgency::Normal,
                                            "started",
                                        )),
                                        _ => None,
                                    };
                                    if let Some((sum, body, urg, label)) = transition {
                                        notifications.push((sum, body, urg));
                                        pipeline_feed_events.push(ProjectActivityEvent {
                                            id: p.id.value().wrapping_mul(1000)
                                                + (p.updated_at.timestamp() as u64 % 1000),
                                            project_id,
                                            project_name: project_title.clone(),
                                            kind: ActivityKind::PipelineStatus,
                                            author: "ci".into(),
                                            mr_iid,
                                            branch: Some(p.branch.clone()),
                                            summary: format!("Pipeline #{} {}", p.id, label).into(),
                                            detail: None,
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

                for ev in pipeline_feed_events {
                    self.recent_activity.push(ev);
                }
                self.recent_activity
                    .sort_by(|a, b| b.created_at.cmp(&a.created_at));
                self.recent_activity.truncate(MAX_ACTIVITY_EVENTS);

                self.hydrated_pipelines.insert(project_id);
                for (summary, body, urgency) in notifications {
                    self.emit_notification(&summary, &body, urgency);
                }

                self.sorted = self.projects_sorted_by_last_activity();
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
                let mut notifications: Vec<(String, String, NotifyUrgency)> = Vec::new();

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
                                if !old_mr.is_conflicted() && mr.is_conflicted() {
                                    notifications.push((
                                        format!("⚡ [{}] !{} has merge conflicts", project_title, mr.iid),
                                        format!("{} ({})", mr.title, mr.source_branch),
                                        NotifyUrgency::Critical,
                                    ));
                                }
                                if mr.upvotes > old_mr.upvotes {
                                    notifications.push((
                                        format!("👍 [{}] !{} approved", project_title, mr.iid),
                                        format!("{} (👍{})", mr.title, mr.upvotes),
                                        NotifyUrgency::Normal,
                                    ));
                                }
                                if old_mr.blocking_discussions_resolved
                                    && !mr.blocking_discussions_resolved
                                {
                                    notifications.push((
                                        format!("🔒 [{}] !{} has unresolved threads", project_title, mr.iid),
                                        mr.title.to_string(),
                                        NotifyUrgency::Normal,
                                    ));
                                } else if !old_mr.blocking_discussions_resolved
                                    && mr.blocking_discussions_resolved
                                {
                                    notifications.push((
                                        format!("✅ [{}] !{} threads resolved", project_title, mr.iid),
                                        mr.title.to_string(),
                                        NotifyUrgency::Normal,
                                    ));
                                }
                            }
                        }
                    }

                    project.update_merge_requests(new_mrs);
                    sender.dispatch(GlimEvent::ProjectUpdated(Box::new(project.clone())));
                }

                self.hydrated_mrs.insert(*project_id);
                for (summary, body, urgency) in notifications {
                    self.emit_notification(&summary, &body, urgency);
                }

                self.sorted = self.projects_sorted_by_last_activity();
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

                let Some(project) = self.find(*project_id).cloned() else {
                    return;
                };

                let mut newly_arrived: Vec<ProjectActivityEvent> = Vec::new();

                for dto in event_dtos {
                    if self.ignore_bot_comments && dto.is_bot_event() {
                        continue;
                    }
                    if let Some(activity) = convert_gitlab_event(dto, &project) {
                        if let Some(prev_max) = prev_max_id {
                            if activity.id > prev_max {
                                newly_arrived.push(activity.clone());
                            }
                        }
                        if !self.recent_activity.iter().any(|existing| existing.id == activity.id) {
                            self.recent_activity.push(activity);
                        }
                    }
                }

                self.recent_activity
                    .sort_by(|a, b| b.created_at.cmp(&a.created_at));
                self.recent_activity.truncate(MAX_ACTIVITY_EVENTS);

                // Emit notifications for newly arrived events in chronological order
                newly_arrived.sort_by(|a, b| a.created_at.cmp(&b.created_at));
                for ev in newly_arrived {
                    let (summary, body, urgency) = format_activity_notification(&ev);
                    self.emit_notification(&summary, &body, urgency);
                }

                // On first startup hydration, emit the single latest activity notification so the user
                // sees immediate confirmation that in-app and OS notifications are active.
                if !self.startup_notified {
                    if let Some(latest) = self.recent_activity.first() {
                        self.startup_notified = true;
                        let (summary, body, urgency) = format_activity_notification(latest);
                        self.emit_notification(&summary, &body, urgency);
                    }
                }
            },

            GlimEvent::JobsLoaded(project_id, pipeline_id, job_dtos) => {
                debug!(project_id = %project_id, pipeline_id = %pipeline_id, job_count = job_dtos.len(), "Processing received jobs");
                let jobs: Vec<Job> = job_dtos
                    .iter()
                    .map(|j| Job::from(j.clone()))
                    .collect();

                let sender = self.sender.clone();
                if let Some(project) = self.find_mut(*project_id) {
                    project.update_jobs(*pipeline_id, jobs);
                    if let Some(commit) = job_dtos.first().map(|j| j.commit.clone().into()) {
                        project.update_commit(*pipeline_id, commit);
                    }
                    sender.dispatch(GlimEvent::ProjectUpdated(Box::new(project.clone())))
                }

                self.sorted = self.projects_sorted_by_last_activity();
            },

            // requests pipelines for a project if they are not already loaded
            GlimEvent::ProjectSelected(id) => {
                debug!(project_id = %id, "Project selected");
                let mut request_pipelines = false;
                if let Some(project) = self.find_mut(*id) {
                    if project.pipelines.is_none() {
                        project.pipelines = Some(Vec::new());
                        request_pipelines = true;
                    }
                };

                if request_pipelines {
                    self.dispatch(GlimEvent::PipelinesFetch(*id));
                };
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

    #[instrument(skip(self, project), fields(project_id = %project.id, project_path = %project.path))]
    fn sync_project(&mut self, mut project: Project) {
        let sender = self.sender.clone();
        let watch = self.should_watch_project(&project);
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
}

pub fn convert_gitlab_event(
    dto: &GitlabEventDto,
    project: &Project,
) -> Option<ProjectActivityEvent> {
    let project_name = project.title();
    let author = dto.author_username.clone();
    let action = dto.action_name.as_str();
    let target_type = dto.target_type.as_deref().unwrap_or("");

    match (action, target_type) {
        ("opened", "MergeRequest") => {
            let mr_iid = dto.target_iid;
            let title = dto.target_title.clone().unwrap_or_default();
            Some(ProjectActivityEvent {
                id: dto.id,
                project_id: project.id,
                project_name,
                kind: ActivityKind::MrOpened,
                author,
                mr_iid,
                branch: None,
                summary: title,
                detail: None,
                created_at: dto.created_at,
            })
        },
        ("accepted" | "merged", "MergeRequest") => {
            let mr_iid = dto.target_iid;
            let title = dto.target_title.clone().unwrap_or_default();
            Some(ProjectActivityEvent {
                id: dto.id,
                project_id: project.id,
                project_name,
                kind: ActivityKind::MrMerged,
                author,
                mr_iid,
                branch: None,
                summary: title,
                detail: None,
                created_at: dto.created_at,
            })
        },
        ("closed", "MergeRequest") => {
            let mr_iid = dto.target_iid;
            let title = dto.target_title.clone().unwrap_or_default();
            Some(ProjectActivityEvent {
                id: dto.id,
                project_id: project.id,
                project_name,
                kind: ActivityKind::MrClosed,
                author,
                mr_iid,
                branch: None,
                summary: title,
                detail: None,
                created_at: dto.created_at,
            })
        },
        ("approved", "MergeRequest") => {
            let mr_iid = dto.target_iid;
            let title = dto.target_title.clone().unwrap_or_default();
            Some(ProjectActivityEvent {
                id: dto.id,
                project_id: project.id,
                project_name,
                kind: ActivityKind::MrApproved,
                author,
                mr_iid,
                branch: None,
                summary: title,
                detail: None,
                created_at: dto.created_at,
            })
        },
        (a, _) if a.starts_with("commented") => {
            let note = dto.note.as_ref()?;
            if note.noteable_type.as_deref() != Some("MergeRequest") {
                return None;
            }
            let mr_iid = note.noteable_iid.or(dto.target_iid);
            let snippet = clean_note_body(&note.body, 90);
            if snippet.is_empty() {
                return None;
            }
            let mr_title = dto.target_title.clone();
            Some(ProjectActivityEvent {
                id: dto.id,
                project_id: project.id,
                project_name,
                kind: ActivityKind::MrComment,
                author,
                mr_iid,
                branch: None,
                summary: snippet,
                detail: mr_title,
                created_at: dto.created_at,
            })
        },
        (a, _) if a.starts_with("pushed") => {
            let push = dto.push_data.as_ref()?;
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
                summary: commit_title,
                detail: if push.commit_count > 1 {
                    Some(format!("{} commits", push.commit_count).into())
                } else {
                    None
                },
                created_at: dto.created_at,
            })
        },
        _ => None,
    }
}

fn format_activity_notification(
    ev: &ProjectActivityEvent,
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
    (summary, body, NotifyUrgency::Normal)
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
        let activity = convert_gitlab_event(&human_comment, &project).unwrap();
        assert_eq!(activity.kind, ActivityKind::MrComment);
        assert_eq!(activity.mr_iid, Some(61));
        assert_eq!(activity.author, "saad");
        assert_eq!(activity.summary, "Looks great, let's merge after CI passes!");
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
}
