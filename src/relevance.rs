use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Duration, Utc};
use compact_str::{format_compact, CompactString};

use crate::{
    domain::{
        ActivityKind, MergeRequest, Pipeline, PipelineStatus, Project, ProjectActivityEvent,
        RowEntryTarget, UserInteraction,
    },
    glim_app::{GlimConfig, RankingMode},
    id::ProjectId,
};

/// Context for computing dynamic relevance scores, user interaction excitation, and stochastic sampling.
#[derive(Clone, Copy, Debug)]
pub struct RelevanceContext<'a> {
    pub activity_events: &'a [ProjectActivityEvent],
    pub user_interactions: &'a [UserInteraction],
    pub current_username: &'a str,
    pub config: &'a GlimConfig,
    pub ranking_mode: RankingMode,
    pub stochastic_nonce: u64,
    pub now: DateTime<Utc>,
}

impl<'a> RelevanceContext<'a> {
    pub fn new(
        config: &'a GlimConfig,
        activity_events: &'a [ProjectActivityEvent],
        user_interactions: &'a [UserInteraction],
        current_username: &'a str,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            activity_events,
            user_interactions,
            current_username,
            config,
            ranking_mode: config.ranking_mode,
            stochastic_nonce: 0,
            now,
        }
    }

    pub fn with_mode(mut self, mode: Option<RankingMode>, stochastic_nonce: u64) -> Self {
        if let Some(m) = mode {
            self.ranking_mode = m;
        }
        self.stochastic_nonce = stochastic_nonce;
        self
    }

    pub fn bucket_seed(&self) -> u64 {
        let rot = self.config.stochastic_rotation_seconds.max(1) as i64;
        let bucket = (self.now.timestamp().max(0) / rot) as u64;
        splitmix64(bucket ^ self.stochastic_nonce.wrapping_mul(0x9e37_79b9_7f4a_7c15))
    }
}

pub fn project_gumbel_noise(ctx: &RelevanceContext<'_>, project_id: ProjectId) -> f64 {
    seeded_gumbel(ctx.bucket_seed(), project_id, "__project__")
}

/// Extracts the merge request IID from GitLab's special MR pipeline refs such as
/// `refs/merge-requests/1/head`, `refs/merge-requests/1/merge`, or `refs/merge-requests/1/train`.
pub fn parse_mr_ref_iid(branch: &str) -> Option<u64> {
    let rest = branch.strip_prefix("refs/merge-requests/")?;
    let iid_str = rest.split('/').next()?;
    iid_str.parse::<u64>().ok()
}

/// A unified, relevance-ranked branch/MR entry to render in a project row.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct RankedBranchEntry<'a> {
    pub branch: CompactString,
    pub pipeline: Option<&'a Pipeline>,
    pub mr: Option<&'a MergeRequest>,
    /// Additional open MR IIDs in the same stacked MR chain that were compacted into this row.
    pub stacked_iids: Vec<u64>,
    pub score: f64,
    pub is_urgent: bool,
    pub target: RowEntryTarget,
}

impl<'a> RankedBranchEntry<'a> {
    pub fn stacked_badge(&self) -> Option<CompactString> {
        match self.stacked_iids.len() {
            0 => None,
            1 => Some(format_compact!("(+1 stacked: !{})", self.stacked_iids[0])),
            n => {
                let first = self.stacked_iids.first().copied().unwrap_or(0);
                let last = self.stacked_iids.last().copied().unwrap_or(0);
                Some(format_compact!("(+{n} stacked: !{first}..!{last})"))
            },
        }
    }
}

/// A first-class CI process / MR entry across all visible repositories for the flat CI view.
#[derive(Clone, Debug)]
pub struct RankedCiProcessEntry<'a> {
    pub project: &'a Project,
    pub entry: RankedBranchEntry<'a>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
struct CandidateBranch<'a> {
    branch: CompactString,
    latest_pipeline: Option<&'a Pipeline>,
    mr: Option<&'a MergeRequest>,
    failed_runs_48h: usize,
    last_updated: DateTime<Utc>,
    score: f64,
    is_urgent: bool,
    stack_root_iid: Option<u64>,
    stacked_iids: Vec<u64>,
}

/// Fast deterministic SplitMix64 hash for seeded Gumbel noise generation.
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Generates a bounded Gumbel(0, 1) variate from a deterministic `(bucket, project_id, branch)` seed.
fn seeded_gumbel(bucket: u64, project_id: ProjectId, branch: &str) -> f64 {
    let mut h = splitmix64(bucket ^ (project_id.value() as u64).wrapping_mul(0x517c_c1b7_2722_0a95));
    for b in branch.as_bytes() {
        h = splitmix64(h ^ (*b as u64));
    }
    // Map h to open interval (0.01, 0.99) to avoid extreme log tails
    let u = 0.01 + 0.98 * ((h >> 11) as f64 / ((1u64 << 53) as f64));
    (-(-u.ln()).ln()).clamp(-1.5, 2.5)
}

/// Ranks, prunes dead branches, compacts stacked MR chains, and selects up to `max_rows`
/// branch/MR entries for `project`.
pub fn rank_project_entries<'a>(
    project: &'a Project,
    ctx: &RelevanceContext<'_>,
    max_rows: usize,
) -> Vec<RankedBranchEntry<'a>> {
    let max_rows = max_rows.max(1);
    let include_mr_pipelines = ctx.config.include_mr_pipelines;
    if !ctx.config.dynamic_relevance {
        return legacy_project_entries(project, max_rows, include_mr_pipelines);
    }
    let now = ctx.now;
    let current_username = ctx.current_username;

    // 1. Unify pipelines by canonical branch (resolving `refs/merge-requests/<iid>/...` to `mr.source_branch`)
    let mut by_branch: HashMap<CompactString, CandidateBranch<'a>> = HashMap::new();
    let cutoff_48h = now - Duration::hours(48);

    if let Some(pipelines) = project.pipelines.as_ref() {
        for p in pipelines {
            if !(p.source.is_interesting(include_mr_pipelines) || p.status.is_active()) {
                continue;
            }
            let mr = project.merge_request_for_branch(&p.branch);
            let canonical_branch = mr
                .map(|m| m.source_branch.clone())
                .unwrap_or_else(|| p.branch.clone());

            let entry = by_branch.entry(canonical_branch.clone()).or_insert_with(|| {
                CandidateBranch {
                    branch: canonical_branch,
                    latest_pipeline: Some(p),
                    mr,
                    failed_runs_48h: 0,
                    last_updated: p.updated_at,
                    score: 0.0,
                    is_urgent: false,
                    stack_root_iid: None,
                    stacked_iids: Vec::new(),
                }
            });

            if p.status == PipelineStatus::Failed && p.updated_at >= cutoff_48h {
                entry.failed_runs_48h += 1;
            }

            if let Some(existing_p) = entry.latest_pipeline {
                let replace = (p.status.is_active() && !existing_p.status.is_active())
                    || (p.status.is_active() == existing_p.status.is_active()
                        && p.updated_at > existing_p.updated_at);
                if replace {
                    entry.latest_pipeline = Some(p);
                }
            }
            if p.updated_at > entry.last_updated {
                entry.last_updated = p.updated_at;
            }
            if entry.mr.is_none() && mr.is_some() {
                entry.mr = mr;
            }
        }
    }

    // Include open MRs that do not have a pipeline in `project.pipelines`
    for mr in &project.merge_requests {
        by_branch
            .entry(mr.source_branch.clone())
            .and_modify(|e| {
                if e.mr.is_none() {
                    e.mr = Some(mr);
                }
                if mr.updated_at > e.last_updated {
                    e.last_updated = mr.updated_at;
                }
            })
            .or_insert_with(|| CandidateBranch {
                branch: mr.source_branch.clone(),
                latest_pipeline: None,
                mr: Some(mr),
                failed_runs_48h: 0,
                last_updated: mr.updated_at,
                score: 0.0,
                is_urgent: false,
                stack_root_iid: None,
                stacked_iids: Vec::new(),
            });
    }

    if by_branch.is_empty() {
        return Vec::new();
    }

    // 2. Index project activity events & user interactions for lifecycle gating & Hawkes excitation
    let mut closed_or_merged_iids: HashSet<u64> = HashSet::new();
    let mut branch_to_historical_iid: HashMap<&str, u64> = HashMap::new();
    let mut mr_excitation: HashMap<u64, (f64, bool, DateTime<Utc>)> = HashMap::new();
    let mut branch_excitation: HashMap<&str, (f64, bool, DateTime<Utc>)> = HashMap::new();

    let ev_half_life = ctx.config.event_half_life_hours.max(1.0);
    let lambda_event = std::f64::consts::LN_2 / ev_half_life;
    let cutoff_7d = now - Duration::days(7);

    for ev in ctx
        .activity_events
        .iter()
        .filter(|e| e.project_id == project.id)
    {
        if matches!(ev.kind, ActivityKind::MrMerged | ActivityKind::MrClosed) {
            if let Some(iid) = ev.mr_iid {
                closed_or_merged_iids.insert(iid);
            }
        }
        if let (Some(branch), Some(iid)) = (ev.branch.as_deref(), ev.mr_iid) {
            branch_to_historical_iid.entry(branch).or_insert(iid);
        }

        if ev.created_at < cutoff_7d {
            continue;
        }

        let age_hours = (now - ev.created_at).num_minutes().max(0) as f64 / 60.0;
        let base_w = match ev.kind {
            ActivityKind::Mention
            | ActivityKind::ReviewRequested
            | ActivityKind::Assigned
            | ActivityKind::MergeTrainRemoved => 25.0,
            ActivityKind::MrApproved => 12.0,
            ActivityKind::MrComment => 8.0,
            ActivityKind::MrOpened => 6.0,
            ActivityKind::BranchPush | ActivityKind::PipelineStatus => 3.0,
            ActivityKind::MrMerged | ActivityKind::MrClosed => 0.0,
        };
        if base_w <= 0.0 {
            continue;
        }
        let decayed = base_w * (-lambda_event * age_hours).exp();
        let is_recent_direct = ev.kind.is_direct_attention() && age_hours <= 72.0;

        if let Some(iid) = ev.mr_iid {
            let entry = mr_excitation
                .entry(iid)
                .or_insert((0.0, false, ev.created_at));
            entry.0 += decayed;
            entry.1 |= is_recent_direct;
            if ev.created_at > entry.2 {
                entry.2 = ev.created_at;
            }
        }
        if let Some(branch) = ev.branch.as_deref() {
            let entry = branch_excitation
                .entry(branch)
                .or_insert((0.0, false, ev.created_at));
            entry.0 += decayed;
            entry.1 |= is_recent_direct;
            if ev.created_at > entry.2 {
                entry.2 = ev.created_at;
            }
        }
    }

    // 2b. Index User Interaction Hawkes Excitation
    let mut user_mr_excitation: HashMap<u64, (f64, DateTime<Utc>)> = HashMap::new();
    let mut user_branch_excitation: HashMap<&str, (f64, DateTime<Utc>)> = HashMap::new();
    let mut user_project_excitation: f64 = 0.0;
    let inter_weight_mult = ctx.config.interaction_weight.max(0.0);
    let inter_half_life = ctx.config.interaction_half_life_hours.max(1.0);
    let lambda_inter = std::f64::consts::LN_2 / inter_half_life;

    if inter_weight_mult > 0.0 {
        for u in ctx
            .user_interactions
            .iter()
            .filter(|u| u.project_id == project.id && u.created_at >= cutoff_7d)
        {
            let age_hours = (now - u.created_at).num_minutes().max(0) as f64 / 60.0;
            let decayed = u.weight * inter_weight_mult * (-lambda_inter * age_hours).exp();
            user_project_excitation += decayed;

            if let Some(iid) = u.mr_iid {
                let entry = user_mr_excitation
                    .entry(iid)
                    .or_insert((0.0, u.created_at));
                entry.0 += decayed;
                if u.created_at > entry.1 {
                    entry.1 = u.created_at;
                }
            }
            if let Some(branch) = u.branch.as_deref() {
                let entry = user_branch_excitation
                    .entry(branch)
                    .or_insert((0.0, u.created_at));
                entry.0 += decayed;
                if u.created_at > entry.1 {
                    entry.1 = u.created_at;
                }
            }
        }
    }

    // 3. Score candidates and prune dead/merged branches
    let age_half_life = ctx.config.age_half_life_hours.max(1.0);
    let lambda_age = std::f64::consts::LN_2 / age_half_life;
    let mut all_candidates: Vec<CandidateBranch<'a>> = Vec::with_capacity(by_branch.len());
    let mut kept_candidates: Vec<CandidateBranch<'a>> = Vec::with_capacity(by_branch.len());

    for (_, mut cand) in by_branch {
        let is_default_branch = cand.branch == project.default_branch
            || cand.branch == "master"
            || cand.branch == "main";
        let is_active_pipeline = cand
            .latest_pipeline
            .is_some_and(|p| p.status.is_active() || p.has_active_jobs());

        // Lifecycle Gate: prune branches without an open MR that have already been merged/closed
        // or passed >6h ago.
        let mut pruned = false;
        if ctx.config.prune_merged_branches
            && !is_default_branch
            && cand.mr.is_none()
            && !is_active_pipeline
        {
            if let Some(p) = cand.latest_pipeline {
                let age_mins = (now - p.updated_at).num_minutes();
                let was_merged_or_closed = branch_to_historical_iid
                    .get(cand.branch.as_str())
                    .is_some_and(|iid| closed_or_merged_iids.contains(iid));

                if (was_merged_or_closed && age_mins > 15)
                    || (p.status != PipelineStatus::Failed && age_mins > 6 * 60)
                    || (p.status == PipelineStatus::Failed && age_mins > 48 * 60)
                {
                    pruned = true;
                }
            } else {
                pruned = true;
            }
        }

        // Compute Hawkes + state + user interaction urgency score
        let mut raw_score = (user_project_excitation * 0.15).min(8.0);
        let mut is_urgent = false;
        let is_recently_updated = (now - cand.last_updated).num_days() <= 7;

        if is_default_branch {
            raw_score += 22.0;
            is_urgent = true;
        }

        if let Some(p) = cand.latest_pipeline {
            if p.status.is_active() || p.has_active_jobs() {
                raw_score += 65.0;
                is_urgent = true;
            } else if p.status == PipelineStatus::Failed {
                raw_score += 42.0;
                let recent_fail = (now - p.updated_at).num_hours() <= 96;
                if is_default_branch || (cand.mr.is_some() && is_recently_updated) || recent_fail {
                    is_urgent = true;
                }
            } else if p.status == PipelineStatus::Manual {
                raw_score += 18.0;
            } else if p.status == PipelineStatus::Success {
                raw_score += 8.0;
            }
        }

        if cand.failed_runs_48h > 0 {
            raw_score += 4.0 * (cand.failed_runs_48h.min(5) as f64);
        }

        let mut user_item_boost = 0.0;
        let mut last_user_touch: Option<DateTime<Utc>> = None;

        if let Some(mr) = cand.mr {
            raw_score += 10.0;
            if mr.is_conflicted() {
                raw_score += 35.0;
                if is_recently_updated {
                    is_urgent = true;
                }
            } else if mr.has_unresolved_threads() {
                raw_score += 18.0;
            } else if mr.is_mergeable() && mr.upvotes > 0 {
                raw_score += 16.0;
            }

            if mr.upvotes > 0 {
                raw_score += 6.0 * (mr.upvotes.min(3) as f64);
            }
            if mr.user_notes_count > 0 {
                raw_score += 2.5 * (mr.user_notes_count.min(10) as f64);
            }
            if !current_username.is_empty()
                && mr.author_username.eq_ignore_ascii_case(current_username)
            {
                raw_score += 12.0;
            }
            if let Some(&(exc, direct, last_ev)) = mr_excitation.get(&mr.iid) {
                raw_score += exc.min(45.0);
                is_urgent |= direct;
                if last_ev > cand.last_updated {
                    cand.last_updated = last_ev;
                }
            }
            if let Some(&(u_exc, u_ts)) = user_mr_excitation.get(&mr.iid) {
                user_item_boost += u_exc;
                last_user_touch = Some(u_ts);
            }
            if mr.draft {
                raw_score *= 0.6;
            }
        }

        if let Some(&(exc, direct, last_ev)) = branch_excitation.get(cand.branch.as_str()) {
            raw_score += (exc * 0.6).min(25.0);
            is_urgent |= direct;
            if last_ev > cand.last_updated {
                cand.last_updated = last_ev;
            }
        }
        if let Some(&(u_exc, u_ts)) = user_branch_excitation.get(cand.branch.as_str()) {
            user_item_boost += u_exc * 0.8;
            if last_user_touch.is_none_or(|prev| u_ts > prev) {
                last_user_touch = Some(u_ts);
            }
        }

        if user_item_boost > 0.0 {
            raw_score += user_item_boost.min(55.0);
            if let Some(u_ts) = last_user_touch {
                if u_ts > cand.last_updated {
                    cand.last_updated = u_ts;
                }
                if user_item_boost >= 15.0 && (now - u_ts).num_hours() <= 12 {
                    is_urgent = true;
                }
            }
        }

        if let Some(commit) = cand.latest_pipeline.and_then(|p| p.commit.as_ref()) {
            if !current_username.is_empty()
                && commit
                    .author_name
                    .to_lowercase()
                    .contains(&current_username.to_lowercase())
            {
                raw_score += 6.0;
            }
        }

        let age_hours = (now - cand.last_updated).num_minutes().max(0) as f64 / 60.0;
        let decay = if is_active_pipeline {
            1.0
        } else {
            (-lambda_age * age_hours).exp().max(0.02)
        };

        cand.score = raw_score * decay;
        if !is_default_branch && !is_active_pipeline && cand.score < 8.0 {
            is_urgent = false;
        }
        cand.is_urgent = is_urgent;

        all_candidates.push(cand.clone());
        if !pruned {
            kept_candidates.push(cand);
        }
    }

    if kept_candidates.is_empty() {
        all_candidates.sort_by(|a, b| b.last_updated.cmp(&a.last_updated));
        if let Some(fallback) = all_candidates.into_iter().next() {
            kept_candidates.push(fallback);
        }
    }

    // 4. Stacked MR Chain Compaction
    let mut compacted = if ctx.config.compact_stacked_mrs {
        let mr_by_source: HashMap<&str, &MergeRequest> = project
            .merge_requests
            .iter()
            .map(|mr| (mr.source_branch.as_str(), mr))
            .collect();

        for cand in &mut kept_candidates {
            if let Some(mr) = cand.mr {
                let mut curr = mr;
                let mut visited = HashSet::new();
                visited.insert(curr.iid);
                while curr.target_branch != project.default_branch
                    && curr.target_branch != "master"
                    && curr.target_branch != "main"
                {
                    if let Some(&parent) = mr_by_source.get(curr.target_branch.as_str()) {
                        if !visited.insert(parent.iid) {
                            break;
                        }
                        curr = parent;
                    } else {
                        break;
                    }
                }
                cand.stack_root_iid = Some(curr.iid);
            }
        }
        compact_stacked_candidates(kept_candidates, &project.default_branch)
    } else {
        kept_candidates
    };

    // 5. Select up to `max_rows` according to `ctx.ranking_mode`
    let tau = ctx.config.stochastic_temperature.clamp(0.15, 3.0);
    let bucket_seed = ctx.bucket_seed();

    let selected: Vec<CandidateBranch<'a>> = match ctx.ranking_mode {
        RankingMode::Stochastic => {
            // 100% Stochastic Plackett-Luce / Gumbel-Top-k sampling across all candidates!
            compacted.sort_by(|a, b| {
                let ga = a.score.max(0.1).ln()
                    + tau * seeded_gumbel(bucket_seed, project.id, a.branch.as_str());
                let gb = b.score.max(0.1).ln()
                    + tau * seeded_gumbel(bucket_seed, project.id, b.branch.as_str());
                gb.partial_cmp(&ga)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| b.last_updated.cmp(&a.last_updated))
            });
            compacted.into_iter().take(max_rows).collect()
        },
        RankingMode::Deterministic => {
            let is_def = |b: &str| {
                b == project.default_branch.as_str() || b == "master" || b == "main"
            };
            compacted.sort_by(|a, b| {
                let a_active = a
                    .latest_pipeline
                    .is_some_and(|p| p.status.is_active() || p.has_active_jobs());
                let b_active = b
                    .latest_pipeline
                    .is_some_and(|p| p.status.is_active() || p.has_active_jobs());
                b_active
                    .cmp(&a_active)
                    .then_with(|| b.is_urgent.cmp(&a.is_urgent))
                    .then_with(|| is_def(&b.branch).cmp(&is_def(&a.branch)))
                    .then_with(|| {
                        b.score
                            .partial_cmp(&a.score)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .then_with(|| b.last_updated.cmp(&a.last_updated))
            });
            compacted.into_iter().take(max_rows).collect()
        },
        RankingMode::Hybrid => {
            let mut urgent: Vec<CandidateBranch<'a>> = Vec::new();
            let mut pool: Vec<CandidateBranch<'a>> = Vec::new();

            for cand in compacted {
                if cand.is_urgent {
                    urgent.push(cand);
                } else {
                    pool.push(cand);
                }
            }

            let is_def = |b: &str| {
                b == project.default_branch.as_str() || b == "master" || b == "main"
            };

            urgent.sort_by(|a, b| {
                let a_active = a
                    .latest_pipeline
                    .is_some_and(|p| p.status.is_active() || p.has_active_jobs());
                let b_active = b
                    .latest_pipeline
                    .is_some_and(|p| p.status.is_active() || p.has_active_jobs());
                b_active
                    .cmp(&a_active)
                    .then_with(|| is_def(&b.branch).cmp(&is_def(&a.branch)))
                    .then_with(|| {
                        b.score
                            .partial_cmp(&a.score)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .then_with(|| b.last_updated.cmp(&a.last_updated))
            });

            let mut sel: Vec<CandidateBranch<'a>> = Vec::with_capacity(max_rows);
            for u in urgent.into_iter().take(max_rows) {
                sel.push(u);
            }

            if sel.len() < max_rows && !pool.is_empty() {
                let rem = max_rows - sel.len();
                pool.sort_by(|a, b| {
                    b.score
                        .partial_cmp(&a.score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| b.last_updated.cmp(&a.last_updated))
                });

                if pool.len() <= rem {
                    sel.extend(pool);
                } else {
                    let ratio = ctx.config.stochastic_ratio.clamp(0.0, 1.0);
                    let stochastic_slots = ((rem as f64) * ratio)
                        .round()
                        .max(if ratio > 0.0 { 1.0 } else { 0.0 })
                        as usize;
                    let stochastic_slots = stochastic_slots.min(rem);
                    let det_slots = rem.saturating_sub(stochastic_slots);
                    let mut iter = pool.into_iter();
                    for _ in 0..det_slots {
                        if let Some(c) = iter.next() {
                            sel.push(c);
                        }
                    }
                    let mut remaining_pool: Vec<CandidateBranch<'a>> = iter.collect();
                    remaining_pool.sort_by(|a, b| {
                        let ga = a.score.max(0.1).ln()
                            + tau * seeded_gumbel(bucket_seed, project.id, a.branch.as_str());
                        let gb = b.score.max(0.1).ln()
                            + tau * seeded_gumbel(bucket_seed, project.id, b.branch.as_str());
                        gb.partial_cmp(&ga)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                    sel.extend(remaining_pool.into_iter().take(stochastic_slots));
                }
            }
            sel
        },
    };

    selected
        .into_iter()
        .filter_map(|cand| {
            let target = if let Some(mr) = cand.mr {
                RowEntryTarget::MergeRequest(mr.iid)
            } else if let Some(p) = cand.latest_pipeline {
                RowEntryTarget::Pipeline(p.id)
            } else {
                return None;
            };
            Some(RankedBranchEntry {
                branch: cand.branch,
                pipeline: cand.latest_pipeline,
                mr: cand.mr,
                stacked_iids: cand.stacked_iids,
                score: cand.score,
                is_urgent: cand.is_urgent,
                target,
            })
        })
        .collect()
}

/// Compacts stacked MR chains (`mr.target_branch` -> `parent_mr.source_branch`) so a 4- or 6-part
/// stack with homogeneous status doesn't monopolize 4-6 separate lines.
fn compact_stacked_candidates<'a>(
    candidates: Vec<CandidateBranch<'a>>,
    default_branch: &str,
) -> Vec<CandidateBranch<'a>> {
    let mut stack_counts: HashMap<u64, usize> = HashMap::new();
    for c in &candidates {
        if let Some(root_iid) = c.stack_root_iid {
            *stack_counts.entry(root_iid).or_insert(0) += 1;
        }
    }

    let mut stack_groups: HashMap<u64, Vec<CandidateBranch<'a>>> = HashMap::new();
    let mut output: Vec<CandidateBranch<'a>> = Vec::with_capacity(candidates.len());

    for c in candidates {
        if let Some(root_iid) = c.stack_root_iid {
            if stack_counts.get(&root_iid).copied().unwrap_or(0) >= 2 {
                stack_groups.entry(root_iid).or_default().push(c);
                continue;
            }
        }
        output.push(c);
    }

    for (_root_iid, mut members) in stack_groups {
        // Sort stack members by IID ascending (stack bottom -> top)
        members.sort_by_key(|m| m.mr.map(|mr| mr.iid).unwrap_or(u64::MAX));

        // Keep any member that is actively running, conflicted, or has comments/upvotes
        // plus the lead/root member of the stack.
        let mut primary_idx = members
            .iter()
            .position(|m| {
                m.mr.is_some_and(|mr| {
                    mr.target_branch == default_branch
                        || mr.target_branch == "master"
                        || mr.target_branch == "main"
                })
            })
            .unwrap_or(0);

        // If the primary member passed/has no pipeline, but a child in the stack failed, promote
        // the first failing or highest-scoring member as the primary representative.
        if let Some(fail_idx) = members.iter().position(|m| {
            m.latest_pipeline
                .is_some_and(|p| p.status == PipelineStatus::Failed || p.status.is_active())
        }) {
            primary_idx = fail_idx;
        }

        let mut folded_iids: Vec<u64> = Vec::new();
        let mut standalone: Vec<CandidateBranch<'a>> = Vec::new();
        let mut primary = members[primary_idx].clone();
        let primary_failed = primary
            .latest_pipeline
            .is_some_and(|p| p.status == PipelineStatus::Failed);

        for (idx, m) in members.into_iter().enumerate() {
            if idx == primary_idx {
                continue;
            }
            let m_failed = m
                .latest_pipeline
                .is_some_and(|p| p.status == PipelineStatus::Failed);
            let is_distinctly_active = m
                .latest_pipeline
                .is_some_and(|p| p.status.is_active() || p.has_active_jobs())
                || (m_failed && !primary_failed)
                || m.mr.is_some_and(|mr| {
                    mr.is_conflicted() || mr.user_notes_count > 2 || mr.upvotes > 0
                });

            if is_distinctly_active {
                standalone.push(m);
            } else if let Some(mr) = m.mr {
                primary.score = primary.score.max(m.score);
                primary.is_urgent |= m.is_urgent;
                folded_iids.push(mr.iid);
            }
        }

        folded_iids.sort_unstable();
        primary.stacked_iids = folded_iids;
        output.push(primary);
        output.extend(standalone);
    }

    output
}

fn legacy_project_entries<'a>(
    project: &'a Project,
    max_rows: usize,
    include_mr_pipelines: bool,
) -> Vec<RankedBranchEntry<'a>> {
    let distinct_by_branch = project.first_pipeline_per_branch(
        max_rows,
        include_mr_pipelines,
        |p| p.status.is_active(),
    );

    let mut entries: Vec<RankedBranchEntry<'a>> = distinct_by_branch
        .iter()
        .map(|&p| {
            let mr = project.merge_request_for_branch(&p.branch);
            let branch = mr
                .map(|m| m.source_branch.clone())
                .unwrap_or_else(|| p.branch.clone());
            let target = if let Some(mr) = mr {
                RowEntryTarget::MergeRequest(mr.iid)
            } else {
                RowEntryTarget::Pipeline(p.id)
            };
            RankedBranchEntry {
                branch,
                pipeline: Some(p),
                mr,
                stacked_iids: Vec::new(),
                score: 1.0,
                is_urgent: p.status.is_active(),
                target,
            }
        })
        .collect();

    if entries.len() < max_rows && !project.merge_requests.is_empty() {
        for mr in &project.merge_requests {
            if entries.len() >= max_rows {
                break;
            }
            let already_shown = entries.iter().any(|e| e.branch == mr.source_branch);
            if !already_shown {
                entries.push(RankedBranchEntry {
                    branch: mr.source_branch.clone(),
                    pipeline: None,
                    mr: Some(mr),
                    stacked_iids: Vec::new(),
                    score: 1.0,
                    is_urgent: false,
                    target: RowEntryTarget::MergeRequest(mr.iid),
                });
            }
        }
    }

    entries
}

/// Computes a project's overall relevance heat $H(P, t)$ and whether the project should be
/// shown under soft `include_projects` auto-wakeup rules.
pub fn compute_project_heat(
    project: &Project,
    ctx: &RelevanceContext<'_>,
) -> (f64, bool) {
    let config = ctx.config;
    let now = ctx.now;
    let path_lower = project.path.to_lowercase();

    // 1. Hard blocklist check
    if let Some(excludes) = &config.exclude_projects {
        if excludes
            .iter()
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .any(|term| path_lower.contains(&term))
        {
            return (0.0, false);
        }
    }

    // 2. Check if project is in `include_projects` (pinned core repos)
    let include_terms: Vec<String> = config
        .include_projects
        .as_ref()
        .map(|list| {
            list.iter()
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();

    let is_pinned = include_terms.is_empty()
        || include_terms
            .iter()
            .any(|term| path_lower.contains(term.as_str()));

    if !config.dynamic_relevance {
        return (if is_pinned { 10.0 } else { 0.0 }, is_pinned);
    }

    let entries = rank_project_entries(project, ctx, 8);

    let branch_heat: f64 = entries.iter().take(6).map(|e| e.score).sum();

    let ev_hl = config.event_half_life_hours.clamp(1.0, 720.0);
    let lambda_proj_ev = std::f64::consts::LN_2 / (ev_hl * 1.25);
    let cutoff_7d = now - Duration::days(7);
    let mut event_heat = 0.0;
    let mut has_recent_direct_attention = false;

    for ev in ctx
        .activity_events
        .iter()
        .filter(|e| e.project_id == project.id && e.created_at >= cutoff_7d)
    {
        let age_hours = (now - ev.created_at).num_minutes().max(0) as f64 / 60.0;
        let w = if ev.kind.is_direct_attention() {
            has_recent_direct_attention = true;
            30.0
        } else {
            match ev.kind {
                ActivityKind::MrOpened | ActivityKind::MrApproved | ActivityKind::MrComment => 6.0,
                ActivityKind::BranchPush | ActivityKind::PipelineStatus => 2.5,
                ActivityKind::MrMerged | ActivityKind::MrClosed => 2.0,
                _ => 2.0,
            }
        };
        event_heat += w * (-lambda_proj_ev * age_hours).exp();
    }

    // User interaction Hawkes excitation for project heat
    let ix_hl = config.interaction_half_life_hours.clamp(1.0, 720.0);
    let lambda_ix = std::f64::consts::LN_2 / ix_hl;
    let ix_scale = config.interaction_weight.clamp(0.0, 5.0);
    let mut interaction_heat = 0.0;
    if ix_scale > 0.0 {
        for ix in ctx
            .user_interactions
            .iter()
            .filter(|i| i.project_id == project.id && i.created_at >= cutoff_7d)
        {
            let age_hours = (now - ix.created_at).num_minutes().max(0) as f64 / 60.0;
            interaction_heat += ix_scale * ix.weight * (-lambda_ix * age_hours).exp();
        }
    }

    let has_recent_open_mr = project
        .merge_requests
        .iter()
        .any(|mr| (now - mr.updated_at).num_days() <= 7);
    let has_recent_pipeline = project.pipelines.as_ref().is_some_and(|ps| {
        ps.iter()
            .any(|p| p.status.is_active() || (now - p.updated_at).num_hours() <= 96)
    });
    let active_within_7d =
        (now - project.last_activity_at).num_days() <= 7 || interaction_heat >= 5.0;

    let auto_wake = active_within_7d
        && (has_recent_direct_attention
            || has_recent_open_mr
            || has_recent_pipeline
            || event_heat >= 8.0
            || interaction_heat >= 6.0);

    let should_show = is_pinned || auto_wake;
    let pinned_bonus = if is_pinned { 15.0 } else { 0.0 };
    let total_heat =
        pinned_bonus + branch_heat + event_heat.min(80.0) + interaction_heat.min(60.0);

    (total_heat, should_show)
}

/// Computes the dynamic vertical split (`projects_height`, `Option<feed_height>`) and
/// per-project pipeline row budgets (`HashMap<ProjectId, usize>`).
pub fn compute_dynamic_split_and_budgets(
    projects: &[Project],
    selected_project_id: Option<ProjectId>,
    area_height: u16,
    show_activity_feed: bool,
    activity_feed_focused: bool,
    filter_input_active: bool,
    ctx: &RelevanceContext<'_>,
) -> (u16, Option<u16>, HashMap<ProjectId, usize>) {
    let config = ctx.config;
    let activity_events = ctx.activity_events;
    let now = ctx.now;
    let mut budgets: HashMap<ProjectId, usize> = HashMap::new();
    let filter_h: u16 = if filter_input_active { 3 } else { 0 };

    if projects.is_empty() {
        let projects_h = if show_activity_feed && area_height > 10 {
            7 + filter_h
        } else {
            area_height
        };
        let feed_h = if show_activity_feed && area_height > projects_h {
            Some(area_height - projects_h)
        } else {
            None
        };
        return (projects_h, feed_h, budgets);
    }

    if !config.dynamic_relevance && !config.dynamic_split {
        for p in projects {
            budgets.insert(p.id, config.max_pipelines_per_project.max(1));
        }
        let show_feed = show_activity_feed && config.activity_feed_lines > 0;
        if show_feed && area_height > 10 {
            let min_feed_h = (config.activity_feed_lines + 2).min(area_height.saturating_sub(6));
            let content_rows: u16 = projects
                .iter()
                .map(|proj| {
                    proj.row_targets_and_height(
                        config.max_pipelines_per_project,
                        config.include_mr_pipelines,
                    )
                    .1
                })
                .sum();
            let needed_projects_h = content_rows.max(4) + 3 + filter_h;
            let max_projects_h = area_height.saturating_sub(min_feed_h).max(6);
            let projects_h = needed_projects_h.min(max_projects_h);
            return (projects_h, Some(area_height.saturating_sub(projects_h)), budgets);
        }
        return (area_height, None, budgets);
    }

    let show_feed = show_activity_feed && config.activity_feed_lines > 0 && area_height > 12;

    // 1. Determine max height available for the Projects table based on focus state
    let max_projects_h = if !show_feed {
        area_height
    } else if config.dynamic_split {
        if activity_feed_focused {
            // Event Log Focused: compress CI table to ~30% header so Activity Feed gets ~70%
            let min_header = (projects.len() as u16 * 3 + 2 + filter_h).min(area_height / 2);
            ((area_height * 30) / 100)
                .max(min_header)
                .clamp(7, area_height.saturating_sub(8))
        } else {
            // CI Focused: give CI table up to ~78-82% height and keep a compact 6-8 line ticker
            let has_urgent_mention = activity_events.iter().take(15).any(|e| {
                e.kind.is_direct_attention() && (now - e.created_at).num_hours() <= 12
            });
            let compact_ticker_h = if has_urgent_mention { 8 } else { 6 };
            let target_feed_h = compact_ticker_h.min(config.activity_feed_lines + 2);
            area_height.saturating_sub(target_feed_h).max(8)
        }
    } else {
        let min_feed_h = (config.activity_feed_lines + 2).min(area_height.saturating_sub(6));
        area_height.saturating_sub(min_feed_h).max(6)
    };

    let available_table_rows = max_projects_h.saturating_sub(2 + filter_h).max(3) as usize;

    // 2. Compute each project's natural candidate count and heat
    struct ProjAlloc {
        id: ProjectId,
        natural_count: usize,
        min_row_h: usize,
        weight: f64,
        allocated: usize,
    }

    let max_cap_per_proj = if activity_feed_focused && config.dynamic_split {
        3
    } else {
        (config.max_pipelines_per_project + 4).max(6)
    };

    let ranking_mode = ctx.ranking_mode;
    let tau = config.stochastic_temperature.clamp(0.05, 5.0);

    let mut allocs: Vec<ProjAlloc> = projects
        .iter()
        .map(|p| {
            let candidates = rank_project_entries(p, ctx, max_cap_per_proj);
            let (heat, _) = compute_project_heat(p, ctx);
            let natural_count = candidates.len().max(1);
            // Project cell has 3 lines if `open MRs` line is shown, otherwise 2 lines
            let min_row_h = if activity_feed_focused && config.dynamic_split {
                2
            } else {
                3
            };
            let base_alloc = min_row_h.min(natural_count).max(1);
            let sel_boost = if selected_project_id == Some(p.id) && !activity_feed_focused {
                1.25
            } else {
                1.0
            };
            let stochastic_factor = if ranking_mode == RankingMode::Stochastic {
                (0.5 * tau * project_gumbel_noise(ctx, p.id)).exp()
            } else {
                1.0
            };
            let weight = heat.max(1.0).powf(0.65) * sel_boost * stochastic_factor;
            ProjAlloc {
                id: p.id,
                natural_count,
                min_row_h,
                weight,
                allocated: base_alloc,
            }
        })
        .collect();

    let used_rows: usize = allocs
        .iter()
        .map(|a| a.allocated.max(a.min_row_h))
        .sum();
    let mut rem_rows = available_table_rows.saturating_sub(used_rows);

    // 3. Distribute remaining vertical rows proportionally to project heat
    while rem_rows > 0 {
        let total_weight: f64 = allocs
            .iter()
            .filter(|a| a.allocated < a.natural_count)
            .map(|a| a.weight)
            .sum();

        if total_weight <= 0.0 {
            break;
        }

        // Pick the project with the highest unfulfilled weighted share
        let Some(best_idx) = allocs
            .iter()
            .enumerate()
            .filter(|(_, a)| a.allocated < a.natural_count)
            .max_by(|(_, a), (_, b)| {
                let ratio_a = a.weight / (a.allocated as f64);
                let ratio_b = b.weight / (b.allocated as f64);
                ratio_a
                    .partial_cmp(&ratio_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(idx, _)| idx)
        else {
            break;
        };

        let prev_h = allocs[best_idx].allocated.max(allocs[best_idx].min_row_h);
        allocs[best_idx].allocated += 1;
        let new_h = allocs[best_idx].allocated.max(allocs[best_idx].min_row_h);
        if new_h > prev_h {
            rem_rows = rem_rows.saturating_sub(new_h - prev_h);
        }
    }

    let mut total_content_rows: u16 = 0;
    for a in allocs {
        let row_h = a.allocated.max(a.min_row_h) as u16;
        total_content_rows += row_h;
        budgets.insert(a.id, a.allocated.max(1));
    }

    if show_feed {
        let needed_projects_h = total_content_rows.max(4) + 2 + filter_h;
        let projects_h = needed_projects_h.min(max_projects_h);
        let feed_h = area_height.saturating_sub(projects_h);
        (projects_h, Some(feed_h), budgets)
    } else {
        (area_height, None, budgets)
    }
}

/// Ranks all active CI processes and open MRs across all `projects` as a global flat stream
/// of first-class rows (ordered by active -> urgent failures/conflicts -> relevance + Gumbel-Top-k exploration).
pub fn rank_global_ci_runs<'a>(
    projects: &'a [Project],
    ctx: &RelevanceContext<'_>,
    max_rows: usize,
) -> Vec<RankedCiProcessEntry<'a>> {
    let max_rows = max_rows.max(1);
    let mut all_entries: Vec<RankedCiProcessEntry<'a>> = Vec::new();

    for project in projects {
        let entries = rank_project_entries(project, ctx, 20);
        for entry in entries {
            let p_ts = entry.pipeline.map(|p| p.updated_at);
            let mr_ts = entry.mr.map(|m| m.updated_at);
            let updated_at = match (p_ts, mr_ts) {
                (Some(a), Some(b)) => a.max(b),
                (Some(a), None) => a,
                (None, Some(b)) => b,
                (None, None) => project.last_activity_at,
            };
            all_entries.push(RankedCiProcessEntry {
                project,
                entry,
                updated_at,
            });
        }
    }

    if all_entries.is_empty() {
        return Vec::new();
    }

    if !ctx.config.dynamic_relevance {
        all_entries.sort_by(|a, b| {
            let a_active = a
                .entry
                .pipeline
                .is_some_and(|p| p.status.is_active() || p.has_active_jobs());
            let b_active = b
                .entry
                .pipeline
                .is_some_and(|p| p.status.is_active() || p.has_active_jobs());
            b_active
                .cmp(&a_active)
                .then_with(|| b.updated_at.cmp(&a.updated_at))
        });
        all_entries.truncate(max_rows);
        return all_entries;
    }

    let ranking_mode = ctx.ranking_mode;
    let tau = ctx.config.stochastic_temperature.clamp(0.05, 5.0);
    let bucket = ctx.bucket_seed();

    // 100% Stochastic Plackett-Luce / Gumbel-Top-k across all global CI runs
    if ranking_mode == RankingMode::Stochastic {
        all_entries.sort_by(|a, b| {
            let ga = a.entry.score.max(0.1).ln()
                + tau * seeded_gumbel(bucket, a.project.id, a.entry.branch.as_str());
            let gb = b.entry.score.max(0.1).ln()
                + tau * seeded_gumbel(bucket, b.project.id, b.entry.branch.as_str());
            gb.partial_cmp(&ga)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.updated_at.cmp(&a.updated_at))
        });
        all_entries.truncate(max_rows);
        return all_entries;
    }

    let mut urgent: Vec<RankedCiProcessEntry<'a>> = Vec::new();
    let mut pool: Vec<RankedCiProcessEntry<'a>> = Vec::new();

    for item in all_entries {
        if item.entry.is_urgent {
            urgent.push(item);
        } else {
            pool.push(item);
        }
    }

    urgent.sort_by(|a, b| {
        let a_active = a
            .entry
            .pipeline
            .is_some_and(|p| p.status.is_active() || p.has_active_jobs());
        let b_active = b
            .entry
            .pipeline
            .is_some_and(|p| p.status.is_active() || p.has_active_jobs());
        let a_broken = a
            .entry
            .pipeline
            .is_some_and(|p| p.status == PipelineStatus::Failed)
            || a.entry.mr.is_some_and(|m| m.is_conflicted());
        let b_broken = b
            .entry
            .pipeline
            .is_some_and(|p| p.status == PipelineStatus::Failed)
            || b.entry.mr.is_some_and(|m| m.is_conflicted());

        b_active
            .cmp(&a_active)
            .then_with(|| b_broken.cmp(&a_broken))
            .then_with(|| {
                b.entry
                    .score
                    .partial_cmp(&a.entry.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| b.updated_at.cmp(&a.updated_at))
    });

    let mut selected: Vec<RankedCiProcessEntry<'a>> = Vec::with_capacity(max_rows);
    for u in urgent.into_iter().take(max_rows) {
        selected.push(u);
    }

    if selected.len() < max_rows && !pool.is_empty() {
        let rem = max_rows - selected.len();
        pool.sort_by(|a, b| {
            b.entry
                .score
                .partial_cmp(&a.entry.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.updated_at.cmp(&a.updated_at))
        });

        let ratio = if ranking_mode == RankingMode::Deterministic {
            0.0
        } else {
            ctx.config.stochastic_ratio.clamp(0.0, 1.0)
        };

        if pool.len() <= rem || ratio <= 0.0 {
            selected.extend(pool.into_iter().take(rem));
        } else {
            let stochastic_slots = ((rem as f64) * ratio).round() as usize;
            let stochastic_slots = stochastic_slots.clamp(1, rem);
            let det_slots = rem.saturating_sub(stochastic_slots);
            let mut iter = pool.into_iter();
            for _ in 0..det_slots {
                if let Some(c) = iter.next() {
                    selected.push(c);
                }
            }
            let mut remaining_pool: Vec<RankedCiProcessEntry<'a>> = iter.collect();
            remaining_pool.sort_by(|a, b| {
                let ga = a.entry.score.max(0.1).ln()
                    + tau * seeded_gumbel(bucket, a.project.id, a.entry.branch.as_str());
                let gb = b.entry.score.max(0.1).ln()
                    + tau * seeded_gumbel(bucket, b.project.id, b.entry.branch.as_str());
                gb.partial_cmp(&ga)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            selected.extend(remaining_pool.into_iter().take(stochastic_slots));
        }
    }

    selected
}

/// Computes the vertical split (`projects_height`, `Option<feed_height>`) and row limit for the flat CI processes view.
pub fn compute_flat_ci_split(
    total_processes: usize,
    area_height: u16,
    show_activity_feed: bool,
    activity_feed_focused: bool,
    filter_input_active: bool,
    config: &GlimConfig,
    activity_events: &[ProjectActivityEvent],
    now: DateTime<Utc>,
) -> (u16, Option<u16>) {
    let filter_h: u16 = if filter_input_active { 3 } else { 0 };
    let show_feed = show_activity_feed && config.activity_feed_lines > 0 && area_height > 12;

    if !show_feed {
        return (area_height, None);
    }

    let max_projects_h = if config.dynamic_split {
        if activity_feed_focused {
            // Event feed focused: compress flat CI processes to top ~25-30% summary rows
            ((area_height * 28) / 100)
                .max(6 + filter_h)
                .clamp(6 + filter_h, area_height.saturating_sub(8))
        } else {
            // CI processes focused: allocate up to ~75-80% to CI processes and keep a live event ticker
            let has_urgent_mention = activity_events.iter().take(15).any(|e| {
                e.kind.is_direct_attention() && (now - e.created_at).num_hours() <= 12
            });
            let compact_ticker_h = if has_urgent_mention { 9 } else { 7 };
            let target_feed_h = compact_ticker_h.min(config.activity_feed_lines + 3);
            area_height.saturating_sub(target_feed_h).max(8)
        }
    } else {
        let min_feed_h = (config.activity_feed_lines + 2).min(area_height.saturating_sub(6));
        area_height.saturating_sub(min_feed_h).max(6)
    };

    let needed_h = (total_processes.max(2) as u16) + 2 + filter_h;
    let projects_h = needed_h.min(max_projects_h);
    let feed_h = area_height.saturating_sub(projects_h);
    (projects_h, Some(feed_h))
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;
    use crate::{
        domain::{
            MergeRequestDto, PipelineDto, PipelineSource, ProjectDto, StatisticsDto,
            UserInteraction, UserInteractionKind,
        },
        id::PipelineId,
    };

    fn make_project(id: u32, path: &str) -> Project {
        Project::from(ProjectDto {
            id: ProjectId::new(id),
            path_with_namespace: path.into(),
            description: None,
            default_branch: "master".into(),
            ssh_url_to_repo: "git@gitlab.nvestiv.com:test.git".into(),
            web_url: format!("https://gitlab.nvestiv.com/{path}").into(),
            last_activity_at: Utc::now(),
            statistics: StatisticsDto::default(),
        })
    }

    fn make_pipeline(
        id: u32,
        project_id: u32,
        branch: &str,
        status: PipelineStatus,
        source: PipelineSource,
        updated_at: DateTime<Utc>,
    ) -> Pipeline {
        Pipeline::from(PipelineDto {
            id: PipelineId::new(id),
            project_id: ProjectId::new(project_id),
            status,
            source,
            branch: branch.into(),
            web_url: "https://gitlab.nvestiv.com/p".into(),
            created_at: updated_at - TimeDelta::minutes(5),
            updated_at,
        })
    }

    fn make_mr(
        iid: u64,
        project_id: u32,
        source_branch: &str,
        target_branch: &str,
        author: &str,
        updated_at: DateTime<Utc>,
    ) -> MergeRequest {
        MergeRequest::from(MergeRequestDto {
            id: 1000 + iid,
            iid,
            project_id: ProjectId::new(project_id),
            title: format!("MR !{iid}").into(),
            description: None,
            state: "opened".into(),
            source_branch: source_branch.into(),
            target_branch: target_branch.into(),
            draft: false,
            detailed_merge_status: Some("mergeable".into()),
            merge_status: None,
            has_conflicts: false,
            blocking_discussions_resolved: true,
            user_notes_count: 0,
            upvotes: 0,
            author: crate::domain::MergeRequestAuthorDto {
                username: author.into(),
            },
            updated_at,
            web_url: format!("https://gitlab.nvestiv.com/mr/{iid}").into(),
        })
    }

    #[test]
    fn test_parse_mr_ref_iid() {
        assert_eq!(parse_mr_ref_iid("refs/merge-requests/1/head"), Some(1));
        assert_eq!(parse_mr_ref_iid("refs/merge-requests/99/merge"), Some(99));
        assert_eq!(parse_mr_ref_iid("feat/my-branch"), None);
    }

    #[test]
    fn test_unifies_detached_mr_ref_and_prunes_merged_branches() {
        let now = Utc::now();
        let mut proj = make_project(35, "nvestiv/website");
        let mr1 = make_mr(
            1,
            35,
            "build/zola-migration",
            "master",
            "paul",
            now - TimeDelta::hours(2),
        );
        proj.update_merge_requests(vec![mr1]);

        let p_push_failed = make_pipeline(
            101,
            35,
            "build/zola-migration",
            PipelineStatus::Failed,
            PipelineSource::Push,
            now - TimeDelta::hours(5),
        );
        let p_mr_head_success = make_pipeline(
            102,
            35,
            "refs/merge-requests/1/head",
            PipelineStatus::Success,
            PipelineSource::MergeRequestEvent,
            now - TimeDelta::hours(1),
        );
        let p_old_merged_branch = make_pipeline(
            100,
            35,
            "chore/already-merged",
            PipelineStatus::Success,
            PipelineSource::Push,
            now - TimeDelta::hours(12),
        );
        proj.update_pipelines(vec![p_push_failed, p_mr_head_success, p_old_merged_branch]);

        let config = GlimConfig::default();
        let ctx = RelevanceContext::new(&config, &[], &[], "paul", now);
        let entries = rank_project_entries(&proj, &ctx, 8);
        // `refs/merge-requests/1/head` and `build/zola-migration` should be unified into a single entry
        // showing the newer `Success` pipeline, and `chore/already-merged` (>6h old with no open MR) is pruned!
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].branch, "build/zola-migration");
        assert_eq!(entries[0].pipeline.unwrap().id, PipelineId::new(102));
        assert_eq!(entries[0].mr.unwrap().iid, 1);
    }

    #[test]
    fn test_compacts_stacked_mr_chains() {
        let now = Utc::now();
        let mut proj = make_project(24, "caasa/connect");
        let mrs = vec![
            make_mr(70, 24, "caa-335/1-base", "master", "paul", now - TimeDelta::days(3)),
            make_mr(71, 24, "caa-335/2-lex", "caa-335/1-base", "paul", now - TimeDelta::days(3)),
            make_mr(72, 24, "caa-335/3-mdm", "caa-335/2-lex", "paul", now - TimeDelta::days(3)),
            make_mr(73, 24, "caa-335/4-sync", "caa-335/3-mdm", "paul", now - TimeDelta::days(3)),
        ];
        proj.update_merge_requests(mrs);

        let config = GlimConfig::default();
        let ctx = RelevanceContext::new(&config, &[], &[], "paul", now);
        let entries = rank_project_entries(&proj, &ctx, 8);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].branch, "caa-335/1-base");
        assert_eq!(entries[0].stacked_iids, vec![71, 72, 73]);
        assert_eq!(
            entries[0].stacked_badge().as_deref(),
            Some("(+3 stacked: !71..!73)")
        );
    }

    #[test]
    fn test_dynamic_split_expands_ci_or_feed_based_on_focus() {
        let now = Utc::now();
        let mut p1 = make_project(24, "caasa/connect");
        p1.update_merge_requests(vec![
            make_mr(87, 24, "feat/a", "master", "paul", now),
            make_mr(88, 24, "feat/b", "master", "paul", now),
            make_mr(89, 24, "feat/c", "master", "paul", now),
            make_mr(90, 24, "feat/d", "master", "paul", now),
            make_mr(91, 24, "feat/e", "master", "paul", now),
        ]);
        let p2 = make_project(4, "nvestiv/iris-ex");
        let projects = vec![p1, p2];
        let config = GlimConfig::default();
        let ctx = RelevanceContext::new(&config, &[], &[], "paul", now);

        let (ci_proj_h, ci_feed_h, ci_budgets) = compute_dynamic_split_and_budgets(
            &projects,
            Some(ProjectId::new(24)),
            40,
            true,
            false, // CI focused
            false,
            &ctx,
        );

        let (ev_proj_h, ev_feed_h, ev_budgets) = compute_dynamic_split_and_budgets(
            &projects,
            Some(ProjectId::new(24)),
            40,
            true,
            true, // Event feed focused
            false,
            &ctx,
        );

        // When Event Feed is focused, feed height expands significantly and CI project row budget compresses
        assert!(ev_feed_h.unwrap() > ci_feed_h.unwrap());
        assert!(ci_proj_h > ev_proj_h);
        assert!(ci_budgets[&ProjectId::new(24)] > ev_budgets[&ProjectId::new(24)]);
    }

    #[test]
    fn test_rank_global_ci_runs_orders_across_repos() {
        let now = Utc::now();
        let mut p1 = make_project(24, "caasa/connect");
        p1.update_pipelines(vec![make_pipeline(
            1,
            24,
            "master",
            PipelineStatus::Success,
            PipelineSource::Push,
            now - TimeDelta::hours(2),
        )]);
        let mut p2 = make_project(4, "nvestiv/iris-ex");
        p2.update_pipelines(vec![make_pipeline(
            2,
            4,
            "feat/broken",
            PipelineStatus::Failed,
            PipelineSource::Push,
            now - TimeDelta::minutes(10),
        )]);
        let mut p3 = make_project(35, "nvestiv/website");
        p3.update_pipelines(vec![make_pipeline(
            3,
            35,
            "feat/running",
            PipelineStatus::Running,
            PipelineSource::Push,
            now - TimeDelta::minutes(1),
        )]);

        let projects = vec![p1, p2, p3];
        let config = GlimConfig::default();
        let ctx = RelevanceContext::new(&config, &[], &[], "paul", now);
        let global = rank_global_ci_runs(&projects, &ctx, 10);
        assert_eq!(global.len(), 3);
        // Active running pipeline first, then broken failed pipeline, then passing master branch
        assert_eq!(global[0].project.path, "nvestiv/website");
        assert_eq!(global[0].entry.branch, "feat/running");
        assert_eq!(global[1].project.path, "nvestiv/iris-ex");
        assert_eq!(global[1].entry.branch, "feat/broken");
        assert_eq!(global[2].project.path, "caasa/connect");
        assert_eq!(global[2].entry.branch, "master");
    }

    #[test]
    fn test_user_interaction_boosts_relevance() {
        let now = Utc::now();
        let mut proj = make_project(24, "caasa/connect");
        let mr_a = make_mr(10, 24, "feat/alpha", "master", "teammate", now - TimeDelta::hours(2));
        let mr_b = make_mr(11, 24, "feat/beta", "master", "teammate", now - TimeDelta::hours(5));
        proj.update_merge_requests(vec![mr_a, mr_b]);

        let config = GlimConfig {
            ranking_mode: RankingMode::Deterministic,
            ..GlimConfig::default()
        };

        // Without interaction, newer `feat/alpha` ranks above `feat/beta`
        let ctx_no_ix = RelevanceContext::new(&config, &[], &[], "paul", now);
        let base_entries = rank_project_entries(&proj, &ctx_no_ix, 5);
        assert_eq!(base_entries[0].branch, "feat/alpha");

        // Record a user interaction opening MR !11 (`feat/beta`)
        let interactions = vec![UserInteraction {
            id: 1,
            project_id: ProjectId::new(24),
            mr_iid: Some(11),
            branch: Some("feat/beta".into()),
            pipeline_id: None,
            kind: UserInteractionKind::OpenMergeRequestWeb,
            weight: UserInteractionKind::OpenMergeRequestWeb.base_weight(),
            created_at: now - TimeDelta::minutes(5),
        }];
        let ctx_with_ix = RelevanceContext::new(&config, &[], &interactions, "paul", now);
        let boosted_entries = rank_project_entries(&proj, &ctx_with_ix, 5);
        assert_eq!(boosted_entries[0].branch, "feat/beta");
        assert!(boosted_entries[0].score > base_entries[0].score);
    }

    #[test]
    fn test_stochastic_mode_reroll_changes_sample() {
        let now = Utc::now();
        let mut proj = make_project(24, "caasa/connect");
        let mrs: Vec<MergeRequest> = (1..=8)
            .map(|i| {
                make_mr(
                    i,
                    24,
                    &format!("feat/branch-{i}"),
                    "master",
                    "paul",
                    now - TimeDelta::hours(2),
                )
            })
            .collect();
        proj.update_merge_requests(mrs);

        let config = GlimConfig {
            ranking_mode: RankingMode::Stochastic,
            stochastic_temperature: 1.2,
            ..GlimConfig::default()
        };

        let ctx0 = RelevanceContext::new(&config, &[], &[], "paul", now).with_mode(None, 0);
        let sample0: Vec<CompactString> = rank_project_entries(&proj, &ctx0, 4)
            .into_iter()
            .map(|e| e.branch)
            .collect();

        // Spinning the stochastic nonce produces a different Plackett-Luce / Gumbel-Top-k draw
        let mut found_different = false;
        for nonce in 1..=5 {
            let ctx_n =
                RelevanceContext::new(&config, &[], &[], "paul", now).with_mode(None, nonce);
            let sample_n: Vec<CompactString> = rank_project_entries(&proj, &ctx_n, 4)
                .into_iter()
                .map(|e| e.branch)
                .collect();
            if sample_n != sample0 {
                found_different = true;
                break;
            }
        }
        assert!(found_different);
    }
}
