use std::{path::PathBuf, process::exit};

use clap::Parser;

use crate::{
    app_init::{initialize_app, AppComponents},
    config::{default_config_path, default_db_path},
    glim_app::{GlimConfig, RankingMode},
    relevance::{
        compute_dynamic_split_and_budgets, compute_project_heat, rank_global_ci_runs,
        rank_project_entries, RelevanceContext,
    },
    rendering::render_main_ui,
    result::Result,
    stores::{load_or_create_quack_token, load_readonly_snapshot, resolve_duckdb_binary},
};

mod app_init;
mod client;
mod config;
mod dispatcher;
mod domain;
mod effect_registry;
mod event;
mod glim_app;
mod gruvbox;
mod id;
mod input;
mod logging;
mod notice_service;
mod os_notify;
mod relevance;
mod rendering;
mod result;
mod stores;
mod theme;
mod tui;
mod ui;

/// A TUI for monitoring GitLab CI/CD pipelines and projects backed by DuckDB 2.0 Quack protocol
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Alternate path to the configuration file.
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,
    /// Print the path to the configuration file and exit.
    #[arg(short, long)]
    print_config_path: bool,
    /// Print the DuckDB 2.0 Quack protocol ATTACH snippet for concurrent CLI queries and exit.
    #[arg(long)]
    print_quack_attach: bool,
    /// Verify configuration and DuckDB 2.0 availability and exit.
    #[arg(long)]
    verify_config: bool,
    /// Inspect dynamic relevance scores, stacked MR compaction, and split budgets as JSON and exit.
    #[arg(long)]
    inspect: bool,
}

fn main() -> Result<()> {
    #[cfg(target_os = "linux")]
    unsafe {
        unsafe extern "C" {
            fn mallopt(param: i32, value: i32) -> i32;
        }
        const M_ARENA_MAX: i32 = -8;
        let _ = mallopt(M_ARENA_MAX, 1);
    }

    let args = Args::parse();
    let config_path = args.config.unwrap_or_else(default_config_path);

    if args.print_config_path {
        println!("{}", config_path.display());
        exit(0);
    }

    let debug = std::env::var("GLIM_DEBUG").is_ok();

    let config: GlimConfig = if config_path.exists() {
        confy::load_path(&config_path)
            .map_err(|e| crate::result::GlimError::config_load_error(config_path.clone(), e))?
    } else {
        GlimConfig::default()
    };

    if args.print_quack_attach {
        let token = load_or_create_quack_token();
        println!(
            "LOAD quack;\nCREATE SECRET (TYPE quack, TOKEN '{}');\nATTACH 'quack:127.0.0.1:{}' AS glim;\nSELECT id, path, default_branch, last_activity_at, open_mr_count FROM glim.projects ORDER BY last_activity_at DESC LIMIT 20;",
            token, config.quack_port
        );
        return Ok(());
    }

    if args.verify_config {
        let duckdb_bin = resolve_duckdb_binary();
        let db_path = default_db_path();
        println!("config_path: {}", config_path.display());
        println!("duckdb_binary: {}", duckdb_bin.display());
        println!("database_path: {}", db_path.display());
        println!(
            "quack_server: {} (127.0.0.1:{})",
            if config.quack_server_enabled {
                "enabled"
            } else {
                "disabled"
            },
            config.quack_port
        );
        println!("gitlab_url: {}", config.gitlab_url);
        return Ok(());
    }

    if args.inspect {
        let now = chrono::Utc::now();
        let (all_projects, events, interactions) = load_readonly_snapshot(&config);
        let ctx = RelevanceContext::new(&config, &events, &interactions, "paul", now);
        let mut visible_projects = Vec::new();
        let mut project_summaries = Vec::new();

        for p in &all_projects {
            let (heat, should_show) = compute_project_heat(p, &ctx);
            if should_show {
                visible_projects.push((p.clone(), heat));
            }
        }
        visible_projects.sort_by(|(pa, ha), (pb, hb)| {
            pb.has_active_pipelines()
                .cmp(&pa.has_active_pipelines())
                .then_with(|| hb.partial_cmp(ha).unwrap_or(std::cmp::Ordering::Equal))
        });
        let ordered_projects: Vec<_> = visible_projects.iter().map(|(p, _)| p.clone()).collect();
        let first_id = ordered_projects.first().map(|p| p.id);

        let (ci_proj_h, ci_feed_h, ci_budgets) = compute_dynamic_split_and_budgets(
            &ordered_projects,
            first_id,
            40,
            true,
            false,
            false,
            &ctx,
        );
        let (ev_proj_h, ev_feed_h, ev_budgets) = compute_dynamic_split_and_budgets(
            &ordered_projects,
            first_id,
            40,
            true,
            true,
            false,
            &ctx,
        );

        for (proj, heat) in &visible_projects {
            let ci_budget = ci_budgets.get(&proj.id).copied().unwrap_or(6);
            let ev_budget = ev_budgets.get(&proj.id).copied().unwrap_or(2);
            let entries = rank_project_entries(proj, &ctx, ci_budget);
            let entry_json: Vec<serde_json::Value> = entries
                .iter()
                .map(|e| {
                    serde_json::json!({
                        "branch": e.branch,
                        "score": (e.score * 100.0).round() / 100.0,
                        "is_urgent": e.is_urgent,
                        "pipeline_status": e.pipeline.map(|p| &p.status),
                        "mr_iid": e.mr.map(|m| m.iid),
                        "stacked_badge": e.stacked_badge(),
                    })
                })
                .collect();
            project_summaries.push(serde_json::json!({
                "path": proj.path,
                "heat": (heat * 100.0).round() / 100.0,
                "ci_focus_row_budget": ci_budget,
                "event_focus_row_budget": ev_budget,
                "entries": entry_json,
            }));
        }

        let flat_runs = rank_global_ci_runs(&ordered_projects, &ctx, 16);
        let flat_json: Vec<serde_json::Value> = flat_runs
            .iter()
            .map(|r| {
                serde_json::json!({
                    "project": r.project.path,
                    "branch": r.entry.branch,
                    "score": (r.entry.score * 100.0).round() / 100.0,
                    "is_urgent": r.entry.is_urgent,
                    "pipeline_status": r.entry.pipeline.map(|p| &p.status),
                    "mr_iid": r.entry.mr.map(|m| m.iid),
                    "stacked_badge": r.entry.stacked_badge(),
                })
            })
            .collect();

        let stochastic_ctx = ctx.with_mode(Some(RankingMode::Stochastic), 1);
        let stochastic_runs = rank_global_ci_runs(&ordered_projects, &stochastic_ctx, 10);
        let stochastic_json: Vec<serde_json::Value> = stochastic_runs
            .iter()
            .map(|r| {
                serde_json::json!({
                    "project": r.project.path,
                    "branch": r.entry.branch,
                    "score": (r.entry.score * 100.0).round() / 100.0,
                    "pipeline_status": r.entry.pipeline.map(|p| &p.status),
                    "mr_iid": r.entry.mr.map(|m| m.iid),
                })
            })
            .collect();

        let report = serde_json::json!({
            "terminal_height_assumed": 40,
            "relevance_config": {
                "ranking_mode": config.ranking_mode.label(),
                "ci_view_mode": format!("{:?}", config.ci_view_mode).to_lowercase(),
                "stochastic_temperature": config.stochastic_temperature,
                "stochastic_ratio": config.stochastic_ratio,
                "stochastic_rotation_seconds": config.stochastic_rotation_seconds,
                "event_half_life_hours": config.event_half_life_hours,
                "age_half_life_hours": config.age_half_life_hours,
                "interaction_half_life_hours": config.interaction_half_life_hours,
                "interaction_weight": config.interaction_weight,
                "compact_stacked_mrs": config.compact_stacked_mrs,
                "prune_merged_branches": config.prune_merged_branches,
            },
            "user_interactions_count": interactions.len(),
            "ci_focus_split": {
                "projects_height": ci_proj_h,
                "activity_feed_height": ci_feed_h,
            },
            "event_focus_split": {
                "projects_height": ev_proj_h,
                "activity_feed_height": ev_feed_h,
            },
            "flat_ci_processes": flat_json,
            "stochastic_100_sample": stochastic_json,
            "visible_projects": project_summaries,
        });
        println!("{}", serde_json::to_string_pretty(&report).unwrap());
        return Ok(());
    }

    // Create a shared runtime for async operations
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .map_err(|e| {
            crate::result::GlimError::GeneralError(format!("Failed to create runtime: {e}").into())
        })?;

    let AppComponents {
        mut app,
        mut tui,
        mut widget_states,
        mut effects,
        poller,
        _log_guard,
    } = rt.block_on(async { initialize_app(config_path, config, debug).await })?;

    // Start the poller in the background
    rt.spawn(async move {
        if let Err(e) = poller.start().await {
            tracing::error!("GitLab poller failed: {}", e);
        }
    });

    while app.is_running() {
        widget_states.last_frame = app.process_timers();
        tui.receive_events(|event| {
            widget_states.apply(&app, &mut effects, &event);
            app.apply(event, &mut widget_states, &mut effects);
        });
        if !app.is_running() {
            break;
        }
        tui.draw(|f| render_main_ui(f, &app, &mut widget_states, &mut effects))?;
        tui.set_fast_tick(effects.is_running());
    }

    tui.exit()?;
    app.shutdown();
    drop(app);
    rt.shutdown_background();
    Ok(())
}
