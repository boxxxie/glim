use std::{path::PathBuf, process::exit};

use clap::Parser;

use crate::{
    app_init::{initialize_app, AppComponents},
    config::{default_config_path, default_db_path},
    glim_app::GlimConfig,
    rendering::render_main_ui,
    result::Result,
    stores::{load_or_create_quack_token, resolve_duckdb_binary},
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
        tui.draw(|f| render_main_ui(f, &app, &mut widget_states, &mut effects))?;
        tui.set_fast_tick(effects.is_running());
    }

    tui.exit()?;
    Ok(())
}
