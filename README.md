# `glim` — GitLab Pipelines, Merge Requests & Activity TUI Monitor

[![Crate Badge]][Crate] [![Deps.rs Badge]][Deps.rs]

![GitLab Pipelines](screenshots/gitlab_pipelines.png)

[![GitLab Projects](screenshots/gitlab_projects_thumbnail.png)](screenshots/gitlab_projects.png)
[![Pipeline Actions](screenshots/pipeline_actions_thumbnail.png)](screenshots/pipeline_actions.png)

`glim` (`glim-tui`) is a fast, keyboard- and mouse-driven Terminal User Interface (TUI) for monitoring GitLab CI/CD pipelines, merge requests, live repository activity, and mentions/todos across projects.

Built in Rust with [ratatui](https://ratatui.rs/), `crossterm`, [tachyonfx](https://github.com/junkdog/tachyonfx), and `tokio`, `glim` persists projects, pipelines, merge requests, commits, activity events, and user interaction telemetry into a local **DuckDB 2.0** database (`~/.local/share/glim/glim.duckdb`) and serves it over the **Quack protocol** (`quack:127.0.0.1:9497`) for zero-latency SQL inspection while the TUI is running.

---

## Features

- **Unified CI Pipeline & Merge Request Monitoring**:
  - Unifies detached MR pipeline refs (`refs/merge-requests/<iid>/head`) with their source branch and displays real-time MR status badges (draft, mergeable, conflicts, unresolved threads, approvals).
  - **Grouped vs. Flat CI View (`v`)**: Switch between grouping branches/MRs inside repository rows (`grouped`) or listing ranked CI processes and open MRs as first-class single-line rows across all repositories (`flat`).
  - **Stacked MR Compaction**: Automatically collapses stacked MR chains (`branch_b -> branch_a -> main`) into a single row with a `(+N stacked: !first..!last)` badge and prunes stale merged/closed non-MR branches.
- **Stochastic & Hawkes Dynamic Relevance Engine**:
  - Scores branches, MRs, and repositories using pipeline failure signals, MR conflict/approval state, exponential age decay, and Hawkes process excitation from both GitLab activity events and your own TUI interactions (`user_interactions`).
  - **Three Ranking Modes (`s` to cycle, `S` to reroll)**:
    - `hybrid` (default): Pins urgent items (running pipelines, recent failures/conflicts, default branch) and samples remaining slots using Plackett-Luce / Gumbel-Top-$k$ exploration.
    - `stochastic`: 100% Gumbel-Top-$k$ stochastic sampling weighted by relevance score.
    - `deterministic`: Pure relevance score ordering with zero randomness.
- **Live Activity Feed & Focus-Driven Split (`m` / `Tab`)**:
  - Real-time bottom feed of MR lifecycle events, review comments, branch pushes, @mentions, review requests, and GitLab todos with automatic pagination and deduplication.
  - **Dynamic Focus Split (`Tab`)**: Dynamically reallocates vertical viewport lines between the CI table and the activity feed depending on which pane has focus.
  - **Activity & Project Popups (`Enter` / `o`)**: Inspect full multi-line commit messages, job/stage breakdowns, and navigate previous/next activity events (`Left`/`Right` or `h`/`l`) inside the modal inspector.
- **Interactive Mouse Support**:
  - Hovering over project rows, individual branch/MR entries, or activity events underlines the target and clicking opens the corresponding MR, pipeline, or commit directly in your browser.
- **Configurable Desktop & In-App Notifications (`notify-send`)**:
  - Granular toggles and urgency levels (`low`, `normal`, `critical`) for pipeline transitions (running, passed, failed), MR conflicts/approvals/threads/comments, branch pushes, and direct `@mentions`/todos, with clickable links and optional bot-comment filtering.
- **Low-Memory DuckDB 2.0 + Quack Server**:
  - Instant offline-warm startup from `~/.local/share/glim/glim.duckdb` (`projects`, `pipelines`, `merge_requests`, `activity_events`, `commits`, `user_interactions`), tuned with single-arena `malloc` (`M_ARENA_MAX=1`) and a single-worker DuckDB engine (`~13.6 MB` private heap).

---

## Prerequisites

- A terminal emulator with 24-bit color support (e.g. [kitty](https://sw.kovidgoyal.net/kitty/), Ghostty, Alacritty, WezTerm)
- A GitLab Personal Access Token (PAT) with `read_api` scope
- `duckdb` (v1.2+ / 2.0 with `quack` extension support) on your `PATH` or in `~/.local/bin/duckdb`
- `libssl-dev` (when building from source)

---

## Installation

### From Source

```bash
cargo install --path .
```

### From Crates.io / Arch Linux

```bash
# Crates.io (upstream release)
cargo install glim-tui

# Arch Linux
pacman -S glim
```

---

## CLI Usage

```text
$ glim --help
A TUI for monitoring GitLab CI/CD pipelines and projects

Usage: glim [OPTIONS]

Options:
  -c, --config <FILE>       Alternate path to the configuration file
  -p, --print-config-path   Print the path to the configuration file and exit
      --print-quack-attach  Print the DuckDB 2.0 Quack protocol ATTACH snippet for concurrent CLI queries and exit
      --verify-config       Verify configuration and DuckDB 2.0 availability and exit
      --inspect             Inspect dynamic relevance scores, stacked MR compaction, and split budgets as JSON and exit
  -h, --help                Print help
  -V, --version             Print version
```

### Common Commands

```bash
# Launch interactive TUI
glim

# Use an alternate config file (e.g. for multiple GitLab instances)
glim --config ~/.config/glim-work.toml

# Verify config, DuckDB binary, database path, and Quack server status
glim --verify-config

# Inspect live relevance scores, row budgets, and stochastic rankings as JSON
glim --inspect

# Query the live DuckDB 2.0 cache via Quack protocol while glim is running
duckdb -json -c "$(glim --print-quack-attach)"
```

---

## Keybindings

### Main View

| Key | Action |
| :--- | :--- |
| `j` / `Down`, `k` / `Up` | Move selection down / up in the focused pane |
| `PageDown` / `PageUp` | Page down / up in the focused pane |
| `Home` / `End` | Jump to first / last row in the focused pane |
| `Tab` / `Shift+Tab` | Toggle focus between CI table and Activity Feed (dynamically resizes split) |
| `Enter` / `o` | Open Project Details popup (CI pane) or Activity Event Details popup (Activity pane) |
| `w` | Open selected project, MR, or pipeline in default browser |
| `v` | Toggle CI view mode between **Grouped** (by repository) and **Flat** (1-line CI rows) |
| `s` | Cycle relevance ranking mode (`hybrid` ➔ `100% stochastic` ➔ `deterministic`) |
| `S` | Reroll stochastic Gumbel-Top-$k$ sample immediately |
| `m` | Toggle bottom Activity Feed panel visibility |
| `f` / `/` | Filter projects by search string (`Esc` to clear) |
| `p` | Manually refresh pipelines for the selected project |
| `r` | Refresh all projects, MRs, pipelines, and events |
| `a` | Replay last in-app notification banner |
| `c` | Open interactive Configuration popup |
| `F12` | Capture terminal screen |
| `q` | Quit `glim` |

### Project & Activity Details Popups

| Key | Action |
| :--- | :--- |
| `j` / `Down`, `k` / `Up` | Select pipeline or action item |
| `h` / `Left`, `l` / `Right` | Navigate to previous / next activity event (in Activity Details popup) |
| `Enter` / `o` | Open Pipeline Actions menu or trigger selected activity action |
| `w` | Open MR, commit, or project URL in browser |
| `Esc` / `q` | Close popup |

---

## Configuration (`~/.config/glim.toml`)

If you launch `glim` without an existing configuration file, it will prompt you for your GitLab URL and Personal Access Token (`read_api` scope). Note that the PAT is stored in plain text in `glim.toml`.

Example `~/.config/glim.toml`:

```toml
gitlab_url = "https://gitlab.example.com/api/v4"
gitlab_token = "glpat-xxxxxxxxxxxxxxxxxxxx"
log_level = "Error"

# Project filtering & pipeline limits
max_pipelines_per_project = 8
max_recent_pipelines = 12
max_age_days = 30
include_mr_pipelines = true
include_projects = ["my-group/backend", "my-group/frontend"]
exclude_projects = ["my-group/archive-repo"]

# Polling & layout
projects_poll_seconds = 15
jobs_poll_seconds = 10
activity_feed_lines = 12

# Visual effects & TachyonFX shaders
animations = true
glitch_effect = true
table_sweep_effect = true
popup_effects = true
notification_effects = true

# Dynamic relevance, view modes & stochastic ranking
dynamic_relevance = true
dynamic_split = true
ci_view_mode = "grouped"          # "grouped" or "flat"
ranking_mode = "hybrid"           # "hybrid", "stochastic", or "deterministic"
stochastic_temperature = 0.75
stochastic_ratio = 0.35
stochastic_rotation_seconds = 90
event_half_life_hours = 18.0
age_half_life_hours = 36.0
interaction_half_life_hours = 24.0
interaction_weight = 1.0
compact_stacked_mrs = true
prune_merged_branches = true

# DuckDB 2.0 Quack protocol server
quack_server_enabled = true
quack_port = 9497

# Desktop (notify-send) & in-app notifications
os_notifications = true
in_app_notifications = true
notify_on_startup = true
os_notification_timeout_ms = 8000
in_app_notification_timeout_ms = 5000
ignore_bot_comments = true

notify_pipeline_failed = true
notify_pipeline_passed = true
notify_pipeline_running = true
notify_mr_conflicts = true
notify_mr_approved = true
notify_mr_threads = true
notify_mr_events = true
notify_mr_comments = true
notify_branch_push = true
notify_mentions_and_todos = true

notify_urgency_pipeline_failed = "normal"   # "low", "normal", or "critical"
notify_urgency_pipeline_passed = "normal"
notify_urgency_pipeline_running = "normal"
notify_urgency_mr_conflicts = "normal"
notify_urgency_mentions = "normal"
notify_urgency_default = "normal"
```

---

## License

MIT

[Crate Badge]: https://img.shields.io/crates/v/glim-tui.svg
[Crate]: https://crates.io/crates/glim-tui
[Deps.rs Badge]: https://deps.rs/repo/github/junkdog/glim/status.svg
[Deps.rs]: https://deps.rs/repo/github/junkdog/glim
