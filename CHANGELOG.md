# Changelog

## glim 0.3.0 - 2026-11-01

### Added
- **DuckDB 2.0 & Quack Protocol Storage Engine:** Replaced flat JSONL cache files with `~/.local/share/glim/glim.duckdb` (`projects`, `pipelines`, `merge_requests`, `activity_events`, `commits` tables) and a persistent background Quack protocol server on `quack:127.0.0.1:9497` authenticated via `~/.local/share/glim/quack.token`.
- **Automatic Legacy JSONL Migration:** Automatically deduplicates and imports existing `~/.local/share/glim/cache/{projects,activity_events,commits}.jsonl` files into `glim.duckdb` on startup and renames them to `*.jsonl.migrated`.
- **CLI Inspection Flags:** Added `--print-quack-attach` and `--verify-config` flags to `glim`.

### Changed
- **Memory & Thread Optimization (TUI):** Restricted glibc `malloc` to a single arena (`mallopt(M_ARENA_MAX, 1)`) and configured the Tokio runtime with a single worker thread (`worker_threads = 1`), reducing TUI threads from `20` to `7` and virtual memory (`VSZ`) from `1.01 GB` to `61.0 MB` (`~13.6 MB` private heap).
- **Low-Memory DuckDB 2.0 Configuration:** Configured `glim`'s DuckDB 2.0 worker with single-threaded execution (`threads=1`, `external_threads=1`, `async_threads=1`), `MALLOC_ARENA_MAX=1`, an `8MB` buffer pool limit (`allocator_flush_threshold='512KB'`), and `wal_autocheckpoint='256KB'` plus startup `CHECKPOINT;`.

## glim 0.2.1 - 2025-07-21

### Changed
- Animations/transitions now default to enabled

## glim 0.2.0 - 2025-07-21

### Added
- GitLab service integration
- Vim key bindings support
- Project search filtering
- Structured logging with tracing framework
- Visual effects system with EffectRegistry
- Animation toggle configuration option

### Changed
- Refactored GitLab client architecture
- Updated to use CompactString for better performance
- Migrated to color-eyre for error handling
- Updated tachyonfx and ratatui dependencies
- Log files now saved to OS-appropriate cache directories (Linux: ~/.cache/glim, macOS: ~/Library/Caches/glim, Windows: %LOCALAPPDATA%\glim\cache)

### Fixed
- Fixed application panic issues

### Removed
- Removed internal log widget (replaced with proper logging)

## glim 0.1.0 - 2024-10-05

Initial release.
