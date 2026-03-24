# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Data migration pipeline: export datasets from Dataiku → upload to OneLake → load into Delta/Warehouse tables
- Chunked upload to OneLake with configurable chunk size (default 4 MB), retry logic, progress callback
- `upload_via_azcopy()` for large file uploads (>100 MB auto-detection, fallback to httpx)
- `query_row_count()` for post-load row count verification (source vs target)
- Incremental/watermark export: `filter_column` and `filter_value` on Dataiku dataset export
- `get_watermark()` / `update_watermark()` helpers for incremental migration tracking
- `run_data_migration()` orchestrating full export → upload → load → verify pipeline
- Data migration config options: `export_format`, `chunk_size_mb`, `compression`, `upload_method`, `load_mode`
- 29 new tests in `test_data_migration.py` covering full pipeline, upload methods, watermark, row counts
- `--dry-run` flag on `migrate` — prints execution plan (waves, agents, asset counts) without running
- Rich progress bars during migration (per-agent wave tracking with spinner, bar, elapsed time)
- `--quiet` / `-q` flag to suppress progress bars
- `interactive` command — guided migration wizard with prompts for project, workspace, agent selection
- `config validate` subcommand — validates YAML syntax, Pydantic schema, env vars, timeout settings
- `status` command — shows current migration state from registry (assets by type/state, agent results, checkpoints)
- `--output-format` / `-f` flag (table/json/yaml) on discover, migrate, validate, config validate, status
- `validate_config()` function in `core/config.py` — returns structured issues list with error/warning levels
- `get_execution_plan()` method on Orchestrator — dry-run plan with agent descriptions and asset counts
- `get_status()` method on Orchestrator — migration state summary from registry and results
- Progress callbacks (`on_agent_start`, `on_agent_done`) in `run_pipeline()` for live UI updates
- CLI test suite expanded to 53 tests (was 15) — covers all new commands, flags, and output formats
- `_format_output()` and `_format_table()` output formatting helpers
- `rich>=13.7.0` dependency for progress bars
- Comprehensive `examples/` directory with real input/output migration samples
- Registry checkpointing after each orchestrator wave (`save_checkpoint`)
- Pipeline resume via `--resume` flag (skip completed agents on restart)
- Selective agent re-run via `--rerun <agent>` (resets + downstream cascade)
- Selective asset processing via `--asset-ids <id1,id2>` filter
- Checkpoint cleanup on successful pipeline completion (`--keep-checkpoints` to retain)
- `get_completed_agents()` — detects which agents have finished based on asset states
- `reset_assets_for_agent()` — resets agent assets to DISCOVERED for re-processing
- `filter_asset_ids()` — keeps only specified assets in the registry
- CLI test suite (15 tests) covering all commands and flags
- Logger test suite (6 tests) covering setup and format options
- GitHub Actions CI with Python 3.10 / 3.12 / 3.13 matrix
- Dockerfile and .dockerignore for containerised deployment
- MANIFEST.in for proper sdist/wheel packaging
- End-to-end integration test (19 tests) with synthetic 14-asset project
- Large-project performance test (282 assets, <30 s, <200 MB memory)
- Per-agent execution timeout via `asyncio.wait_for`
- Circuit-breaker pattern (skip agent after N consecutive failures)
- `agent_timeout_seconds` and `circuit_breaker_threshold` config options
- SETUP.md and TROUBLESHOOTING.md user documentation
- CHANGELOG.md (this file)

### Changed
- Updated .gitignore with packaging and IDE artefacts
- Updated pyproject.toml URLs to point to GitHub repository

## [0.1.0] — 2025-01-01

### Added — Phase 1: Core Infrastructure
- CLI entry point (`click`-based) with `discover`, `migrate`, `validate` commands
- YAML configuration loader (Pydantic v2)
- Asset Registry with in-memory store and JSON persistence
- Asset data model with `AssetType` enum and `MigrationState` state machine
- Abstract `BaseAgent` with lifecycle (IDLE → RUNNING → COMPLETED / FAILED)
- Structured logging via `structlog`

### Added — Phase 2: Discovery & Connectors
- Discovery Agent — scans Dataiku project via REST API, catalogs all asset types
- Dataiku REST API client (`httpx`) with API key auth and pagination
- Fabric REST API client with Azure AD auth and retry logic

### Added — Phase 3: SQL Translation
- SQL Migration Agent — converts Oracle and PostgreSQL recipes to T-SQL / Spark SQL
- Core SQL translator powered by `sqlglot`
- Oracle → T-SQL translation rules (NVL, SYSDATE, ROWNUM, CONNECT BY, DECODE)
- PostgreSQL → T-SQL translation rules (::cast, ILIKE, LATERAL, ||)
- CTAS, window-function, and temp-table handling

### Added — Phase 4: Python & Visual Recipe Conversion
- Python Migration Agent — rewrites Dataiku SDK calls to PySpark equivalents
- Python → Fabric Notebook (.ipynb) converter via `nbformat`
- Dataiku SDK → PySpark call mapping (Dataset, Folder, imports)
- Visual Recipe Agent — converts Join, Group By, Filter, Window, Sort, Pivot, Prepare recipes to SQL

### Added — Phase 5: Dataset, Connection & Pipeline Migration
- Dataset Migration Agent — migrates schemas to Lakehouse (Delta) or Warehouse tables
- Connection Mapper Agent — maps Dataiku connections to Fabric equivalents (Gateway, OneLake, Shortcut)
- Flow → Pipeline Agent — converts Dataiku flow DAGs and scenarios to Fabric Data Pipeline JSON
- Pipeline trigger generation from Dataiku scenarios

### Added — Phase 6: Orchestration & CLI
- DAG-based orchestrator with wave execution (dependency-aware parallel dispatch)
- Agent retry logic with configurable max retries
- Full CLI integration: `discover`, `migrate`, `validate`, `report` commands
- HTML and JSON report generation

### Added — Phase 7: Validation Agent
- Schema comparison (column names, types, nullability)
- SQL syntax validation via `sqlglot.transpile`
- Notebook structure validation (cell types, metadata, kernel spec)
- Pipeline structure validation (activities, dependencies, triggers)
- Connection validation (gateway, shortcut, linked service)
- Aggregated HTML + JSON validation reports

[Unreleased]: https://github.com/cyphou/DataikuToFabric/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/cyphou/DataikuToFabric/releases/tag/v0.1.0
