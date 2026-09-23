# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Full Dataiku REST API coverage audit against the official spec (auth, API key permission model, and the exhaustive `dss/api/*` endpoint catalog). Closed the highest-value gaps found:
	- New `AssetType.WEBAPP` / `AssetType.STREAMING_ENDPOINT` — Dataiku webapps and streaming endpoints (Kafka/etc.) are now discovered via new `DataikuClient.list_webapps()` / `list_streaming_endpoints()` methods instead of being silently invisible to the migration; both have no direct Fabric equivalent so each discovered asset carries an explicit `review_flags` entry requiring manual re-implementation
	- New `AssetType.JUPYTER_NOTEBOOK` — ad-hoc project notebooks are now discovered via `DataikuClient.list_jupyter_notebooks()` / `get_jupyter_notebook()` and stored with the full nbformat payload when available. They are flagged for manual Fabric notebook migration because they are outside the Dataiku Flow recipe graph
	- New `AssetType.PROJECT_LIBRARY` — project library contents and `external-libraries.json` dependency metadata are now captured through `DataikuClient.list_project_library_contents()` / `get_project_library_file()` so Python recipe and notebook migration can account for custom packages
	- Dataset discovery now captures Dataiku Data Quality status and rule configuration in each dataset's `metadata.data_quality` via `get_project_data_quality_status()` and `get_dataset_data_quality_rules()`, preserving validation signals for downstream migration QA
	- Saved-model discovery now captures version listings and detailed version metadata via `list_saved_model_versions()` / `get_saved_model_version_details()`, preserving active-version, training, and code-environment information for downstream model migration
	- New `AssetType.API_SERVICE` — Dataiku API services (prediction/custom endpoints) are now discovered via `DataikuClient.list_api_services()` with generated package inventory from `list_api_service_packages()` when available. They are flagged for manual Fabric/Azure endpoint migration because they represent deployable serving dependencies, not Power BI/Fabric semantic-model assets
	- `DataikuClient.get_project_variables()` fetches project-level `standard`/`local` variables (used for `${var}` substitution throughout recipes/scenarios/SQL) and attaches them to the `FLOW` asset's metadata so translators/reports can surface unresolved variable references instead of them being invisible
	- Discovery failures for webapps, streaming endpoints, Jupyter notebooks, project libraries, Data Quality status/rules, saved-model versions/details, and project variables each degrade gracefully with a review flag, following the same non-fatal pattern already used for connections/saved models/dashboards
	- Documented, deliberately out-of-scope for now (lower priority, no code changes): Wiki articles, Discussions, and dashboard insights (chart-level detail within a dashboard)
- `migration.migrate_data` config flag (also `migrate --with-data`) wires the previously-dead `run_data_migration()` pipeline into `DatasetMigrationAgent` — `migrate` can now actually export/upload/load data, not just generate DDL, with one dataset's data-migration failure isolated from the rest via a review flag
- `publish-powerbi` CLI command — generates a minimal DirectLake Power BI/Fabric SemanticModel (TMDL: `database.tmdl`/`model.tmdl`/`expressions.tmdl`/`tables/*.tmdl`) over already-migrated lakehouse datasets and publishes it via a new `FabricClient.create_semantic_model()`. Reuses the DirectLake TMDL structure pattern from the sibling TableauToPowerBI project (reimplemented, since that generator is coupled to Tableau-specific extraction internals)

### Fixed
- `run_data_migration()` no longer leaks the local exported staging file to disk forever after a successful upload — it's now deleted once the data has reached its destination in OneLake, and only kept when a step fails (for debugging/retry)
- `run_data_migration()` now catches exceptions from any pipeline step and returns `status: "failed"` instead of propagating a raw exception, so migrating many datasets can isolate one failure from the rest
- `export_dataset()` and `export_dataset_to_file()` (Dataiku data extraction) now retry transient errors (429/5xx/connection errors) with the same backoff policy as `_request()`, instead of failing on the first blip during a large export
- `export_dataset_to_file()` now streams into a `.part` temp file and renames it atomically on success, so an interrupted/failed download never leaves a corrupt or truncated file at the final output path
- Discovery agent no longer loses all previously-discovered assets when a single recipe's detail fetch or a single dataset's schema fetch fails — each item is now handled independently and flagged for review instead of aborting the whole `discover` run
- `get_flow()` and `list_scenarios()` failures now degrade gracefully (review flag) instead of aborting discovery, matching the pattern already used for connections, saved models, and dashboards
- A fatal discovery error (e.g. `list_datasets()` itself failing) now persists whatever assets were already discovered to `registry.json` before returning, instead of silently discarding all progress from that run
- Discovery agent no longer fails the entire `discover` run when `list_connections()` (Dataiku's `/admin/connections/` endpoint) is unreachable with a project-scoped API key; it now logs a review flag and continues discovering all other asset types, matching the existing graceful-degradation pattern already used for saved models and dashboards

### Added
- `test-connection` CLI command — verifies Dataiku server connectivity and auth with a single lightweight call, reporting an actionable category (`unauthorized`, `forbidden`, `not_found`, `connection_error`, `timeout`) instead of requiring a full `discover` run to diagnose issues
- `dataiku.proxy_url` config option for explicit outbound proxy configuration (in addition to automatically honored `HTTPS_PROXY`/`HTTP_PROXY` env vars) for corporate/gateway-fronted Dataiku deployments
- Dataiku client now follows HTTP redirects (`follow_redirects=True`), needed for gateways/reverse proxies that rewrite paths or upgrade http→https

### Security
- `serve` command now supports `--auth-mode` (`api_key`/`bearer`) with a required secret env var; previously the underlying auth support existed but the CLI never wired it, so `serve` always ran unauthenticated regardless of intent
- API server compares API keys/bearer tokens with `hmac.compare_digest` instead of `==` to avoid timing side-channels
- `POST /api/jobs` rejects requests whose declared `Content-Length` exceeds 10 MB (or is non-numeric) before reading the body, preventing an unbounded-read memory exhaustion vector
- Fabric OAuth token acquisition (`_acquire_token`) now actually honors `fabric.auth_method` (`azure_cli`, `managed_identity`, `environment`, `service_principal`) instead of always using `DefaultAzureCredential`; added `client_secret_env` config field required for `service_principal`

### Fixed
- Dataiku client now authenticates via `Authorization: Bearer <api_key>` header instead of the `?apiKey=` query parameter, which some Dataiku deployments (e.g. behind gateways/reverse proxies) reject with a 401 even for valid keys

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
- Dataiku TLS configuration support with `verify_ssl` and `ca_bundle_path`
- `validate_config()` TLS warnings for insecure mode (`verify_ssl=false`) and missing CA bundle paths
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
- Added `chardet<6` dependency constraint to avoid `RequestsDependencyWarning` in test/runtime tooling environments

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
