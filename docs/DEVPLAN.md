# Development Plan — Dataiku to Fabric Migration Toolkit

> Phased roadmap from scaffold to production-ready migration tool.
> **Last updated:** 2026-03-23 (Phase 8 done — planning Phases 9–18)

---

## Current Status

| Module | Status | Notes |
|--------|--------|-------|
| Documentation | **Done** | README, AGENTS, ARCHITECTURE, DEVPLAN |
| Core (config, registry, logger) | **Done** | Pydantic config, JSON registry, structlog |
| Base agent contract | **Done** | ABC with execute/validate/rollback |
| Orchestrator | **Done** | DAG-based wave execution, parallel dispatch, retry logic |
| CLI | **Done** | Commands defined, connectors wired |
| Dataiku client | **Done** | Pagination, auth, all endpoints, streaming export, row count |
| Fabric client | **Done** | Azure Identity auth, async polling, OneLake upload, warehouse DDL |
| Discovery agent | **Done** | All 10 asset types, dependency resolution |
| SQL translator | **Done** | sqlglot transpile, validate_sql, multi-statement |
| Oracle rules | **Done** | DECODE (nested), NVL2, TO_DATE/CHAR/NUMBER, DUAL, sequences, LISTAGG, RETURNING, MERGE, CONNECT BY flags |
| PostgreSQL rules | **Done** | LIMIT/OFFSET, SERIAL, BOOLEAN, TEXT, NOW, EXTRACT, ILIKE, \|\|→+, LATERAL→CROSS APPLY, RETURNING→OUTPUT |
| SQL migration agent | **Done** | Dialect detection, multi-statement, error handling |
| Python translator | **Done** | 20+ SDK patterns, 10 pandas→PySpark, AST+regex detection |
| Python migration agent | **Done** | Full convert+validate, notebook generation |
| Visual recipe agent | **Done** | 10 generators: join, vstack, group, filter, window, sort, distinct, topn, pivot, prepare |
| Dataset agent | **Done** | Type mapping with precision, DDL (Lakehouse+Warehouse), partitioning, schema comparison, export manifests |
| Flow pipeline agent | **Done** | DAG builder, topological sort, 6 activity types, 6 trigger types, converted asset refs |
| Connection agent | **Done** | 12 connection types, gateway/shortcut/pipeline templates, credential mapping |
| Validation agent | **Done** | Schema comparison, row count, SQL syntax, notebook structure, pipeline integrity, connection mapping, review flags |
| Tests | **489 passing** | Oracle (33), PostgreSQL (31), SQL agent (24), discovery (19), client (14), translators (17), python translator (17), python agent (32), visual recipe (16), visual agent (40), dataset agent (55), connection agent (39), flow pipeline agent (65), validation agent (85), conftest fixtures |

---

## Phase Plan

| Phase | Focus | Gate |
|-------|-------|------|
| **Phase 1** | ~~Core foundation — fix blockers, wire CLI, add `validate_sql`~~ | **DONE** — 64 tests |
| **Phase 2** | ~~Discovery & extraction — Dataiku client, discovery agent~~ | **DONE** — 33 tests |
| **Phase 3** | ~~SQL translation — Oracle+PG rules, multi-stmt~~ | **DONE** — 88 tests |
| **Phase 4** | ~~Python + Visual recipes — SDK patterns, Prepare recipe~~ | **DONE** — 93 tests |
| **Phase 5** | ~~Dataset + Connection — DDL, type mapping, templates~~ | **DONE** — 94 tests |
| **Phase 6** | ~~Flow → Pipeline — DAG builder, trigger mapping~~ | **DONE** — 65 tests |
| **Phase 7** | ~~Validation — schema match, row counts, HTML/JSON~~ | **DONE** — 85 tests |
| **Phase 8** | ~~Integration, packaging, CI, Dockerfile, docs~~ | **DONE** — 512 tests |
| Phase 9 | Checkpoint, resume & selective re-run | Pipeline survives restart, asset-level retry |
| Phase 10 | CLI hardening — dry-run, progress, interactive | CLI test suite, all flags work |
| Phase 11 | Data migration — export, upload, row-count verify | Round-trip data for sample dataset |
| Phase 12 | Fabric deployment — agents call create APIs | Notebook + pipeline + DDL deployed to workspace |
| Phase 13 | Security — Key Vault, credential rotation, audit | Zero secrets in logs or config files |
| Phase 14 | Rollback & error recovery | `rollback` command deletes deployed assets |
| Phase 15 | Observability — OpenTelemetry, metrics, correlation IDs | Traces visible in Application Insights |
| Phase 16 | Advanced recipes — R, Spark-Scala, custom plugins | Unsupported types flagged, not silently dropped |
| Phase 17 | Dashboard → Power BI migration | Dashboard agent generates .pbix / REST deploy |
| Phase 18 | ML Model → Fabric ML migration | Model export, ONNX convert, AzureML register |

---

## Phase 1 — Core Foundation

> Fix the blocking issues that prevent any agent from running end-to-end.

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 1.1 | Add `validate_sql()` function | `translators/sql_translator.py` | Parse SQL with sqlglot; return error list |
| 1.2 | Wire connectors into CLI context | `cli.py` | Create `DataikuClient` + `FabricClient`, add to `context.connectors` |
| 1.3 | Add Azure Identity auth to Fabric client | `connectors/fabric_client.py` | `DefaultAzureCredential` + token acquisition |
| 1.4 | Fix test assertions (type mismatches) | `tests/test_translators.py`, `tests/test_python_translator.py`, `tests/test_visual_recipe.py` | Match actual function return types |
| 1.5 | Fix Dataiku client API auth header | `connectors/dataiku_client.py` | Use `apiKey` query param (Dataiku convention) |
| 1.6 | Add `__init__.py` exports for public API | `agents/__init__.py`, `translators/__init__.py`, `connectors/__init__.py` |

### Gate

```
pytest tests/ — all green
py -m src.cli --help — shows discover/migrate/validate/report
py -m src.cli discover --project X --config config/config.template.yaml — fails gracefully (no server)
```

---

## Phase 2 — Discovery & Extraction

> Make discovery agent fully functional with mocked + real Dataiku APIs.

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 2.1 | Add pagination to Dataiku client | `connectors/dataiku_client.py` | Handle paginated list endpoints |
| 2.2 | Fix Dataiku API paths | `connectors/dataiku_client.py` | Match actual Dataiku DSS REST API (`/public/api/projects/…`) |
| 2.3 | Add saved models + dashboards discovery | `agents/discovery_agent.py` | Complete asset discovery for all 10 types |
| 2.4 | Create mock Dataiku API fixtures | `tests/fixtures/` | Sample JSON responses for every endpoint |
| 2.5 | Write discovery agent unit tests | `tests/test_discovery_agent.py` | 15+ test cases with mocked client |
| 2.6 | Write Dataiku client unit tests | `tests/test_dataiku_client.py` | HTTP mocking with `respx` |
| 2.7 | Integration test with real project (optional) | `tests/integration/` | Needs live Dataiku DSS |

### Gate

```
pytest tests/test_discovery_agent.py — 15+ tests green
Registry JSON contains all asset types with correct metadata
Dependencies resolved correctly between recipes and datasets
```

---

## Phase 3 — SQL Translation

> Production-quality SQL dialect conversion with comprehensive test coverage.

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 3.1 | Multi-statement SQL splitting | `translators/sql_translator.py` | Split `BEGIN…END`, multiple `;`-separated stmts |
| 3.2 | Oracle edge cases | `translators/oracle_to_tsql.py` | `DECODE` nested, `CONNECT BY` recursive CTE, `MERGE`, `RETURNING` |
| 3.3 | PostgreSQL edge cases | `translators/postgres_to_tsql.py` | `RETURNING` → `OUTPUT`, `ON CONFLICT` → `MERGE`, `GENERATE_SERIES` |
| 3.4 | SQL migration agent hardening | `agents/sql_migration_agent.py` | Handle missing payload, connection dialect detection |
| 3.5 | Oracle test suite (30 cases) | `tests/test_oracle_translation.py` | NVL, TO_DATE, ROWNUM, DECODE, CONNECT BY, MERGE, LISTAGG, PL/SQL flags |
| 3.6 | PostgreSQL test suite (30 cases) | `tests/test_postgres_translation.py` | Cast, LIMIT, SERIAL, BOOLEAN, JSON, ARRAY, ON CONFLICT, RETURNING flags |
| 3.7 | SQL agent integration tests | `tests/test_sql_agent.py` | Full flow: registry → agent → output files |

### Gate

```
pytest tests/test_oracle_translation.py tests/test_postgres_translation.py — 60 tests green
Generated T-SQL passes validate_sql() for all cases
Review flags correctly raised for unconvertible constructs
```

---

## Phase 4 — Python + Visual Recipes

### Tasks

| # | Task | File(s) |
|---|------|---------|
| 4.1 | Expand SDK patterns (20+) | `translators/python_to_notebook.py` |
| 4.2 | Pandas → PySpark suggestions | `translators/python_to_notebook.py` |
| 4.3 | AST-based detection (replace regex) | `translators/python_to_notebook.py` |
| 4.4 | Prepare recipe converter | `agents/visual_recipe_agent.py` |
| 4.5 | Pivot recipe converter | `agents/visual_recipe_agent.py` |
| 4.6 | Python test suite (15 cases) | `tests/test_python_agent.py` |
| 4.7 | Visual recipe test suite (20 cases) | `tests/test_visual_agent.py` |

---

## Phase 5 — Dataset + Connection Migration

### Tasks

| # | Task | File(s) |
|---|------|---------|
| 5.1 | Azure Identity integration | `connectors/fabric_client.py` |
| 5.2 | Async 202 polling for long ops | `connectors/fabric_client.py` |
| 5.3 | Data export from Dataiku (Parquet) | `connectors/dataiku_client.py` |
| 5.4 | OneLake upload | `connectors/fabric_client.py` |
| 5.5 | DDL execution (Warehouse) | `agents/dataset_agent.py` |
| 5.6 | Connection credential mapping | `agents/connection_agent.py` |

---

## Phase 6 — Flow to Pipeline

### Tasks

| # | Task | File(s) |
|---|------|---------|
| 6.1 | Dynamic DAG from registry deps | `core/orchestrator.py` |
| 6.2 | Pipeline activity references | `agents/flow_pipeline_agent.py` |
| 6.3 | All trigger types | `agents/flow_pipeline_agent.py` |
| 6.4 | Pipeline deploy to Fabric | `connectors/fabric_client.py` |

---

## Phase 7 — Validation & Reporting

### Tasks

| # | Task | File(s) |
|---|------|---------|
| 7.1 | Schema comparison (source vs target) | `agents/validation_agent.py` |
| 7.2 | Row count validation | `agents/validation_agent.py` |
| 7.3 | SQL syntax validation (all outputs) | `agents/validation_agent.py` |
| 7.4 | HTML report generator | `cli.py`, `models/report.py` |
| 7.5 | JSON report generator | `cli.py`, `models/report.py` |

---

## Phase 8 — Integration, Packaging & Release ✅

> Harden the toolkit for production use: end-to-end tests, containerisation, docs, packaging.
>
> **Status: DONE** — 512 tests passing, all deliverables shipped.

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 8.1 | **End-to-end integration test** | `tests/integration/test_e2e_pipeline.py` | Full cycle: mock Dataiku discovery → convert SQL/Python/visual/datasets/connections/flows → validate → report. Uses a synthetic Dataiku project fixture with all asset types. No live APIs — all connectors mocked/stubbed. Gate: single `pytest tests/integration/` invocation covers the whole DAG. |
| 8.2 | **Large-project performance test** | `tests/integration/test_perf.py` | Generate a synthetic project with 200+ recipes and 50+ datasets. Measure `.run_pipeline()` wall-clock time. Assert <30 s on CI. Profile memory with `tracemalloc`. |
| 8.3 | **Error handling audit & hardening** | `core/orchestrator.py`, all agents | Add per-agent execution timeout (`asyncio.wait_for`). Add circuit-breaker counter (skip agent after N consecutive failures). Ensure every agent wraps unexpected exceptions into `AgentResult(FAILED)` with diagnostic details. |
| 8.4 | **Dockerfile + .dockerignore** | `Dockerfile`, `.dockerignore` | Multi-stage build: `python:3.12-slim` → install from wheel → expose `dataiku-to-fabric` entry point. Health-check via `--version`. `.dockerignore` for `output/`, `logs/`, `.git/`, `__pycache__/`. |
| 8.5 | **GitHub Actions CI** | `.github/workflows/ci.yml` | Matrix: Python 3.10 / 3.12 / 3.13. Steps: install deps → `pytest --cov` (fail under 80%) → build wheel → (on tag) publish to PyPI via trusted publisher. |
| 8.6 | **pip-installable wheel** | `pyproject.toml`, `MANIFEST.in` | Add `MANIFEST.in` to include templates, config template. Verify `pip install .` + `dataiku-to-fabric --version` works in a clean venv. |
| 8.7 | **User documentation** | `docs/SETUP.md`, `docs/TROUBLESHOOTING.md` | Setup guide (prerequisites, install, config, first run). Troubleshooting (common errors, connectivity, auth). Add to README TOC. |
| 8.8 | **Update .gitignore** | `.gitignore` | Add `*.egg-info/`, `dist/`, `build/`, `.venv/`, `venv/`, `.DS_Store`, `htmlcov/`. |
| 8.9 | **CHANGELOG & release template** | `CHANGELOG.md` | Document Phases 1-7 retroactively. Add `[Unreleased]` section for ongoing changes. Follow Keep-a-Changelog format. |
| 8.10 | **README refresh** | `README.md` | Update status table to reflect all phases done. Add badges (tests, coverage, PyPI version). Add "Quickstart" snippet with real commands. |

### Gate

```
pytest tests/ tests/integration/ — all green, ≥80% coverage
pip install . && dataiku-to-fabric --version → 0.1.0
docker build -t dataiku-to-fabric . && docker run --rm dataiku-to-fabric --version
docs/SETUP.md + docs/TROUBLESHOOTING.md reviewed
```

### Execution order

```
8.8  .gitignore cleanup               (no deps)
8.4  Dockerfile + .dockerignore        (no deps)
8.6  MANIFEST.in + wheel verify        (no deps)
8.3  Error handling hardening          (no deps)
  ↓
8.1  E2E integration test             (needs 8.3 — hardened agents)
8.2  Perf test                        (needs 8.1 — same fixture infra)
  ↓
8.5  GitHub Actions CI                (needs 8.1 + 8.6 — tests + wheel)
  ↓
8.7  User docs                        (needs all code stable)
8.9  CHANGELOG                        (needs all code stable)
8.10 README refresh                   (needs all done)
```

---

## Phase 9 — Checkpoint, Resume & Selective Re-run

> Make the pipeline crash-safe: persist state between waves, skip completed work on restart, re-run individual agents or assets.

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 9.1 | **Checkpoint after each wave** | `core/orchestrator.py`, `core/registry.py` | Call `registry.save(checkpoint_path)` after every wave completes. File name includes wave index (`checkpoint_wave_3.json`). |
| 9.2 | **Resume from checkpoint** | `core/orchestrator.py`, `cli.py` | `--resume <checkpoint_file>` flag. Load registry, detect which agents have `COMPLETED` results, skip them. Only run agents whose inputs changed or who never ran. |
| 9.3 | **Asset-level state skip** | `core/orchestrator.py`, `agents/base_agent.py` | Before executing, each agent checks its assets in the registry. Assets already in `CONVERTED` / `DEPLOYED` / `VALIDATED` state are skipped unless `--force` is passed. |
| 9.4 | **Selective agent re-run** | `core/orchestrator.py`, `cli.py` | `migrate --rerun sql_converter` re-runs only the named agent (and downstream dependents). Resets those assets to `DISCOVERED` state first. |
| 9.5 | **Selective asset re-run** | `core/orchestrator.py`, `cli.py` | `migrate --asset-ids recipe_1,recipe_2` processes only the listed assets through whatever agents are needed. |
| 9.6 | **Checkpoint cleanup** | `core/orchestrator.py` | On successful pipeline completion, remove intermediate checkpoint files (keep only `registry.json`). Configurable `keep_checkpoints: true` flag. |
| 9.7 | **Tests** | `tests/test_checkpoint_resume.py` | 20+ tests: write checkpoint, load checkpoint, skip completed wave, force re-run, asset-level filter, cleanup. |

### Gate

```
Kill pipeline mid-run → restart with --resume → completes without re-running finished agents
migrate --rerun sql_converter → only SQL + downstream agents run
migrate --asset-ids X,Y → only those assets processed
20+ tests green
```

### Execution order

```
9.1  Checkpoint save          (no deps)
9.2  Resume loader            (needs 9.1)
9.3  Asset-level skip         (needs 9.1)
  ↓
9.4  Selective agent re-run   (needs 9.2 + 9.3)
9.5  Selective asset re-run   (needs 9.3)
9.6  Checkpoint cleanup       (needs 9.1)
  ↓
9.7  Tests                    (needs all)
```

---

## Phase 10 — CLI Hardening

> Make the CLI production-ready: dry-run, progress feedback, interactive prompts, config validation, full test coverage.

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 10.1 | **`--dry-run` flag** | `cli.py`, `core/orchestrator.py` | Print the execution plan (agent order, asset counts per agent) without executing. Format: table or JSON. |
| 10.2 | **Progress bars** | `cli.py`, `core/orchestrator.py` | `tqdm` or `rich.progress` — one bar per agent wave, sub-bar per asset. Update as agents complete. Hide with `--quiet`. |
| 10.3 | **Interactive mode** | `cli.py` | `migrate --interactive` — prompt for project key, workspace, agents to include/exclude, confirm before running. Uses `click.prompt()` / `questionary`. |
| 10.4 | **`config validate` subcommand** | `cli.py`, `core/config.py` | Parse config YAML, validate against Pydantic schema, test Dataiku/Fabric connectivity (optional `--test-connections`). |
| 10.5 | **`--output-format` flag** | `cli.py` | `--output-format json|table|yaml` for all commands. Default: `table` for terminal, `json` for piped output. |
| 10.6 | **`status` command** | `cli.py`, `core/registry.py` | Show current migration state: per-agent status, per-asset counts by state, last checkpoint, errors summary. |
| 10.7 | **CLI test suite** | `tests/test_cli.py` | 30+ tests: invoke each command with `click.testing.CliRunner`, verify output format, error handling, flag combinations. |

### Gate

```
dataiku-to-fabric migrate --dry-run → prints plan, exits 0
dataiku-to-fabric config validate → reports OK or errors
dataiku-to-fabric status → shows registry state
30+ CLI tests green
```

---

## Phase 11 — Data Migration

> Actually move data from Dataiku to Fabric: export datasets, upload to OneLake, load into Delta/Warehouse tables, verify row counts.

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 11.1 | **Dataiku dataset export** | `connectors/dataiku_client.py` | `export_dataset(project, name, format)` → streams Parquet/CSV to local temp file. Support `format=parquet|csv`. Handle large datasets with chunked download. |
| 11.2 | **OneLake upload (httpx)** | `connectors/fabric_client.py` | Enhance `upload_to_lakehouse()` with chunked upload (4 MB blocks), progress callback, retry on partial failure. |
| 11.3 | **azcopy integration** | `connectors/fabric_client.py`, new `utils/azcopy.py` | For files >100 MB, shell out to `azcopy copy` with SAS token or Azure Identity auth. Auto-detect azcopy on PATH, fallback to httpx upload. |
| 11.4 | **Delta table load** | `agents/dataset_agent.py` | After upload, call Fabric SQL endpoint to `COPY INTO` or `CREATE TABLE AS SELECT` from staged files. Support append vs. overwrite modes. |
| 11.5 | **Incremental / CDC stub** | `agents/dataset_agent.py`, `models/asset.py` | Config option `incremental_column` per dataset. If set, export only rows where `column > last_watermark`. Store watermark in registry. Not a full CDC system — just a watermark-based filter. |
| 11.6 | **Post-load row count validation** | `agents/dataset_agent.py`, `agents/validation_agent.py` | After load, query `SELECT COUNT(*)` on both source (Dataiku) and target (Fabric). Compare. Flag mismatch in validation report. |
| 11.7 | **Config: data migration options** | `core/config.py`, `config/config.template.yaml` | Add `export_format`, `chunk_size_mb`, `compression` (snappy/gzip/none), `upload_method` (httpx/azcopy), `load_mode` (overwrite/append). |
| 11.8 | **Tests** | `tests/test_data_migration.py` | 25+ tests: mock Dataiku export → local file → mock OneLake upload → verify row counts. Test chunked upload, azcopy fallback, incremental watermark. |

### Gate

```
Export sample dataset from Dataiku → upload to OneLake → load into Lakehouse table → row counts match
Incremental re-export only fetches new rows
25+ tests green
```

---

## Phase 12 — Fabric Deployment

> Close the loop: agents don't just generate files — they deploy to a live Fabric workspace. Notebooks, pipelines, DDL, and connections are created via Fabric REST API.

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 12.1 | **Deploy toggle** | `core/config.py`, `config/config.template.yaml` | `deploy_to_fabric: bool` config option (default `false`). If false, agents write files locally only. If true, agents also call Fabric APIs. |
| 12.2 | **Notebook deployment** | `agents/python_migration_agent.py`, `connectors/fabric_client.py` | After generating `.ipynb`, call `fabric_client.create_notebook()`. Store returned `item_id` in registry asset metadata. Handle 409 Conflict (notebook already exists) with update-or-skip logic. |
| 12.3 | **Pipeline deployment** | `agents/flow_pipeline_agent.py`, `connectors/fabric_client.py` | After generating `pipeline.json`, call `fabric_client.create_pipeline()`. Store `item_id`. Handle long-running operation polling. |
| 12.4 | **DDL execution** | `agents/dataset_agent.py`, `connectors/fabric_client.py` | After generating DDL, call `fabric_client.execute_sql()` on Warehouse endpoint. Handle CREATE TABLE + CREATE VIEW. Wrap in transaction if supported. |
| 12.5 | **Connection / shortcut creation** | `agents/connection_agent.py`, `connectors/fabric_client.py` | Add `create_shortcut()`, `create_linked_service()` to Fabric client. Connection agent calls them when `deploy_to_fabric=true`. |
| 12.6 | **Idempotency** | all agents, `connectors/fabric_client.py` | All create operations must be idempotent: check if item exists by name before creating. If exists, compare content hash → update if changed, skip if identical. |
| 12.7 | **Deployment report** | `agents/validation_agent.py`, `models/report.py` | Add deployed item URLs to validation report. New section: "Deployment Status" with item_id, URL, create/update timestamp. |
| 12.8 | **Tests** | `tests/test_fabric_deployment.py` | 25+ tests: mock `fabric_client.create_*()` calls, verify agents invoke them, test idempotency (skip/update logic), test 409 handling, verify item_ids stored in registry. |

### Gate

```
migrate --deploy → notebooks, pipelines, tables created in Fabric workspace
Re-run → existing items skipped or updated (not duplicated)
25+ tests green
```

---

## Phase 13 — Security & Secrets Management

> Eliminate plaintext secrets: integrate Azure Key Vault, add credential rotation, strip secrets from logs, add audit trail.

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 13.1 | **Azure Key Vault integration** | new `connectors/keyvault_client.py`, `core/config.py` | Config: `secrets_provider: keyvault` + `keyvault_url`. Use `azure-keyvault-secrets` SDK to fetch `DATAIKU_API_KEY`, `FABRIC_CLIENT_SECRET` at startup. Cache in memory. |
| 13.2 | **Secret reference syntax** | `core/config.py` | Config values like `api_key: "kv://my-vault/dataiku-api-key"` → auto-resolved from Key Vault. Support `env://VAR_NAME` (current behaviour) and `kv://vault/secret`. |
| 13.3 | **Credential rotation** | `connectors/fabric_client.py`, `connectors/keyvault_client.py` | Token refresh: fabric_client re-acquires token when within 5 min of expiry. Key Vault secrets re-fetched every `credential_ttl_minutes` (default 60). |
| 13.4 | **Secret scrubbing in logs** | `core/logger.py` | structlog processor that detects and redacts API keys, tokens, connection strings in log output. Pattern: replace values matching known secret field names or `Bearer .*` patterns. |
| 13.5 | **Audit logging** | `core/logger.py`, `core/orchestrator.py` | Separate audit log file: records who ran what, when, which credentials were accessed, which Fabric items were modified. Structured JSON format. |
| 13.6 | **Config: security options** | `core/config.py`, `config/config.template.yaml` | Add `secrets_provider` (env/keyvault), `keyvault_url`, `credential_ttl_minutes`, `audit_log_path`. |
| 13.7 | **Tests** | `tests/test_security.py` | 20+ tests: mock Key Vault, verify secret resolution, test scrubbing (no secrets in log output), test rotation (expired token → refresh), test audit log entries. |

### Gate

```
Config with kv:// references → secrets fetched from Key Vault at startup
Log output contains zero plaintext secrets (verified by test)
Audit log records all credential accesses
20+ tests green
```

---

## Phase 14 — Rollback & Error Recovery

> Implement real rollback: undo deployed assets, classify error severity, provide recovery commands.

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 14.1 | **Error severity enum** | `models/asset.py` | `enum ErrorSeverity { TRANSIENT, PERMANENT, WARNING }`. Transient: retry-able (network, throttle). Permanent: bad SQL, missing schema. Warning: review flag, non-blocking. |
| 14.2 | **Agent rollback implementations** | all agents | Each agent implements `rollback()`: SQL agent → `DROP TABLE/VIEW IF EXISTS`; Python agent → delete notebook via API; Pipeline agent → delete pipeline; Dataset agent → drop table; Connection agent → delete shortcut/linked service. |
| 14.3 | **Rollback command** | `cli.py`, `core/orchestrator.py` | `migrate --rollback` reads registry, finds all assets in `DEPLOYED` state, calls each agent's `rollback()` in reverse dependency order. |
| 14.4 | **Selective rollback** | `cli.py`, `core/orchestrator.py` | `migrate --rollback --agents sql_converter` or `--rollback --asset-ids X,Y`. Only rolls back specified scope. |
| 14.5 | **Error classification in agents** | all agents | Each agent wraps errors with `ErrorSeverity`. Orchestrator uses severity: TRANSIENT → retry, PERMANENT → fail immediately (no retry), WARNING → continue. |
| 14.6 | **Recovery suggestions** | `models/report.py`, `agents/validation_agent.py` | Validation report includes "Suggested Actions" per failed asset: e.g., "SQL syntax error on line 12 → review Oracle CONNECT BY clause" or "Transient failure → re-run with `--rerun sql_converter`". |
| 14.7 | **Tests** | `tests/test_rollback.py` | 25+ tests: deploy assets → rollback → verify deleted. Test selective rollback, reverse order, error classification, recovery suggestions in report. |

### Gate

```
deploy → rollback → Fabric workspace is clean
Transient errors retried, permanent errors fail-fast with suggestions
25+ tests green
```

---

## Phase 15 — Observability

> Add OpenTelemetry traces, Prometheus metrics, correlation IDs, and Application Insights export for production monitoring.

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 15.1 | **OpenTelemetry SDK integration** | new `core/telemetry.py`, `core/orchestrator.py` | Initialize TracerProvider + MeterProvider. Create spans: one per pipeline run, one per wave, one per agent execution. Attributes: agent_name, asset_count, duration_ms, status. |
| 15.2 | **Correlation IDs** | `core/telemetry.py`, `core/logger.py` | Generate `run_id` (UUID) at pipeline start. Propagate via `contextvars`. Inject into all log records and spans. Link parent→child spans for wave→agent hierarchy. |
| 15.3 | **Metrics** | `core/telemetry.py` | Counters: `assets_processed_total{agent,status}`, `errors_total{agent,severity}`. Histograms: `agent_duration_seconds{agent}`, `data_bytes_uploaded`. Gauges: `active_agents`. |
| 15.4 | **Application Insights exporter** | `core/telemetry.py`, `core/config.py` | Config: `telemetry.exporter: appinsights` + `connection_string`. Use `azure-monitor-opentelemetry-exporter`. Also support `console` and `otlp` exporters. |
| 15.5 | **Diagnostic context in exceptions** | `agents/base_agent.py`, all agents | Custom `MigrationError(message, agent, asset_id, severity, context_dict)`. Automatically attached to spans and logs. |
| 15.6 | **Config: telemetry options** | `core/config.py`, `config/config.template.yaml` | Add `telemetry.enabled`, `telemetry.exporter` (console/otlp/appinsights), `telemetry.endpoint`, `telemetry.sampling_rate`. |
| 15.7 | **Tests** | `tests/test_telemetry.py` | 15+ tests: verify spans created, correlation IDs propagated, metrics incremented, secrets not in span attributes, exporter selection. |

### Gate

```
Pipeline run → traces visible in Application Insights (or console)
Metrics: per-agent duration + error counts exported
Correlation ID present in all logs and spans
15+ tests green
```

---

## Phase 16 — Advanced Recipe Support

> Handle non-standard recipe types: R recipes, Spark-Scala, custom plugins, Dataiku-managed notebooks. Ensure nothing is silently dropped.

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 16.1 | **Recipe type classification** | `agents/discovery_agent.py`, `models/asset.py` | New `AssetType` values: `RECIPE_R`, `RECIPE_SPARK_SCALA`, `RECIPE_CUSTOM_PLUGIN`. Discovery agent assigns correct type instead of bucketing R as Python. |
| 16.2 | **R recipe handler** | new `agents/r_recipe_agent.py` | Detect R code. Generate a Fabric R notebook (if R kernel available) or flag for manual migration. Extract data reads/writes, add `-- MANUAL REVIEW` comments. |
| 16.3 | **Spark-Scala recipe handler** | new `agents/scala_recipe_agent.py` | Detect Scala code. Flag for manual conversion (Scala notebooks not natively supported in Fabric). Generate a documentation notebook with the original Scala code + PySpark migration notes. |
| 16.4 | **Custom plugin detection** | `agents/discovery_agent.py`, `agents/python_migration_agent.py` | Detect `import custom_*` or Dataiku plugin recipe types. Register as `RECIPE_CUSTOM_PLUGIN`. Add `review_flag: "Custom plugin — requires manual migration"`. |
| 16.5 | **Unsupported recipe report** | `agents/validation_agent.py`, `models/report.py` | New report section: "Unsupported Recipes" listing all R, Scala, custom plugin assets with migration suggestions. |
| 16.6 | **Tests** | `tests/test_advanced_recipes.py` | 20+ tests: R detection, Scala detection, custom plugin flag, unsupported report section, no silent drops. |

### Gate

```
R recipe → flagged, not silently treated as Python
Custom plugin → flagged with review note
Unsupported Recipes section in validation report
20+ tests green
```

---

## Phase 17 — Dashboard → Power BI Migration

> Convert Dataiku dashboards to Power BI reports: extract data sources, map widgets to visuals, deploy via Power BI REST API.

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 17.1 | **Dashboard metadata parser** | new `translators/dashboard_parser.py` | Parse Dataiku dashboard JSON: extract pages, tiles, chart types, dataset bindings, filter definitions. Build an intermediate model (`DashboardModel`). |
| 17.2 | **Power BI report model** | new `models/powerbi.py` | Pydantic models for Power BI report structure: pages, visuals, data source references, filters, slicers. Matches Power BI REST API / PBIR format. |
| 17.3 | **Widget → Visual mapper** | new `translators/widget_to_visual.py` | Map Dataiku chart types → Power BI visuals: bar→clusteredBarChart, line→lineChart, pie→pieChart, table→table, KPI→card. Flag unsupported widgets for manual review. |
| 17.4 | **Dashboard migration agent** | new `agents/dashboard_agent.py` | Orchestrates: parse dashboard → map widgets → generate Power BI report definition → optionally deploy. Handles multi-page dashboards. |
| 17.5 | **Power BI REST API client** | `connectors/fabric_client.py` | Add `create_report()`, `create_semantic_model()` methods. Use Fabric / Power BI REST API for deployment. Handle workspace + capacity association. |
| 17.6 | **Data source rebinding** | `agents/dashboard_agent.py`, `translators/widget_to_visual.py` | Dataiku dataset references → Fabric Lakehouse/Warehouse table references. Use registry to resolve migrated table names. |
| 17.7 | **Tests** | `tests/test_dashboard_migration.py` | 25+ tests: parse dashboard JSON, map 10+ chart types, generate report model, handle missing data sources, deploy mock. |

### Gate

```
Sample Dataiku dashboard JSON → Power BI report definition generated
Known chart types mapped, unknown flagged
Data sources rebound to migrated Fabric tables
25+ tests green
```

---

## Phase 18 — ML Model → Fabric ML Migration

> Export Dataiku saved models, convert to portable formats, register with Azure ML / Fabric ML, validate prediction parity.

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 18.1 | **Model export from Dataiku** | `connectors/dataiku_client.py` | `export_model(project, model_id)` → download model artifact (`.pkl`, `.h5`, `.onnx`, model folder). Store metadata: framework, features, target, metrics. |
| 18.2 | **Model format detection** | new `translators/model_converter.py` | Detect framework: scikit-learn, XGBoost, LightGBM, TensorFlow/Keras, PyTorch, custom. Route to appropriate converter. |
| 18.3 | **ONNX conversion** | `translators/model_converter.py` | Convert scikit-learn/XGBoost/LightGBM models to ONNX via `skl2onnx` / `onnxmltools`. TensorFlow/PyTorch → `tf2onnx` / `torch.onnx.export`. Flag unsupported models. |
| 18.4 | **Azure ML registration** | new `connectors/azureml_client.py` | Register converted model in Azure ML workspace. Upload model artifact, set tags (source_project, source_model, framework, accuracy). Create model endpoint (optional). |
| 18.5 | **ML model migration agent** | new `agents/ml_model_agent.py` | Orchestrates: export → detect → convert → register. Stores Azure ML model URI in registry. Handles batch of models. |
| 18.6 | **Prediction parity validation** | `agents/validation_agent.py` | Export sample prediction set from Dataiku model. Run same inputs through registered Fabric/AzureML model. Compare predictions (tolerance threshold). Flag divergence. |
| 18.7 | **Config: ML options** | `core/config.py`, `config/config.template.yaml` | Add `ml.azureml_workspace`, `ml.resource_group`, `ml.convert_to_onnx` (bool), `ml.prediction_tolerance`. |
| 18.8 | **Tests** | `tests/test_ml_migration.py` | 20+ tests: export mock model, detect framework, ONNX conversion (mock), AzureML register (mock), prediction parity check. |

### Gate

```
Scikit-learn model exported → ONNX converted → registered in Azure ML
Predictions match within tolerance between source and target
20+ tests green
```

---

## Phase Dependency Graph (9–18)

```
Phase 9  — Checkpoint & Resume    (no deps, infra)
Phase 10 — CLI Hardening          (no deps, infra)
  ↓
Phase 11 — Data Migration         (benefits from 9 for resume)
Phase 12 — Fabric Deployment      (benefits from 9 for idempotency)
  ↓
Phase 13 — Security               (needs 12 — secrets used in deployment)
Phase 14 — Rollback               (needs 12 — rollback deletes deployed items)
  ↓
Phase 15 — Observability          (needs 12 — traces deployment calls)
Phase 16 — Advanced Recipes       (no hard deps, can run in parallel)
  ↓
Phase 17 — Dashboard → Power BI   (needs 12 for deployment, 11 for data sources)
Phase 18 — ML Model → Fabric ML   (needs 12 for deployment, 11 for data)
```

---

## Risk Register

| # | Risk | Impact | Mitigation |
|---|------|--------|------------|
| 1 | Complex PL/SQL cannot be auto-converted | High | Flag for human review, provide side-by-side diff |
| 2 | Dataiku API rate limits | Medium | Backoff + response caching |
| 3 | Fabric API changes | Medium | Abstract behind client layer, version-pin |
| 4 | Large datasets too slow to migrate | High | Parallel upload, azcopy, incremental (Phase 11) |
| 5 | Custom Dataiku plugins | Medium | Plugin detection + manual-review flag (Phase 16) |
| 6 | Python SDK differences across Dataiku versions | Low | Comprehensive test suite |
| 7 | Fabric workspace quotas / throttling | Medium | Retry + circuit breaker + configurable parallelism |
| 8 | ONNX conversion fails for complex models | High | Flag unsupported, keep original artifact (Phase 18) |
| 9 | Power BI visual parity with Dataiku dashboards | High | Map known types, flag unknown for manual review (Phase 17) |
| 10 | Secret leakage in logs or exception traces | High | Log scrubbing + audit trail (Phase 13) |
| 11 | Checkpoint files grow large on big projects | Low | Compact JSON, optional compression |
| 12 | Azure Key Vault latency impacts startup | Low | Cache secrets in memory, TTL refresh (Phase 13) |

## Key Decisions

| # | Decision | Choice | Rationale |
|---|----------|--------|-----------|
| 1 | SQL parser | sqlglot | Best multi-dialect SQL parser |
| 2 | Notebook format | nbformat v4 | Standard .ipynb, Fabric-compatible |
| 3 | Pipeline format | Fabric REST API JSON | Native format |
| 4 | Data format | Parquet | Columnar, Delta-compatible |
| 5 | Upload | azcopy + Fabric REST | Fast for large files |
| 6 | Config | YAML + Pydantic | Human-readable, validated |
| 7 | Async | asyncio + httpx | Native Python async |
| 8 | Auth | Azure Identity (DefaultAzureCredential) | Standard Azure auth chain |
