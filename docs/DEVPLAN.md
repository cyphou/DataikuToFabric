# Development Plan — Dataiku to Fabric Migration Toolkit

> Phased roadmap from scaffold to production-ready migration tool.
> **Last updated:** 2026-03-23 (Phase 6)

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
| Validation agent | Scaffold | Basic state checks, no deep validation |
| Tests | **404 passing** | Oracle (33), PostgreSQL (31), SQL agent (24), discovery (19), client (14), translators (17), python translator (17), python agent (32), visual recipe (16), visual agent (40), dataset agent (55), connection agent (39), flow pipeline agent (65), conftest fixtures |

---

## Phase Plan

| Phase | Focus | Gate |
|-------|-------|------|
| **Phase 1** | ~~Core foundation — fix blockers, wire CLI, add `validate_sql`~~ | **DONE** — 64 tests green |
| **Phase 2** | ~~Discovery & extraction — Dataiku client, discovery agent, test fixtures~~ | **DONE** — 33 tests green |
| **Phase 3** | ~~SQL translation — fix agent, Oracle+PG tests (30+30), multi-stmt~~ | **DONE** — 88 tests green (33+31+24) |
| **Phase 4** | ~~Python + Visual recipes — expand SDK patterns, Prepare recipe~~ | **DONE** — 93 tests green (32+17+16+40) |
| **Phase 5** | ~~Dataset + Connection migration — DDL, type mapping, connection templates~~ | **DONE** — 94 tests green (55+39) |
| **Phase 6** | ~~Flow → Pipeline — DAG builder, trigger mapping, activity references~~ | **DONE** — 65 tests green |
| Phase 7 | Validation — deep schema match, row counts, report HTML/JSON | Validation report generated |
| Phase 8 | Integration testing, perf, packaging, docs | E2E green, pip-installable |

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

## Phase 8 — Integration & Release

### Tasks

| # | Task |
|---|------|
| 8.1 | End-to-end integration test |
| 8.2 | Performance testing (large projects) |
| 8.3 | Error handling audit |
| 8.4 | User documentation (setup, troubleshooting) |
| 8.5 | pip install / Docker packaging |

---

## Risk Register

| # | Risk | Impact | Mitigation |
|---|------|--------|------------|
| 1 | Complex PL/SQL cannot be auto-converted | High | Flag for human review, provide side-by-side diff |
| 2 | Dataiku API rate limits | Medium | Backoff + response caching |
| 3 | Fabric API changes | Medium | Abstract behind client layer, version-pin |
| 4 | Large datasets too slow to migrate | High | Parallel upload, incremental migration |
| 5 | Custom Dataiku plugins | Medium | Plugin adapter framework (Phase 8+) |
| 6 | Python SDK differences across Dataiku versions | Low | Comprehensive test suite |

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
