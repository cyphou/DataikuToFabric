# Upgrade Plan — DataikuToFabric v2.0

> Feature parity roadmap incorporating proven patterns from **TableauToPowerBI v37.0.0** (8,511 tests, 14 agents, 76+ modules).
>
> **Created:** 2025-01-20
> **Source baseline:** DataikuToFabric Phase 11 (688 tests, 9 agents, 56 source files)
> **Reference project:** TableauToPowerBI v37.0.0

---

## Executive Summary

DataikuToFabric has a solid foundation (Phases 1–11) covering extraction, SQL/Python/visual translation, data migration, and CLI. The existing DEVPLAN (Phases 12–18) addresses deployment, security, rollback, observability, advanced recipes, dashboards, and ML models.

This upgrade plan **extends and reorganizes** Phases 12–18 and adds **Phases 19–30** by porting proven patterns from TableauToPowerBI. The goal is to reach **enterprise-grade maturity** with:

- **Self-healing & auto-fix** (50+ model-side + 21 report-side healers in T2PBI)
- **Pre-migration assessment** with readiness scoring
- **QA suite** with automated validation → fix → governance → comparison
- **REST API server** for programmatic access
- **Schema drift detection** between migration runs
- **Lineage map** with HTML visualization
- **Plugin system** with 7 hook points
- **Multi-project merge** & shared model consolidation
- **Equivalence testing** & regression snapshots
- **Telemetry dashboard** with decision logging
- **Enterprise wave planning** with effort estimation
- **Bundle deployment** for atomic multi-artifact deploy
- **CI/CD pipeline** from lint to production

---

## Feature Mapping: TableauToPowerBI → DataikuToFabric

### ✅ Already Implemented (Align & Enhance)

| T2PBI Feature | DataikuToFabric Equivalent | Gap / Enhancement Needed |
|---------------|---------------------------|--------------------------|
| CLI with `--batch` | CLI with `migrate`, `discover`, `validate` (Phase 10) | Add `--batch` for multi-project, `--workers N` |
| Checkpoint / resume | `--resume`, `--rerun`, `--asset-ids` (Phase 9) | ✅ Feature-complete |
| SQL translation (180+ DAX) | SQL translation via sqlglot (Oracle→T-SQL, PG→T-SQL) (Phase 3) | ✅ Feature-complete for scope |
| Python/notebook generation | Python→PySpark + nbformat (Phase 4) | Add AST-based optimization pass |
| Visual recipe conversion | 10 generators (Phase 4) | ✅ Feature-complete |
| Data migration pipeline | Export→Upload→Load→Verify (Phase 11) | ✅ Feature-complete |
| Validation & reporting | Schema match, row count, HTML/JSON (Phase 7) | Enhance with QA suite pattern |
| Dry-run mode | `--dry-run` (Phase 10) | ✅ Feature-complete |
| Interactive wizard | `--interactive` (Phase 10) | Add Streamlit web UI (new phase) |
| Config validation | `config validate` (Phase 10) | ✅ Feature-complete |
| Progress bars | Rich progress (Phase 10) | ✅ Feature-complete |

### 🔄 Partially Covered (Existing DEVPLAN Phases 12–18)

| T2PBI Feature | DEVPLAN Phase | Enhancement from T2PBI |
|---------------|---------------|------------------------|
| Fabric deployment | Phase 12 | Add idempotency checks, content hashing, bundle deploy |
| Key Vault / credential vault | Phase 13 | Add multi-tenant vault (3 backends: env, KV, JSON) |
| Rollback engine | Phase 14 | Add pre-deploy snapshot, selective rollback |
| OpenTelemetry / App Insights | Phase 15 | Add decision telemetry (every conversion branch logged) |
| Dashboard → Power BI | Phase 17 | Add visual equivalence testing, widget type coverage |
| ML model export | Phase 18 | Add prediction parity validation |

### 🆕 New Features to Port (New Phases 19–30)

| T2PBI Feature | Priority | New Phase |
|---------------|----------|-----------|
| Pre-migration assessment (`--assess`) | **P0** | Phase 19 |
| QA suite (`--qa`) | **P0** | Phase 20 |
| Self-healing engine | **P1** | Phase 21 |
| Schema drift detection (`--check-drift`) | **P1** | Phase 22 |
| Lineage map + HTML dashboard | **P1** | Phase 23 |
| REST API server | **P1** | Phase 24 |
| Plugin system (7 hook points) | **P2** | Phase 25 |
| Equivalence testing & regression suite | **P2** | Phase 26 |
| Multi-project merge & shared models | **P2** | Phase 27 |
| Enterprise wave planning | **P2** | Phase 28 |
| Web UI (Streamlit wizard) | **P3** | Phase 29 |
| Bundle deployment & CI/CD pipeline | **P3** | Phase 30 |

---

## Revised Phase Plan (v2.0)

### Existing Phases (Enhanced)

| Phase | Focus | Status | v2.0 Enhancements |
|-------|-------|--------|-------------------|
| 1–10 | Foundation → CLI | ✅ Done | — |
| 11 | Data migration | ✅ Done | — |
| **12** | Fabric deployment | Planned | + Content hashing, bundle deploy prep |
| **13** | Security & secrets | Planned | + Multi-backend credential vault |
| **14** | Rollback & recovery | Planned | + Pre-deploy snapshot, auto-rollback on failure |
| **15** | Observability | Planned | + Decision telemetry logging |
| **16** | Advanced recipes | Planned | + Plugin detection & SDK pattern library |
| **17** | Dashboard → Power BI | Planned | + Visual type coverage metrics |
| **18** | ML model → Fabric ML | Planned | + Prediction parity report |

### New Phases (from TableauToPowerBI)

| Phase | Focus | Tests | Priority |
|-------|-------|-------|----------|
| **19** | Pre-migration assessment | 25+ | P0 |
| **20** | QA suite | 30+ | P0 |
| **21** | Self-healing engine | 35+ | P1 |
| **22** | Schema drift detection | 20+ | P1 |
| **23** | Lineage map & visualization | 25+ | P1 |
| **24** | REST API server | 20+ | P1 |
| **25** | Plugin system | 20+ | P2 |
| **26** | Equivalence testing & regression | 25+ | P2 |
| **27** | Multi-project merge | 30+ | P2 |
| **28** | Enterprise wave planning | 20+ | P2 |
| **29** | Web UI (Streamlit) | 15+ | P3 |
| **30** | Bundle deployment & CI/CD | 20+ | P3 |

**Target:** ~1,050 total tests (688 existing + ~360 new)

---

## Phase 19 — Pre-Migration Assessment

> Analyze a Dataiku project before migration and produce a readiness score with risk analysis.
> *Ported from: `assessment.py`, `server_assessment.py`, `strategy_advisor.py`*

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 19.1 | **Assessment model** | new `models/assessment.py` | Pydantic models: `AssessmentResult`, `CategoryScore`, `RiskItem`, `StrategyRecommendation`. Categories: SQL Complexity, Python SDK Usage, Visual Recipes, Data Volume, Connections, Dependencies, Custom Plugins, Scenarios. |
| 19.2 | **Project analyzer** | new `src/analyzers/project_analyzer.py` | Scan all assets via discovery agent. Score each category 0–100. Compute weighted overall readiness score. Assign grade: A (≥90), B (≥75), C (≥60), D (≥40), F (<40). |
| 19.3 | **SQL complexity scorer** | `src/analyzers/project_analyzer.py` | Parse SQL recipes: count JOINs, subqueries, CTEs, window functions, PL/SQL blocks, CONNECT BY, MERGE. Higher complexity = lower score. Detect unsupported constructs. |
| 19.4 | **Strategy advisor** | new `src/analyzers/strategy_advisor.py` | Based on assessment: recommend migration strategy. Options: Full Auto (A/B grade), Guided (C grade), Manual Review Required (D/F grade). Recommend Lakehouse vs Warehouse per dataset. Recommend batch size & parallelism. |
| 19.5 | **Assessment HTML report** | new `src/reports/assessment_report.py` | Radar chart (8 categories), stat cards, risk table with severity, per-recipe complexity heatmap. |
| 19.6 | **CLI integration** | `cli.py` | `dataiku-to-fabric assess --project KEY --config config.yaml` → runs assessment without migration. `--output-format html|json`. |
| 19.7 | **Tests** | `tests/test_assessment.py` | 25+ tests: score calculation, grade assignment, strategy recommendation, HTML generation, edge cases (empty project, all-custom project). |

### Gate

```
assess --project X → readiness score, radar chart, strategy recommendation
Grade correctly assigned based on thresholds
25+ tests green
```

---

## Phase 20 — QA Suite

> Automated quality assurance pipeline: validate → auto-fix → governance check → comparison → report.
> *Ported from: `governance.py`, `comparison_report.py`, `cross_validator.py`*

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 20.1 | **QA orchestrator** | new `src/qa/qa_suite.py` | 5-stage pipeline: Validate → AutoFix → Governance → Compare → Report. Each stage can run independently or as full suite. Returns `QAResult` with per-stage pass/fail/warnings. |
| 20.2 | **Governance checker** | new `src/qa/governance.py` | Classify data quality: endorsement labels (GREEN=certified, YELLOW=promoted, RED=review needed). PII sensitivity detection via column name patterns (email, ssn, phone, address). CSV export of governance report. |
| 20.3 | **Comparison report generator** | new `src/qa/comparison_report.py` | Side-by-side diff: source Dataiku recipe vs generated Fabric SQL/notebook. Highlight differences with line-level markers. HTML report with syntax highlighting. |
| 20.4 | **Cross-artifact validator** | new `src/qa/cross_validator.py` | Validate consistency across migrated artifacts: pipeline references valid notebooks, DDL column names match dataset schemas, connection strings resolve, no orphan assets. |
| 20.5 | **Migration fidelity scoring** | new `src/qa/fidelity.py` | Per-asset fidelity: Exact (100% converted), Approximate (converted with review flags), Unsupported (skipped). Roll up to overall migration confidence score. |
| 20.6 | **CLI integration** | `cli.py` | `dataiku-to-fabric qa --project KEY` runs full QA suite. `--no-compare` skips comparison. `--fix` enables auto-fix stage. Outputs `qa_report.json`. |
| 20.7 | **Tests** | `tests/test_qa_suite.py` | 30+ tests: full pipeline, governance labels, PII detection, comparison diff, cross-validation errors, fidelity scoring. |

### Gate

```
qa --project X → qa_report.json with all 5 stages
Governance labels assigned to all datasets
Fidelity score computed per asset
30+ tests green
```

---

## Phase 21 — Self-Healing Engine

> Automatically detect and fix common migration issues instead of just flagging them.
> *Ported from: `self_healing_v3.py`, `self_healing_report.py`, `repair_strategies.py`*

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 21.1 | **Healer framework** | new `src/healers/base_healer.py` | Abstract `BaseHealer` with `detect(artifact) → list[Issue]` and `heal(artifact, issue) → HealResult`. Healers registered in a registry. |
| 21.2 | **SQL healers (15+)** | new `src/healers/sql_healers.py` | Fix: unbalanced brackets/quotes, invalid column aliases, duplicate aliases, missing semicolons, incorrect JOIN syntax, unknown functions → suggest equivalent, empty WHERE clause, invalid GROUP BY references. |
| 21.3 | **Notebook healers (10+)** | new `src/healers/notebook_healers.py` | Fix: missing spark session init, unreplaced Dataiku SDK calls, missing imports, empty code cells, invalid cell metadata, duplicate cell IDs. |
| 21.4 | **Pipeline healers (10+)** | new `src/healers/pipeline_healers.py` | Fix: dangling activity references, circular dependencies, missing trigger definitions, invalid cron expressions, duplicate activity names, unreferenced parameters. |
| 21.5 | **Schema healers (10+)** | new `src/healers/schema_healers.py` | Fix: reserved word column names (wrap in brackets), unsupported data types → nearest equivalent, duplicate column names (suffix), nullable mismatches. |
| 21.6 | **Healing report** | `src/healers/base_healer.py`, `src/reports/` | Per-fix report: what was detected, what was changed, before/after diff. Summary: N issues found, M auto-fixed, K require manual review. |
| 21.7 | **CLI integration** | `cli.py` | `--auto-fix` flag on `migrate` and `qa` commands. `--fix-report` generates healing report. `--no-fix` disables auto-fix (validate-only). |
| 21.8 | **Tests** | `tests/test_self_healing.py` | 35+ tests: each healer type, before/after validation, idempotency (running fix twice = no change), report generation. |

### Gate

```
Broken SQL artifact → auto-fixed → passes validation
35+ tests green
Healing report shows before/after for each fix
```

---

## Phase 22 — Schema Drift Detection

> Compare migration outputs between runs to detect schema changes, regressions, or drift.
> *Ported from: `schema_drift.py`*

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 22.1 | **Snapshot model** | new `src/drift/snapshot.py` | `MigrationSnapshot`: captures schema (tables, columns, types), recipe count, pipeline structure, connection count, content hashes. Serializable to JSON. |
| 22.2 | **Snapshot capture** | `src/drift/snapshot.py` | After each migration, auto-save snapshot to `snapshots/snapshot_YYYYMMDD_HHMMSS.json`. Configurable retention (keep last N). |
| 22.3 | **Drift detector** | new `src/drift/drift_detector.py` | Compare two snapshots: detect added/removed/modified/renamed objects. Track: new tables, dropped columns, type changes, new recipes, removed connections. Classify severity: BREAKING, WARNING, INFO. |
| 22.4 | **Drift report** | new `src/reports/drift_report.py` | HTML report: side-by-side diff, severity badges, change timeline (if >2 snapshots). JSON export for CI integration. |
| 22.5 | **CLI integration** | `cli.py` | `dataiku-to-fabric check-drift --baseline snapshots/prev.json` compares current vs baseline. `--fail-on-breaking` exits non-zero if breaking changes found (CI gate). |
| 22.6 | **Tests** | `tests/test_schema_drift.py` | 20+ tests: capture snapshot, detect added column, detect removed table, detect type change, severity classification, report generation, retention policy. |

### Gate

```
Run migration twice with schema change → drift report shows differences
--fail-on-breaking exits 1 on breaking change
20+ tests green
```

---

## Phase 23 — Lineage Map & Visualization

> Build a complete data lineage graph and render it as an interactive HTML dashboard.
> *Ported from: lineage_map.json, dependency_graph.py, HTML flow diagrams*

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 23.1 | **Lineage model** | new `src/lineage/lineage_model.py` | Pydantic models: `LineageNode` (dataset/recipe/pipeline/connection), `LineageEdge` (input→recipe, recipe→output), `LineageGraph`. |
| 23.2 | **Lineage builder** | new `src/lineage/lineage_builder.py` | Build lineage graph from registry: traverse dependencies, resolve cross-recipe flows, annotate with migration status (migrated/pending/failed). Export to `lineage_map.json`. |
| 23.3 | **Impact analysis** | `src/lineage/lineage_builder.py` | Given an asset, compute downstream impact: what breaks if this table changes? Upstream trace: where does this data come from? |
| 23.4 | **HTML lineage dashboard** | new `src/reports/lineage_report.py` | Interactive HTML: Mermaid flow diagram, node coloring by status, click-to-inspect, filter by type. Stat cards: total nodes, edges, migrated %, failed %. |
| 23.5 | **CLI integration** | `cli.py` | `dataiku-to-fabric lineage --project KEY` generates lineage map + HTML. `--impact ASSET_ID` shows downstream impact. `--trace ASSET_ID` shows upstream trace. |
| 23.6 | **Tests** | `tests/test_lineage.py` | 25+ tests: graph construction, impact analysis, upstream trace, circular dependency handling, HTML generation, empty project. |

### Gate

```
lineage → lineage_map.json + lineage_report.html
Impact analysis for a dataset shows all downstream consumers
25+ tests green
```

---

## Phase 24 — REST API Server

> HTTP API for programmatic migration access (CI/CD, external tools, web UI backend).
> *Ported from: `api_server.py` (stdlib http.server, zero dependencies)*

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 24.1 | **API server** | new `src/api/server.py` | Stdlib `http.server.HTTPServer` — zero extra dependencies. Routes: `POST /migrate`, `POST /assess`, `GET /status/{job_id}`, `GET /download/{job_id}`, `GET /health`. |
| 24.2 | **Job manager** | new `src/api/job_manager.py` | In-memory job store. Each migration/assessment runs in a background thread. States: PENDING → RUNNING → COMPLETED / FAILED. Auto-cleanup after configurable TTL (default 24h). Max concurrent jobs (default 10). |
| 24.3 | **POST /migrate** | `src/api/server.py` | Accept JSON body: `{project_key, config_overrides, agents, dry_run}`. Start migration job, return `{job_id, status_url}`. |
| 24.4 | **POST /assess** | `src/api/server.py` | Accept JSON body: `{project_key}`. Run assessment (Phase 19), return `{job_id}`. |
| 24.5 | **GET /download/{id}** | `src/api/server.py` | Serve migration output as ZIP: SQL files, notebooks, pipelines, reports. Content-Disposition attachment. |
| 24.6 | **CLI integration** | `cli.py` | `dataiku-to-fabric serve --port 8080` starts the API server. `--workers N` for concurrent job limit. |
| 24.7 | **Tests** | `tests/test_api_server.py` | 20+ tests: start/stop server, submit migration, check status, download output, health check, max jobs, TTL cleanup, error handling. |

### Gate

```
serve --port 8080 → server starts
POST /migrate → job starts, GET /status → COMPLETED, GET /download → ZIP
20+ tests green
```

---

## Phase 25 — Plugin System

> Extensible architecture with hook points for custom transformations.
> *Ported from: `plugins.py` (7 hook points, config-based discovery)*

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 25.1 | **Plugin framework** | new `src/plugins/plugin_manager.py` | `PluginManager` with hook registration. Hooks: `pre_discovery`, `post_discovery`, `pre_translation`, `post_translation`, `transform_sql`, `transform_notebook`, `custom_recipe_handler`. |
| 25.2 | **Plugin interface** | new `src/plugins/base_plugin.py` | Abstract `BasePlugin` with `name`, `version`, `hooks() → dict[str, Callable]`. Plugins return modified artifacts or `None` (pass-through). |
| 25.3 | **Plugin discovery** | `src/plugins/plugin_manager.py` | Auto-discover plugins from `plugins/` directory (convention-based) or explicit registration in config: `plugins: [my_plugin.MyPlugin]`. |
| 25.4 | **Hook execution** | `src/plugins/plugin_manager.py`, `core/orchestrator.py` | Orchestrator calls `plugin_manager.run_hook("pre_discovery", context)` at each hook point. Plugins execute in registration order. Errors in plugins logged but don't block migration. |
| 25.5 | **Sample plugin** | new `plugins/sample_plugin.py` | Example plugin that adds custom SQL comments to all translated SQL, demonstrating the hook API. |
| 25.6 | **Tests** | `tests/test_plugins.py` | 20+ tests: plugin registration, hook execution order, error isolation, config-based discovery, pass-through behavior. |

### Gate

```
Plugin registered → hooks fire at correct points
Plugin error → logged, migration continues
20+ tests green
```

---

## Phase 26 — Equivalence Testing & Regression Suite

> Cross-platform validation and snapshot-based non-regression testing.
> *Ported from: `equivalence_tester.py`, `regression_suite.py`*

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 26.1 | **Equivalence tester** | new `src/testing/equivalence_tester.py` | Compare source (Dataiku) vs target (Fabric) query results. Execute same query on both sides. Compare row counts, column names, sample values. Tolerance threshold for numeric comparisons. |
| 26.2 | **Regression snapshot** | new `src/testing/regression_suite.py` | Auto-generate regression snapshots after migration: recipe count, column count per table, content hashes, SQL output hashes. Save to `regression/baseline_YYYYMMDD.json`. |
| 26.3 | **Regression comparator** | `src/testing/regression_suite.py` | Compare current migration output against baseline snapshot. Flag regressions: missing recipes, changed SQL output, schema differences. |
| 26.4 | **CLI integration** | `cli.py` | `--equivalence` flag runs cross-platform comparison. `--save-baseline` saves regression snapshot. `--check-regression` compares against saved baseline. `--fail-on-regression` exits non-zero. |
| 26.5 | **Tests** | `tests/test_equivalence_regression.py` | 25+ tests: equivalence comparison, tolerance handling, snapshot capture, regression detection, baseline management. |

### Gate

```
--save-baseline → snapshot saved
--check-regression → regressions detected if migration output changed
25+ tests green
```

---

## Phase 27 — Multi-Project Merge & Shared Models

> Consolidate multiple Dataiku projects into shared Fabric assets.
> *Ported from: `shared_model.py`, `merge_assessment.py`, `merge_config.py`*

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 27.1 | **Merge assessment** | new `src/merge/merge_assessment.py` | Given N Dataiku projects: fingerprint datasets (column names + types), compute overlap matrix (Jaccard similarity), score merge candidates (0–100). Recommend: merge (>70), partial merge (40–70), keep separate (<40). |
| 27.2 | **Merge configuration** | new `src/merge/merge_config.py` | `MergeConfig` model: which projects to merge, conflict resolution rules (rename/skip/override), target Lakehouse/Warehouse. |
| 27.3 | **Dataset deduplication** | new `src/merge/deduplicator.py` | Detect duplicate datasets across projects (same schema, overlapping data). Propose single consolidated table with source tracking. |
| 27.4 | **Shared pipeline builder** | `src/merge/`, `agents/flow_pipeline_agent.py` | Merge individual project flows into a single consolidated Fabric pipeline with proper dependency ordering. |
| 27.5 | **Merge report** | new `src/reports/merge_report.py` | HTML report: N×N overlap heatmap, merge candidates, conflict summary, deduplication savings. |
| 27.6 | **CLI integration** | `cli.py` | `dataiku-to-fabric merge --projects A,B,C --config merge.yaml` runs multi-project merge analysis. `--execute` actually performs the merge migration. |
| 27.7 | **Tests** | `tests/test_merge.py` | 30+ tests: overlap calculation, merge scoring, conflict resolution, deduplication, pipeline merge, report generation. |

### Gate

```
merge --projects A,B,C → overlap heatmap, merge recommendations
--execute → consolidated Fabric assets
30+ tests green
```

---

## Phase 28 — Enterprise Wave Planning

> Topology-aware migration planning with effort estimation and wave assignment.
> *Ported from: `migration_planner.py`, `dependency_graph.py`, Enterprise Guide*

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 28.1 | **Wave planner** | new `src/planner/wave_planner.py` | Assign assets to migration waves based on dependency order and complexity. Wave tiers: Easy (< 4h), Medium (4–12h), Complex (12–40h+). |
| 28.2 | **Effort estimator** | new `src/planner/effort_estimator.py` | Per-asset effort estimate based on: SQL complexity score, Python SDK call count, visual recipe count, data volume, connection complexity. Output in person-hours. |
| 28.3 | **Dependency topology** | `src/planner/wave_planner.py`, `core/registry.py` | Site-wide dependency graph: which projects depend on shared datasets, cross-project flows. Topological sort for wave assignment. |
| 28.4 | **Resource allocation** | `src/planner/wave_planner.py` | Suggest team allocation: SQL expert (Oracle/PG), Python expert, Fabric admin, data engineer. Based on asset mix in each wave. |
| 28.5 | **Planning report** | new `src/reports/planning_report.py` | HTML report: Gantt-style wave timeline, effort breakdown by category, resource allocation, critical path highlight. |
| 28.6 | **CLI integration** | `cli.py` | `dataiku-to-fabric plan --projects A,B,C` generates wave plan. `--waves N` sets max wave count. `--team-size N` constrains parallelism. |
| 28.7 | **Tests** | `tests/test_wave_planning.py` | 20+ tests: wave assignment, effort estimation, dependency ordering, resource allocation, report generation. |

### Gate

```
plan --projects A,B,C → wave plan with effort estimates
Dependency order respected (no wave depends on later wave)
20+ tests green
```

---

## Phase 29 — Web UI (Streamlit)

> Interactive web interface for non-CLI users.
> *Ported from: `web/app.py` (Streamlit 6-step wizard)*

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 29.1 | **Streamlit app scaffold** | new `web/app.py` | 6-step wizard: Connect → Discover → Assess → Migrate → Validate → Download. Session state management. |
| 29.2 | **Connect page** | `web/app.py` | Input Dataiku URL + API key, Fabric workspace + credentials. Test connections. Save to session config. |
| 29.3 | **Discover page** | `web/app.py` | Run discovery agent, display asset table (type, name, dependencies). Filter/search. Select assets to migrate. |
| 29.4 | **Assess page** | `web/app.py` | Run assessment (Phase 19), display radar chart, readiness score, risk table. Strategy recommendation. |
| 29.5 | **Migrate page** | `web/app.py` | Start migration with progress bars. Real-time log streaming. Show agent status (running/completed/failed). |
| 29.6 | **Validate & Download** | `web/app.py` | Run QA suite (Phase 20), display results. Download button for migration output (ZIP). |
| 29.7 | **Docker packaging** | `Dockerfile.web` | Streamlit app in Docker container. Expose port 8501. |
| 29.8 | **Tests** | `tests/test_web_ui.py` | 15+ tests: page rendering, session state, API calls, error handling. |

### Gate

```
streamlit run web/app.py → 6-step wizard functional
Docker build + run → accessible at localhost:8501
15+ tests green
```

---

## Phase 30 — Bundle Deployment & CI/CD

> Atomic multi-artifact deployment and production CI/CD pipeline.
> *Ported from: bundle deployment, CI/CD pipeline, `cutover_manager.py`*

### Tasks

| # | Task | File(s) | What |
|---|------|---------|------|
| 30.1 | **Bundle builder** | new `src/deploy/bundle_builder.py` | Package all migration artifacts into a deployment bundle: notebooks, pipelines, DDL, connections, configs. Single ZIP with manifest. |
| 30.2 | **Atomic deployer** | new `src/deploy/bundle_deployer.py` | Deploy bundle as atomic unit: all-or-nothing. If any item fails, rollback all previously deployed items in this bundle. Uses Phase 14 rollback. |
| 30.3 | **Pre-flight validation** | new `src/deploy/preflight.py` | Before deployment: check Fabric workspace exists, capacity available, permissions valid, no naming conflicts. Abort with actionable errors if checks fail. |
| 30.4 | **Cutover manager** | new `src/deploy/cutover_manager.py` | Generate cutover plan: pre-deploy snapshot, deployment steps, post-deploy validation, rollback trigger conditions. Support parallel run (old + new) mode. |
| 30.5 | **CI/CD pipeline template** | `.github/workflows/migration.yml` | GitHub Actions: Lint (flake8+ruff) → Test (pytest) → Assess → Migrate (staging) → Validate → Deploy (production, manual approval). Matrix: Python 3.10/3.12/3.14. |
| 30.6 | **Environment configs** | `config/environments/` | `development.yaml`, `staging.yaml`, `production.yaml`. Different settings per environment (log level, retry count, approval required). |
| 30.7 | **Tests** | `tests/test_bundle_deploy.py` | 20+ tests: bundle creation, atomic deploy, rollback on failure, preflight checks, cutover plan generation. |

### Gate

```
Bundle deploy → all items deployed or none (atomic)
Preflight catches missing workspace, insufficient permissions
CI/CD pipeline: lint → test → validate → stage → prod
20+ tests green
```

---

## Revised Phase Dependency Graph (12–30)

```
Phase 12 — Fabric Deployment        (foundation for 14, 17, 18, 30)
Phase 13 — Security                 (needs 12)
Phase 14 — Rollback                 (needs 12)
Phase 15 — Observability            (needs 12)
  ↓
Phase 16 — Advanced Recipes         (parallel, no hard deps)
Phase 17 — Dashboard → PBI          (needs 12)
Phase 18 — ML Model → Fabric ML     (needs 12)
  ↓
Phase 19 — Assessment               (no deps, can start after 11)  ← P0
Phase 20 — QA Suite                 (benefits from 19 for scoring) ← P0
  ↓
Phase 21 — Self-Healing             (needs 20 for QA integration)  ← P1
Phase 22 — Schema Drift             (no deps, parallel)           ← P1
Phase 23 — Lineage Map              (no deps, parallel)           ← P1
Phase 24 — REST API Server          (needs 19, 20 for endpoints)  ← P1
  ↓
Phase 25 — Plugin System            (needs stable agent API)      ← P2
Phase 26 — Equivalence & Regression (needs 12 for cross-platform) ← P2
Phase 27 — Multi-Project Merge      (needs 19, 23 for analysis)   ← P2
Phase 28 — Enterprise Planning      (needs 19, 23 for scoring)    ← P2
  ↓
Phase 29 — Web UI                   (needs 19, 20, 24 as backend) ← P3
Phase 30 — Bundle Deploy & CI/CD    (needs 12, 14 for deploy+rollback) ← P3
```

---

## Agent Expansion Plan

Current: **9 agents** → Target: **14 agents** (matching T2PBI's 14-agent model)

| # | Agent | New? | Owns |
|---|-------|------|------|
| 1 | `@orchestrator` | Existing | `cli.py`, `core/orchestrator.py`, `core/config.py`, `core/registry.py` |
| 2 | `@extractor` | Existing | `agents/discovery_agent.py`, `connectors/dataiku_client.py` |
| 3 | `@sql_converter` | Existing | `agents/sql_migration_agent.py`, `translators/sql_translator.py`, `translators/oracle_to_tsql.py`, `translators/postgres_to_tsql.py` |
| 4 | `@python_converter` | Existing | `agents/python_migration_agent.py`, `translators/python_to_notebook.py` |
| 5 | `@visual_converter` | Existing | `agents/visual_recipe_agent.py` |
| 6 | `@dataset_migrator` | Existing | `agents/dataset_agent.py` |
| 7 | `@pipeline_builder` | Existing | `agents/flow_pipeline_agent.py` |
| 8 | `@connection_mapper` | Existing | `agents/connection_agent.py`, `connectors/fabric_client.py` |
| 9 | `@validator` | Existing | `agents/validation_agent.py`, `models/report.py` |
| 10 | **`@assessor`** | **New** | `analyzers/`, `reports/assessment_report.py` |
| 11 | **`@healer`** | **New** | `healers/`, QA auto-fix pipeline |
| 12 | **`@deployer`** | **New** | `deploy/`, bundle builder, cutover manager |
| 13 | **`@planner`** | **New** | `planner/`, wave planning, effort estimation |
| 14 | **`@tester`** | Existing | `tests/`, equivalence, regression |

---

## New Module Structure

```
src/
├── agents/          # Existing 8 migration agents
├── analyzers/       # NEW — Phase 19: assessment, strategy
│   ├── project_analyzer.py
│   └── strategy_advisor.py
├── api/             # NEW — Phase 24: REST API server
│   ├── server.py
│   └── job_manager.py
├── connectors/      # Existing — Dataiku + Fabric clients
├── core/            # Existing — config, registry, logger, orchestrator
├── deploy/          # NEW — Phase 30: bundle, cutover, preflight
│   ├── bundle_builder.py
│   ├── bundle_deployer.py
│   ├── cutover_manager.py
│   └── preflight.py
├── drift/           # NEW — Phase 22: schema drift
│   ├── snapshot.py
│   └── drift_detector.py
├── healers/         # NEW — Phase 21: self-healing engine
│   ├── base_healer.py
│   ├── sql_healers.py
│   ├── notebook_healers.py
│   ├── pipeline_healers.py
│   └── schema_healers.py
├── lineage/         # NEW — Phase 23: lineage mapping
│   ├── lineage_model.py
│   └── lineage_builder.py
├── merge/           # NEW — Phase 27: multi-project merge
│   ├── merge_assessment.py
│   ├── merge_config.py
│   └── deduplicator.py
├── models/          # Existing + Phase 19 assessment models
├── planner/         # NEW — Phase 28: wave planning
│   ├── wave_planner.py
│   └── effort_estimator.py
├── plugins/         # NEW — Phase 25: plugin system
│   ├── plugin_manager.py
│   └── base_plugin.py
├── qa/              # NEW — Phase 20: QA suite
│   ├── qa_suite.py
│   ├── governance.py
│   ├── comparison_report.py
│   ├── cross_validator.py
│   └── fidelity.py
├── reports/         # NEW — centralized report generation
│   ├── assessment_report.py
│   ├── drift_report.py
│   ├── lineage_report.py
│   ├── merge_report.py
│   └── planning_report.py
├── testing/         # NEW — Phase 26: equivalence & regression
│   ├── equivalence_tester.py
│   └── regression_suite.py
└── translators/     # Existing — SQL + Python translators

web/                 # NEW — Phase 29: Streamlit UI
├── app.py
└── Dockerfile.web

plugins/             # NEW — Phase 25: user plugins directory
└── sample_plugin.py
```

---

## CLI Command Summary (v2.0)

| Command | Phase | Description |
|---------|-------|-------------|
| `discover` | 2 | Scan Dataiku project, catalog assets |
| `migrate` | 3–11 | Run migration pipeline |
| `validate` | 7 | Run validation checks |
| `report` | 7 | Generate migration report |
| `config validate` | 10 | Validate config file |
| `status` | 10 | Show migration state |
| **`assess`** | **19** | Pre-migration readiness assessment |
| **`qa`** | **20** | Run QA suite (validate→fix→govern→compare) |
| **`check-drift`** | **22** | Compare migration snapshots |
| **`lineage`** | **23** | Generate lineage map + HTML |
| **`serve`** | **24** | Start REST API server |
| **`plan`** | **28** | Generate enterprise wave plan |
| **`merge`** | **27** | Multi-project merge analysis |
| `migrate --deploy` | 12 | Deploy to Fabric workspace |
| `migrate --rollback` | 14 | Rollback deployed assets |
| `migrate --auto-fix` | 21 | Enable self-healing |
| `migrate --incremental` | 11 | Incremental (watermark) migration |
| `migrate --save-baseline` | 26 | Save regression baseline |
| `migrate --check-regression` | 26 | Check against baseline |

---

## CLI Flags Summary (v2.0)

| Flag | Phase | Description |
|------|-------|-------------|
| `--dry-run` | 10 | Print plan without executing |
| `--interactive` | 10 | Interactive wizard mode |
| `--resume` | 9 | Resume from checkpoint |
| `--rerun AGENT` | 9 | Re-run specific agent |
| `--asset-ids X,Y` | 9 | Process specific assets |
| `--output-format` | 10 | json / table / yaml |
| `--quiet` | 10 | Suppress progress output |
| **`--deploy`** | **12** | Deploy to Fabric |
| **`--rollback`** | **14** | Rollback deployed assets |
| **`--auto-fix`** | **21** | Enable self-healing |
| **`--no-compare`** | **20** | Skip comparison report |
| **`--fail-on-breaking`** | **22** | CI gate for drift |
| **`--fail-on-regression`** | **26** | CI gate for regression |
| **`--save-baseline`** | **26** | Save regression snapshot |
| **`--workers N`** | **24/28** | Parallel worker count |
| **`--equivalence`** | **26** | Cross-platform validation |
| **`--batch`** | **27/28** | Multi-project batch mode |

---

## Milestone Targets

| Milestone | Phases | Tests | Key Deliverable |
|-----------|--------|-------|-----------------|
| **v1.0 GA** | 1–11 | 688 | Core migration pipeline |
| **v1.5** | 12–15 | ~800 | Deployment + security + observability |
| **v2.0** | 16–20 | ~950 | Assessment + QA + advanced recipes |
| **v2.5** | 21–24 | ~1,050 | Self-healing + drift + lineage + API |
| **v3.0** | 25–30 | ~1,200 | Enterprise: plugins, merge, planning, web UI, CI/CD |

---

## Recommended Execution Order

### Sprint 1 (P0 — Immediate Value)
1. **Phase 19** — Assessment (no deps, standalone value)
2. **Phase 20** — QA Suite (enhances existing validation)

### Sprint 2 (P0+P1 — Deployment Loop)
3. **Phase 12** — Fabric Deployment (enables real usage)
4. **Phase 14** — Rollback (safety net for deployment)
5. **Phase 13** — Security (required before production)

### Sprint 3 (P1 — Intelligence)
6. **Phase 21** — Self-Healing (auto-fix reduces manual work)
7. **Phase 22** — Schema Drift (CI gate for changes)
8. **Phase 23** — Lineage Map (project understanding)

### Sprint 4 (P1 — Operations)
9. **Phase 15** — Observability (production monitoring)
10. **Phase 24** — REST API (programmatic access)

### Sprint 5 (P2 — Enterprise)
11. **Phase 25** — Plugin System
12. **Phase 26** — Equivalence & Regression
13. **Phase 16** — Advanced Recipes

### Sprint 6 (P2 — Scale)
14. **Phase 27** — Multi-Project Merge
15. **Phase 28** — Enterprise Wave Planning
16. **Phase 17** — Dashboard → Power BI
17. **Phase 18** — ML Model → Fabric ML

### Sprint 7 (P3 — Polish)
18. **Phase 29** — Web UI
19. **Phase 30** — Bundle Deploy & CI/CD

---

## Risk Register (New Items)

| # | Risk | Impact | Mitigation |
|---|------|--------|------------|
| 13 | Self-healing makes wrong fixes | High | Auto-fix generates before/after diff, requires `--auto-fix` explicit opt-in |
| 14 | REST API security (unauthenticated) | High | Add API key auth, bind to localhost by default, CORS restrictions |
| 15 | Plugin code execution (arbitrary code) | High | Plugin sandbox: no file system writes outside output/, log all plugin actions |
| 16 | Multi-project merge data loss | High | Merge assessment before execution, backup before merge, rollback support |
| 17 | Schema drift false positives | Medium | Configurable ignore rules, severity thresholds |
| 18 | Streamlit dependency for web UI | Low | Web UI is optional, CLI is primary interface |
| 19 | Assessment accuracy (over/under-scoring) | Medium | Calibrate against real migrations, adjustable weights |
| 20 | Bundle deploy partial failure in non-transactional API | High | Track deployed items, rollback list, retry failed items |
