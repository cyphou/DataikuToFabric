# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
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
