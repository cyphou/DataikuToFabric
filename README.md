# 🚀 Dataiku → Microsoft Fabric Migration Toolkit

[![CI](https://github.com/cyphou/DataikuToFabric/actions/workflows/ci.yml/badge.svg)](https://github.com/cyphou/DataikuToFabric/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **Automated, agent-driven migration platform** to convert Dataiku projects into native Microsoft Fabric assets — SQL, Notebooks, Pipelines, Lakehouses & Power BI.

---

## 📋 Table of Contents

| # | Section | Description |
|---|---------|-------------|
| 1 | [Overview](#-overview) | What this toolkit does |
| 2 | [Architecture](#-high-level-architecture) | System design & data flow |
| 3 | [Agents](#-agent-catalog) | All migration agents |
| 4 | [Migration Scope](#-migration-scope) | Dataiku → Fabric mapping |
| 5 | [Getting Started](#-getting-started) | Setup & prerequisites |
| 6 | [Examples](#-examples) | Real input/output migration samples |
| 7 | [Project Structure](#-project-structure) | Folder layout |
| 8 | [Docs](#-documentation) | Setup, troubleshooting, changelog |

---

## 🎯 Overview

The **Dataiku-to-Fabric Migration Toolkit** is a modular, agent-based system that:

- 🔍 **Discovers** all assets in a Dataiku project (SQL recipes, Python recipes, datasets, flows, connections)
- 🔄 **Converts** each asset type to its native Fabric equivalent
- ✅ **Validates** the migrated output for correctness and compatibility
- 📦 **Deploys** converted assets into a target Fabric workspace

### Why Agent-Based?

Each Dataiku asset type has unique semantics. A dedicated agent per asset type ensures:

| Benefit | Detail |
|---------|--------|
| 🎯 **Precision** | Deep knowledge of source & target formats |
| 🔀 **Parallelism** | Independent agents can run concurrently |
| 🧩 **Modularity** | Add/remove agents without affecting others |
| 🔁 **Retry** | Failed agents can re-run in isolation |

---

## 🏗️ High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     🎛️  ORCHESTRATION AGENT                        │
│         Coordinates discovery, conversion, validation & deploy      │
└────────┬──────────┬──────────┬──────────┬──────────┬───────────────┘
         │          │          │          │          │
         ▼          ▼          ▼          ▼          ▼
┌────────────┐┌──────────┐┌──────────┐┌──────────┐┌──────────────┐
│ 🔍 DISCOVERY││ 📝 SQL   ││ 🐍 PYTHON││ 📊 VISUAL││ 🔗 CONNECTION│
│   AGENT    ││ AGENT    ││  AGENT   ││  RECIPE  ││   AGENT      │
│            ││          ││          ││  AGENT   ││              │
│ Scan all   ││ Convert  ││ Convert  ││ Convert  ││ Map Dataiku  │
│ Dataiku    ││ SQL      ││ Python   ││ Join/    ││ connections  │
│ assets     ││ recipes  ││ recipes  ││ Group/   ││ to Fabric    │
│            ││ to T-SQL ││ to Note- ││ Filter   ││              │
│            ││ / Spark  ││ books    ││ to SQL   ││              │
└─────┬──────┘└────┬─────┘└────┬─────┘└────┬─────┘└──────┬───────┘
      │            │           │           │             │
      ▼            ▼           ▼           ▼             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    📦 ASSET REGISTRY (JSON)                      │
│   Stores discovered assets, conversion status, dependencies      │
└────────────────────────────┬────────────────────────────────────┘
                             │
                ┌────────────┼────────────┐
                ▼            ▼            ▼
         ┌───────────┐┌──────────┐┌─────────────┐
         │ 🗄️ DATASET ││ 🔄 FLOW  ││ ✅ VALIDATION│
         │  MIGRATION ││ PIPELINE ││    AGENT     │
         │   AGENT   ││  AGENT   ││              │
         │           ││          ││ Test migrated│
         │ Migrate   ││ Convert  ││ assets for   │
         │ schemas & ││ Dataiku  ││ correctness  │
         │ data to   ││ flows to ││              │
         │ Lakehouse ││ Fabric   ││              │
         │           ││ Pipelines││              │
         └───────────┘└──────────┘└──────────────┘
                             │
                             ▼
         ┌───────────────────────────────────────┐
         │      ☁️  MICROSOFT FABRIC WORKSPACE    │
         │                                       │
         │  🗄️ Lakehouse    📊 Data Warehouse     │
         │  📓 Notebooks    🔀 Data Pipelines     │
         │  📈 Power BI     🔗 Connections        │
         └───────────────────────────────────────┘
```

---

## 🤖 Agent Catalog

| # | Agent | Icon | Responsibility | Input | Output |
|---|-------|------|----------------|-------|--------|
| 1 | **Orchestrator** | 🎛️ | Coordinates all agents, manages state & retries | Migration config | Migration report |
| 2 | **Discovery** | 🔍 | Scans Dataiku project via API, catalogs all assets | Dataiku project key | Asset registry JSON |
| 3 | **SQL Migration** | 📝 | Converts SQL recipes → T-SQL / Spark SQL | SQL recipe definitions | Fabric SQL scripts |
| 4 | **Python Migration** | 🐍 | Converts Python recipes → Fabric Notebooks | Python recipe code | `.ipynb` notebooks |
| 5 | **Visual Recipe** | 📊 | Converts visual recipes (Join, Group, Filter, etc.) → SQL | Visual recipe JSON | SQL / Spark SQL |
| 6 | **Dataset Migration** | 🗄️ | Migrates schemas & data → Lakehouse / Warehouse tables | Dataset metadata | Table definitions + data |
| 7 | **Flow → Pipeline** | 🔄 | Converts Dataiku flows/scenarios → Fabric Data Pipelines | Flow graph JSON | Pipeline JSON definitions |
| 8 | **Connection Mapper** | 🔗 | Maps Dataiku connections → Fabric connections/linked services | Connection configs | Fabric connection configs |
| 9 | **Validation** | ✅ | Tests migrated assets — schema match, row counts, logic | Migrated assets | Validation report |

> 📖 Full specifications → [docs/AGENTS.md](docs/AGENTS.md)

---

## 🗺️ Migration Scope

### Dataiku → Fabric Asset Mapping

```
┌──────────────────────────┐         ┌──────────────────────────────┐
│       DATAIKU             │         │      MICROSOFT FABRIC         │
│                          │         │                              │
│  📝 SQL Recipe ──────────────────▶  📝 Warehouse SQL / Spark SQL  │
│                          │         │                              │
│  🐍 Python Recipe ───────────────▶  📓 Fabric Notebook (PySpark)  │
│                          │         │                              │
│  📊 Visual Recipe ───────────────▶  📝 Generated SQL Queries      │
│  (Join/Group/Filter/     │         │                              │
│   Window/Sort/etc.)      │         │                              │
│                          │         │                              │
│  🗄️ Dataset (managed) ───────────▶  🗄️ Lakehouse Table (Delta)    │
│                          │         │                              │
│  🗄️ Dataset (SQL table) ─────────▶  📊 Warehouse Table            │
│                          │         │                              │
│  📁 Managed Folder ──────────────▶  📁 Lakehouse Files Section    │
│                          │         │                              │
│  🔄 Flow Graph ──────────────────▶  🔀 Data Pipeline              │
│                          │         │                              │
│  ⏰ Scenario (Schedule) ─────────▶  ⏰ Pipeline Trigger            │
│                          │         │                              │
│  🔗 Connection ──────────────────▶  🔗 Fabric Connection           │
│  (Oracle / PostgreSQL /  │         │  (OneLake / Shortcut /       │
│   S3 / HDFS / etc.)     │         │   Gateway / Linked Svc)      │
│                          │         │                              │
│  📈 Dashboard ───────────────────▶  📈 Power BI Report             │
│                          │         │                              │
│  🧠 ML Model ───────────────────▶  🧠 Fabric ML Model             │
└──────────────────────────┘         └──────────────────────────────┘
```

### SQL Dialect Translation Matrix

| Dataiku SQL Feature | Fabric Target | Notes |
|--------------------|---------------|-------|
| `SELECT ... FROM` | T-SQL / Spark SQL | Direct mapping |
| Oracle-specific syntax (`ROWNUM`, `CONNECT BY`, `NVL`) | T-SQL equivalents (`TOP`/`ROW_NUMBER`, recursive CTE, `ISNULL`) | Agent handles translation |
| PostgreSQL syntax (`::`, `ILIKE`, `LATERAL`) | T-SQL equivalents | Syntax adaptation layer |
| `CREATE TABLE AS SELECT` | `CREATE TABLE ... AS SELECT` (Spark) or `SELECT INTO` (T-SQL) | Depends on target |
| Window functions | Full support in both targets | Minor syntax differences |
| Stored procedures | T-SQL stored procedures | Requires manual review flag |
| Temp tables (`#tmp`) | Spark temp views / T-SQL temp tables | Context-dependent |

---

## 🚀 Getting Started

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| 🐍 Python | 3.10+ | Core runtime |
| ☁️ Azure CLI | Latest | Fabric API authentication |
| 📦 Fabric REST API | v1 | Deploy assets to Fabric |
| 🔧 Dataiku API | v12+ | Extract project assets |
| 🧪 pytest | 7+ | Testing framework |

### Quick Start

```bash
# 1. Install
pip install "dataiku-to-fabric[all]"
#    — or from source —
git clone https://github.com/cyphou/DataikuToFabric.git
cd DataikuToFabric && pip install -e ".[all]"

# 2. Configure
cp config/config.template.yaml config/config.yaml
# Edit config.yaml with your Dataiku & Fabric credentials

# 3. Discover assets
dataiku-to-fabric discover --project MY_DATAIKU_PROJECT

# 4. Run full migration
dataiku-to-fabric migrate --project MY_DATAIKU_PROJECT --target MY_FABRIC_WORKSPACE

# 5. Validate
dataiku-to-fabric validate --project MY_DATAIKU_PROJECT
```

> 📖 Full setup instructions → [docs/SETUP.md](docs/SETUP.md) · Troubleshooting → [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)

---

## � Examples

The [`examples/`](examples/) directory contains a **complete, real migration walkthrough** for a sample **CUSTOMER_ANALYTICS** Dataiku project — including source assets and the actual output produced by each agent.

| Folder | Contents |
|--------|----------|
| [`examples/input/`](examples/input/) | Dataiku project export — 9 recipes, 5 datasets, 6 connections, 3 scenarios, 1 flow |
| [`examples/output/sql/`](examples/output/sql/) | 8 converted SQL files (Oracle → T-SQL, PostgreSQL → T-SQL, visual recipe → SQL) |
| [`examples/output/notebooks/`](examples/output/notebooks/) | PySpark notebook generated from a Python recipe |
| [`examples/output/ddl/`](examples/output/ddl/) | 5 DDL scripts (Warehouse + Lakehouse / Delta) |
| [`examples/output/pipelines/`](examples/output/pipelines/) | Fabric Data Pipeline JSON + 3 trigger definitions |
| [`examples/output/connections/`](examples/output/connections/) | 6 mapped connection configs (Gateway, OneLake, Lakehouse) |
| [`examples/output/reports/`](examples/output/reports/) | Validation report with per-agent results and review flags |

> 📖 Full walkthrough → [examples/README.md](examples/README.md) · Sample config → [examples/sample_config.yaml](examples/sample_config.yaml)

---

## �📂 Project Structure

```
DataikuToFabric/
├── 📄 README.md                      ← You are here
├── 📄 CHANGELOG.md                   ← Version history
├── 📄 requirements.txt               ← Python dependencies
├── 📄 pyproject.toml                 ← Packaging & tool config
├── 📄 Dockerfile                     ← Container build
├── 📄 MANIFEST.in                    ← sdist includes
│
├── ⚙️ config/
│   ├── config.template.yaml          ← Configuration template
│   └── config.yaml                   ← Local config (git-ignored)
│
├── 📖 docs/
│   ├── AGENTS.md                     ← Agent specifications
│   ├── ARCHITECTURE.md               ← Core system design
│   ├── DEVPLAN.md                    ← Development plan & roadmap
│   ├── SETUP.md                      ← Installation & first-run guide
│   └── TROUBLESHOOTING.md            ← Common errors & fixes
│
├── 🐍 src/
│   ├── __init__.py
│   ├── cli.py                        ← CLI entry point
│   ├── core/
│   │   ├── orchestrator.py           ← 🎛️ DAG-based orchestrator
│   │   ├── registry.py               ← 📦 Asset registry
│   │   ├── config.py                 ← ⚙️ Config loader
│   │   └── logger.py                 ← 📋 Structured logging
│   │
│   ├── agents/
│   │   ├── base_agent.py             ← Abstract agent interface
│   │   ├── discovery_agent.py        ← 🔍 Discovery agent
│   │   ├── sql_migration_agent.py    ← 📝 SQL migration agent
│   │   ├── python_migration_agent.py ← 🐍 Python migration agent
│   │   ├── visual_recipe_agent.py    ← 📊 Visual recipe agent
│   │   ├── dataset_agent.py          ← 🗄️ Dataset migration agent
│   │   ├── flow_pipeline_agent.py    ← 🔄 Flow → Pipeline agent
│   │   ├── connection_agent.py       ← 🔗 Connection mapper agent
│   │   └── validation_agent.py       ← ✅ Validation agent
│   │
│   ├── connectors/
│   │   ├── dataiku_client.py         ← Dataiku API client
│   │   └── fabric_client.py          ← Fabric REST API client
│   │
│   ├── translators/
│   │   ├── sql_translator.py         ← SQL dialect translation
│   │   ├── oracle_to_tsql.py         ← Oracle → T-SQL rules
│   │   ├── postgres_to_tsql.py       ← PostgreSQL → T-SQL rules
│   │   └── python_to_notebook.py     ← Python → .ipynb converter
│   │
│   └── models/
│       ├── asset.py                  ← Asset data model
│       ├── migration_state.py        ← Migration state machine
│       └── report.py                 ← Report data model
│
├── 🧪 tests/
│   ├── test_*.py                     ← Unit tests (566+)
│   ├── fixtures/                     ← Test fixtures & samples
│   └── integration/
│       ├── test_e2e_pipeline.py      ← End-to-end pipeline tests
│       └── test_perf.py              ← Performance / scale tests
│
├── 📝 templates/
│   ├── notebook_template.ipynb       ← Base Fabric notebook template
│   ├── pipeline_template.json        ← Base pipeline template
│   └── warehouse_table.sql           ← DDL template
│
└── 🔧 .github/
    ├── workflows/ci.yml              ← GitHub Actions CI
    ├── agents/                       ← Multi-agent definitions
    └── copilot-instructions.md       ← Copilot project rules
```

---

## 📊 Status

| Phase | Status | Description |
|-------|--------|-------------|
| 1 — Core Infrastructure | 🟢 Done | CLI, config, registry, asset model, base agent, logging |
| 2 — Discovery & Connectors | 🟢 Done | Discovery agent, Dataiku + Fabric API clients |
| 3 — SQL Translation | 🟢 Done | Oracle + PostgreSQL → T-SQL / Spark SQL via sqlglot |
| 4 — Python & Visual Recipes | 🟢 Done | Python → Notebook converter, visual recipe → SQL |
| 5 — Dataset, Connection & Pipeline | 🟢 Done | Dataset, connection mapper, flow → pipeline agents |
| 6 — Orchestration & CLI | 🟢 Done | DAG orchestrator, retry logic, full CLI |
| 7 — Validation | 🟢 Done | Schema, SQL, notebook, pipeline, connection validation + reports |
| 8 — Integration & Packaging | 🟢 Done | E2E tests, perf tests, Docker, CI, docs, CHANGELOG |
| 9 — Checkpoint & Resume | 🟢 Done | Checkpoint/resume, selective re-run, asset filtering (621 tests, 85% cov) |
| 10 — CLI Hardening | 🟢 Done | --dry-run, Rich progress bars, interactive wizard, config validate, status, --output-format (659 tests) |

---

## 📖 Documentation

| Doc | Description |
|-----|-------------|
| [docs/SETUP.md](docs/SETUP.md) | Installation, configuration & first run |
| [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Common errors & solutions |
| [docs/AGENTS.md](docs/AGENTS.md) | Full agent specifications |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Core system design |
| [docs/DEVPLAN.md](docs/DEVPLAN.md) | Development roadmap |
| [CHANGELOG.md](CHANGELOG.md) | Version history |
| [examples/README.md](examples/README.md) | Migration examples & walkthrough |

---

> 📖 **Next:** See [docs/SETUP.md](docs/SETUP.md) to get started, [docs/AGENTS.md](docs/AGENTS.md) for full agent specs, and [docs/DEVPLAN.md](docs/DEVPLAN.md) for the dev roadmap.
