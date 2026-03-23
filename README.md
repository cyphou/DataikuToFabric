# 🚀 Dataiku → Microsoft Fabric Migration Toolkit

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
| 6 | [Project Structure](#-project-structure) | Folder layout |

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
# 1. Clone & setup
cd DataikuToFabric
pip install -r requirements.txt

# 2. Configure
cp config/config.template.yaml config/config.yaml
# Edit config.yaml with your Dataiku & Fabric credentials

# 3. Discover assets
python -m src.cli discover --project MY_DATAIKU_PROJECT

# 4. Run full migration
python -m src.cli migrate --project MY_DATAIKU_PROJECT --target MY_FABRIC_WORKSPACE

# 5. Validate
python -m src.cli validate --project MY_DATAIKU_PROJECT
```

---

## 📂 Project Structure

```
DataikuToFabric/
├── 📄 README.md                      ← You are here
├── 📄 requirements.txt               ← Python dependencies
├── ⚙️ config/
│   ├── config.template.yaml          ← Configuration template
│   └── config.yaml                   ← Local config (git-ignored)
│
├── 📖 docs/
│   ├── AGENTS.md                     ← Agent specifications
│   ├── DEVPLAN.md                    ← Development plan & roadmap
│   ├── ARCHITECTURE.md               ← Core system design
│   └── diagrams/                     ← Architecture diagrams
│
├── 🐍 src/
│   ├── __init__.py
│   ├── cli.py                        ← CLI entry point
│   ├── core/
│   │   ├── __init__.py
│   │   ├── orchestrator.py           ← 🎛️ Orchestration agent
│   │   ├── registry.py               ← 📦 Asset registry
│   │   ├── config.py                 ← ⚙️ Config loader
│   │   └── logger.py                 ← 📋 Structured logging
│   │
│   ├── agents/
│   │   ├── __init__.py
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
│   │   ├── __init__.py
│   │   ├── dataiku_client.py         ← Dataiku API client
│   │   └── fabric_client.py          ← Fabric REST API client
│   │
│   ├── translators/
│   │   ├── __init__.py
│   │   ├── sql_translator.py         ← SQL dialect translation
│   │   ├── oracle_to_tsql.py         ← Oracle → T-SQL rules
│   │   ├── postgres_to_tsql.py       ← PostgreSQL → T-SQL rules
│   │   └── python_to_notebook.py     ← Python → .ipynb converter
│   │
│   └── models/
│       ├── __init__.py
│       ├── asset.py                  ← Asset data model
│       ├── migration_state.py        ← Migration state machine
│       └── report.py                 ← Report data model
│
├── 🧪 tests/
│   ├── __init__.py
│   ├── test_discovery.py
│   ├── test_sql_migration.py
│   ├── test_python_migration.py
│   ├── test_validation.py
│   └── fixtures/
│       ├── sample_sql_recipe.json
│       ├── sample_python_recipe.json
│       └── sample_flow.json
│
└── 📝 templates/
    ├── notebook_template.ipynb       ← Base Fabric notebook template
    ├── pipeline_template.json        ← Base pipeline template
    └── warehouse_table.sql           ← DDL template
```

---

## 📊 Status

| Phase | Status | Target |
|-------|--------|--------|
| 📖 Planning & Architecture | 🟢 In Progress | Sprint 0 |
| 🔍 Discovery Agent | ⚪ Not Started | Sprint 1 |
| 📝 SQL Migration Agent | ⚪ Not Started | Sprint 1-2 |
| 🐍 Python Migration Agent | ⚪ Not Started | Sprint 2 |
| 📊 Visual Recipe Agent | ⚪ Not Started | Sprint 2-3 |
| 🗄️ Dataset Migration Agent | ⚪ Not Started | Sprint 3 |
| 🔄 Flow → Pipeline Agent | ⚪ Not Started | Sprint 3-4 |
| ✅ Validation Agent | ⚪ Not Started | Sprint 4 |
| 🎛️ Orchestrator + CLI | ⚪ Not Started | Sprint 4-5 |
| 🧪 Integration Testing | ⚪ Not Started | Sprint 5 |

---

> 📖 **Next:** See [docs/AGENTS.md](docs/AGENTS.md) for full agent specs, [docs/DEVPLAN.md](docs/DEVPLAN.md) for the dev roadmap, and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the core system design.
