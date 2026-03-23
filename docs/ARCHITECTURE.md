# 🏛️ Core System Architecture — Dataiku → Fabric Migration Toolkit

> Deep dive into the internal design: components, data flow, state management, and extension points.

---

## 📋 Table of Contents

| # | Section |
|---|---------|
| 1 | [System Overview](#-system-overview) |
| 2 | [Component Architecture](#-component-architecture) |
| 3 | [Data Flow](#-data-flow) |
| 4 | [Asset Registry Design](#-asset-registry-design) |
| 5 | [Configuration System](#️-configuration-system) |
| 6 | [State Machine](#-state-machine) |
| 7 | [Error Handling & Resilience](#-error-handling--resilience) |
| 8 | [Security Model](#-security-model) |
| 9 | [Extension Points](#-extension-points) |
| 10 | [Technology Stack](#-technology-stack) |

---

## 🌐 System Overview

The toolkit follows a **pipeline architecture** with an **agent-based execution model**. The core principle is:

> **Discover → Convert → Validate → Deploy**

```
┌──────────────────────────────────────────────────────────────────┐
│                    MIGRATION TOOLKIT                              │
│                                                                  │
│  ┌──────────┐    ┌───────────────────────────────────┐          │
│  │ 🖥️ CLI    │───▶│          🎛️ ORCHESTRATOR          │          │
│  │          │    │                                   │          │
│  │ Commands:│    │  ┌─────────┐  ┌────────────────┐  │          │
│  │ discover │    │  │ Config  │  │ Agent Dispatch │  │          │
│  │ migrate  │    │  │ Loader  │  │ Engine         │  │          │
│  │ validate │    │  └─────────┘  └───────┬────────┘  │          │
│  │ report   │    │                       │           │          │
│  └──────────┘    └───────────────────────┼───────────┘          │
│                                          │                      │
│                    ┌─────────────────────┼──────────────────┐   │
│                    │      AGENT POOL     │                  │   │
│                    │                     ▼                  │   │
│                    │  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐     │   │
│                    │  │ 🔍  │ │ 📝  │ │ 🐍  │ │ 📊  │ ... │   │
│                    │  └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘     │   │
│                    │     │      │      │      │          │   │
│                    └─────┼──────┼──────┼──────┼──────────┘   │
│                          │      │      │      │              │
│                    ┌─────▼──────▼──────▼──────▼───────────┐  │
│                    │       📦 ASSET REGISTRY               │  │
│                    │  (Central state store — JSON files)    │  │
│                    └──────────────┬────────────────────────┘  │
│                                  │                            │
│  ┌───────────────────────────────┼────────────────────────┐  │
│  │               CONNECTORS     │                         │  │
│  │  ┌────────────────┐  ┌──────▼─────────┐               │  │
│  │  │ 🔧 Dataiku     │  │ ☁️ Fabric       │               │  │
│  │  │    Client      │  │    Client       │               │  │
│  │  │  (REST API)    │  │  (REST API)     │               │  │
│  │  └────────────────┘  └────────────────┘               │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌──────────────────────────────────────────────────────────┐│
│  │                  TRANSLATORS                              ││
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐   ││
│  │  │ Oracle   │ │ Postgres │ │ Python   │ │ Visual   │   ││
│  │  │ → T-SQL  │ │ → T-SQL  │ │ → Notebook│ │ → SQL    │   ││
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘   ││
│  └──────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────┘
```

---

## 🧱 Component Architecture

### Layer Diagram

```
┌──────────────────────────────────────────┐
│            PRESENTATION LAYER             │
│  🖥️ CLI (Click)  │  📊 Report Generator   │
├──────────────────────────────────────────┤
│            ORCHESTRATION LAYER            │
│  🎛️ Orchestrator  │  📋 State Machine     │
├──────────────────────────────────────────┤
│              AGENT LAYER                  │
│  🔍 Discovery  │  📝 SQL  │  🐍 Python    │
│  📊 Visual  │  🗄️ Dataset  │  🔄 Flow     │
│  🔗 Connection  │  ✅ Validation           │
├──────────────────────────────────────────┤
│            TRANSLATION LAYER              │
│  🔄 SQL Translator  │  📓 Notebook Gen    │
│  🔶 Oracle Rules  │  🐘 PostgreSQL Rules  │
├──────────────────────────────────────────┤
│            CONNECTOR LAYER                │
│  🔧 Dataiku Client  │  ☁️ Fabric Client   │
├──────────────────────────────────────────┤
│              MODEL LAYER                  │
│  📐 Asset  │  📋 State  │  📊 Report      │
└──────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Module | Responsibility |
|-----------|--------|----------------|
| **CLI** | `src/cli.py` | Parse commands, invoke orchestrator, display results |
| **Orchestrator** | `src/core/orchestrator.py` | DAG resolution, parallel dispatch, retry, reporting |
| **Registry** | `src/core/registry.py` | CRUD for discovered/migrated assets, query by type/state |
| **Config** | `src/core/config.py` | Load YAML config, validate, provide typed access |
| **Logger** | `src/core/logger.py` | Structured JSON logging with correlation IDs |
| **Agents** | `src/agents/*.py` | Domain-specific migration logic |
| **Translators** | `src/translators/*.py` | Syntax conversion (SQL dialects, Python SDK) |
| **Connectors** | `src/connectors/*.py` | API clients for Dataiku and Fabric |
| **Models** | `src/models/*.py` | Data classes for assets, states, reports |

---

## 🔄 Data Flow

### End-to-End Migration Flow

```
 ① DISCOVER                  ② CONVERT                   ③ VALIDATE
 ──────────                  ─────────                   ──────────

 Dataiku                     Local                       Fabric
 Project                     Processing                  Workspace
    │                           │                           │
    │  GET /recipes/            │                           │
    │  GET /datasets/           │                           │
    │  GET /flow/               │                           │
    ▼                           │                           │
 ┌──────────┐                  │                           │
 │ raw API  │    parse &        │                           │
 │ response │──────────────▶ ┌──┴──────────┐               │
 └──────────┘  classify      │ Asset       │               │
                              │ Registry   │               │
                              │ (JSON)     │               │
                              └──┬──────────┘               │
                                 │  for each asset          │
                                 ▼                          │
                              ┌────────────┐               │
                              │ Translator │               │
                              │ Pipeline   │               │
                              └──┬─────────┘               │
                                 │                          │
                   ┌─────────────┼─────────────┐           │
                   ▼             ▼             ▼           │
              ┌─────────┐ ┌──────────┐ ┌──────────┐       │
              │ .sql    │ │ .ipynb   │ │ pipeline │       │
              │ scripts │ │ notebook │ │ .json    │       │
              └────┬────┘ └────┬─────┘ └────┬─────┘       │
                   │           │            │              │
                   └───────────┼────────────┘              │
                               │  deploy via               │
                               │  Fabric API               │
                               ▼                           │
                          ┌────────────┐              ┌────┴───┐
                          │ Fabric     │─────────────▶│ Assets │
                          │ REST API   │  created     │in WS   │
                          └────────────┘              └────────┘
                                                          │
                                                    ③ Validate
                                                          │
                                                    ┌─────▼──────┐
                                                    │ Validation │
                                                    │ Report     │
                                                    └────────────┘
```

---

## 📦 Asset Registry Design

The registry is the **single source of truth** for all migration state.

### Schema

```json
{
  "$schema": "asset-registry-v1",
  "project": {
    "dataiku_key": "CUSTOMER_360",
    "dataiku_url": "https://dataiku.company.com",
    "fabric_workspace": "ws-customer-360",
    "migration_id": "mig-20260323-001",
    "created_at": "2026-03-23T10:00:00Z"
  },
  "assets": [
    {
      "id": "recipe_sql_compute_customers",
      "type": "recipe.sql",
      "name": "compute_customers",
      "state": "discovered",
      "dependencies": ["dataset_raw_customers", "dataset_raw_orders"],
      "metadata": { ... },
      "target": null,
      "errors": [],
      "review_flags": [],
      "timestamps": {
        "discovered_at": "2026-03-23T10:01:00Z",
        "converted_at": null,
        "validated_at": null,
        "deployed_at": null
      }
    }
  ],
  "statistics": {
    "total": 50,
    "by_state": {
      "discovered": 50,
      "converting": 0,
      "converted": 0,
      "deploying": 0,
      "deployed": 0,
      "validated": 0,
      "failed": 0
    },
    "by_type": {
      "recipe.sql": 15,
      "recipe.python": 10,
      "recipe.visual": 8,
      "dataset": 12,
      "flow": 1,
      "connection": 4
    }
  }
}
```

### Registry Operations

```
  ┌────────────────────────────────────────────┐
  │           AssetRegistry API                │
  ├────────────────────────────────────────────┤
  │                                            │
  │  📝 Write Operations:                      │
  │  ├── add_asset(asset)                      │
  │  ├── update_state(id, new_state)           │
  │  ├── set_target(id, fabric_asset_info)     │
  │  ├── add_error(id, error)                  │
  │  └── add_review_flag(id, flag)             │
  │                                            │
  │  🔍 Query Operations:                      │
  │  ├── get_asset(id) → Asset                 │
  │  ├── get_by_type(type) → List[Asset]       │
  │  ├── get_by_state(state) → List[Asset]     │
  │  ├── get_dependencies(id) → List[Asset]    │
  │  └── get_statistics() → Stats              │
  │                                            │
  │  📊 Reporting:                              │
  │  ├── to_json() → str                       │
  │  └── generate_report() → MigrationReport   │
  │                                            │
  └────────────────────────────────────────────┘
```

---

## ⚙️ Configuration System

### Config File Structure

```yaml
# config/config.yaml

# ─── Source: Dataiku ─────────────────────────
dataiku:
  url: "https://dataiku.company.com"
  api_key_env: "DATAIKU_API_KEY"      # Env var name (never store secrets in config)
  project_key: "CUSTOMER_360"
  timeout_seconds: 30
  max_retries: 3

# ─── Target: Microsoft Fabric ───────────────
fabric:
  workspace_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  lakehouse_name: "lh_customer_360"
  warehouse_name: "wh_customer_360"
  auth_method: "azure_cli"            # azure_cli | service_principal | managed_identity
  tenant_id_env: "AZURE_TENANT_ID"
  client_id_env: "AZURE_CLIENT_ID"

# ─── Migration Settings ─────────────────────
migration:
  target_sql_dialect: "tsql"          # tsql | spark_sql
  default_storage: "lakehouse"        # lakehouse | warehouse
  parallel_agents: true
  max_concurrent_agents: 4
  fail_fast: false
  output_dir: "./output"

# ─── Orchestrator ────────────────────────────
orchestrator:
  max_retries: 3
  retry_delay_seconds: 5
  report_format:
    - html
    - json

# ─── Logging ─────────────────────────────────
logging:
  level: "INFO"                       # DEBUG | INFO | WARNING | ERROR
  format: "json"                      # json | text
  file: "./logs/migration.log"
```

### Config Loader Design

```
  config.yaml ──▶ YAML Parser ──▶ Schema Validator ──▶ Config Object
                                       │
                                       ▼
                                 ┌───────────┐
                                 │ Pydantic  │
                                 │ Model     │
                                 │ (typed)   │
                                 └───────────┘
                                       │
                              Env var resolution
                              ($DATAIKU_API_KEY)
                                       │
                                       ▼
                                 ┌───────────┐
                                 │ Resolved  │
                                 │ Config    │
                                 └───────────┘
```

---

## 🔄 State Machine

Each asset follows this state machine:

```
                          ┌─────────────┐
                          │ DISCOVERED  │
                          └──────┬──────┘
                                 │ Agent picks up
                                 ▼
                          ┌─────────────┐
                     ┌───▶│ CONVERTING  │◀──┐
                     │    └──────┬──────┘   │
                     │           │          │
                     │    ┌──────┴──────┐   │
                     │    │   success?  │   │
                     │    └──┬──────┬───┘   │
                     │       │      │       │
                     │   YES │      │ NO    │ retry
                     │       ▼      ▼       │
                     │ ┌─────────┐ ┌────────┴┐
                     │ │CONVERTED│ │ FAILED  │
                     │ └────┬────┘ └─────────┘
                     │      │
                     │      ▼
                     │ ┌─────────┐
                     │ │DEPLOYING│
                     │ └────┬────┘
                     │      │
                     │ ┌────┴─────┐
                     │ │ success? │
                     │ └──┬────┬──┘
                     │    │    │
                     │YES │    │ NO
                     │    ▼    ▼
                     │┌────────┐┌────────┐
                     ││DEPLOYED││DEPLOY_ │
                     │└───┬────┘│FAILED  │
                     │    │     └────────┘
                     │    ▼
                     │┌──────────┐
                     ││VALIDATING│
                     │└────┬─────┘
                     │     │
                     │┌────┴─────┐
                     ││ success? │
                     │└──┬────┬──┘
                     │   │    │
                     │YES│    │ NO
                     │   ▼    ▼
                     │┌─────────┐┌──────────┐
                     ││VALIDATED││VALIDATION│───▶ (can re-convert)
                     │└─────────┘│_FAILED   │
                     │           └──────────┘
                     │
                     └── retry (from any FAILED state)
```

### State Transitions

| From | To | Trigger |
|------|-----|---------|
| `discovered` | `converting` | Agent starts processing |
| `converting` | `converted` | Conversion successful |
| `converting` | `failed` | Conversion error |
| `converted` | `deploying` | Deploy initiated |
| `deploying` | `deployed` | Deploy successful |
| `deploying` | `deploy_failed` | Deploy error |
| `deployed` | `validating` | Validation started |
| `validating` | `validated` | All checks pass |
| `validating` | `validation_failed` | Some checks fail |
| `*_failed` | `converting` | Retry triggered |

---

## 🛡️ Error Handling & Resilience

### Error Categories

```
┌─────────────────────────────────────────────────────────┐
│                  ERROR TAXONOMY                          │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  🔴 FATAL (abort migration)                             │
│  ├── Authentication failure (Dataiku or Fabric)         │
│  ├── Project not found                                  │
│  └── Configuration invalid                              │
│                                                         │
│  🟡 RECOVERABLE (retry agent)                           │
│  ├── API timeout / rate limit (429)                     │
│  ├── Transient network error                            │
│  └── Fabric API throttling                              │
│                                                         │
│  🔵 SKIPPABLE (flag & continue)                         │
│  ├── Unsupported SQL syntax                             │
│  ├── Unknown visual recipe step                         │
│  ├── Custom Dataiku plugin reference                    │
│  └── PL/SQL block too complex                           │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Retry Strategy

```
  Attempt 1 ──▶ FAIL ──▶ wait 5s ──▶ Attempt 2 ──▶ FAIL ──▶ wait 10s ──▶ Attempt 3 ──▶ FAIL ──▶ ABORT
                          (base)                     (2×base)                (4×base)
                          
  Exponential backoff with jitter:
    delay = base_delay × (2 ^ attempt) + random(0, 1)
```

---

## 🔐 Security Model

### Credential Management

```
  ┌─────────────────────────────┐
  │    CREDENTIAL SOURCES       │
  │    (Priority order)         │
  ├─────────────────────────────┤
  │                             │
  │  1. 🔐 Azure Key Vault     │ ← Recommended for production
  │        │                    │
  │  2. 🏗️ Environment Vars    │ ← Dev / CI
  │        │                    │
  │  3. 🖥️ Azure CLI token     │ ← Local dev (az login)
  │        │                    │
  │  4. 📋 Managed Identity    │ ← Azure-hosted agents
  │                             │
  └─────────────────────────────┘
```

### Security Principles

| # | Principle | Implementation |
|---|-----------|---------------|
| 1 | **No secrets in code** | Env vars or Key Vault references only |
| 2 | **Least privilege** | Dataiku: read-only API key. Fabric: contributor role |
| 3 | **Audit trail** | All API calls logged with correlation ID |
| 4 | **Data in transit** | TLS 1.2+ for all API calls |
| 5 | **Data at rest** | Staging files encrypted (OS-level) |
| 6 | **API key rotation** | Support for refreshable tokens |

---

## 🔌 Extension Points

The system is designed for extensibility:

### Adding a New SQL Dialect

```python
# 1. Create translator: src/translators/mysql_to_tsql.py
class MySQLToTSQL(BaseDialectTranslator):
    source_dialect = "mysql"
    target_dialect = "tsql"
    
    rules = [
        Rule("IFNULL(a, b)", "ISNULL(a, b)"),
        Rule("LIMIT N", "TOP N"),
        # ...
    ]

# 2. Register in sql_translator.py
DIALECT_MAP["mysql"] = MySQLToTSQL
```

### Adding a New Agent

```python
# 1. Create agent: src/agents/dashboard_agent.py
class DashboardAgent(BaseAgent):
    name = "dashboard_migration"
    description = "Converts Dataiku dashboards to Power BI reports"
    
    async def execute(self, context):
        # Migration logic
        ...

# 2. Register in orchestrator config
agents:
  enabled:
    - dashboard_migration
```

### Plugin Architecture

```
  ┌─────────────┐      ┌────────────┐      ┌─────────────┐
  │ Core System │◀────▶│ Plugin API │◀────▶│ Custom      │
  │             │      │            │      │ Plugin      │
  │ • Registry  │      │ • register │      │             │
  │ • Orch.     │      │ • execute  │      │ • mysql     │
  │ • Config    │      │ • validate │      │ • bigquery  │
  │             │      │            │      │ • dashboard │
  └─────────────┘      └────────────┘      └─────────────┘
```

---

## 🧰 Technology Stack

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| **Language** | Python | 3.10+ | Core runtime |
| **CLI** | Click | 8.x | Command-line interface |
| **SQL Parsing** | sqlglot | Latest | Multi-dialect SQL parser/transpiler |
| **Notebook** | nbformat | 5.x | Jupyter notebook format |
| **HTTP** | httpx | 0.25+ | Async HTTP client (Dataiku & Fabric APIs) |
| **Data** | pyarrow | 14+ | Parquet read/write |
| **Config** | Pydantic | 2.x | Config validation & typing |
| **Config File** | PyYAML | 6.x | YAML parsing |
| **Graph** | networkx | 3.x | DAG resolution for flows |
| **Async** | asyncio | stdlib | Concurrent agent execution |
| **Logging** | structlog | 23+ | Structured JSON logging |
| **Testing** | pytest | 7+ | Unit & integration tests |
| **Upload** | azcopy | Latest | Fast OneLake uploads |
| **Auth** | azure-identity | Latest | Azure authentication |

### Dependency Diagram

```
                     ┌─────────────┐
                     │   cli.py    │
                     │  (Click)    │
                     └──────┬──────┘
                            │
                     ┌──────▼──────┐
                     │ orchestrator│
                     │ (asyncio)   │
                     └──────┬──────┘
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │ agents/* │ │connectors│ │translators│
        │          │ │          │ │           │
        │BaseAgent │ │httpx     │ │sqlglot    │
        │          │ │azure-id  │ │nbformat   │
        │          │ │          │ │ast        │
        └──────────┘ └──────────┘ └──────────┘
              │             │             │
              └─────────────┼─────────────┘
                            ▼
                     ┌──────────────┐
                     │   models/*   │
                     │  (Pydantic)  │
                     └──────────────┘
```

---

## 📊 Performance Considerations

| Concern | Strategy |
|---------|----------|
| **Large projects (100+ assets)** | Parallel agent execution (configurable concurrency) |
| **Large datasets (GB+)** | Parquet export + azcopy parallel upload |
| **API rate limits** | Exponential backoff + request queuing |
| **Memory** | Stream processing for large files, don't load all in memory |
| **Idempotency** | Each agent can re-run safely; registry tracks state |

---

> 📖 **Back to:** [README.md](../README.md) · [AGENTS.md](AGENTS.md) · [DEVPLAN.md](DEVPLAN.md)
