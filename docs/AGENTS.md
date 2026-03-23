# Multi-Agent Architecture — Dataiku to Fabric Migration

This project uses a **9-agent specialization model**. Each agent has scoped domain knowledge, file ownership, and clear boundaries.

## Quick Reference

| Agent | Invoke When | Owns |
|-------|-------------|------|
| **@orchestrator** | Pipeline coordination, CLI, batch, retry logic | `cli.py`, `core/orchestrator.py`, `core/config.py`, `core/registry.py`, `core/logger.py` |
| **@extractor** | Scanning Dataiku projects, discovering assets | `agents/discovery_agent.py`, `connectors/dataiku_client.py` |
| **@sql_converter** | Converting SQL recipes (Oracle/PG→T-SQL/Spark SQL) | `agents/sql_migration_agent.py`, `translators/sql_translator.py`, `translators/oracle_to_tsql.py`, `translators/postgres_to_tsql.py` |
| **@python_converter** | Converting Python recipes to Notebooks | `agents/python_migration_agent.py`, `translators/python_to_notebook.py` |
| **@visual_converter** | Converting visual recipes to SQL | `agents/visual_recipe_agent.py` |
| **@dataset_migrator** | Migrating schemas and data | `agents/dataset_agent.py` |
| **@pipeline_builder** | Converting flows/scenarios to Pipelines | `agents/flow_pipeline_agent.py` |
| **@connection_mapper** | Mapping connections to Fabric equivalents | `agents/connection_agent.py`, `connectors/fabric_client.py` |
| **@validator** | Validating migrated assets | `agents/validation_agent.py`, `models/report.py` |
| **@tester** | Writing and fixing tests | `tests/*` |

## Architecture Diagram

```
                    ┌──────────────┐
                    │ Orchestrator │  ← CLI entry, pipeline coordination
                    └──────┬───────┘
                           │
          ┌────────────────┼────────────────┐
          │                │                │
    ┌─────▼─────┐   ┌─────▼─────┐   ┌─────▼─────┐
    │ Extractor  │   │ SQL Conv. │   │ Python    │
    │ (Dataiku)  │   │ (sqlglot) │   │ Conv.     │
    └────────────┘   └───────────┘   └───────────┘
                                           │
                    ┌──────────────────┬────┴────┐
                    │                  │         │
              ┌─────▼─────┐    ┌──────▼──┐  ┌───▼────────┐
              │  Dataset   │    │Pipeline│  │ Connection │
              │  Migrator  │    │Builder │  │  Mapper    │
              └────────────┘    └─────────┘  └────────────┘

              ┌──────────────┐
              │  Validator   │  ← Runs after all conversion agents
              └──────────────┘

              ┌────────────────────────────────────────┐
              │              Tester                     │
              │    (Cross-cutting — reads all, writes   │
              │     only to tests/)                     │
              └────────────────────────────────────────┘
```

## Data Flow

```
1. Orchestrator receives CLI command (cli.py)
2. Orchestrator delegates to Extractor → Asset Registry (JSON)
3. Orchestrator delegates conversion agents (parallel where possible):
   a. SQL Converter → T-SQL / Spark SQL scripts
   b. Python Converter → Fabric Notebooks (.ipynb)
   c. Visual Converter → Generated SQL queries
   d. Dataset Migrator → Lakehouse/Warehouse DDL + data
   e. Pipeline Builder → Fabric Pipeline JSON
   f. Connection Mapper → Fabric connection configs
4. Orchestrator delegates to Validator → validation report
5. (Optional) Deploy migrated assets to Fabric workspace
```

## Handoff Protocol

When an agent encounters work outside its domain:

1. **Complete your part** — finish everything within your file scope
2. **State the handoff** — clearly describe what needs to happen next
3. **Name the target agent** — e.g., "Hand off to @sql_converter for T-SQL translation"
4. **List artifacts** — specify files, functions, and data structures involved
5. **Include context** — provide any intermediate results (dicts, JSON) the next agent needs

## File Ownership Rules

- **One owner per file** — each source file has exactly one owning agent
- **Read access is universal** — any agent can read any file for context
- **Write access is restricted** — only the owning agent modifies a file
- **Tester is special** — reads all source files, writes only to `tests/`
- **All communication via Asset Registry** — agents never call each other directly

## When NOT to Use Specialized Agents

Use the **default agent** (or @orchestrator) for:
- Quick questions about the project
- Multi-domain tasks that touch 3+ agents
- Documentation updates (README, CHANGELOG, etc.)
- Sprint planning and gap analysis
- Git operations (commit, push, branch)

## Agent Files

All agent definitions are in `.github/agents/`:
- `shared.instructions.md` — Base rules inherited by all agents
- `orchestrator.agent.md` — Pipeline coordination
- `extractor.agent.md` — Dataiku API scanning
- `sql_converter.agent.md` — SQL dialect translation
- `python_converter.agent.md` — Python → Notebook conversion
- `visual_converter.agent.md` — Visual recipe → SQL
- `dataset_migrator.agent.md` — Schema & data migration
- `pipeline_builder.agent.md` — Flow → Pipeline conversion
- `connection_mapper.agent.md` — Connection mapping
- `validator.agent.md` — Asset validation
- `tester.agent.md` — Test creation and validation

---

## Detailed Agent Specifications

> Each agent is autonomous, stateless per invocation, and communicates via the **Asset Registry**.

### Table of Contents

| # | Agent | Quick Link |
|---|-------|------------|
| 0 | [Agent Contract (Base)](#-agent-contract-base-interface) | Common interface |
| 1 | [Orchestrator Agent](#️-1-orchestrator-agent) | Coordination & state |
| 2 | [Discovery Agent](#-2-discovery-agent) | Asset scanning |
| 3 | [SQL Migration Agent](#-3-sql-migration-agent) | SQL conversion |
| 4 | [Python Migration Agent](#-4-python-migration-agent) | Python → Notebooks |
| 5 | [Visual Recipe Agent](#-5-visual-recipe-agent) | Visual → SQL |
| 6 | [Dataset Migration Agent](#️-6-dataset-migration-agent) | Data transfer |
| 7 | [Flow → Pipeline Agent](#-7-flow--pipeline-agent) | Flow conversion |
| 8 | [Connection Mapper Agent](#-8-connection-mapper-agent) | Connection mapping |
| 9 | [Validation Agent](#-9-validation-agent) | Quality assurance |

---

## 📐 Agent Contract (Base Interface)

Every agent implements the following contract:

```python
class BaseAgent(ABC):
    """
    Base contract for all migration agents.
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Unique agent identifier."""
        
    @property
    @abstractmethod
    def description(self) -> str:
        """Human-readable description."""
    
    @abstractmethod
    async def execute(self, context: MigrationContext) -> AgentResult:
        """
        Run the agent's migration logic.
        
        Args:
            context: Contains config, registry, connectors, and logger.
        Returns:
            AgentResult with status, converted assets, and diagnostics.
        """
    
    @abstractmethod
    async def validate(self, context: MigrationContext) -> ValidationResult:
        """
        Self-check: verify own output is correct.
        """
    
    @abstractmethod
    def rollback(self, context: MigrationContext) -> None:
        """
        Undo changes made by this agent (best-effort).
        """
```

### Agent Lifecycle

```
    ┌─────────┐
    │  IDLE   │
    └────┬────┘
         │  execute() called
         ▼
    ┌─────────┐     success     ┌───────────┐
    │ RUNNING ├────────────────▶│ COMPLETED │
    └────┬────┘                 └───────────┘
         │  error
         ▼
    ┌─────────┐  retry < max    ┌─────────┐
    │ FAILED  ├────────────────▶│ RUNNING │ (retry)
    └────┬────┘                 └─────────┘
         │  max retries
         ▼
    ┌──────────┐
    │ ABORTED  │
    └──────────┘
```

### Communication Pattern

```
 Agent A ──▶ Asset Registry ──▶ Agent B
              (JSON store)
              
 • Agents never call each other directly
 • All state passes through the registry
 • Orchestrator reads registry to decide next steps
```

---

## 🎛️ 1. Orchestrator Agent

### Purpose
Central coordinator that manages the migration lifecycle, dispatches agents, tracks progress, and handles failures.

### Responsibilities

| # | Task | Detail |
|---|------|--------|
| 1 | **Initialization** | Load config, create registry, authenticate to Dataiku & Fabric APIs |
| 2 | **Dependency Resolution** | Build a DAG of agent execution order based on asset dependencies |
| 3 | **Dispatch** | Launch agents in parallel where dependencies allow |
| 4 | **State Management** | Track per-asset migration state (`pending` → `in_progress` → `done` / `failed`) |
| 5 | **Retry Logic** | Re-run failed agents up to `max_retries` (configurable) |
| 6 | **Reporting** | Generate final migration report (HTML + JSON) |
| 7 | **Checkpoint & Resume** | Save registry state after each wave; resume from last checkpoint on `--resume` |
| 8 | **Selective Re-Run** | Re-run specific agents (and downstream) via `--rerun`; filter assets via `--asset-ids` |

### Execution Order (DAG)

```
                    ┌──────────────┐
                    │  🔍 Discovery │ ── Always runs first
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │ 🔗 Conn. │ │ 🗄️ Data- │ │ 📊 Visual│  ── Can run in
        │  Mapper  │ │   sets   │ │  Recipe  │     parallel
        └────┬─────┘ └────┬─────┘ └────┬─────┘
             │            │            │
             ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │ 📝 SQL   │ │ 🐍 Python│ │ 🔄 Flow  │  ── Depends on
        │Migration │ │Migration │ │ Pipeline │     connections
        └────┬─────┘ └────┬─────┘ └────┬─────┘     & datasets
             │            │            │
             └────────────┼────────────┘
                          ▼
                    ┌──────────────┐
                    │ ✅ Validation │ ── Runs last
                    └──────────────┘
```

### Configuration

```yaml
orchestrator:
  max_retries: 3
  parallel_agents: true
  max_concurrent: 4
  fail_fast: false          # Continue other agents on failure
  agent_timeout_seconds: 300
  circuit_breaker_threshold: 3
  report_format: [html, json]
  notifications:
    on_complete: true
    on_failure: true
```

### CLI Flags

| Flag | Description |
|------|-------------|
| `--project` | Dataiku project key |
| `--target` | Fabric workspace name/ID |
| `--agents` | Run only specific agents |
| `--resume` | Resume from last checkpoint |
| `--rerun` | Re-run specific agent(s) and downstream dependents |
| `--asset-ids` | Comma-separated asset IDs to process |
| `--keep-checkpoints` | Don't clean up checkpoint files after success |
| `--fail-fast` | Stop on first agent failure |

---

## 🔍 2. Discovery Agent

### Purpose
Scan a Dataiku project and catalog every asset into the Asset Registry.

### Scanned Asset Types

| # | Asset Type | Dataiku API Endpoint | Registry Category |
|---|-----------|---------------------|-------------------|
| 1 | SQL Recipes | `GET /projects/{key}/recipes/` | `recipes.sql` |
| 2 | Python Recipes | `GET /projects/{key}/recipes/` | `recipes.python` |
| 3 | Visual Recipes | `GET /projects/{key}/recipes/` | `recipes.visual` |
| 4 | Datasets | `GET /projects/{key}/datasets/` | `datasets` |
| 5 | Managed Folders | `GET /projects/{key}/managedfolders/` | `folders` |
| 6 | Connections | `GET /admin/connections/` | `connections` |
| 7 | Flows | `GET /projects/{key}/flow/` | `flows` |
| 8 | Scenarios | `GET /projects/{key}/scenarios/` | `scenarios` |
| 9 | Saved Models | `GET /projects/{key}/savedmodels/` | `models` |
| 10 | Dashboards | `GET /projects/{key}/dashboards/` | `dashboards` |

### Output: Asset Registry Entry

```json
{
  "asset_id": "recipe_sql_compute_customers",
  "asset_type": "recipe.sql",
  "name": "compute_customers",
  "source_project": "CUSTOMER_360",
  "metadata": {
    "sql_dialect": "oracle",
    "input_datasets": ["raw_customers", "raw_orders"],
    "output_datasets": ["computed_customers"],
    "code": "SELECT c.*, COUNT(o.id) as order_count FROM ..."
  },
  "migration_state": "discovered",
  "dependencies": ["dataset_raw_customers", "dataset_raw_orders"],
  "target_fabric_asset": null,
  "errors": []
}
```

### Discovery Flow

```
  Dataiku API                    Discovery Agent              Asset Registry
      │                               │                           │
      │◀── GET /projects/{key} ───────│                           │
      │──── Project metadata ────────▶│                           │
      │                               │                           │
      │◀── GET /recipes/ ─────────────│                           │
      │──── All recipes ─────────────▶│                           │
      │                               ├── Parse & classify ──────▶│
      │◀── GET /datasets/ ────────────│                           │
      │──── All datasets ────────────▶│                           │
      │                               ├── Extract schemas ───────▶│
      │◀── GET /flow/ ────────────────│                           │
      │──── Flow graph ──────────────▶│                           │
      │                               ├── Build dependency map ──▶│
      │                               │                           │
      │                               ├── Final: registry.json ──▶│
      │                               │                           │
```

---

## 📝 3. SQL Migration Agent

### Purpose
Convert Dataiku SQL recipes to Fabric-compatible SQL (T-SQL for Data Warehouse, Spark SQL for Lakehouse).

### Translation Pipeline

```
 ┌───────────────┐    ┌──────────────┐    ┌────────────────┐    ┌────────────┐
 │ Source SQL     │───▶│ SQL Parser   │───▶│ Dialect        │───▶│ Fabric SQL │
 │ (Oracle/PG/   │    │ (sqlglot)    │    │ Translator     │    │ Script     │
 │  MySQL/etc.)  │    │              │    │ (rule engine)  │    │            │
 └───────────────┘    └──────────────┘    └────────────────┘    └────────────┘
                                                │
                                                ▼
                                          ┌────────────┐
                                          │ Human      │
                                          │ Review     │
                                          │ Flags ⚠️   │
                                          └────────────┘
```

### Translation Rules

#### 🔶 Oracle → T-SQL

| Oracle Syntax | T-SQL Equivalent | Auto? |
|--------------|------------------|-------|
| `NVL(a, b)` | `ISNULL(a, b)` | ✅ |
| `NVL2(a, b, c)` | `IIF(a IS NOT NULL, b, c)` | ✅ |
| `SYSDATE` | `GETDATE()` | ✅ |
| `ROWNUM <= N` | `TOP N` / `ROW_NUMBER()` | ✅ |
| `CONNECT BY` | Recursive CTE | ✅ |
| `DECODE(...)` | `CASE WHEN ... END` | ✅ |
| `TO_DATE(str, fmt)` | `CONVERT(DATE, str, style)` | ✅ |
| `TO_CHAR(date, fmt)` | `FORMAT(date, fmt)` | ✅ |
| `MINUS` | `EXCEPT` | ✅ |
| `(+)` outer join | `LEFT/RIGHT JOIN` | ✅ |
| `SEQUENCES.NEXTVAL` | `NEXT VALUE FOR seq` | ✅ |
| PL/SQL blocks | T-SQL procedures | ⚠️ Manual review |
| `DBMS_*` packages | No direct equivalent | 🔴 Flag |
| Materialized views | Indexed views / Spark tables | ⚠️ Manual review |

#### 🐘 PostgreSQL → T-SQL

| PostgreSQL Syntax | T-SQL Equivalent | Auto? |
|------------------|------------------|-------|
| `::type` (cast) | `CAST(x AS type)` | ✅ |
| `ILIKE` | `LOWER(x) LIKE LOWER(y)` | ✅ |
| `LATERAL` | `CROSS APPLY` / `OUTER APPLY` | ✅ |
| `LIMIT N OFFSET M` | `OFFSET M ROWS FETCH NEXT N ROWS ONLY` | ✅ |
| `SERIAL` / `BIGSERIAL` | `IDENTITY(1,1)` | ✅ |
| `BOOLEAN` | `BIT` | ✅ |
| `TEXT` | `NVARCHAR(MAX)` | ✅ |
| `JSONB` operators | `OPENJSON` / `JSON_VALUE` | ✅ |
| `ARRAY` types | JSON array or temp table | ⚠️ Manual review |
| `GENERATE_SERIES` | Recursive CTE / numbers table | ✅ |
| PL/pgSQL functions | T-SQL procedures | ⚠️ Manual review |
| Extensions (`pg_trgm`, etc.) | No equivalent | 🔴 Flag |

### Output Per Recipe

```
output/
├── sql/
│   ├── compute_customers.sql          ← Converted T-SQL
│   ├── compute_customers.review.md    ← Human review notes (if any ⚠️)
│   └── compute_customers.mapping.json ← Source→target column mapping
```

---

## 🐍 4. Python Migration Agent

### Purpose
Convert Dataiku Python recipes into Fabric Notebooks (`.ipynb`), adapting Dataiku SDK calls to Fabric/PySpark equivalents.

### Translation Map

| Dataiku SDK Call | Fabric Equivalent | Notes |
|-----------------|-------------------|-------|
| `dataiku.Dataset("name").get_dataframe()` | `spark.read.table("lakehouse.name")` | Spark DataFrame |
| `dataiku.Dataset("name").write_dataframe(df)` | `df.write.mode("overwrite").saveAsTable("name")` | Delta table |
| `dataiku.Folder("name").get_path()` | `"/lakehouse/default/Files/name"` | Lakehouse Files |
| `dataiku.get_custom_variables()` | `mssparkutils.notebook.run()` params | Pipeline parameters |
| `dataiku.SQLExecutor2(dataset)` | `spark.sql(...)` | Spark SQL executor |
| `dataiku.Model("name")` | `mlflow.load_model(...)` | ML model loading |
| `import dataiku.core.pandasutils` | Remove / replace with pandas | Direct pandas |

### Notebook Structure

```
┌─────────────────────────────────────────────────┐
│  📓 Fabric Notebook: compute_customers          │
├─────────────────────────────────────────────────┤
│  Cell 1 (Markdown):                             │
│  # Migrated from Dataiku                        │
│  # Original recipe: compute_customers           │
│  # Migration date: 2026-03-23                   │
│  # ⚠️ Review flags: [none]                      │
├─────────────────────────────────────────────────┤
│  Cell 2 (Code):                                 │
│  # Configuration & Parameters                   │
│  from pyspark.sql import SparkSession           │
│  spark = SparkSession.builder.getOrCreate()     │
├─────────────────────────────────────────────────┤
│  Cell 3 (Code):                                 │
│  # Input: Load datasets                         │
│  df_customers = spark.read.table("customers")   │
│  df_orders = spark.read.table("orders")         │
├─────────────────────────────────────────────────┤
│  Cell 4 (Code):                                 │
│  # [Migrated logic — user's original code]      │
│  result = df_customers.join(df_orders, ...)     │
├─────────────────────────────────────────────────┤
│  Cell 5 (Code):                                 │
│  # Output: Write results                        │
│  result.write.mode("overwrite")                 │
│    .saveAsTable("computed_customers")            │
└─────────────────────────────────────────────────┘
```

---

## 📊 5. Visual Recipe Agent

### Purpose
Convert Dataiku visual recipes (which are configured via UI, stored as JSON) into equivalent SQL queries or Spark transformations.

### Supported Visual Recipes

| Visual Recipe | Conversion Target | Complexity |
|--------------|-------------------|------------|
| 🔗 **Join** | `SELECT ... FROM a JOIN b ON ...` | 🟢 Low |
| 📊 **Group By** | `SELECT ... GROUP BY ... HAVING ...` | 🟢 Low |
| 🔽 **Filter** | `SELECT ... WHERE ...` | 🟢 Low |
| ↕️ **Sort** | `SELECT ... ORDER BY ...` | 🟢 Low |
| 📐 **Window** | `SELECT ..., ROW_NUMBER() OVER(...)` | 🟡 Medium |
| 🔀 **Union** | `SELECT ... UNION ALL SELECT ...` | 🟢 Low |
| ✂️ **Split** | Multiple `SELECT ... WHERE ...` | 🟡 Medium |
| 🔄 **Pivot** | `PIVOT (... FOR ... IN (...))` | 🟡 Medium |
| 📋 **Prepare** (data cleaning) | Chained `CASE WHEN` / string functions | 🔴 High |
| 📊 **Top N** | `ROW_NUMBER() ... WHERE rn <= N` | 🟢 Low |
| 🗑️ **Distinct** | `SELECT DISTINCT ...` | 🟢 Low |
| 📐 **Sample** | `TABLESAMPLE` / `ORDER BY NEWID()` | 🟡 Medium |

### Prepare Recipe — Step Conversion

The **Prepare recipe** is the most complex visual recipe. Each "step" becomes a SQL expression:

| Prepare Step | SQL Equivalent |
|-------------|----------------|
| Find & Replace | `REPLACE(col, 'old', 'new')` |
| Trim whitespace | `TRIM(col)` / `LTRIM(RTRIM(col))` |
| Change case | `UPPER(col)` / `LOWER(col)` |
| Split column | `SUBSTRING(col, ...)` or `STRING_SPLIT(col, ',')` |
| Merge columns | `CONCAT(col1, ' ', col2)` |
| Parse date | `CONVERT(DATE, col, style)` |
| Fill empty | `ISNULL(col, 'default')` |
| Remove rows where | `WHERE NOT (condition)` |
| Flag with formula | `CASE WHEN formula THEN 1 ELSE 0 END AS flag` |
| Rename column | `col AS new_name` |
| Delete column | Omit from SELECT |
| Deduplicate | `ROW_NUMBER() OVER(PARTITION BY key ORDER BY ...)` |

---

## 🗄️ 6. Dataset Migration Agent

### Purpose
Migrate Dataiku dataset schemas and data to Fabric Lakehouse (Delta tables) or Data Warehouse tables.

### Decision Matrix: Lakehouse vs. Warehouse

```
                Is the target primarily   
                used for SQL analytics?   
                        │                 
               ┌────────┴────────┐        
               │ YES             │ NO     
               ▼                 ▼        
        ┌─────────────┐  ┌─────────────┐
        │ 📊 Data      │  │ 🗄️ Lakehouse │
        │  Warehouse   │  │  (Delta)    │
        │              │  │             │
        │ • T-SQL DDL  │  │ • Spark DDL │
        │ • Indexes    │  │ • No index  │
        │ • Stats      │  │ • Schema    │
        │ • Views      │  │   evolution │
        └─────────────┘  └─────────────┘
```

### Schema Migration

| Dataiku Type | Warehouse Type | Lakehouse (Delta) Type |
|-------------|----------------|----------------------|
| `string` | `NVARCHAR(MAX)` | `STRING` |
| `int` | `INT` | `INT` |
| `bigint` | `BIGINT` | `BIGINT` |
| `float` | `FLOAT` | `FLOAT` |
| `double` | `FLOAT` | `DOUBLE` |
| `boolean` | `BIT` | `BOOLEAN` |
| `date` | `DATE` | `DATE` |
| `array` | `NVARCHAR(MAX)` (JSON) | `ARRAY<...>` |
| `map` | `NVARCHAR(MAX)` (JSON) | `MAP<...>` |
| `object` | `NVARCHAR(MAX)` (JSON) | `STRUCT<...>` |

### Data Transfer Strategy

```
 ┌────────────┐    Export    ┌──────────┐   Upload    ┌───────────┐
 │  Dataiku   │────(CSV/────▶│ Staging  │────────────▶│ OneLake   │
 │  Dataset   │   Parquet)  │  (local) │  (azcopy)  │ /Files    │
 └────────────┘             └──────────┘             └─────┬─────┘
                                                           │
                                                     COPY INTO /
                                                     spark.read
                                                           │
                                                           ▼
                                                    ┌───────────┐
                                                    │ Delta     │
                                                    │ Table     │
                                                    └───────────┘
```

---

## 🔄 7. Flow → Pipeline Agent

### Purpose
Convert Dataiku flow graphs and scenarios into Fabric Data Pipeline definitions.

### Dataiku Flow → Fabric Pipeline Mapping

| Dataiku Concept | Fabric Equivalent |
|----------------|-------------------|
| Flow graph | Data Pipeline |
| Recipe node | Pipeline Activity |
| SQL Recipe | SQL Script Activity / Notebook Activity |
| Python Recipe | Notebook Activity |
| Dataset (intermediate) | Pipeline dataset reference |
| Scenario | Pipeline + Trigger |
| Scenario step | Pipeline Activity |
| Scenario trigger (time-based) | Schedule Trigger |
| Scenario trigger (dataset-change) | Event Trigger (via Event Grid) |
| Scenario reporter | Pipeline alert / notification |

### Pipeline Generation

```
   DATAIKU FLOW                           FABRIC PIPELINE
   ─────────────                          ───────────────
                                          
   ┌─────┐    ┌─────┐                    ┌─────────────┐
   │ DS1 │───▶│ R1  │──┐                 │ Activity 1  │
   └─────┘    │(SQL)│  │                 │ (SQL Script)│
              └─────┘  │    ┌─────┐      └──────┬──────┘
                       ├───▶│ DS3 │             │
              ┌─────┐  │    └─────┘      ┌──────▼──────┐
   ┌─────┐───▶│ R2  │──┘                 │ Activity 2  │
   │ DS2 │    │(Py) │                    │ (Notebook)  │
   └─────┘    └─────┘                    └──────┬──────┘
                                                │
                 │                        ┌──────▼──────┐
                 ▼                        │ Activity 3  │
              ┌─────┐                     │ (Notebook)  │
   ┌─────┐───▶│ R3  │──▶┌─────┐         └─────────────┘
   │ DS3 │    │(Py) │   │ DS4 │
   └─────┘    └─────┘   └─────┘
```

---

## 🔗 8. Connection Mapper Agent

### Purpose
Map Dataiku data connections to their Fabric equivalents.

### Connection Mapping Table

| Dataiku Connection Type | Fabric Equivalent | Migration Path |
|------------------------|-------------------|----------------|
| Oracle (JDBC) | Fabric Data Pipeline (Oracle connector) | Gateway + linked service |
| PostgreSQL (JDBC) | Fabric Data Pipeline (PostgreSQL connector) | Gateway + linked service |
| SQL Server (JDBC) | Fabric Data Warehouse (direct) | Native connection |
| Amazon S3 | OneLake Shortcut (S3) | S3 shortcut config |
| Azure Blob Storage | OneLake Shortcut (ADLS Gen2) | Shortcut config |
| HDFS | OneLake (file upload) | Data migration |
| Google BigQuery | Fabric Data Pipeline (BQ connector) | Pipeline copy activity |
| Snowflake | Fabric Data Pipeline (Snowflake connector) | Pipeline copy activity |
| Local filesystem | Lakehouse Files | Upload to OneLake |
| Custom (plugin) | Varies | ⚠️ Manual review required |

### Connection Output

```json
{
  "connection_id": "oracle_prod",
  "dataiku_type": "Oracle",
  "fabric_type": "OnPremisesDataGateway",
  "config": {
    "gateway_name": "prod-gateway",
    "server": "oracle-prod.company.com",
    "database": "PRODDB",
    "authentication": "service_principal"
  },
  "status": "mapped",
  "manual_steps": [
    "Install and configure On-Premises Data Gateway",
    "Register gateway in Fabric workspace"
  ]
}
```

---

## ✅ 9. Validation Agent

### Purpose
Verify the correctness of all migrated assets through automated testing.

### Validation Checks

| # | Check | Type | Description |
|---|-------|------|-------------|
| 1 | 🧾 **Schema Match** | Structural | Column names, types, and order match source |
| 2 | 🔢 **Row Count** | Data | Source vs. target row counts match |
| 3 | 📊 **Sample Comparison** | Data | Random sample of N rows compared value-by-value |
| 4 | ✅ **SQL Syntax** | Syntactic | Generated SQL parses without errors |
| 5 | 📓 **Notebook Execution** | Runtime | Notebooks run without errors in Fabric |
| 6 | 🔀 **Pipeline Structure** | Structural | Pipeline JSON validates against Fabric schema |
| 7 | 🔗 **Connection Test** | Connectivity | Target connections are reachable |
| 8 | 📈 **Aggregate Match** | Data | SUM/AVG/MIN/MAX of numeric columns match |
| 9 | ⚠️ **Review Flags** | Quality | Completeness of human review items |

### Validation Report Structure

```
📋 Migration Validation Report
═══════════════════════════════════════════════
📅 Date: 2026-03-23
🏗️ Project: CUSTOMER_360
📍 Target: workspace-prod

SUMMARY
───────────────────────────────────────────────
  ✅ Passed:   42 / 50 checks
  ⚠️ Warnings:  5 / 50 checks  (manual review)
  🔴 Failed:    3 / 50 checks

DETAIL
───────────────────────────────────────────────
  ✅ Schema: compute_customers — 12/12 columns match
  ✅ Rows:   compute_customers — 1,234,567 rows (match)
  ⚠️ SQL:    legacy_report     — Uses DBMS_OUTPUT (flagged)
  🔴 Rows:   daily_aggregates  — Source: 365, Target: 360
  ...
```

---

## 🔗 Agent Interaction Diagram

```
                                    ┌─────────────────┐
              ┌────────────────────▶│  DATAIKU API     │
              │  Read assets        └────────┬────────┘
              │                              │
              │                              ▼
         ┌────┴────┐               ┌─────────────────┐
         │ 🔍 Disc. │──────────────▶│ 📦 ASSET        │
         │  Agent   │  Write        │    REGISTRY     │
         └─────────┘  registry      │                 │
                                    │  Shared state   │
         ┌─────────┐  Read         │  for all agents │
         │ 📝 SQL   │◀─────────────│                 │
         │  Agent   │──────────────▶│                 │
         └─────────┘  Write result  │                 │
                                    │                 │
         ┌─────────┐               │                 │
         │ 🐍 Py    │◀────────────▶│                 │
         │  Agent   │              │                 │
         └─────────┘               │                 │
                                    │                 │
         ┌─────────┐               │                 │
         │ 📊 Visual│◀────────────▶│                 │
         │  Agent   │              └────────┬────────┘
         └─────────┘                        │
                                            ▼
         ┌─────────┐               ┌─────────────────┐
         │ ✅ Valid. │──────────────▶│  FABRIC API     │
         │  Agent   │  Deploy       │  (REST)         │
         └─────────┘               └─────────────────┘
```

---

## 📏 Agent Sizing Estimates

| Agent | Dev Complexity | Key Libraries | Estimated Files |
|-------|---------------|---------------|-----------------|
| 🎛️ Orchestrator | 🟡 Medium | `asyncio`, `networkx` | 3 |
| 🔍 Discovery | 🟢 Low | `dataiku-api-client` | 2 |
| 📝 SQL Migration | 🔴 High | `sqlglot`, custom rules | 5+ |
| 🐍 Python Migration | 🟡 Medium | `ast`, `nbformat` | 3 |
| 📊 Visual Recipe | 🟡 Medium | Custom SQL generator | 3 |
| 🗄️ Dataset Migration | 🟡 Medium | `pyarrow`, `azcopy` | 3 |
| 🔄 Flow → Pipeline | 🟡 Medium | `networkx`, Fabric SDK | 3 |
| 🔗 Connection Mapper | 🟢 Low | Config mapping | 2 |
| ✅ Validation | 🟡 Medium | `pytest`, connectors | 3 |

---

> 📖 **Next:** See [DEVPLAN.md](DEVPLAN.md) for the sprint-by-sprint development roadmap.
