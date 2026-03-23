<!-- Copilot instructions for the Dataiku to Fabric migration project -->

# Project: Dataiku to Microsoft Fabric Migration

Automated migration of Dataiku projects (recipes, datasets, flows, connections) to native Microsoft Fabric assets (Lakehouse, Warehouse, Notebooks, Data Pipelines, Power BI).

## Architecture — Agent-Based Pipeline

```
Dataiku API → [Discovery Agent] → Asset Registry (JSON)
                                       ↓
              [SQL Migration Agent] ←── Registry ──→ [Python Migration Agent]
              [Visual Recipe Agent] ←── Registry ──→ [Dataset Agent]
              [Flow→Pipeline Agent] ←── Registry ──→ [Connection Agent]
                                       ↓
                              [Validation Agent]
                                       ↓
                              Microsoft Fabric Workspace
```

1. **Discovery** (`src/agents/discovery_agent.py`): Scans Dataiku project via API, catalogs all assets (recipes, datasets, flows, connections, scenarios)
2. **SQL Migration** (`src/agents/sql_migration_agent.py` + `src/translators/`): Converts SQL recipes (Oracle/PostgreSQL) to T-SQL or Spark SQL via sqlglot
3. **Python Migration** (`src/agents/python_migration_agent.py` + `src/translators/python_to_notebook.py`): Rewrites Dataiku SDK calls to PySpark, generates .ipynb notebooks
4. **Visual Recipe** (`src/agents/visual_recipe_agent.py`): Converts visual recipes (Join, Group, Filter, Window, Pivot, Prepare) to SQL
5. **Dataset Migration** (`src/agents/dataset_agent.py`): Migrates schemas and data to Lakehouse (Delta) or Warehouse tables
6. **Flow→Pipeline** (`src/agents/flow_pipeline_agent.py`): Converts Dataiku flow DAGs and scenarios to Fabric Data Pipeline JSON
7. **Connection Mapping** (`src/agents/connection_agent.py`): Maps Dataiku connections to Fabric equivalents (Gateway, OneLake shortcut, etc.)
8. **Validation** (`src/agents/validation_agent.py`): Verifies schema match, row counts, SQL syntax, notebook structure, pipeline integrity

## Project Structure

- **src/core/**: Orchestration, config, registry, logging
  - `orchestrator.py`: DAG-based agent execution, parallel dispatch, retry logic, checkpoint/resume, selective re-run
  - `registry.py`: Asset Registry (in-memory + JSON persistence) — central state store, checkpoint operations
  - `config.py`: Pydantic-based YAML config loader
  - `logger.py`: Structured logging (structlog)
- **src/agents/**: All 9 migration agents (discovery, sql, python, visual, dataset, flow, connection, validation) + base class
  - `base_agent.py`: Abstract `BaseAgent` with lifecycle (IDLE → RUNNING → COMPLETED/FAILED)
- **src/connectors/**: External service clients
  - `dataiku_client.py`: Dataiku REST API client (API key auth, pagination)
  - `fabric_client.py`: Fabric REST API client (Azure AD auth, retry logic)
- **src/translators/**: SQL and code translators
  - `sql_translator.py`: Core sqlglot-based SQL dialect translation
  - `oracle_to_tsql.py`: Oracle → T-SQL custom rules (NVL, ROWNUM, CONNECT BY, DECODE, SYSDATE)
  - `postgres_to_tsql.py`: PostgreSQL → T-SQL custom rules (::cast, ILIKE, LATERAL, ||)
  - `python_to_notebook.py`: Python → Fabric Notebook (.ipynb) converter
- **src/models/**: Data models (Pydantic)
  - `asset.py`: Asset, AssetType, MigrationState enums and models
  - `migration_state.py`: State machine (Discovered → Converting → Converted → Deploying → Deployed → Validated)
  - `report.py`: Validation report model
- **tests/**: Unit and integration tests (pytest + pytest-asyncio)
- **docs/**: Architecture, agents, dev plan, mapping reference
- **templates/**: Notebook, pipeline, DDL templates
- **.github/agents/**: Multi-agent definitions (9 agent files + shared instructions)

## Multi-Agent Architecture

This project uses a **9-agent specialization model**. See `docs/AGENTS.md` for the full architecture and `.github/agents/` for per-agent definitions.

| Agent | Invoke When | Owns |
|-------|-------------|------|
| **@orchestrator** | Pipeline coordination, CLI, retry logic | `cli.py`, `core/orchestrator.py`, `core/config.py`, `core/registry.py` |
| **@extractor** | Scan Dataiku project, discover assets | `agents/discovery_agent.py`, `connectors/dataiku_client.py` |
| **@sql_converter** | Convert SQL recipes (Oracle/PG→T-SQL) | `agents/sql_migration_agent.py`, `translators/sql_translator.py`, `translators/oracle_to_tsql.py`, `translators/postgres_to_tsql.py` |
| **@python_converter** | Convert Python recipes to Notebooks | `agents/python_migration_agent.py`, `translators/python_to_notebook.py` |
| **@visual_converter** | Convert visual recipes to SQL | `agents/visual_recipe_agent.py` |
| **@dataset_migrator** | Migrate schemas and data | `agents/dataset_agent.py` |
| **@pipeline_builder** | Convert flows to Pipelines | `agents/flow_pipeline_agent.py` |
| **@connection_mapper** | Map connections to Fabric | `agents/connection_agent.py`, `connectors/fabric_client.py` |
| **@validator** | Validate migrated assets | `agents/validation_agent.py`, `models/report.py` |
| **@tester** | Write and fix tests | `tests/*` |

## Key Technologies

- **Python 3.10+** with async/await
- **sqlglot** for SQL dialect translation (oracle, postgres → tsql, spark)
- **nbformat** for Jupyter notebook generation
- **Pydantic v2** for config and data models
- **httpx** for async HTTP (Dataiku & Fabric APIs)
- **networkx** for flow DAG processing
- **structlog** for structured logging
- **Click** for CLI
- **pytest + pytest-asyncio** for testing
- **Azure Identity** for Fabric authentication

## SQL Dialect Translation

| Source | Function | Target (T-SQL) |
|--------|----------|----------------|
| Oracle | `NVL(a, b)` | `ISNULL(a, b)` |
| Oracle | `SYSDATE` | `GETDATE()` |
| Oracle | `ROWNUM` | `ROW_NUMBER() OVER (...)` |
| Oracle | `CONNECT BY` | Recursive CTE |
| Oracle | `DECODE(x,a,b,c)` | `CASE x WHEN a THEN b ELSE c END` |
| PostgreSQL | `x::type` | `CAST(x AS type)` |
| PostgreSQL | `ILIKE` | `LOWER(x) LIKE LOWER(y)` |
| PostgreSQL | `LATERAL` | `CROSS APPLY` |

## Dataiku SDK → PySpark Mapping

| Dataiku | PySpark Equivalent |
|---------|-------------------|
| `dataiku.Dataset("x").get_dataframe()` | `spark.read.format("delta").load("Tables/x")` |
| `dataiku.Dataset("x").write_dataframe(df)` | `df.write.format("delta").mode("overwrite").save("Tables/x")` |
| `dataiku.Folder("x").get_path()` | `"/lakehouse/default/Files/x"` |
| `import dataiku` | PySpark imports |

## Orchestrator Capabilities

- **Checkpoint**: Saves registry state after each wave to `checkpoint_wave_N.json`
- **Resume** (`--resume`): Loads saved state, skips completed agents, continues from interruption point
- **Selective re-run** (`--rerun agent_name`): Resets specific agent(s) + downstream dependents, re-processes
- **Asset filtering** (`--asset-ids "id1,id2"`): Filters registry to specific assets, skips discovery
- **Checkpoint cleanup**: Auto-removes checkpoint files on success (opt out with `--keep-checkpoints`)
- **Circuit breaker**: Skips agent after N consecutive failures (`circuit_breaker_threshold` in config)
