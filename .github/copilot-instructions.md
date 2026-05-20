<!-- Copilot instructions for the Dataiku to Microsoft Fabric migration project -->

# Project: Dataiku to Microsoft Fabric Migration

Automated migration of Dataiku artifacts to Microsoft Fabric format.

## Architecture — Pipeline

```
Dataiku source → [src/agents, src/analyzers, src/api, src/connectors, src/core, src/drift, src/healers, src/lineage, src/merge, src/models, src/plugins, src/qa, src/reports, src/testing, src/translators, src/__pycache__] → Extraction → [output] → Microsoft Fabric
```

## Project Structure

- **Source / Extraction**: `src/agents/`, `src/analyzers/`, `src/api/`, `src/connectors/`, `src/core/`, `src/drift/`, `src/healers/`, `src/lineage/`, `src/merge/`, `src/models/`, `src/plugins/`, `src/qa/`, `src/reports/`, `src/testing/`, `src/translators/`, `src/__pycache__/`
- **Target / Generation**: `output/`
- **Tests**: `tests/` (38 test files)
- **Docs**: `docs/`

## Key Modules

- **Generation**:
  - `src\lineage\lineage_builder.py`
- **Assessment**:
  - `src\agents\validation_agent.py`
  - `src\analyzers\strategy_advisor.py`
  - `src\merge\merge_assessment.py`
  - `src\qa\cross_validator.py`
  - `src\reports\assessment_report.py`
- **Deployment**:
  - `src\connectors\dataiku_client.py`
  - `src\connectors\fabric_client.py`
- **Orchestration**:
  - `src\cli.py`
  - `src\core\orchestrator.py`
- **Utilities**:
  - `examples\run_demo.py`
  - `plugins\sample_plugin.py`
  - `src\__init__.py`
  - `src\agents\__init__.py`
  - `src\agents\base_agent.py`
  - `src\agents\connection_agent.py`
  - `src\agents\dataset_agent.py`
  - `src\agents\discovery_agent.py`
  - `src\agents\flow_pipeline_agent.py`
  - `src\agents\python_migration_agent.py`
  - `src\agents\sql_migration_agent.py`
  - `src\agents\visual_recipe_agent.py`
  - `src\analyzers\__init__.py`
  - `src\analyzers\project_analyzer.py`
  - `src\api\__init__.py`
  - ... and 41 more

## Hard Constraints

1. **Read before write** — never assume file contents from memory
2. **Test after every change** — run `pytest tests/ --tb=short -q`
3. **No duplicate functions** — always search for an existing name before creating one
4. **Git hygiene** — commit only when tests pass, conventional messages (`feat:`, `fix:`, `test:`, `docs:`)

## Multi-Agent Architecture

This project uses a specialized agent architecture. See `docs/AGENTS.md` for the full
architecture diagram and `.github/agents/` for per-agent definitions.

## Workflow Rules

### 1. Plan Before Build
- For multi-step work, create a plan before starting
- If something goes sideways, STOP and re-plan

### 2. Read Before Write
- **Always read target code before editing**
- Read `copilot-instructions.md` at session start for project rules

### 3. Testing Contract
- Run `pytest tests/ --tb=short -q` after EVERY implementation change
- If tests fail → fix them before reporting completion
- New features **require** new tests
- Never weaken test assertions to make tests pass

### 4. Scope Discipline
- Only modify files directly related to the task
- No drive-by refactors
- Prefer the smallest change that solves the problem
