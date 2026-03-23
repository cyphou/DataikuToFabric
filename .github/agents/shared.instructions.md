---
description: "Shared rules for all agents in the Dataiku to Fabric migration project. USE FOR: enforcing project-wide constraints, coding standards, and safety rules."
---

# Shared Project Rules — Dataiku to Fabric Migration

All agents MUST follow these rules. They apply to every file in the project.

## Pipeline Architecture

```
Dataiku API → [Discovery] → Asset Registry (JSON) → [Conversion Agents] → Fabric Assets
                                                    → SQL (T-SQL / Spark SQL)
                                                    → Notebooks (.ipynb / PySpark)
                                                    → Pipelines (JSON)
                                                    → Lakehouse / Warehouse DDL
```

- **Source**: `src/connectors/` — Dataiku API client
- **Agents**: `src/agents/` — 9 migration agents
- **Translation**: `src/translators/` — SQL dialect translators (Oracle→T-SQL, PostgreSQL→T-SQL)
- **Target**: `src/connectors/` — Fabric REST API client
- **Core**: `src/core/` — orchestrator, registry, config, logging
- **Models**: `src/models/` — asset, state machine, report data models
- **Tests**: `tests/` — unit and integration tests
- **Docs**: `docs/` — architecture, dev plan, agents, mapping reference

## Hard Constraints

1. **Read before write** — never assume file contents from memory
2. **No duplicate functions** — always `grep_search` for an existing name before creating one
3. **Test after every change** — run `pytest tests/ --tb=short -q`
4. **Git hygiene** — commit only when tests pass, conventional messages (`feat:`, `fix:`, `test:`, `docs:`)
5. **Security first** — never store credentials in code, use env vars or Azure Key Vault

## Python Conventions

- Python 3.10+ compatible
- Async-first (`async def`) for all agent `execute()` methods
- Pydantic v2 for data models and config
- `unittest.TestCase` or `pytest` for all test classes
- Type annotations on all public functions
- No docstrings on code you didn't write

## Learned Pitfalls (Global)

- `replace_string_in_file` fails on duplicate matches — use unique surrounding context
- Never weaken test assertions to make tests pass
- Stage only files related to the current task
- Dataiku API pagination uses `list_*` methods — always iterate all pages
- sqlglot dialect names are lowercase: `"tsql"`, `"spark"`, `"oracle"`, `"postgres"`
- Fabric REST API uses Bearer token auth — refresh tokens proactively

## Cross-Agent Handoff Protocol

When your task requires work outside your domain:
1. Complete your part fully (including tests for your domain)
2. State clearly what the next agent needs to do
3. List the exact files and functions involved
4. Provide any intermediate artifacts (JSON, dict structures)

## Key References

- Project rules: `.github/copilot-instructions.md`
- Development plan: `docs/DEVPLAN.md`
- Architecture: `docs/ARCHITECTURE.md`
- Agent specifications: `docs/AGENTS.md`
- Agent definitions: `.github/agents/`
