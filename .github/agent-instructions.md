# Agent Behavior Instructions — Dataiku to Fabric Migration

Rules for AI coding agents working in this codebase. Read `.github/copilot-instructions.md` for full project context (architecture, file map, SQL dialect mappings, SDK replacements).

**Multi-agent architecture**: This project uses a 9-agent specialization model. See `docs/AGENTS.md` for the full architecture diagram, and `.github/agents/` for per-agent definitions.

---

## Project Context (Quick Reference)

- **Pipeline**: Dataiku API → Discovery → Asset Registry → Conversion Agents → Validation → Fabric
- **Source**: `src/connectors/dataiku_client.py` (Dataiku API extraction)
- **Agents**: `src/agents/` (9 migration agents)
- **Translation**: `src/translators/` (SQL + Python translators)
- **Target**: `src/connectors/fabric_client.py` (Fabric deployment)
- **Orchestration**: `src/core/orchestrator.py` — DAG execution, checkpoint/resume, selective re-run
- **Registry**: `src/core/registry.py` — Asset state store with checkpoint operations
- **Tests**: `pytest tests/ --tb=short -q` (566 tests, 85% coverage)
- **Python**: 3.10+, async-first, Pydantic v2
- **Dev plan**: `docs/DEVPLAN.md` — check current sprint before starting work
- **Agents**: 9 specialized agents in `.github/agents/` — see `docs/AGENTS.md`

---

## Learned Lessons (DO NOT Repeat These Mistakes)

### sqlglot Pitfalls
- Dialect names are always lowercase: `"oracle"`, `"postgres"`, `"tsql"`, `"spark"`
- `sqlglot.transpile()` returns a list — always take `[0]` for single statements
- Some Oracle constructs (CONNECT BY, PL/SQL blocks) need custom handling beyond sqlglot
- Always validate transpiled SQL with `sqlglot.parse()` to catch syntax errors

### Dataiku API Pitfalls
- API key goes in header: `Authorization: Bearer {key}`
- Recipe `type` field determines the conversion agent to use
- Visual recipe params are nested JSON — parse carefully
- Flow endpoints return flat edge lists, not nested DAGs — build the graph yourself
- Some API responses are paginated — always check for continuation tokens

### Fabric API Pitfalls
- Bearer tokens expire — implement refresh logic
- Item creation is async — poll for completion status
- Notebook definitions use base64-encoded content
- Pipeline JSON has strict schema validation — test with Fabric API before deployment

### Tool Gotchas
- `replace_string_in_file` fails on duplicate matches — use unique surrounding context
- Never weaken test assertions to make tests pass
- Read the file FIRST, then edit — never assume content from memory
- Stage only files related to the current task

---

## Workflow Rules

### 1. Plan Before Build
- Use your task tracker for multi-step work
- Read `docs/DEVPLAN.md` first to understand scope and sequencing

### 2. Test Everything
- Run `pytest tests/ --tb=short -q` after every change
- New features require corresponding test cases
- Do NOT commit code with failing tests

### 3. Cross-Agent Handoff
- Complete your part fully before handing off
- State clearly what the next agent needs to do
- List exact files, functions, and data structures
- Use conventional commit messages: `feat:`, `fix:`, `test:`, `docs:`

### 4. Security
- Never store credentials in code — use env vars or Azure Key Vault
- Validate all file paths for traversal attacks
- Sanitize SQL inputs — use parameterized queries where possible
- Review flags for manual review on complex/risky conversions
