# Next Roadmap 2026 - DataikuToFabric

Created: 2026-06-24
Scope: Post-Phase-27 execution plan aligned with current DataikuToFabric status and latest TableauToPowerBI direction.

## Baseline Snapshot

DataikuToFabric current baseline:
- Phases 1-27 are implemented.
- Core migration, QA, self-healing, drift detection, lineage, API server, plugin framework, equivalence testing, and multi-project merge are in place.
- Test baseline is stable (latest full run in this workspace: 880 passed).

TableauToPowerBI reference snapshot reviewed:
- Current line is at v40.0.0.
- Latest emphasis areas include VS Code extension workflows, interactive notebook APIs, plugin SDK hardening, and marketplace/dependency improvements.

## 2026 Objective

Shift from "feature-complete migration engine" to "enterprise operating platform" with:
- safer deployment automation,
- production observability,
- better portfolio-scale planning,
- stronger developer ergonomics.

## Priority Plan

### Wave A (0-6 weeks): Close core production gaps

1. Phase 12 hardening - Fabric deployment idempotency
- Add content hashing for generated artifacts before deploy.
- Skip unchanged assets by default.
- Persist deployment manifest for traceability.

2. Phase 14 hardening - rollback reliability
- Add pre-deploy snapshot capture.
- Implement selective rollback by asset type and deployment transaction.
- Add rollback simulation mode for dry-run validation.

3. Phase 13 hardening - secrets and credential policy
- Add credential provider chain: env -> vault -> file fallback (explicitly disabled by default).
- Enforce redaction on all log events and reports.
- Add policy checks to fail migration if secrets are in config payload.

Exit gate:
- deploy -> rollback cycle validated on integration fixtures.
- deployment manifest + snapshot artifacts produced for each run.
- no plain secrets in logs across tests.

### Wave B (6-12 weeks): Operability and scale

1. Phase 15 - observability expansion
- Add decision telemetry per major conversion decision path.
- Add correlation IDs across agents and API endpoints.
- Produce operations dashboard JSON summary for external ingestion.

2. Phase 24 expansion - API operations mode
- Add auth middleware options (API key and bearer token modes).
- Add job pagination/filtering and batch endpoints parity.
- Add webhook callback for job completion.

3. Phase 28 - enterprise wave planning
- Add effort estimator model per project (size, complexity, unsupported features).
- Produce multi-project migration wave plan with staffing assumptions.
- Add planner CLI command output in json and html.

Exit gate:
- full migration run can be traced end-to-end by correlation ID.
- API supports secure remote orchestration.
- plan command generates deterministic wave proposals from fixture inputs.

### Wave C (12-20 weeks): Experience and ecosystem

1. Phase 29 - Web UI for non-CLI users
- Build a thin workflow UI on top of existing API primitives.
- Include assess, migrate, qa, drift, lineage, and status views.
- Keep CLI as source of truth; UI is orchestration only.

2. Plugin system v2 alignment (Phase 25 extension)
- Formalize plugin versioning, capability declarations, and compatibility checks.
- Add plugin health checks and isolation test harness.
- Publish plugin authoring guide with examples.

3. Marketplace/dependency model
- Add optional local registry for reusable migration patterns.
- Add dependency resolution between pattern packs.
- Add signed bundle verification for promoted packs.

Exit gate:
- a user can complete assess -> migrate -> qa from UI without custom scripting.
- plugin compatibility failures are detected before runtime execution.

## Suggested Backlog Breakdown

P0 (do now):
- Deployment idempotency + manifest.
- Pre-deploy snapshot + selective rollback.
- Secret redaction policy enforcement.

P1 (next):
- Decision telemetry and correlation IDs.
- API auth and batch operations.
- Enterprise wave planner first release.

P2 (later):
- Web UI orchestration.
- Plugin SDK v2 style lifecycle and compatibility.
- Pattern marketplace and dependency graph.

## Recommended Metrics

Delivery metrics:
- deployment success rate,
- rollback success rate,
- mean time to diagnose failed migration,
- manual intervention rate per migration run,
- migration confidence score trend (from QA/fidelity).

Quality metrics:
- test pass rate,
- regression drift incidents,
- self-healing precision (safe fix acceptance rate),
- unsupported artifact rate over time.

## First Sprint Proposal (2 weeks)

1. Add deploy content hash + manifest storage.
2. Add pre-deploy snapshot artifact and rollback dry-run.
3. Add logging redaction guardrails and tests.
4. Add docs update to show deploy/rollback operational workflow.

Definition of done:
- all new behaviors covered by tests,
- full test suite green,
- docs updated with operator runbook examples.
