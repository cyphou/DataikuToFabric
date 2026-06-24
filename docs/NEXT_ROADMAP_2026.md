# Next Roadmap 2026 - DataikuToFabric

**Updated**: 2026-06-24 (20:10 UTC)  
**Status**: Phases 28, 15 Complete + Wave A Recovery Complete, moving to Phase 24 (API Operations)

## Baseline Snapshot

DataikuToFabric current baseline (as of 2026-06-24):
- Phases 1-27: Core migration engine fully implemented
- **Phase 28 (NEW)**: Enterprise wave planner with multi-project effort estimation ✅
- **Wave A Recovery (NEW)**: Self-healing recovery orchestration system ✅
- Test baseline: **1081 tests passing** (zero regressions)
- Deployment: Full idempotency + snapshot + rollback ready

**Completed Wave A (Production Hardening)**:
- ✅ Phase 12 — Deployment idempotency & manifest persistence
- ✅ Phase 14 — Pre-deploy snapshots & selective rollback
- ✅ Phase 13 — Secrets redaction & credential policies
- ✅ Phase 28 — Enterprise wave planner (effort estimation, multi-project waves)
- ✅ Wave A Recovery — Health monitoring, failure detection, circuit breakers

**Status**: Wave A 100% complete, all production gaps closed. Phase 15 (Observability Expansion) complete.

**Validation Checkpoint (Latest)**:
- Full regression + migration test run completed on 2026-06-24
- Result: **1081 passed, 0 failed, 1 warning** in 288.51s
- Scope included: integration pipeline, data migration, translators, recovery, Wave A ops integration, Wave B observability integration

---

## Completed Deliverables (Session 2026-06-24)

### Phase 15 - Observability Expansion ✅
- `src/core/observability.py` — Extended DecisionEvent telemetry framework
  - Added DecisionCategory enum with 8 categories: conversion, deployment, recovery, authentication, validation, migration_planning, api_operation, unknown
  - Added recovery-specific fields to DecisionEvent: recovery_strategy, failure_classification, confidence_score, health_status
  - Extended DecisionTelemetry.summary() to include recovery metrics aggregation
  - Enhanced OpsDashboardWriter to accept recovery_summary parameter for operations dashboards
  - All decision events include correlation_id for end-to-end tracing
- `src/core/recovery.py` — Integrated telemetry recording into RecoveryOrchestrator
  - Added optional decision_telemetry parameter for pluggable observability
  - check_health() records health check decisions with probe metrics
  - detect_failure() records failure classification with confidence scores
  - attempt_recovery() records recovery strategy selection and policy matching
  - mark_recovery_success/failure() records outcome metrics with duration
- Test coverage: 27 recovery tests passing, backward compatibility verified
- Commit: f9086d5

### Phase 28 - Enterprise Wave Planner
- `src/analyzers/wave_planner.py` — Deterministic effort estimation & wave grouping
- CLI command: `dtf plan --projects PROJ_A,PROJ_B --wave-capacity 40 --output report.html`
- Output formats: JSON, HTML (with project estimates, staffing assumptions)
- Test coverage: 59 tests in test_cli.py

### Wave A Recovery Orchestration
- `src/core/recovery.py` — Complete health monitoring & recovery system (843 lines)
  - 8 components: HealthProbe, CircuitBreaker, FailureDetector, RecoveryPolicy, RecoveryOrchestrator, RecoveryState, RecoveryAction, RecoveryClassification
  - 9 failure classifications: TIMEOUT, CONNECTIVITY, VALIDATION, AUTHENTICATION, PERMISSION, RESOURCE_EXHAUSTED, CORRUPTION, DEPENDENCY, UNKNOWN
  - 4 recovery strategies: RETRY, HEAL, ROLLBACK, ISOLATE
  - Persistent state tracking with audit trails
- `tests/test_recovery.py` — 27 comprehensive tests (all passing)
- `docs/SELF_HEALING_RECOVERY.md` — Complete API reference & usage guide

---

## 2026 Objective (Updated)

**NOW UNDERWAY**: Shift from "production-hardened engine" to "enterprise operating platform" with:
- ✅ Safer deployment automation (Wave A complete)
- ⏳ Production observability (Wave B in progress)
- ✅ Better portfolio-scale planning (Wave A complete with Phase 28)
- ⏳ Stronger developer ergonomics (Wave C prep)

---

## Execution Plan

### Wave B (6-12 weeks): Operability & Scale [IN PROGRESS]

**Phase 29 Entry Points**:

1. **Phase 15 - Observability Expansion ✅ (COMPLETE)**
   - ✅ Extend decision telemetry to all agent conversion paths
   - ✅ Add correlation ID propagation across API endpoints
   - ✅ Integrate recovery actions into decision telemetry
   - ✅ Generate operations dashboard JSON for SIEM/monitoring integration
   - **Completed**: 2026-06-24 19:15 UTC
   - **Effort**: 8-12 story points (completed in 2 weeks)
   - **Tests**: 27 recovery tests passing, backward compatible

2. **Phase 24 Expansion - API Operations Mode (P1 — NEXT)**
   - Implement auth middleware: API key + bearer token support (JWT + OAuth 2.0)
   - Add job filtering, pagination, sorting on existing endpoints
   - Implement batch endpoints for multi-project operations (POST /jobs/batch, GET /jobs/batch/{batch_id})
   - Add webhook callbacks for long-running jobs
   - Recovery integration: Trigger recovery on job failure, return recovery_summary in job status
   - **Estimated effort**: 10-14 story points (2-3 weeks)
   - **Dependencies**: api/server.py, Phase 15 complete ✅
   - **Exit criteria**: remote orchestration with TLS + RBAC ready
   - **Start**: 2026-06-24 (after Phase 15 completion)

3. **Phase 29 Prep - Web UI Orchestration (P2 — defer)**
   - Build thin FastAPI frontend for assess/migrate/qa/drift/lineage/status
   - Keep CLI as source of truth, UI as thin orchestration layer
   - Integrate with recovery dashboard
   - **Estimated effort**: 20-25 story points (4-5 weeks)
   - **Dependencies**: Phase 15 (telemetry) ✅, Phase 24 (API ops in progress)
   - **Start**: After Wave B core (Week 10-12)

**Wave B Exit Gate**:
- Full migration traced end-to-end by correlation ID ✓ (Phase 15 complete)
- API supports secure remote orchestration (Phase 24 in progress)
- Web UI beta release for early access (Phase 29 staged)

### Wave C (12-20 weeks): Experience & Ecosystem [PLANNING]

1. **Plugin System v2 Alignment (Phase 25 Extension)**
   - Formalize versioning & capability declarations
   - Add plugin health checks & isolation test harness
   - Publish authoring guide with examples
   - **Target start**: Week 14

2. **Marketplace & Dependency Model**
   - Local registry for reusable migration patterns
   - Pattern pack dependency resolution
   - Signed bundle verification for promoted packs
   - **Target start**: Week 18

---

## Recommended Immediate Actions (Next Sprint: 2 weeks)

### Sprint Goal: Phase 24 API Operations Delivery

**Tasks**:

1. **Implement API auth middleware** (5 points)
   - Add API key validation (static + dynamic with Key Vault)
   - Add bearer token validation (JWT + OAuth 2.0)
   - Add RBAC middleware for project-level access control
   - Files: `src/api/server.py`

2. **Add batch job endpoints** (4 points)
   - POST /jobs/batch — submit multiple projects in one request
   - GET /jobs/batch/{batch_id} — track batch status
   - GET /jobs?project=X&status=Y — filtering + pagination
   - Files: `src/api/job_manager.py`

3. **Integrate recovery into job lifecycle** (3 points)
   - On job failure, automatically attempt recovery
   - Track recovery attempts in job history
   - Return recovery_summary in job status response
   - Files: `src/api/job_manager.py`, `src/core/orchestrator.py`

4. **Update operational runbook** (2 points)
   - Document deploy → health check → recovery workflow
   - Add examples for API key provisioning
   - Add troubleshooting guide for common recovery scenarios
   - Files: `docs/SELF_HEALING_RECOVERY.md`, `docs/SETUP.md`

**Definition of Done**:
- All regression tests pass (current baseline: 1081/1081)
- New Phase 24 auth + batch tests added and passing
- Decision telemetry visible in ops_dashboard JSON
- API docs updated with auth + batch examples
- Runbook covers full recovery workflow

---

## Success Metrics

### Delivery Metrics
- Deployment idempotency: 100% unchanged assets skipped ✅
- Rollback success rate: target 95%+ (testable via snapshot.py)
- Health check pass rate: target 98%+ (testable via recovery.py)
- Recovery automation rate: target 80%+ (without manual intervention)

### Quality Metrics
- Test pass rate: 100% (currently 1081/1081) ✅
- Regression drift: 0 incidents (tracked per phase)
- Self-healing precision: target 90%+ (safe fix acceptance)
- Unsupported artifact detection: target 100% (pre-migration)

---

## Dependency Chain

```
Wave A (Complete) ✅
  ├─ Phase 12 ✅
  ├─ Phase 14 ✅
  ├─ Phase 13 ✅
  ├─ Phase 28 ✅
  └─ Recovery ✅
        ↓
Wave B (In Progress)
   ├─ Phase 15 (Observability) ✅
   │   └─ DecisionEvent + Correlation IDs complete
   │       └─ Recovery telemetry integration complete
   ├─ Phase 24 (API Ops) ← CURRENT FOCUS
  │   └─ Auth + Batch + Webhooks
  │       └─ Recovery orchestration API
  └─ Phase 29 (Web UI) ← STAGE 2
      └─ Dashboard for recovery actions
        ↓
Wave C (Planning)
  ├─ Plugin v2
  ├─ Marketplace
  └─ Advanced analytics
```

---

## Risk Register

| Risk | Impact | Mitigation |
|------|--------|-----------|
| Recovery policy effectiveness | HIGH | Test failure scenarios exhaustively (done in 27 tests) |
| Correlation ID overhead | MEDIUM | Use context vars, measure perf impact in Phase 15 |
| API auth complexity | MEDIUM | Use standard patterns (JWT, API keys), defer OAuth 2.0 |
| Web UI scope creep | HIGH | Keep UI thin (orchestration only), defer advanced features |

---

## Timeline Estimate

- **Week 1-2** (NOW): Phase 15 + Phase 24 foundations
- **Week 3-4**: Phase 24 completion + integration testing
- **Week 5-6**: Phase 29 UI scaffolding
- **Week 7-8**: Web UI core features (assess, migrate, qa)
- **Week 9-10**: Phase 25 plugin v2 design
- **Week 11-12**: Wave B wrap-up + Phase 29 beta launch
- **Week 13+**: Wave C marketplace

---

## Commit History (Session 2026-06-24)

1. ✅ `db79fab` — Phase 28 enterprise wave planner
2. ✅ `a57276e` — Wave A self-healing recovery orchestration

**Next commit**: Phase 15 observability expansion (DecisionEvent + correlation IDs)
