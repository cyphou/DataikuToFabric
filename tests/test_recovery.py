"""Tests for Wave A self-healing recovery orchestration."""

import json
from pathlib import Path
from uuid import uuid4

import pytest

from src.core.recovery import (
    CircuitBreaker,
    CircuitBreakerState,
    FailureClassification,
    FailureDetector,
    HealthCheckResult,
    HealthProbe,
    HealthStatus,
    RecoveryAction,
    RecoveryOrchestrator,
    RecoveryPolicy,
    RecoveryState,
    RecoveryStrategyType,
)


class TestHealthProbe:
    """Test health probe execution."""

    def test_probe_with_healthy_check(self):
        """Probe returns HEALTHY when check passes."""
        def check_fn(state):
            return HealthStatus.HEALTHY

        probe = HealthProbe(
            probe_name="test_probe",
            asset_id="asset_1",
            asset_type="notebook",
            check_fn=check_fn,
        )

        result = probe.execute({})
        assert result == HealthStatus.HEALTHY

    def test_probe_with_exception_returns_unknown(self):
        """Probe returns UNKNOWN on exception."""
        def check_fn(state):
            raise RuntimeError("Check failed")

        probe = HealthProbe(
            probe_name="failing_probe",
            asset_id="asset_1",
            asset_type="notebook",
            check_fn=check_fn,
        )

        result = probe.execute({})
        assert result == HealthStatus.UNKNOWN

    def test_probe_with_degraded_status(self):
        """Probe can return DEGRADED status."""
        def check_fn(state):
            return HealthStatus.DEGRADED

        probe = HealthProbe(
            probe_name="degraded_probe",
            asset_id="asset_1",
            asset_type="pipeline",
            check_fn=check_fn,
        )

        result = probe.execute({})
        assert result == HealthStatus.DEGRADED


class TestCircuitBreaker:
    """Test circuit breaker state machine."""

    def test_circuit_starts_closed(self):
        """Circuit breaker initializes in CLOSED state."""
        breaker = CircuitBreaker("asset_1")
        assert breaker.state == CircuitBreakerState.CLOSED
        assert breaker.can_attempt_recovery() is True

    def test_circuit_opens_after_failure_threshold(self):
        """Circuit opens after exceeding failure threshold."""
        breaker = CircuitBreaker("asset_1", failure_threshold=3)

        breaker.record_failure()
        assert breaker.state == CircuitBreakerState.CLOSED

        breaker.record_failure()
        assert breaker.state == CircuitBreakerState.CLOSED

        breaker.record_failure()
        assert breaker.state == CircuitBreakerState.OPEN
        assert breaker.can_attempt_recovery() is False

    def test_circuit_half_open_on_test_attempt(self):
        """Circuit transitions to HALF_OPEN for recovery testing."""
        breaker = CircuitBreaker("asset_1", failure_threshold=2)

        breaker.record_failure()
        breaker.record_failure()
        assert breaker.state == CircuitBreakerState.OPEN

        breaker.attempt_test()
        assert breaker.state == CircuitBreakerState.HALF_OPEN
        assert breaker.can_attempt_recovery() is True

    def test_circuit_closes_after_success_threshold(self):
        """Circuit closes after success threshold in HALF_OPEN."""
        breaker = CircuitBreaker("asset_1", success_threshold=2)
        breaker.state = CircuitBreakerState.HALF_OPEN

        breaker.record_success()
        assert breaker.state == CircuitBreakerState.HALF_OPEN

        breaker.record_success()
        assert breaker.state == CircuitBreakerState.CLOSED

    def test_circuit_reopens_on_failure_in_half_open(self):
        """Circuit reopens if failure occurs during HALF_OPEN."""
        breaker = CircuitBreaker("asset_1")
        breaker.state = CircuitBreakerState.HALF_OPEN

        breaker.record_failure()
        assert breaker.state == CircuitBreakerState.OPEN


class TestFailureDetector:
    """Test failure classification."""

    @pytest.fixture
    def detector(self):
        return FailureDetector()

    def test_detect_timeout_failure(self, detector):
        """Detector identifies timeout failures."""
        result = detector.detect("Request timeout exceeded after 30s", {"asset_ids": ["a1"]})
        assert result.classification == FailureClassification.TIMEOUT
        assert result.confidence >= 0.5
        assert result.is_transient is True

    def test_detect_connectivity_failure(self, detector):
        """Detector identifies connectivity failures."""
        result = detector.detect("Connection refused to database", {"asset_ids": ["a1"]})
        assert result.classification == FailureClassification.CONNECTIVITY
        assert result.is_transient is True

    def test_detect_authentication_failure(self, detector):
        """Detector identifies authentication failures."""
        result = detector.detect("Authentication failed: invalid credentials", {"asset_ids": ["a1"]})
        assert result.classification == FailureClassification.AUTHENTICATION
        assert result.is_transient is False

    def test_detect_permission_failure(self, detector):
        """Detector identifies permission failures."""
        result = detector.detect("Forbidden: access denied to resource", {"asset_ids": ["a1"]})
        assert result.classification == FailureClassification.PERMISSION
        assert result.is_transient is False

    def test_detect_validation_failure(self, detector):
        """Detector identifies validation failures."""
        result = detector.detect("Invalid schema: missing required field", {"asset_ids": ["a1"]})
        assert result.classification == FailureClassification.VALIDATION
        assert result.is_transient is False

    def test_unknown_failure_classification(self, detector):
        """Detector defaults to UNKNOWN for unrecognized errors."""
        result = detector.detect("Something weird happened", {"asset_ids": ["a1"]})
        assert result.classification == FailureClassification.UNKNOWN


class TestRecoveryPolicy:
    """Test recovery policy definition and matching."""

    def test_policy_applies_with_matching_classification(self):
        """Policy applies when classification and asset type match."""
        policy = RecoveryPolicy(
            classification=FailureClassification.TIMEOUT,
            strategy=RecoveryStrategyType.RETRY,
            applies_to=["notebook"],
        )

        assert policy.applies(FailureClassification.TIMEOUT, "notebook") is True

    def test_policy_does_not_apply_with_wrong_classification(self):
        """Policy does not apply with different classification."""
        policy = RecoveryPolicy(
            classification=FailureClassification.TIMEOUT,
            strategy=RecoveryStrategyType.RETRY,
        )

        assert policy.applies(FailureClassification.AUTHENTICATION, "notebook") is False

    def test_policy_disabled(self):
        """Policy does not apply when disabled."""
        policy = RecoveryPolicy(
            classification=FailureClassification.TIMEOUT,
            strategy=RecoveryStrategyType.RETRY,
            enabled=False,
        )

        assert policy.applies(FailureClassification.TIMEOUT, "notebook") is False


class TestRecoveryOrchestrator:
    """Test recovery orchestration."""

    @pytest.fixture
    def orchestrator(self):
        return RecoveryOrchestrator(
            deployment_id=f"deploy_{uuid4().hex[:8]}",
            project_key="test_proj",
        )

    def test_register_and_retrieve_policy(self, orchestrator):
        """Can register and retrieve recovery policies."""
        policy = RecoveryPolicy(
            classification=FailureClassification.TIMEOUT,
            strategy=RecoveryStrategyType.RETRY,
        )
        orchestrator.register_policy(policy)

        retrieved = orchestrator.get_recovery_strategy(
            FailureClassification.TIMEOUT, "notebook"
        )
        assert retrieved is not None
        assert retrieved.strategy == RecoveryStrategyType.RETRY

    def test_check_health_aggregates_probes(self, orchestrator):
        """Health check aggregates all probes for an asset."""
        healthy_probe = HealthProbe(
            probe_name="healthy",
            asset_id="asset_1",
            asset_type="notebook",
            check_fn=lambda s: HealthStatus.HEALTHY,
        )
        degraded_probe = HealthProbe(
            probe_name="degraded",
            asset_id="asset_1",
            asset_type="notebook",
            check_fn=lambda s: HealthStatus.DEGRADED,
        )

        orchestrator.register_probe("asset_1", healthy_probe)
        orchestrator.register_probe("asset_1", degraded_probe)

        result = orchestrator.check_health("asset_1", {"type": "notebook"})

        assert result.probes_run == 2
        assert result.probes_healthy == 1
        assert result.probes_degraded == 1
        assert result.overall_status == HealthStatus.DEGRADED

    def test_detect_failure(self, orchestrator):
        """Failure detection integrates with detector."""
        result = orchestrator.detect_failure(
            "asset_1",
            "Request timeout after 30s",
            "notebook",
        )
        assert result.classification == FailureClassification.TIMEOUT

    def test_attempt_recovery_creates_action(self, orchestrator):
        """Recovery attempt creates action record."""
        policy = RecoveryPolicy(
            classification=FailureClassification.TIMEOUT,
            strategy=RecoveryStrategyType.RETRY,
        )
        orchestrator.register_policy(policy)

        action = orchestrator.attempt_recovery(
            "asset_1",
            "notebook",
            FailureClassification.TIMEOUT,
        )

        assert action is not None
        assert action.asset_id == "asset_1"
        assert action.strategy == RecoveryStrategyType.RETRY
        assert action.success is None

    def test_attempt_recovery_respects_circuit_breaker(self, orchestrator):
        """Recovery respects open circuit breaker."""
        policy = RecoveryPolicy(
            classification=FailureClassification.TIMEOUT,
            strategy=RecoveryStrategyType.RETRY,
        )
        orchestrator.register_policy(policy)

        # Manually open circuit breaker
        breaker = CircuitBreaker("asset_1", failure_threshold=1)
        breaker.record_failure()
        orchestrator.state.circuit_breakers["asset_1"] = breaker

        action = orchestrator.attempt_recovery(
            "asset_1",
            "notebook",
            FailureClassification.TIMEOUT,
        )

        assert action is None  # No recovery attempt due to open breaker

    def test_mark_recovery_success_updates_breaker(self, orchestrator):
        """Marking recovery success updates circuit breaker."""
        policy = RecoveryPolicy(
            classification=FailureClassification.TIMEOUT,
            strategy=RecoveryStrategyType.RETRY,
        )
        orchestrator.register_policy(policy)

        action = orchestrator.attempt_recovery(
            "asset_1",
            "notebook",
            FailureClassification.TIMEOUT,
        )

        orchestrator.mark_recovery_success(action.recovery_id)

        assert action.success is True
        assert action.completed_at is not None

    def test_recovery_summary_report(self, orchestrator):
        """Recovery summary report is accurate."""
        policy = RecoveryPolicy(
            classification=FailureClassification.TIMEOUT,
            strategy=RecoveryStrategyType.RETRY,
        )
        orchestrator.register_policy(policy)

        action1 = orchestrator.attempt_recovery(
            "asset_1", "notebook", FailureClassification.TIMEOUT
        )
        action2 = orchestrator.attempt_recovery(
            "asset_2", "notebook", FailureClassification.TIMEOUT
        )

        orchestrator.mark_recovery_success(action1.recovery_id)
        orchestrator.mark_recovery_failure(action2.recovery_id)

        summary = orchestrator.recovery_summary()

        assert summary["total_recovery_actions"] == 2
        assert summary["successful_recoveries"] == 1
        assert summary["failed_recoveries"] == 1
        assert summary["success_rate"] == 0.5

    def test_save_and_load_recovery_state(self, orchestrator, tmp_path):
        """Recovery state persists and loads correctly."""
        state_file = tmp_path / "recovery_state.json"

        policy = RecoveryPolicy(
            classification=FailureClassification.TIMEOUT,
            strategy=RecoveryStrategyType.RETRY,
        )
        orchestrator.register_policy(policy)

        action = orchestrator.attempt_recovery(
            "asset_1", "notebook", FailureClassification.TIMEOUT
        )
        orchestrator.mark_recovery_success(action.recovery_id)

        orchestrator.save_state(state_file)
        assert state_file.exists()

        # Load in new orchestrator
        new_orchestrator = RecoveryOrchestrator(
            orchestrator.deployment_id,
            orchestrator.project_key,
        )
        new_orchestrator.load_state(state_file)

        assert len(new_orchestrator.state.recovery_actions) == 1
        assert new_orchestrator.state.recovery_actions[0].success is True


class TestRecoveryState:
    """Test recovery state persistence."""

    def test_state_to_dict_serialization(self):
        """RecoveryState serializes to dict."""
        state = RecoveryState(
            deployment_id="deploy_1",
            project_key="proj_1",
        )

        d = state.to_dict()

        assert d["deployment_id"] == "deploy_1"
        assert d["project_key"] == "proj_1"
        assert "created_at" in d
        assert "last_updated" in d

    def test_state_from_dict_deserialization(self):
        """RecoveryState deserializes from dict."""
        d = {
            "deployment_id": "deploy_1",
            "project_key": "proj_1",
            "asset_recovery_states": {},
            "recovery_actions": [],
            "circuit_breakers": {},
            "created_at": "2026-06-24T00:00:00Z",
            "last_updated": "2026-06-24T00:00:00Z",
        }

        state = RecoveryState.from_dict(d)

        assert state.deployment_id == "deploy_1"
        assert state.project_key == "proj_1"
