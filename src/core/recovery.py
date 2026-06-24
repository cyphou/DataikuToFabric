"""Wave A hardening - Self-healing recovery orchestration system.

Provides:
- ``HealthProbe`` — check asset health status post-deployment.
- ``CircuitBreaker`` — prevent cascading failures with state transitions.
- ``FailureDetector`` — recognize and classify failure patterns.
- ``RecoveryPolicy`` — define automatic remediation strategies.
- ``RecoveryAction`` — track recovery attempts and outcomes.
- ``RecoveryOrchestrator`` — coordinate health monitoring, detection, and remediation.
- ``RecoveryState`` — persistent state for recovery tracking.

Design principles:
- Deterministic: Same failures → same recovery strategy.
- Safe: Circuit breakers prevent infinite loops.
- Observable: All decisions logged with correlation IDs.
- Testable: Policies defined as data, not code.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, TYPE_CHECKING

from src.core.logger import get_logger
from src.core.observability import get_correlation_id

if TYPE_CHECKING:
    from src.core.observability import DecisionTelemetry

logger = get_logger(__name__)


# ── Health probe types ────────────────────────────────────────────────────────


class HealthStatus(str, Enum):
    """Asset health status after deployment."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class CircuitBreakerState(str, Enum):
    """Circuit breaker state for failure prevention."""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failures detected, stop attempts
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class HealthProbe:
    """Configuration and result of a single health check."""
    probe_name: str
    asset_id: str
    asset_type: str
    check_fn: Callable[[dict[str, Any]], HealthStatus] = field(repr=False)
    timeout_seconds: float = 30.0
    critical: bool = False  # If True, unhealthy probe fails entire asset

    def execute(self, asset_state: dict[str, Any]) -> HealthStatus:
        """Run the probe check function."""
        try:
            return self.check_fn(asset_state)
        except Exception as e:
            logger.warning(
                f"Health probe {self.probe_name} failed for {self.asset_id}: {e}"
            )
            return HealthStatus.UNKNOWN


@dataclass
class HealthCheckResult:
    """Result of all probes for a single asset."""
    asset_id: str
    asset_type: str
    probes_run: int
    probes_healthy: int
    probes_degraded: int
    probes_unhealthy: int
    overall_status: HealthStatus
    probe_details: list[dict[str, Any]]
    checked_at: str

    @property
    def is_healthy(self) -> bool:
        return self.overall_status == HealthStatus.HEALTHY

    @property
    def is_critical_failure(self) -> bool:
        return self.overall_status == HealthStatus.UNHEALTHY


class CircuitBreaker:
    """Prevent cascading failures with state machine."""

    __slots__ = (
        "asset_id", "state", "failure_count", "success_count",
        "failure_threshold", "success_threshold", "last_state_change"
    )

    def __init__(
        self,
        asset_id: str,
        failure_threshold: int = 3,
        success_threshold: int = 2,
    ) -> None:
        self.asset_id = asset_id
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold
        self.last_state_change = datetime.now(timezone.utc).isoformat()

    def record_failure(self) -> CircuitBreakerState:
        """Record a failure; transition to OPEN if threshold exceeded."""
        self.failure_count += 1
        self.success_count = 0  # Reset success counter

        if self.state == CircuitBreakerState.CLOSED:
            if self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                self.last_state_change = datetime.now(timezone.utc).isoformat()
                logger.warning(
                    f"Circuit breaker OPEN for {self.asset_id} after "
                    f"{self.failure_count} failures"
                )
        elif self.state == CircuitBreakerState.HALF_OPEN:
            # Failure during recovery test → reopen
            self.state = CircuitBreakerState.OPEN
            self.last_state_change = datetime.now(timezone.utc).isoformat()

        return self.state

    def record_success(self) -> CircuitBreakerState:
        """Record a success; transition from HALF_OPEN to CLOSED if threshold met."""
        self.success_count += 1
        self.failure_count = 0  # Reset failure counter

        if self.state == CircuitBreakerState.HALF_OPEN:
            if self.success_count >= self.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self.last_state_change = datetime.now(timezone.utc).isoformat()
                logger.info(
                    f"Circuit breaker CLOSED for {self.asset_id} after "
                    f"{self.success_count} successful probes"
                )

        return self.state

    def can_attempt_recovery(self) -> bool:
        """Return True if recovery attempt should be tried."""
        if self.state == CircuitBreakerState.CLOSED:
            return True  # Attempt recovery on failures
        if self.state == CircuitBreakerState.HALF_OPEN:
            return True  # Test recovery in half-open
        return False  # Circuit is OPEN

    def attempt_test(self) -> None:
        """Transition from OPEN to HALF_OPEN for recovery testing."""
        if self.state == CircuitBreakerState.OPEN:
            self.state = CircuitBreakerState.HALF_OPEN
            self.failure_count = 0
            self.success_count = 0
            self.last_state_change = datetime.now(timezone.utc).isoformat()
            logger.info(f"Circuit breaker HALF_OPEN for {self.asset_id}")


class FailureClassification(str, Enum):
    """Types of failures recognized by the detector."""
    TIMEOUT = "timeout"
    CONNECTIVITY = "connectivity"
    VALIDATION = "validation"
    AUTHENTICATION = "authentication"
    PERMISSION = "permission"
    RESOURCE_EXHAUSTED = "resource_exhausted"
    CORRUPTION = "corruption"
    DEPENDENCY = "dependency"
    UNKNOWN = "unknown"


@dataclass
class FailureDetectionResult:
    """Result of failure classification."""
    classification: FailureClassification
    confidence: float  # 0.0 to 1.0
    root_cause: str
    affected_assets: list[str]
    is_transient: bool  # True if likely recoverable


class FailureDetector:
    """Recognize and classify failure patterns."""

    def __init__(self):
        self.patterns: dict[FailureClassification, list[str]] = {
            FailureClassification.TIMEOUT: ["timeout", "exceeded", "timed out", "deadline"],
            FailureClassification.CONNECTIVITY: [
                "connection refused", "unreachable", "no route", "network"
            ],
            FailureClassification.AUTHENTICATION: [
                "unauthorized", "authentication failed", "invalid credentials"
            ],
            FailureClassification.PERMISSION: ["forbidden", "permission denied", "access denied"],
            FailureClassification.VALIDATION: ["invalid", "malformed", "schema violation"],
            FailureClassification.CORRUPTION: ["corrupt", "integrity", "checksum"],
            FailureClassification.DEPENDENCY: ["missing", "not found", "dependency"],
        }

    def detect(self, error_message: str, context: dict[str, Any]) -> FailureDetectionResult:
        """Classify an error and suggest recovery strategy."""
        message_lower = error_message.lower()
        classifications: dict[FailureClassification, float] = {}

        # Score each classification by pattern matching
        for classification, patterns in self.patterns.items():
            score = sum(1.0 for p in patterns if p in message_lower) / len(patterns)
            if score > 0:
                classifications[classification] = score

        # Pick highest confidence, default to UNKNOWN
        best_classification = max(
            classifications.items(), key=lambda x: x[1]
        ) if classifications else (FailureClassification.UNKNOWN, 0.0)

        classification, confidence = best_classification
        is_transient = classification in {
            FailureClassification.TIMEOUT,
            FailureClassification.CONNECTIVITY,
            FailureClassification.RESOURCE_EXHAUSTED,
        }

        return FailureDetectionResult(
            classification=classification,
            confidence=min(confidence, 0.95),
            root_cause=error_message[:200],
            affected_assets=context.get("asset_ids", []),
            is_transient=is_transient,
        )


class RecoveryStrategyType(str, Enum):
    """Recovery strategies."""
    RETRY = "retry"
    HEAL = "heal"
    ROLLBACK = "rollback"
    ISOLATE = "isolate"
    NONE = "none"


@dataclass
class RecoveryPolicy:
    """Define recovery strategy for a failure classification."""
    classification: FailureClassification
    strategy: RecoveryStrategyType
    max_attempts: int = 3
    backoff_seconds: float = 5.0
    applies_to: list[str] = field(default_factory=list)  # asset types
    enabled: bool = True

    def applies(self, classification: FailureClassification, asset_type: str) -> bool:
        """Check if this policy applies to the given failure and asset."""
        return (
            self.classification == classification
            and (not self.applies_to or asset_type in self.applies_to)
            and self.enabled
        )


@dataclass
class RecoveryAction:
    """Track a single recovery attempt."""
    recovery_id: str
    asset_id: str
    strategy: RecoveryStrategyType
    attempt_number: int
    started_at: str
    completed_at: str | None = None
    success: bool | None = None
    details: dict[str, Any] = field(default_factory=dict)
    correlation_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "recovery_id": self.recovery_id,
            "asset_id": self.asset_id,
            "strategy": self.strategy.value,
            "attempt_number": self.attempt_number,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "success": self.success,
            "details": self.details,
            "correlation_id": self.correlation_id,
        }


@dataclass
class RecoveryState:
    """Persistent state for tracking recovery operations."""
    deployment_id: str
    project_key: str
    asset_recovery_states: dict[str, dict[str, Any]] = field(default_factory=dict)
    recovery_actions: list[RecoveryAction] = field(default_factory=list)
    circuit_breakers: dict[str, CircuitBreaker] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    last_updated: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "deployment_id": self.deployment_id,
            "project_key": self.project_key,
            "asset_recovery_states": self.asset_recovery_states,
            "recovery_actions": [a.to_dict() for a in self.recovery_actions],
            "circuit_breakers": {
                k: {
                    "state": v.state.value,
                    "failure_count": v.failure_count,
                    "success_count": v.success_count,
                }
                for k, v in self.circuit_breakers.items()
            },
            "created_at": self.created_at,
            "last_updated": self.last_updated,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> RecoveryState:
        state = cls(
            deployment_id=d["deployment_id"],
            project_key=d["project_key"],
            asset_recovery_states=d.get("asset_recovery_states", {}),
            recovery_actions=[],
        )
        
        # Reconstruct recovery actions
        for action_dict in d.get("recovery_actions", []):
            action = RecoveryAction(
                recovery_id=action_dict["recovery_id"],
                asset_id=action_dict["asset_id"],
                strategy=RecoveryStrategyType(action_dict["strategy"]),
                attempt_number=action_dict["attempt_number"],
                started_at=action_dict["started_at"],
                completed_at=action_dict.get("completed_at"),
                success=action_dict.get("success"),
                details=action_dict.get("details", {}),
                correlation_id=action_dict.get("correlation_id"),
            )
            state.recovery_actions.append(action)
        
        # Reconstruct circuit breakers
        for asset_id, cb_dict in d.get("circuit_breakers", {}).items():
            cb = CircuitBreaker(asset_id)
            cb.state = CircuitBreakerState(cb_dict.get("state", "closed"))
            cb.failure_count = cb_dict.get("failure_count", 0)
            cb.success_count = cb_dict.get("success_count", 0)
            state.circuit_breakers[asset_id] = cb
        return state


class RecoveryOrchestrator:
    """Coordinate health monitoring, failure detection, and remediation."""

    def __init__(
        self,
        deployment_id: str,
        project_key: str,
        state_file: Path | None = None,
        decision_telemetry: DecisionTelemetry | None = None,
    ):
        self.deployment_id = deployment_id
        self.project_key = project_key
        self.state_file = state_file
        self.state = RecoveryState(deployment_id, project_key)
        self.detector = FailureDetector()
        self.policies: dict[FailureClassification, RecoveryPolicy] = {}
        self.probes: dict[str, list[HealthProbe]] = {}  # asset_id → probes
        self.decision_telemetry = decision_telemetry  # Optional telemetry recorder (Phase 15)

    def register_policy(self, policy: RecoveryPolicy) -> None:
        """Register a recovery policy."""
        self.policies[policy.classification] = policy

    def register_probe(self, asset_id: str, probe: HealthProbe) -> None:
        """Register a health probe for an asset."""
        if asset_id not in self.probes:
            self.probes[asset_id] = []
        self.probes[asset_id].append(probe)

    def check_health(self, asset_id: str, asset_state: dict[str, Any]) -> HealthCheckResult:
        """Execute all probes for an asset."""
        asset_probes = self.probes.get(asset_id, [])

        healthy = 0
        degraded = 0
        unhealthy = 0
        details = []

        for probe in asset_probes:
            status = probe.execute(asset_state)
            details.append({
                "probe_name": probe.probe_name,
                "status": status.value,
                "critical": probe.critical,
            })

            if status == HealthStatus.HEALTHY:
                healthy += 1
            elif status == HealthStatus.DEGRADED:
                degraded += 1
            else:
                unhealthy += 1

        # Overall status: UNHEALTHY if any critical probe failed
        overall = HealthStatus.UNHEALTHY if unhealthy > 0 else (
            HealthStatus.DEGRADED if degraded > 0 else HealthStatus.HEALTHY
        )

        result = HealthCheckResult(
            asset_id=asset_id,
            asset_type=asset_state.get("type", "unknown"),
            probes_run=len(asset_probes),
            probes_healthy=healthy,
            probes_degraded=degraded,
            probes_unhealthy=unhealthy,
            overall_status=overall,
            probe_details=details,
            checked_at=datetime.now(timezone.utc).isoformat(),
        )
        
        # Record health check decision event (Phase 15)
        if self.decision_telemetry:
            self.decision_telemetry.record(
                category="recovery",
                decision="health_check",
                asset_id=asset_id,
                reason=f"Health probes: {healthy} healthy, {degraded} degraded, {unhealthy} unhealthy",
                health_status=overall.value,
                metadata={
                    "probes_run": len(asset_probes),
                    "probes_healthy": healthy,
                    "probes_degraded": degraded,
                    "probes_unhealthy": unhealthy,
                }
            )

        return result

    def detect_failure(
        self, asset_id: str, error_message: str, asset_type: str
    ) -> FailureDetectionResult:
        """Classify a failure for the given asset."""
        result = self.detector.detect(error_message, {"asset_ids": [asset_id], "type": asset_type})
        
        # Record failure classification decision event (Phase 15)
        if self.decision_telemetry:
            self.decision_telemetry.record(
                category="recovery",
                decision="failure_detected",
                asset_id=asset_id,
                reason=result.root_cause,
                failure_classification=result.classification.value,
                confidence_score=result.confidence,
                metadata={
                    "is_transient": result.is_transient,
                    "affected_assets": result.affected_assets,
                }
            )
        
        return result

    def get_recovery_strategy(
        self, classification: FailureClassification, asset_type: str
    ) -> RecoveryPolicy | None:
        """Find applicable recovery policy."""
        policy = self.policies.get(classification)
        if policy and policy.applies(classification, asset_type):
            return policy
        return None

    def attempt_recovery(
        self,
        asset_id: str,
        asset_type: str,
        classification: FailureClassification,
    ) -> RecoveryAction | None:
        """Attempt recovery for a failed asset."""
        import uuid

        # Check circuit breaker
        if asset_id not in self.state.circuit_breakers:
            self.state.circuit_breakers[asset_id] = CircuitBreaker(asset_id)

        breaker = self.state.circuit_breakers[asset_id]
        if not breaker.can_attempt_recovery():
            logger.warning(f"Circuit breaker OPEN for {asset_id}, skipping recovery")
            
            # Record circuit breaker state decision (Phase 15)
            if self.decision_telemetry:
                self.decision_telemetry.record(
                    category="recovery",
                    decision="recovery_blocked",
                    asset_id=asset_id,
                    reason="Circuit breaker is OPEN",
                    metadata={
                        "circuit_breaker_state": breaker.state.value,
                        "failure_count": breaker.failure_count,
                    }
                )
            return None

        # Find policy
        policy = self.get_recovery_strategy(classification, asset_type)
        if not policy:
            logger.info(f"No recovery policy for {classification} on {asset_type}")
            
            # Record no policy found decision (Phase 15)
            if self.decision_telemetry:
                self.decision_telemetry.record(
                    category="recovery",
                    decision="no_policy",
                    asset_id=asset_id,
                    reason=f"No recovery policy for {classification}",
                    failure_classification=classification.value,
                )
            return None

        # Create recovery action
        recovery_id = f"rec_{uuid.uuid4().hex[:12]}"
        attempt_num = len(
            [a for a in self.state.recovery_actions if a.asset_id == asset_id]
        ) + 1

        if attempt_num > policy.max_attempts:
            logger.error(
                f"Max recovery attempts ({policy.max_attempts}) exceeded for {asset_id}"
            )
            breaker.record_failure()
            
            # Record max attempts exceeded decision (Phase 15)
            if self.decision_telemetry:
                self.decision_telemetry.record(
                    category="recovery",
                    decision="max_attempts_exceeded",
                    asset_id=asset_id,
                    reason=f"Max recovery attempts ({policy.max_attempts}) exceeded",
                    recovery_strategy=policy.strategy.value,
                )
            return None

        action = RecoveryAction(
            recovery_id=recovery_id,
            asset_id=asset_id,
            strategy=policy.strategy,
            attempt_number=attempt_num,
            started_at=datetime.now(timezone.utc).isoformat(),
            correlation_id=get_correlation_id(),
        )

        self.state.recovery_actions.append(action)
        self.state.last_updated = datetime.now(timezone.utc).isoformat()

        logger.info(
            f"Recovery action {recovery_id}: {policy.strategy.value} for "
            f"{asset_id} (attempt {attempt_num}/{policy.max_attempts})"
        )
        
        # Record recovery strategy decision event (Phase 15)
        if self.decision_telemetry:
            self.decision_telemetry.record(
                category="recovery",
                decision="recovery_started",
                asset_id=asset_id,
                reason=f"Classification: {classification.value}",
                recovery_strategy=policy.strategy.value,
                failure_classification=classification.value,
                metadata={
                    "recovery_id": recovery_id,
                    "attempt_number": attempt_num,
                    "max_attempts": policy.max_attempts,
                    "backoff_seconds": policy.backoff_seconds,
                }
            )

        return action

    def mark_recovery_success(self, recovery_id: str, details: dict[str, Any] | None = None):
        """Mark recovery attempt as successful."""
        for action in self.state.recovery_actions:
            if action.recovery_id == recovery_id:
                action.completed_at = datetime.now(timezone.utc).isoformat()
                action.success = True
                action.details = details or {}

                # Update circuit breaker
                if action.asset_id in self.state.circuit_breakers:
                    breaker = self.state.circuit_breakers[action.asset_id]
                    breaker.record_success()

                logger.info(f"Recovery action {recovery_id} succeeded")
                
                # Record recovery success decision event (Phase 15)
                if self.decision_telemetry:
                    self.decision_telemetry.record(
                        category="recovery",
                        decision="recovery_succeeded",
                        asset_id=action.asset_id,
                        reason=f"Recovery action {recovery_id} completed successfully",
                        recovery_strategy=action.strategy.value,
                        metadata={
                            "recovery_id": recovery_id,
                            "attempt_number": action.attempt_number,
                            "duration_seconds": (
                                (datetime.fromisoformat(action.completed_at) - datetime.fromisoformat(action.started_at)).total_seconds()
                                if action.completed_at else None
                            ),
                            "details": action.details,
                        }
                    )
                break

        self.state.last_updated = datetime.now(timezone.utc).isoformat()

    def mark_recovery_failure(self, recovery_id: str, details: dict[str, Any] | None = None):
        """Mark recovery attempt as failed."""
        for action in self.state.recovery_actions:
            if action.recovery_id == recovery_id:
                action.completed_at = datetime.now(timezone.utc).isoformat()
                action.success = False
                action.details = details or {}

                # Update circuit breaker
                if action.asset_id in self.state.circuit_breakers:
                    breaker = self.state.circuit_breakers[action.asset_id]
                    breaker.record_failure()

                logger.warning(f"Recovery action {recovery_id} failed")
                
                # Record recovery failure decision event (Phase 15)
                if self.decision_telemetry:
                    self.decision_telemetry.record(
                        category="recovery",
                        decision="recovery_failed",
                        asset_id=action.asset_id,
                        reason=f"Recovery action {recovery_id} failed",
                        recovery_strategy=action.strategy.value,
                        metadata={
                            "recovery_id": recovery_id,
                            "attempt_number": action.attempt_number,
                            "duration_seconds": (
                                (datetime.fromisoformat(action.completed_at) - datetime.fromisoformat(action.started_at)).total_seconds()
                                if action.completed_at else None
                            ),
                            "details": action.details,
                        }
                    )
                break

        self.state.last_updated = datetime.now(timezone.utc).isoformat()

    def save_state(self, path: Path | None = None) -> None:
        """Persist recovery state to disk."""
        target = path or self.state_file
        if not target:
            logger.warning("No state file path provided for recovery state persistence")
            return

        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(self.state.to_dict(), indent=2), encoding="utf-8")
        logger.info(f"Recovery state saved to {target}")

    def load_state(self, path: Path | None = None) -> None:
        """Load recovery state from disk."""
        target = path or self.state_file
        if not target or not target.exists():
            logger.debug(f"Recovery state file not found: {target}")
            return

        data = json.loads(target.read_text(encoding="utf-8"))
        self.state = RecoveryState.from_dict(data)
        logger.info(f"Recovery state loaded from {target}")

    def recovery_summary(self) -> dict[str, Any]:
        """Generate recovery summary report."""
        total_actions = len(self.state.recovery_actions)
        successful = sum(1 for a in self.state.recovery_actions if a.success is True)
        failed = sum(1 for a in self.state.recovery_actions if a.success is False)

        circuit_states = {
            asset_id: breaker.state.value
            for asset_id, breaker in self.state.circuit_breakers.items()
        }

        return {
            "deployment_id": self.deployment_id,
            "project_key": self.project_key,
            "total_recovery_actions": total_actions,
            "successful_recoveries": successful,
            "failed_recoveries": failed,
            "success_rate": successful / total_actions if total_actions > 0 else 0.0,
            "circuit_breaker_states": circuit_states,
            "created_at": self.state.created_at,
            "last_updated": self.state.last_updated,
        }
