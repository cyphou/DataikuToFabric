"""Validation agent — verifies correctness of all migrated assets."""

from __future__ import annotations

from typing import Any

from src.agents.base_agent import AgentResult, AgentStatus, BaseAgent, ValidationResult
from src.core.logger import get_logger
from src.models.asset import MigrationState

logger = get_logger(__name__)


class ValidationAgent(BaseAgent):
    """Runs validation checks across all migrated assets."""

    @property
    def name(self) -> str:
        return "validator"

    @property
    def description(self) -> str:
        return "Validate all migrated assets for correctness and completeness"

    async def execute(self, context: Any) -> AgentResult:
        """Run validation across all converted assets."""
        registry = context.registry
        all_assets = registry.get_all()

        total = len(all_assets)
        converted = [a for a in all_assets if a.state in (
            MigrationState.CONVERTED, MigrationState.DEPLOYED, MigrationState.VALIDATED
        )]
        failed = [a for a in all_assets if a.state in (
            MigrationState.FAILED, MigrationState.DEPLOY_FAILED, MigrationState.VALIDATION_FAILED
        )]
        discovered_only = [a for a in all_assets if a.state == MigrationState.DISCOVERED]

        review_flags: list[str] = []
        errors: list[str] = []

        # Check 1: All assets were processed
        if discovered_only:
            for asset in discovered_only:
                review_flags.append(f"Asset not converted: {asset.name} ({asset.type.value})")

        # Check 2: No critical failures
        for asset in failed:
            for err in asset.errors:
                errors.append(f"{asset.name}: {err}")

        # Check 3: All converted assets have target info
        for asset in converted:
            if not asset.target_fabric_asset:
                review_flags.append(f"Converted but no target info: {asset.name}")

        # Check 4: Review flags summary
        for asset in all_assets:
            for flag in asset.review_flags:
                review_flags.append(f"{asset.name}: {flag}")

        # Mark validated assets
        for asset in converted:
            if not asset.errors:
                registry.update_state(asset.id, MigrationState.VALIDATED)
            else:
                registry.update_state(asset.id, MigrationState.VALIDATION_FAILED)

        registry.save()

        return AgentResult(
            agent_name=self.name,
            status=AgentStatus.COMPLETED,
            assets_processed=total,
            assets_converted=len(converted),
            assets_failed=len(failed),
            review_flags=review_flags,
            errors=errors,
            details={
                "total_assets": total,
                "converted": len(converted),
                "failed": len(failed),
                "not_processed": len(discovered_only),
                "statistics": registry.get_statistics(),
            },
        )

    async def validate(self, context: Any) -> ValidationResult:
        """Meta-validation: check that validation itself ran correctly."""
        registry = context.registry
        stats = registry.get_statistics()
        checks_run = 1
        failures: list[str] = []

        # Verify that all assets have a terminal state
        by_state = stats.get("by_state", {})
        non_terminal = by_state.get("converting", 0) + by_state.get("deploying", 0) + by_state.get("validating", 0)
        if non_terminal > 0:
            failures.append(f"{non_terminal} assets stuck in non-terminal state")

        return ValidationResult(
            passed=len(failures) == 0,
            checks_run=checks_run,
            checks_passed=checks_run - len(failures),
            checks_failed=len(failures),
            failures=failures,
        )
