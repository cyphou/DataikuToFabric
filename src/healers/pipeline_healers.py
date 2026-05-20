"""Pipeline healers — auto-fix Fabric Data Pipeline JSON issues."""

from __future__ import annotations

import json
from typing import Any

from src.healers.base_healer import BaseHealer, HealerCategory, HealResult


class MissingTimeoutHealer(BaseHealer):
    """Add default timeout to pipeline activities missing one."""

    @property
    def name(self) -> str:
        return "add_activity_timeout"

    @property
    def category(self) -> HealerCategory:
        return HealerCategory.PIPELINE

    @property
    def description(self) -> str:
        return "Add default timeout to pipeline activities"

    def can_heal(self, content: str, metadata: dict[str, Any]) -> bool:
        try:
            data = json.loads(content)
            activities = data.get("properties", {}).get("activities", [])
            return any("timeout" not in a.get("policy", {}) for a in activities if "policy" in a)
        except (json.JSONDecodeError, AttributeError):
            return False

    def heal(self, content: str, metadata: dict[str, Any]) -> HealResult:
        try:
            data = json.loads(content)
            fixed_count = 0
            for activity in data.get("properties", {}).get("activities", []):
                if "policy" in activity and "timeout" not in activity["policy"]:
                    activity["policy"]["timeout"] = "01:00:00"
                    fixed_count += 1

            if fixed_count > 0:
                after = json.dumps(data, indent=2)
                return HealResult(
                    healer_name=self.name,
                    asset_name=metadata.get("name", "unknown"),
                    applied=True,
                    description=f"Added timeout to {fixed_count} activities",
                    before=content,
                    after=after,
                )
        except (json.JSONDecodeError, AttributeError):
            pass

        return HealResult(
            healer_name=self.name,
            asset_name=metadata.get("name", "unknown"),
            applied=False,
            description="No timeout fixes needed",
        )


class MissingRetryHealer(BaseHealer):
    """Add default retry policy to activities."""

    @property
    def name(self) -> str:
        return "add_retry_policy"

    @property
    def category(self) -> HealerCategory:
        return HealerCategory.PIPELINE

    @property
    def description(self) -> str:
        return "Add default retry policy to pipeline activities"

    def can_heal(self, content: str, metadata: dict[str, Any]) -> bool:
        try:
            data = json.loads(content)
            activities = data.get("properties", {}).get("activities", [])
            return any("policy" not in a for a in activities)
        except (json.JSONDecodeError, AttributeError):
            return False

    def heal(self, content: str, metadata: dict[str, Any]) -> HealResult:
        try:
            data = json.loads(content)
            fixed_count = 0
            for activity in data.get("properties", {}).get("activities", []):
                if "policy" not in activity:
                    activity["policy"] = {
                        "timeout": "01:00:00",
                        "retry": 2,
                        "retryIntervalInSeconds": 30,
                    }
                    fixed_count += 1

            if fixed_count > 0:
                after = json.dumps(data, indent=2)
                return HealResult(
                    healer_name=self.name,
                    asset_name=metadata.get("name", "unknown"),
                    applied=True,
                    description=f"Added retry policy to {fixed_count} activities",
                    before=content,
                    after=after,
                )
        except (json.JSONDecodeError, AttributeError):
            pass

        return HealResult(
            healer_name=self.name,
            asset_name=metadata.get("name", "unknown"),
            applied=False,
            description="No retry fixes needed",
        )


class DependencyOrderHealer(BaseHealer):
    """Ensure activity dependencies reference existing activities."""

    @property
    def name(self) -> str:
        return "fix_dependency_refs"

    @property
    def category(self) -> HealerCategory:
        return HealerCategory.PIPELINE

    @property
    def description(self) -> str:
        return "Remove invalid activity dependency references"

    def can_heal(self, content: str, metadata: dict[str, Any]) -> bool:
        try:
            data = json.loads(content)
            activities = data.get("properties", {}).get("activities", [])
            names = {a.get("name") for a in activities}
            for a in activities:
                for dep in a.get("dependsOn", []):
                    if dep.get("activity") not in names:
                        return True
        except (json.JSONDecodeError, AttributeError):
            pass
        return False

    def heal(self, content: str, metadata: dict[str, Any]) -> HealResult:
        try:
            data = json.loads(content)
            activities = data.get("properties", {}).get("activities", [])
            names = {a.get("name") for a in activities}
            removed = 0

            for a in activities:
                if "dependsOn" in a:
                    original_len = len(a["dependsOn"])
                    a["dependsOn"] = [
                        d for d in a["dependsOn"] if d.get("activity") in names
                    ]
                    removed += original_len - len(a["dependsOn"])

            if removed > 0:
                after = json.dumps(data, indent=2)
                return HealResult(
                    healer_name=self.name,
                    asset_name=metadata.get("name", "unknown"),
                    applied=True,
                    description=f"Removed {removed} invalid dependency reference(s)",
                    before=content,
                    after=after,
                )
        except (json.JSONDecodeError, AttributeError):
            pass

        return HealResult(
            healer_name=self.name,
            asset_name=metadata.get("name", "unknown"),
            applied=False,
            description="No dependency fixes needed",
        )


def get_pipeline_healers() -> list[BaseHealer]:
    """Return all pipeline healers."""
    return [
        MissingTimeoutHealer(),
        MissingRetryHealer(),
        DependencyOrderHealer(),
    ]
