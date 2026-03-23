"""Asset data models for the migration registry."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class AssetType(str, Enum):
    """Types of Dataiku assets that can be migrated."""

    RECIPE_SQL = "recipe.sql"
    RECIPE_PYTHON = "recipe.python"
    RECIPE_VISUAL = "recipe.visual"
    DATASET = "dataset"
    MANAGED_FOLDER = "managed_folder"
    CONNECTION = "connection"
    FLOW = "flow"
    SCENARIO = "scenario"
    SAVED_MODEL = "saved_model"
    DASHBOARD = "dashboard"


class MigrationState(str, Enum):
    """State of an asset in the migration pipeline."""

    DISCOVERED = "discovered"
    CONVERTING = "converting"
    CONVERTED = "converted"
    DEPLOYING = "deploying"
    DEPLOYED = "deployed"
    VALIDATING = "validating"
    VALIDATED = "validated"
    FAILED = "failed"
    DEPLOY_FAILED = "deploy_failed"
    VALIDATION_FAILED = "validation_failed"


class AssetTimestamps(BaseModel):
    """Timestamps tracking asset lifecycle."""

    discovered_at: datetime | None = None
    converted_at: datetime | None = None
    deployed_at: datetime | None = None
    validated_at: datetime | None = None


class Asset(BaseModel):
    """A single Dataiku asset tracked in the migration registry."""

    id: str = Field(description="Unique asset identifier")
    type: AssetType = Field(description="Asset type classification")
    name: str = Field(description="Human-readable asset name")
    source_project: str = Field(description="Dataiku project key")
    state: MigrationState = Field(default=MigrationState.DISCOVERED)
    metadata: dict[str, Any] = Field(default_factory=dict)
    dependencies: list[str] = Field(default_factory=list)
    target_fabric_asset: dict[str, Any] | None = None
    errors: list[str] = Field(default_factory=list)
    review_flags: list[str] = Field(default_factory=list)
    timestamps: AssetTimestamps = Field(default_factory=AssetTimestamps)
