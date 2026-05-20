"""Merge config — Pydantic models for merge settings."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class ConflictResolution(str, Enum):
    KEEP_FIRST = "keep_first"
    KEEP_LATEST = "keep_latest"
    RENAME = "rename"
    MANUAL = "manual"


class MergeConfig(BaseModel):
    """Configuration for multi-project merge."""

    target_workspace: str = Field(default="merged", description="Target workspace name")
    conflict_resolution: ConflictResolution = Field(
        default=ConflictResolution.RENAME,
        description="How to handle duplicate asset names",
    )
    prefix_with_project: bool = Field(
        default=True,
        description="Prefix asset names with project key to avoid conflicts",
    )
    deduplicate_datasets: bool = Field(
        default=True,
        description="Identify and merge identical datasets across projects",
    )
    deduplicate_connections: bool = Field(
        default=True,
        description="Merge identical connections into shared definitions",
    )
    similarity_threshold: float = Field(
        default=0.85,
        description="Minimum similarity score to consider assets duplicates (0-1)",
        ge=0.0,
        le=1.0,
    )
