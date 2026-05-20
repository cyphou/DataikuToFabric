"""Strategy advisor — recommends migration strategy based on assessment."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from src.analyzers.project_analyzer import AssessmentResult, ReadinessGrade
from src.core.registry import AssetRegistry
from src.models.asset import AssetType


class MigrationStrategy(str, Enum):
    """Recommended migration approach."""
    FULL_AUTO = "full_auto"
    GUIDED = "guided"
    MANUAL_REVIEW = "manual_review"
    HYBRID = "hybrid"


class StorageRecommendation(str, Enum):
    """Recommended Fabric storage target."""
    LAKEHOUSE = "lakehouse"
    WAREHOUSE = "warehouse"
    BOTH = "both"


@dataclass
class DatasetPlacement:
    """Recommended placement for a single dataset."""
    dataset_name: str
    target: StorageRecommendation
    reason: str


@dataclass
class StrategyRecommendation:
    """Full migration strategy recommendation."""
    strategy: MigrationStrategy
    confidence: float  # 0-100
    rationale: str
    dataset_placements: list[DatasetPlacement] = field(default_factory=list)
    recommended_batch_size: int = 10
    recommended_parallelism: int = 2
    estimated_complexity: str = "medium"
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy": self.strategy.value,
            "confidence": round(self.confidence, 1),
            "rationale": self.rationale,
            "estimated_complexity": self.estimated_complexity,
            "recommended_batch_size": self.recommended_batch_size,
            "recommended_parallelism": self.recommended_parallelism,
            "dataset_placements": [
                {"dataset": dp.dataset_name, "target": dp.target.value, "reason": dp.reason}
                for dp in self.dataset_placements
            ],
            "warnings": self.warnings,
        }


def recommend_strategy(
    assessment: AssessmentResult,
    registry: AssetRegistry,
) -> StrategyRecommendation:
    """Produce a migration strategy recommendation based on assessment results.

    Args:
        assessment: Output from assess_project().
        registry: Populated registry for dataset analysis.

    Returns:
        StrategyRecommendation with approach, placements, and sizing.
    """
    warnings: list[str] = []

    # Determine strategy from grade
    if assessment.grade in (ReadinessGrade.A, ReadinessGrade.B):
        strategy = MigrationStrategy.FULL_AUTO
        confidence = assessment.overall_score
        rationale = (
            f"Grade {assessment.grade.value} ({assessment.overall_score:.0f}/100): "
            "project is well-suited for fully automated migration."
        )
        batch_size = 20
        parallelism = 4
        complexity = "low" if assessment.grade == ReadinessGrade.A else "medium"
    elif assessment.grade == ReadinessGrade.C:
        strategy = MigrationStrategy.GUIDED
        confidence = assessment.overall_score
        rationale = (
            f"Grade {assessment.grade.value} ({assessment.overall_score:.0f}/100): "
            "automated migration with expert review at key checkpoints."
        )
        batch_size = 10
        parallelism = 2
        complexity = "medium"
    elif assessment.grade == ReadinessGrade.D:
        strategy = MigrationStrategy.HYBRID
        confidence = assessment.overall_score
        rationale = (
            f"Grade {assessment.grade.value} ({assessment.overall_score:.0f}/100): "
            "some assets auto-migratable, others need manual conversion."
        )
        batch_size = 5
        parallelism = 1
        complexity = "high"
        warnings.append("Several asset categories scored below 40 — expect manual work")
    else:
        strategy = MigrationStrategy.MANUAL_REVIEW
        confidence = assessment.overall_score
        rationale = (
            f"Grade {assessment.grade.value} ({assessment.overall_score:.0f}/100): "
            "project requires significant manual review before migration."
        )
        batch_size = 3
        parallelism = 1
        complexity = "very_high"
        warnings.append("Overall score below 40 — manual migration planning recommended")

    # Dataset placement recommendations
    placements: list[DatasetPlacement] = []
    datasets = registry.get_by_type(AssetType.DATASET)
    for ds in datasets:
        ds_type = ds.metadata.get("type", "").lower()
        schema = ds.metadata.get("schema", [])
        row_count = ds.metadata.get("row_count", 0)

        if ds_type in ("oracle", "postgresql", "sqlserver", "mysql"):
            # Structured relational data → Warehouse
            placements.append(DatasetPlacement(
                dataset_name=ds.name,
                target=StorageRecommendation.WAREHOUSE,
                reason=f"Relational source ({ds_type}) — best in Warehouse for T-SQL queries",
            ))
        elif row_count and row_count > 1_000_000:
            # Large datasets → Lakehouse (Delta)
            placements.append(DatasetPlacement(
                dataset_name=ds.name,
                target=StorageRecommendation.LAKEHOUSE,
                reason=f"Large dataset ({row_count:,} rows) — Lakehouse with Delta for scalability",
            ))
        elif ds_type in ("csv", "parquet", "json", "filesystem"):
            placements.append(DatasetPlacement(
                dataset_name=ds.name,
                target=StorageRecommendation.LAKEHOUSE,
                reason=f"File-based source ({ds_type}) — Lakehouse with Delta tables",
            ))
        else:
            placements.append(DatasetPlacement(
                dataset_name=ds.name,
                target=StorageRecommendation.LAKEHOUSE,
                reason="Default placement — Lakehouse",
            ))

    # Add risk-based warnings
    for risk in assessment.risks:
        if risk.level.value in ("critical", "high"):
            warnings.append(f"[{risk.level.value.upper()}] {risk.description}")

    return StrategyRecommendation(
        strategy=strategy,
        confidence=confidence,
        rationale=rationale,
        dataset_placements=placements,
        recommended_batch_size=batch_size,
        recommended_parallelism=parallelism,
        estimated_complexity=complexity,
        warnings=warnings,
    )
