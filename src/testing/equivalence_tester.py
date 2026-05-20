"""Equivalence tester — verify source and target produce same results."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from src.models.asset import Asset, AssetType


class EquivalenceStatus(str, Enum):
    EQUIVALENT = "equivalent"
    DIFFERENT = "different"
    UNTESTABLE = "untestable"
    ERROR = "error"


@dataclass
class ColumnComparison:
    """Comparison of a single column between source and target."""
    column_name: str
    source_type: str
    target_type: str
    types_match: bool
    nullable_match: bool = True


@dataclass
class EquivalenceResult:
    """Result of equivalence testing for a single asset."""
    asset_name: str
    status: EquivalenceStatus
    column_comparisons: list[ColumnComparison] = field(default_factory=list)
    source_row_count: int | None = None
    target_row_count: int | None = None
    row_count_match: bool = True
    findings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "asset_name": self.asset_name,
            "status": self.status.value,
            "source_row_count": self.source_row_count,
            "target_row_count": self.target_row_count,
            "row_count_match": self.row_count_match,
            "column_count": len(self.column_comparisons),
            "column_mismatches": sum(
                1 for c in self.column_comparisons if not c.types_match
            ),
            "findings": self.findings,
        }


@dataclass
class EquivalenceSuite:
    """Full equivalence test suite results."""
    project_key: str
    results: list[EquivalenceResult] = field(default_factory=list)
    total_tested: int = 0
    total_equivalent: int = 0
    total_different: int = 0
    total_untestable: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "project_key": self.project_key,
            "total_tested": self.total_tested,
            "total_equivalent": self.total_equivalent,
            "total_different": self.total_different,
            "total_untestable": self.total_untestable,
            "pass_rate": (
                self.total_equivalent / self.total_tested * 100
                if self.total_tested > 0 else 0.0
            ),
            "results": [r.to_dict() for r in self.results],
        }


# Type compatibility rules: source -> set of compatible targets
_TYPE_COMPAT: dict[str, set[str]] = {
    "NUMBER": {"DECIMAL", "INT", "BIGINT", "FLOAT", "NUMERIC"},
    "VARCHAR2": {"NVARCHAR", "VARCHAR", "NVARCHAR(MAX)"},
    "CLOB": {"NVARCHAR(MAX)", "TEXT"},
    "DATE": {"DATETIME2", "DATETIME", "DATE"},
    "TIMESTAMP": {"DATETIME2", "DATETIMEOFFSET"},
    "INTEGER": {"INT", "BIGINT"},
    "TEXT": {"NVARCHAR(MAX)", "VARCHAR(MAX)"},
    "BOOLEAN": {"BIT", "BOOLEAN"},
    "SERIAL": {"INT IDENTITY(1,1)", "INT"},
    "BIGSERIAL": {"BIGINT IDENTITY(1,1)", "BIGINT"},
    "BYTEA": {"VARBINARY(MAX)", "VARBINARY"},
    "UUID": {"UNIQUEIDENTIFIER", "NVARCHAR(36)"},
    "JSONB": {"NVARCHAR(MAX)"},
    "JSON": {"NVARCHAR(MAX)"},
}


def _types_compatible(source_type: str, target_type: str) -> bool:
    """Check if source and target types are compatible."""
    if source_type.upper() == target_type.upper():
        return True
    compatible = _TYPE_COMPAT.get(source_type.upper(), set())
    return target_type.upper() in compatible


def test_equivalence(asset: Asset) -> EquivalenceResult:
    """Test equivalence for a single asset by comparing source and target schemas.

    Args:
        asset: Asset with both source metadata and target_fabric_asset info.

    Returns:
        EquivalenceResult with column comparison and row count checks.
    """
    if asset.type != AssetType.DATASET:
        return EquivalenceResult(
            asset_name=asset.name,
            status=EquivalenceStatus.UNTESTABLE,
            findings=["Only datasets can be tested for equivalence"],
        )

    target = asset.target_fabric_asset
    if not target:
        return EquivalenceResult(
            asset_name=asset.name,
            status=EquivalenceStatus.UNTESTABLE,
            findings=["No target info — asset not yet converted"],
        )

    source_schema = asset.metadata.get("schema", [])
    target_schema = target.get("schema", [])

    if not source_schema and not target_schema:
        return EquivalenceResult(
            asset_name=asset.name,
            status=EquivalenceStatus.UNTESTABLE,
            findings=["No schema info on source or target"],
        )

    result = EquivalenceResult(asset_name=asset.name, status=EquivalenceStatus.EQUIVALENT)

    # Row count comparison
    source_rows = asset.metadata.get("row_count")
    target_rows = target.get("row_count")
    result.source_row_count = source_rows
    result.target_row_count = target_rows

    if source_rows is not None and target_rows is not None:
        result.row_count_match = source_rows == target_rows
        if not result.row_count_match:
            result.findings.append(
                f"Row count mismatch: source={source_rows}, target={target_rows}"
            )
            result.status = EquivalenceStatus.DIFFERENT

    # Column comparison
    source_cols = {
        (c.get("name") if isinstance(c, dict) else c): c
        for c in source_schema
    }
    target_cols = {
        (c.get("name") if isinstance(c, dict) else c): c
        for c in target_schema
    }

    for col_name, src_col in source_cols.items():
        src_type = src_col.get("type", "unknown") if isinstance(src_col, dict) else "unknown"

        if col_name not in target_cols:
            result.findings.append(f"Column '{col_name}' missing in target")
            result.status = EquivalenceStatus.DIFFERENT
            continue

        tgt_col = target_cols[col_name]
        tgt_type = tgt_col.get("type", "unknown") if isinstance(tgt_col, dict) else "unknown"

        types_match = _types_compatible(src_type, tgt_type)
        result.column_comparisons.append(ColumnComparison(
            column_name=col_name,
            source_type=src_type,
            target_type=tgt_type,
            types_match=types_match,
        ))

        if not types_match:
            result.findings.append(
                f"Column '{col_name}' type mismatch: {src_type} → {tgt_type}"
            )
            result.status = EquivalenceStatus.DIFFERENT

    # Check for extra target columns
    for col_name in target_cols:
        if col_name not in source_cols:
            result.findings.append(f"Extra column in target: '{col_name}'")

    return result


def run_equivalence_suite(assets: list[Asset]) -> EquivalenceSuite:
    """Run equivalence tests on all dataset assets."""
    suite = EquivalenceSuite(project_key="")
    datasets = [a for a in assets if a.type == AssetType.DATASET]

    for ds in datasets:
        eq_result = test_equivalence(ds)
        suite.results.append(eq_result)
        suite.total_tested += 1

        if eq_result.status == EquivalenceStatus.EQUIVALENT:
            suite.total_equivalent += 1
        elif eq_result.status == EquivalenceStatus.DIFFERENT:
            suite.total_different += 1
        else:
            suite.total_untestable += 1

    return suite
