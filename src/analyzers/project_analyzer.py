"""Project analyzer — scores a Dataiku project's migration readiness."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from src.core.registry import AssetRegistry
from src.models.asset import Asset, AssetType, MigrationState


class ReadinessGrade(str, Enum):
    """Migration readiness grade."""
    A = "A"  # >= 90
    B = "B"  # >= 75
    C = "C"  # >= 60
    D = "D"  # >= 40
    F = "F"  # < 40


class AssessmentCategory(str, Enum):
    """Assessment scoring categories."""
    SQL_COMPLEXITY = "sql_complexity"
    PYTHON_SDK_USAGE = "python_sdk_usage"
    VISUAL_RECIPES = "visual_recipes"
    DATA_VOLUME = "data_volume"
    CONNECTIONS = "connections"
    DEPENDENCIES = "dependencies"
    CUSTOM_PLUGINS = "custom_plugins"
    SCENARIOS = "scenarios"


class RiskLevel(str, Enum):
    """Risk severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class CategoryScore:
    """Score for a single assessment category."""
    category: AssessmentCategory
    score: float  # 0-100
    max_score: float = 100.0
    findings: list[str] = field(default_factory=list)
    asset_count: int = 0


@dataclass
class RiskItem:
    """A risk identified during assessment."""
    level: RiskLevel
    category: AssessmentCategory
    description: str
    affected_assets: list[str] = field(default_factory=list)
    mitigation: str = ""


@dataclass
class AssessmentResult:
    """Complete assessment of a Dataiku project."""
    project_key: str
    overall_score: float = 0.0
    grade: ReadinessGrade = ReadinessGrade.F
    category_scores: list[CategoryScore] = field(default_factory=list)
    risks: list[RiskItem] = field(default_factory=list)
    total_assets: int = 0
    summary: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Serialize for JSON export."""
        return {
            "project_key": self.project_key,
            "overall_score": round(self.overall_score, 1),
            "grade": self.grade.value,
            "total_assets": self.total_assets,
            "summary": self.summary,
            "category_scores": [
                {
                    "category": cs.category.value,
                    "score": round(cs.score, 1),
                    "max_score": cs.max_score,
                    "asset_count": cs.asset_count,
                    "findings": cs.findings,
                }
                for cs in self.category_scores
            ],
            "risks": [
                {
                    "level": r.level.value,
                    "category": r.category.value,
                    "description": r.description,
                    "affected_assets": r.affected_assets,
                    "mitigation": r.mitigation,
                }
                for r in self.risks
            ],
        }


# ── Category weights for overall score ─────────────────────

CATEGORY_WEIGHTS: dict[AssessmentCategory, float] = {
    AssessmentCategory.SQL_COMPLEXITY: 0.25,
    AssessmentCategory.PYTHON_SDK_USAGE: 0.20,
    AssessmentCategory.VISUAL_RECIPES: 0.10,
    AssessmentCategory.DATA_VOLUME: 0.10,
    AssessmentCategory.CONNECTIONS: 0.10,
    AssessmentCategory.DEPENDENCIES: 0.10,
    AssessmentCategory.CUSTOM_PLUGINS: 0.10,
    AssessmentCategory.SCENARIOS: 0.05,
}

# SQL complexity patterns
_COMPLEX_SQL_PATTERNS = [
    (r"\bCONNECT\s+BY\b", "CONNECT BY hierarchical query", 15),
    (r"\bMERGE\s+INTO\b", "MERGE statement", 10),
    (r"\bCURSOR\b", "PL/SQL cursor", 20),
    (r"\bBEGIN\b.*\bEND\b", "PL/SQL block", 20),
    (r"\bPIVOT\b", "PIVOT clause", 5),
    (r"\bUNPIVOT\b", "UNPIVOT clause", 5),
    (r"\bLATERAL\b", "LATERAL join", 10),
    (r"\bFOR\s+XML\b", "FOR XML clause", 10),
    (r"\bXMLTABLE\b", "XMLTABLE function", 15),
    (r"\bROWNUM\b", "ROWNUM (Oracle)", 5),
    (r"\bCONNECT_BY_ROOT\b", "CONNECT_BY_ROOT (Oracle)", 15),
]


def _score_grade(score: float) -> ReadinessGrade:
    """Map a numeric score to a letter grade."""
    if score >= 90:
        return ReadinessGrade.A
    elif score >= 75:
        return ReadinessGrade.B
    elif score >= 60:
        return ReadinessGrade.C
    elif score >= 40:
        return ReadinessGrade.D
    return ReadinessGrade.F


def score_sql_complexity(assets: list[Asset]) -> CategoryScore:
    """Score SQL recipe complexity."""
    sql_assets = [a for a in assets if a.type == AssetType.RECIPE_SQL]
    if not sql_assets:
        return CategoryScore(
            category=AssessmentCategory.SQL_COMPLEXITY,
            score=100.0,
            findings=["No SQL recipes — no SQL risk"],
            asset_count=0,
        )

    total_penalty = 0
    findings: list[str] = []

    for asset in sql_assets:
        payload = asset.metadata.get("payload", "")
        if not payload:
            continue
        for pattern, label, penalty in _COMPLEX_SQL_PATTERNS:
            if re.search(pattern, payload, re.IGNORECASE | re.DOTALL):
                total_penalty += penalty
                findings.append(f"{asset.name}: {label}")

        # Penalize very long SQL
        line_count = payload.count("\n") + 1
        if line_count > 200:
            total_penalty += 10
            findings.append(f"{asset.name}: very long SQL ({line_count} lines)")

    # Normalize penalty: cap at 100, scale by asset count
    avg_penalty = total_penalty / len(sql_assets) if sql_assets else 0
    score = max(0.0, 100.0 - avg_penalty)
    return CategoryScore(
        category=AssessmentCategory.SQL_COMPLEXITY,
        score=score,
        findings=findings,
        asset_count=len(sql_assets),
    )


def score_python_sdk_usage(assets: list[Asset]) -> CategoryScore:
    """Score Python recipe SDK usage complexity."""
    py_assets = [a for a in assets if a.type == AssetType.RECIPE_PYTHON]
    if not py_assets:
        return CategoryScore(
            category=AssessmentCategory.PYTHON_SDK_USAGE,
            score=100.0,
            findings=["No Python recipes — no Python risk"],
            asset_count=0,
        )

    total_penalty = 0
    findings: list[str] = []

    for asset in py_assets:
        payload = asset.metadata.get("payload", "")
        if not payload:
            continue

        # Penalize hard-to-convert patterns
        if "dataiku.Folder" in payload:
            total_penalty += 10
            findings.append(f"{asset.name}: uses dataiku.Folder (managed folder)")
        if "dataiku.api_client" in payload:
            total_penalty += 15
            findings.append(f"{asset.name}: uses internal API client")
        if "dataiku.Model" in payload or "dataiku.saved_model" in payload:
            total_penalty += 20
            findings.append(f"{asset.name}: uses saved model API")
        if re.search(r"import\s+custom_", payload):
            total_penalty += 25
            findings.append(f"{asset.name}: imports custom plugin module")
        if "subprocess" in payload:
            total_penalty += 15
            findings.append(f"{asset.name}: uses subprocess calls")

    avg_penalty = total_penalty / len(py_assets) if py_assets else 0
    score = max(0.0, 100.0 - avg_penalty)
    return CategoryScore(
        category=AssessmentCategory.PYTHON_SDK_USAGE,
        score=score,
        findings=findings,
        asset_count=len(py_assets),
    )


def score_visual_recipes(assets: list[Asset]) -> CategoryScore:
    """Score visual recipe conversion readiness — generally high."""
    visual_assets = [a for a in assets if a.type == AssetType.RECIPE_VISUAL]
    if not visual_assets:
        return CategoryScore(
            category=AssessmentCategory.VISUAL_RECIPES,
            score=100.0,
            findings=["No visual recipes"],
            asset_count=0,
        )

    findings: list[str] = []
    unsupported = 0
    for asset in visual_assets:
        recipe_type = asset.metadata.get("type", "")
        if recipe_type in ("split", "sample"):
            unsupported += 1
            findings.append(f"{asset.name}: {recipe_type} recipe (limited support)")

    score = 100.0 - (unsupported / len(visual_assets) * 30) if visual_assets else 100.0
    return CategoryScore(
        category=AssessmentCategory.VISUAL_RECIPES,
        score=max(0.0, score),
        findings=findings,
        asset_count=len(visual_assets),
    )


def score_data_volume(assets: list[Asset]) -> CategoryScore:
    """Score based on dataset sizes and count."""
    datasets = [a for a in assets if a.type == AssetType.DATASET]
    if not datasets:
        return CategoryScore(
            category=AssessmentCategory.DATA_VOLUME,
            score=100.0,
            findings=["No datasets"],
            asset_count=0,
        )

    findings: list[str] = []
    penalty = 0

    # Large dataset count
    if len(datasets) > 50:
        penalty += 15
        findings.append(f"Large dataset count: {len(datasets)}")
    elif len(datasets) > 100:
        penalty += 30
        findings.append(f"Very large dataset count: {len(datasets)}")

    for ds in datasets:
        row_count = ds.metadata.get("row_count", 0)
        size_bytes = ds.metadata.get("size_bytes", 0)
        if row_count and row_count > 10_000_000:
            penalty += 5
            findings.append(f"{ds.name}: large dataset ({row_count:,} rows)")
        if size_bytes and size_bytes > 1_073_741_824:  # 1 GB
            penalty += 10
            findings.append(f"{ds.name}: very large ({size_bytes / 1_073_741_824:.1f} GB)")

    score = max(0.0, 100.0 - penalty)
    return CategoryScore(
        category=AssessmentCategory.DATA_VOLUME,
        score=score,
        findings=findings,
        asset_count=len(datasets),
    )


def score_connections(assets: list[Asset]) -> CategoryScore:
    """Score connection diversity and compatibility."""
    connections = [a for a in assets if a.type == AssetType.CONNECTION]
    if not connections:
        return CategoryScore(
            category=AssessmentCategory.CONNECTIONS,
            score=100.0,
            findings=["No connections"],
            asset_count=0,
        )

    findings: list[str] = []
    penalty = 0

    WELL_SUPPORTED = {"PostgreSQL", "Oracle", "SQLSERVER", "MySQL", "Azure", "Snowflake", "BigQuery"}
    PARTIAL_SUPPORT = {"HDFS", "S3", "GCS", "MongoDB", "Elasticsearch", "Cassandra"}

    for conn in connections:
        conn_type = conn.metadata.get("type", "unknown")
        if conn_type in WELL_SUPPORTED:
            pass  # no penalty
        elif conn_type in PARTIAL_SUPPORT:
            penalty += 10
            findings.append(f"{conn.name}: {conn_type} (partial support — needs gateway/shortcut)")
        else:
            penalty += 20
            findings.append(f"{conn.name}: {conn_type} (unsupported — manual config needed)")

    score = max(0.0, 100.0 - penalty)
    return CategoryScore(
        category=AssessmentCategory.CONNECTIONS,
        score=score,
        findings=findings,
        asset_count=len(connections),
    )


def score_dependencies(assets: list[Asset]) -> CategoryScore:
    """Score dependency complexity."""
    all_assets = assets
    if not all_assets:
        return CategoryScore(
            category=AssessmentCategory.DEPENDENCIES,
            score=100.0,
            findings=["No assets"],
            asset_count=0,
        )

    findings: list[str] = []
    total_deps = sum(len(a.dependencies) for a in all_assets)
    max_deps = max((len(a.dependencies) for a in all_assets), default=0)

    penalty = 0
    if max_deps > 10:
        penalty += 15
        findings.append(f"Highly connected asset with {max_deps} dependencies")
    if total_deps > 100:
        penalty += 10
        findings.append(f"Complex dependency graph: {total_deps} total edges")

    # Check for circular-like patterns (same prefix in deps)
    for a in all_assets:
        if a.id in a.dependencies:
            penalty += 25
            findings.append(f"{a.name}: self-dependency detected")

    score = max(0.0, 100.0 - penalty)
    return CategoryScore(
        category=AssessmentCategory.DEPENDENCIES,
        score=score,
        findings=findings,
        asset_count=len(all_assets),
    )


def score_custom_plugins(assets: list[Asset]) -> CategoryScore:
    """Score custom plugin usage."""
    findings: list[str] = []
    plugin_count = 0

    for asset in assets:
        recipe_type = asset.metadata.get("type", "")
        payload = asset.metadata.get("payload", "")
        if recipe_type.startswith("custom"):
            plugin_count += 1
            findings.append(f"{asset.name}: custom plugin recipe")
        elif payload and re.search(r"import\s+custom_", payload):
            plugin_count += 1
            findings.append(f"{asset.name}: imports custom module")

    if plugin_count == 0:
        return CategoryScore(
            category=AssessmentCategory.CUSTOM_PLUGINS,
            score=100.0,
            findings=["No custom plugins detected"],
            asset_count=0,
        )

    score = max(0.0, 100.0 - plugin_count * 20)
    return CategoryScore(
        category=AssessmentCategory.CUSTOM_PLUGINS,
        score=score,
        findings=findings,
        asset_count=plugin_count,
    )


def score_scenarios(assets: list[Asset]) -> CategoryScore:
    """Score scenario / scheduling complexity."""
    scenarios = [a for a in assets if a.type == AssetType.SCENARIO]
    if not scenarios:
        return CategoryScore(
            category=AssessmentCategory.SCENARIOS,
            score=100.0,
            findings=["No scenarios"],
            asset_count=0,
        )

    findings: list[str] = []
    penalty = 0

    for sc in scenarios:
        steps = sc.metadata.get("steps", [])
        triggers = sc.metadata.get("triggers", [])
        if len(steps) > 10:
            penalty += 10
            findings.append(f"{sc.name}: complex scenario ({len(steps)} steps)")
        if any(t.get("type") == "custom" for t in triggers):
            penalty += 15
            findings.append(f"{sc.name}: custom trigger type")

    score = max(0.0, 100.0 - penalty)
    return CategoryScore(
        category=AssessmentCategory.SCENARIOS,
        score=score,
        findings=findings,
        asset_count=len(scenarios),
    )


# ── Score functions lookup ─────────────────────────────────

_SCORERS = {
    AssessmentCategory.SQL_COMPLEXITY: score_sql_complexity,
    AssessmentCategory.PYTHON_SDK_USAGE: score_python_sdk_usage,
    AssessmentCategory.VISUAL_RECIPES: score_visual_recipes,
    AssessmentCategory.DATA_VOLUME: score_data_volume,
    AssessmentCategory.CONNECTIONS: score_connections,
    AssessmentCategory.DEPENDENCIES: score_dependencies,
    AssessmentCategory.CUSTOM_PLUGINS: score_custom_plugins,
    AssessmentCategory.SCENARIOS: score_scenarios,
}


def assess_project(registry: AssetRegistry) -> AssessmentResult:
    """Run full assessment on a project registry and produce a readiness score.

    Args:
        registry: Populated AssetRegistry with discovered assets.

    Returns:
        AssessmentResult with category scores, risks, and overall grade.
    """
    all_assets = registry.get_all()
    result = AssessmentResult(
        project_key=registry.project_key,
        total_assets=len(all_assets),
    )

    # Score each category
    for category, scorer in _SCORERS.items():
        cs = scorer(all_assets)
        result.category_scores.append(cs)

    # Compute weighted overall score
    weighted_sum = 0.0
    weight_sum = 0.0
    for cs in result.category_scores:
        w = CATEGORY_WEIGHTS.get(cs.category, 0.1)
        weighted_sum += cs.score * w
        weight_sum += w

    result.overall_score = weighted_sum / weight_sum if weight_sum > 0 else 0.0
    result.grade = _score_grade(result.overall_score)

    # Generate risks from low-scoring categories
    for cs in result.category_scores:
        if cs.score < 40:
            result.risks.append(RiskItem(
                level=RiskLevel.CRITICAL,
                category=cs.category,
                description=f"{cs.category.value} score critically low ({cs.score:.0f}/100)",
                affected_assets=[f for f in cs.findings],
                mitigation="Manual review and remediation required before migration",
            ))
        elif cs.score < 60:
            result.risks.append(RiskItem(
                level=RiskLevel.HIGH,
                category=cs.category,
                description=f"{cs.category.value} score low ({cs.score:.0f}/100)",
                affected_assets=[f for f in cs.findings],
                mitigation="Guided migration recommended with expert review",
            ))
        elif cs.score < 75:
            result.risks.append(RiskItem(
                level=RiskLevel.MEDIUM,
                category=cs.category,
                description=f"{cs.category.value} score moderate ({cs.score:.0f}/100)",
                affected_assets=[f for f in cs.findings],
                mitigation="Auto-migration feasible with post-migration validation",
            ))

    result.summary = (
        f"Project '{registry.project_key}' assessed: {result.total_assets} assets, "
        f"overall score {result.overall_score:.1f}/100 (Grade {result.grade.value}). "
        f"{len(result.risks)} risk(s) identified."
    )

    return result
