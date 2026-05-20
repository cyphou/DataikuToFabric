"""Tests for Phase 19 — Pre-migration Assessment."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.analyzers.project_analyzer import (
    AssessmentCategory,
    AssessmentResult,
    ReadinessGrade,
    RiskLevel,
    assess_project,
    score_sql_complexity,
    score_python_sdk_usage,
    score_visual_recipes,
    score_data_volume,
    score_connections,
    score_dependencies,
    score_custom_plugins,
    score_scenarios,
    _score_grade,
)
from src.analyzers.strategy_advisor import (
    MigrationStrategy,
    StorageRecommendation,
    recommend_strategy,
)
from src.reports.assessment_report import generate_assessment_html, save_assessment_report
from src.core.registry import AssetRegistry
from src.models.asset import Asset, AssetType, MigrationState


# ── Grading ───────────────────────────────────────────────────

class TestGrading:
    def test_grade_a(self):
        assert _score_grade(95) == ReadinessGrade.A

    def test_grade_b(self):
        assert _score_grade(82) == ReadinessGrade.B

    def test_grade_c(self):
        assert _score_grade(65) == ReadinessGrade.C

    def test_grade_d(self):
        assert _score_grade(45) == ReadinessGrade.D

    def test_grade_f(self):
        assert _score_grade(20) == ReadinessGrade.F

    def test_grade_boundary_a(self):
        assert _score_grade(90) == ReadinessGrade.A

    def test_grade_boundary_b(self):
        assert _score_grade(75) == ReadinessGrade.B

    def test_grade_boundary_c(self):
        assert _score_grade(60) == ReadinessGrade.C

    def test_grade_boundary_d(self):
        assert _score_grade(40) == ReadinessGrade.D

    def test_grade_zero(self):
        assert _score_grade(0) == ReadinessGrade.F


# ── Scoring Functions ─────────────────────────────────────────

class TestScoringFunctions:
    def test_score_sql_empty(self):
        result = score_sql_complexity([])
        assert result.score == result.max_score

    def test_score_sql_simple_query(self):
        assets = [
            Asset(id="s1", name="s1", type=AssetType.RECIPE_SQL, source_project="P",
                  metadata={"payload": "SELECT a, b FROM t"})
        ]
        result = score_sql_complexity(assets)
        assert result.score > 0

    def test_score_sql_complex_query(self):
        assets = [
            Asset(id="s1", name="s1", type=AssetType.RECIPE_SQL, source_project="P",
                  metadata={"payload": "SELECT NVL(a, b), DECODE(c, 1, 'x', 'y') FROM t WHERE ROWNUM <= 10 START WITH parent IS NULL CONNECT BY PRIOR id = parent"})
        ]
        result = score_sql_complexity(assets)
        assert result.score < result.max_score

    def test_score_python_empty(self):
        result = score_python_sdk_usage([])
        assert result.score == result.max_score

    def test_score_python_with_sdk(self):
        assets = [
            Asset(id="p1", name="p1", type=AssetType.RECIPE_PYTHON, source_project="P",
                  metadata={"payload": "import dataiku\ndf = dataiku.Dataset('x').get_dataframe()"})
        ]
        result = score_python_sdk_usage(assets)
        assert result.score <= result.max_score

    def test_score_visual_recipes(self):
        assets = [
            Asset(id="v1", name="v1", type=AssetType.RECIPE_VISUAL, source_project="P",
                  metadata={"recipe_type": "join"})
        ]
        result = score_visual_recipes(assets)
        assert result.score > 0

    def test_score_data_volume_small(self):
        assets = [
            Asset(id="d1", name="d1", type=AssetType.DATASET, source_project="P",
                  metadata={"row_count": 1000, "size_mb": 5})
        ]
        result = score_data_volume(assets)
        assert result.score == result.max_score

    def test_score_data_volume_large(self):
        assets = [
            Asset(id="d1", name="d1", type=AssetType.DATASET, source_project="P",
                  metadata={"row_count": 500_000_000, "size_mb": 50_000})
        ]
        result = score_data_volume(assets)
        assert result.score < result.max_score

    def test_score_connections(self):
        assets = [
            Asset(id="c1", name="c1", type=AssetType.CONNECTION, source_project="P",
                  metadata={"type": "Oracle"})
        ]
        result = score_connections(assets)
        assert result.score > 0

    def test_score_dependencies(self):
        assets = [
            Asset(id="d1", name="d1", type=AssetType.RECIPE_SQL, source_project="P",
                  metadata={}, dependencies=["d2", "d3"]),
        ]
        result = score_dependencies(assets)
        assert result.score <= result.max_score

    def test_score_custom_plugins_none(self):
        result = score_custom_plugins([])
        assert result.score == result.max_score

    def test_score_scenarios_empty(self):
        result = score_scenarios([])
        assert result.score == result.max_score


# ── Full Assessment ───────────────────────────────────────────

class TestAssessProject:
    def test_assess_empty_registry(self, registry):
        result = assess_project(registry)
        assert isinstance(result, AssessmentResult)
        assert result.total_assets == 0
        assert result.grade in ReadinessGrade

    def test_assess_populated_registry(self, populated_registry):
        result = assess_project(populated_registry)
        assert result.total_assets > 0
        assert result.overall_score >= 0
        assert result.project_key == "TEST_PROJECT"

    def test_assess_result_to_dict(self, populated_registry):
        result = assess_project(populated_registry)
        d = result.to_dict()
        assert "overall_score" in d
        assert "grade" in d
        assert "category_scores" in d
        assert "risks" in d


# ── Strategy Advisor ──────────────────────────────────────────

class TestStrategyAdvisor:
    def test_recommend_grade_a(self, populated_registry):
        result = assess_project(populated_registry)
        # Force a high score for testing
        result.overall_score = 95
        result.grade = ReadinessGrade.A
        rec = recommend_strategy(result, populated_registry)
        assert rec.strategy == MigrationStrategy.FULL_AUTO

    def test_recommend_grade_f(self, populated_registry):
        result = assess_project(populated_registry)
        result.overall_score = 20
        result.grade = ReadinessGrade.F
        rec = recommend_strategy(result, populated_registry)
        assert rec.strategy == MigrationStrategy.MANUAL_REVIEW

    def test_recommendation_has_placements(self, populated_registry):
        result = assess_project(populated_registry)
        rec = recommend_strategy(result, populated_registry)
        assert isinstance(rec.dataset_placements, list)

    def test_recommendation_to_dict(self, populated_registry):
        result = assess_project(populated_registry)
        rec = recommend_strategy(result, populated_registry)
        d = rec.to_dict()
        assert "strategy" in d
        assert "confidence" in d


# ── Assessment Report ─────────────────────────────────────────

class TestAssessmentReport:
    def test_generate_html(self, populated_registry):
        result = assess_project(populated_registry)
        html = generate_assessment_html(result)
        assert "<html" in html
        assert "Assessment" in html

    def test_save_report(self, tmp_path, populated_registry):
        result = assess_project(populated_registry)
        path = save_assessment_report(result, tmp_path / "assessment.html")
        assert path.exists()
        assert path.read_text(encoding="utf-8").startswith("<!DOCTYPE")
