"""Tests for validation_agent.py and models/report.py (Phase 7).

Covers:
- validate_schema: column matching, type mismatches, missing/extra columns
- validate_row_counts: exact match, tolerance, mismatch
- validate_sql_syntax: valid SQL, syntax errors, missing file, empty file
- validate_notebook_structure: valid notebook, missing cells, bad format
- validate_pipeline_integrity: valid pipeline, missing activity, duplicates, dependency refs, placeholders
- validate_connection_mapping: valid mapping, missing fields, manual steps
- build_report: aggregation, category summaries
- ValidationAgent.execute: integrated with registry, state transitions
- ValidationAgent.validate: meta-validation
- Report models: to_summary, success_rate
- HTML report: generation, content checks
- JSON report: generation, round-trip parse
"""

from __future__ import annotations

import asyncio
import json
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from src.agents.validation_agent import (
    ValidationAgent,
    build_report,
    validate_connection_mapping,
    validate_notebook_structure,
    validate_pipeline_integrity,
    validate_row_counts,
    validate_schema,
    validate_sql_syntax,
)
from src.core.registry import AssetRegistry
from src.models.asset import Asset, AssetType, MigrationState
from src.models.report import (
    AssetValidationResult,
    CategorySummary,
    ValidationCategory,
    ValidationFinding,
    ValidationReport,
    ValidationSeverity,
    generate_html_report,
    generate_json_report,
    save_html_report,
    save_json_report,
)


# ── Test helpers ─────────────────────────────────────────────


@dataclass
class StubMigrationConfig:
    output_dir: str = "./output"
    target_sql_dialect: str = "tsql"


@dataclass
class StubConfig:
    migration: Any = None

    def __post_init__(self):
        self.migration = self.migration or StubMigrationConfig()


@dataclass
class StubContext:
    registry: Any = None
    config: Any = None


def _make_asset(
    asset_id: str,
    name: str,
    asset_type: AssetType = AssetType.RECIPE_SQL,
    state: MigrationState = MigrationState.CONVERTED,
    target: dict | None = None,
    metadata: dict | None = None,
    review_flags: list[str] | None = None,
    errors: list[str] | None = None,
) -> Asset:
    a = Asset(
        id=asset_id,
        type=asset_type,
        name=name,
        source_project="TEST",
        state=state,
        metadata=metadata or {},
        target_fabric_asset=target,
        review_flags=review_flags or [],
        errors=errors or [],
    )
    return a


# ── Schema Validation ────────────────────────────────────────


class TestValidateSchema:
    def test_matching_schemas(self):
        src = [{"name": "id", "type": "INT"}, {"name": "name", "type": "VARCHAR"}]
        tgt = [{"name": "id", "type": "INT"}, {"name": "name", "type": "VARCHAR"}]
        findings = validate_schema(src, tgt, "test_asset")
        assert len(findings) == 1
        assert findings[0].severity == ValidationSeverity.INFO
        assert "matches" in findings[0].message.lower()

    def test_missing_column_in_target(self):
        src = [{"name": "id"}, {"name": "email"}]
        tgt = [{"name": "id"}]
        findings = validate_schema(src, tgt, "test_asset")
        errors = [f for f in findings if f.severity == ValidationSeverity.ERROR]
        assert len(errors) == 1
        assert "email" in errors[0].message.lower()

    def test_extra_column_in_target(self):
        src = [{"name": "id"}]
        tgt = [{"name": "id"}, {"name": "created_at"}]
        findings = validate_schema(src, tgt, "test_asset")
        warnings = [f for f in findings if f.severity == ValidationSeverity.WARNING]
        assert len(warnings) == 1
        assert "created_at" in warnings[0].message.lower()

    def test_type_mismatch(self):
        src = [{"name": "age", "type": "INT"}]
        tgt = [{"name": "age", "type": "BIGINT"}]
        findings = validate_schema(src, tgt, "test_asset")
        warnings = [f for f in findings if f.severity == ValidationSeverity.WARNING]
        assert len(warnings) == 1
        assert "type mismatch" in warnings[0].message.lower()
        assert warnings[0].details["source_type"] == "INT"
        assert warnings[0].details["target_type"] == "BIGINT"

    def test_case_insensitive_name_match(self):
        src = [{"name": "ID", "type": "INT"}]
        tgt = [{"name": "id", "type": "INT"}]
        findings = validate_schema(src, tgt, "test_asset")
        assert len(findings) == 1
        assert findings[0].severity == ValidationSeverity.INFO

    def test_empty_schemas(self):
        findings = validate_schema([], [], "test_asset")
        assert len(findings) == 1
        assert findings[0].severity == ValidationSeverity.INFO

    def test_no_type_skips_comparison(self):
        src = [{"name": "id"}]
        tgt = [{"name": "id"}]
        findings = validate_schema(src, tgt, "test_asset")
        assert len(findings) == 1
        assert findings[0].severity == ValidationSeverity.INFO

    def test_multiple_issues(self):
        src = [{"name": "id", "type": "INT"}, {"name": "name", "type": "VARCHAR"}, {"name": "email"}]
        tgt = [{"name": "id", "type": "BIGINT"}, {"name": "extra"}]
        findings = validate_schema(src, tgt, "test_asset")
        # Missing: name, email | Extra: extra | Type mismatch: id
        errors = [f for f in findings if f.severity == ValidationSeverity.ERROR]
        warnings = [f for f in findings if f.severity == ValidationSeverity.WARNING]
        assert len(errors) == 2  # name missing, email missing
        assert len(warnings) >= 2  # extra column + type mismatch

    def test_category_is_schema(self):
        findings = validate_schema([{"name": "a"}], [{"name": "a"}], "x")
        assert all(f.category == ValidationCategory.SCHEMA for f in findings)


# ── Row Count Validation ─────────────────────────────────────


class TestValidateRowCounts:
    def test_exact_match(self):
        findings = validate_row_counts(1000, 1000, "tbl")
        assert len(findings) == 1
        assert findings[0].severity == ValidationSeverity.INFO
        assert "1000" in findings[0].message

    def test_mismatch(self):
        findings = validate_row_counts(1000, 900, "tbl")
        assert findings[0].severity == ValidationSeverity.ERROR
        assert "mismatch" in findings[0].message.lower()

    def test_within_tolerance(self):
        findings = validate_row_counts(1000, 990, "tbl", tolerance=0.02)
        assert findings[0].severity == ValidationSeverity.WARNING
        assert "tolerance" in findings[0].message.lower()

    def test_outside_tolerance(self):
        findings = validate_row_counts(1000, 900, "tbl", tolerance=0.05)
        assert findings[0].severity == ValidationSeverity.ERROR

    def test_zero_source_exact(self):
        findings = validate_row_counts(0, 0, "tbl")
        assert findings[0].severity == ValidationSeverity.INFO

    def test_zero_source_mismatch(self):
        findings = validate_row_counts(0, 10, "tbl")
        assert findings[0].severity == ValidationSeverity.ERROR

    def test_category_is_completeness(self):
        findings = validate_row_counts(5, 5, "x")
        assert findings[0].category == ValidationCategory.COMPLETENESS


# ── SQL Syntax Validation ────────────────────────────────────


class TestValidateSqlSyntax:
    def test_valid_sql(self, tmp_path):
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT id, name FROM users WHERE active = 1")
        findings = validate_sql_syntax(str(sql_file), "my_recipe")
        assert len(findings) == 1
        assert findings[0].severity == ValidationSeverity.INFO
        assert "valid" in findings[0].message.lower()

    def test_invalid_sql(self, tmp_path):
        sql_file = tmp_path / "bad.sql"
        sql_file.write_text("SELEC broken FROM")
        findings = validate_sql_syntax(str(sql_file), "my_recipe")
        errors = [f for f in findings if f.severity == ValidationSeverity.ERROR]
        assert len(errors) >= 1

    def test_missing_file(self):
        findings = validate_sql_syntax("/nonexistent/path.sql", "missing")
        assert findings[0].severity == ValidationSeverity.ERROR
        assert "not found" in findings[0].message.lower()

    def test_empty_file(self, tmp_path):
        sql_file = tmp_path / "empty.sql"
        sql_file.write_text("")
        findings = validate_sql_syntax(str(sql_file), "empty_recipe")
        assert findings[0].severity == ValidationSeverity.ERROR
        assert "empty" in findings[0].message.lower()

    def test_category_is_sql_syntax(self, tmp_path):
        sql_file = tmp_path / "t.sql"
        sql_file.write_text("SELECT 1")
        findings = validate_sql_syntax(str(sql_file), "x")
        assert findings[0].category == ValidationCategory.SQL_SYNTAX

    def test_multistatement_sql(self, tmp_path):
        sql_file = tmp_path / "multi.sql"
        sql_file.write_text("SELECT 1;\nSELECT 2;")
        findings = validate_sql_syntax(str(sql_file), "multi")
        # sqlglot may parse or may error — just ensure we get findings
        assert len(findings) >= 1


# ── Notebook Structure Validation ────────────────────────────


class TestValidateNotebookStructure:
    def _make_notebook(self, cells=None, nbformat=4, kernelspec=None):
        nb = {
            "nbformat": nbformat,
            "nbformat_minor": 5,
            "metadata": {},
            "cells": cells or [],
        }
        if kernelspec:
            nb["metadata"]["kernelspec"] = kernelspec
        return nb

    def test_valid_notebook(self, tmp_path):
        nb = self._make_notebook(
            cells=[{"cell_type": "code", "source": ["print('hello')"]}],
            kernelspec={"name": "python3", "display_name": "Python 3"},
        )
        path = tmp_path / "test.ipynb"
        path.write_text(json.dumps(nb))
        findings = validate_notebook_structure(str(path), "nb")
        assert len(findings) == 1
        assert findings[0].severity == ValidationSeverity.INFO

    def test_missing_file(self):
        findings = validate_notebook_structure("/no/file.ipynb", "nb")
        assert findings[0].severity == ValidationSeverity.ERROR
        assert "not found" in findings[0].message.lower()

    def test_invalid_json(self, tmp_path):
        path = tmp_path / "bad.ipynb"
        path.write_text("{not json}")
        findings = validate_notebook_structure(str(path), "nb")
        assert findings[0].severity == ValidationSeverity.ERROR

    def test_wrong_nbformat(self, tmp_path):
        nb = self._make_notebook(nbformat=3)
        path = tmp_path / "old.ipynb"
        path.write_text(json.dumps(nb))
        findings = validate_notebook_structure(str(path), "nb")
        errors = [f for f in findings if f.severity == ValidationSeverity.ERROR]
        assert len(errors) >= 1
        assert "nbformat" in errors[0].message.lower()

    def test_no_cells(self, tmp_path):
        nb = self._make_notebook(cells=[])
        path = tmp_path / "empty.ipynb"
        path.write_text(json.dumps(nb))
        findings = validate_notebook_structure(str(path), "nb")
        warnings = [f for f in findings if f.severity == ValidationSeverity.WARNING]
        assert any("no cells" in w.message.lower() for w in warnings)

    def test_cell_missing_type(self, tmp_path):
        nb = self._make_notebook(cells=[{"source": ["x"]}])
        path = tmp_path / "notype.ipynb"
        path.write_text(json.dumps(nb))
        findings = validate_notebook_structure(str(path), "nb")
        errors = [f for f in findings if f.severity == ValidationSeverity.ERROR]
        assert any("cell_type" in e.message for e in errors)

    def test_cell_missing_source(self, tmp_path):
        nb = self._make_notebook(cells=[{"cell_type": "code"}])
        path = tmp_path / "nosrc.ipynb"
        path.write_text(json.dumps(nb))
        findings = validate_notebook_structure(str(path), "nb")
        errors = [f for f in findings if f.severity == ValidationSeverity.ERROR]
        assert any("source" in e.message for e in errors)

    def test_missing_kernelspec(self, tmp_path):
        nb = self._make_notebook(cells=[{"cell_type": "code", "source": [""]}])
        path = tmp_path / "nokernel.ipynb"
        path.write_text(json.dumps(nb))
        findings = validate_notebook_structure(str(path), "nb")
        warnings = [f for f in findings if f.severity == ValidationSeverity.WARNING]
        assert any("kernelspec" in w.message.lower() for w in warnings)

    def test_category_is_notebook_structure(self, tmp_path):
        nb = self._make_notebook(
            cells=[{"cell_type": "code", "source": [""]}],
            kernelspec={"name": "python3"},
        )
        path = tmp_path / "cat.ipynb"
        path.write_text(json.dumps(nb))
        findings = validate_notebook_structure(str(path), "nb")
        assert all(f.category == ValidationCategory.NOTEBOOK_STRUCTURE for f in findings)


# ── Pipeline Integrity Validation ────────────────────────────


class TestValidatePipelineIntegrity:
    def _make_pipeline(self, activities=None):
        return {
            "name": "test_pipeline",
            "properties": {
                "activities": activities or [],
            },
        }

    def test_valid_pipeline(self, tmp_path):
        pipeline = self._make_pipeline([
            {"name": "step1", "type": "SynapseNotebook"},
            {"name": "step2", "type": "Script", "dependsOn": [{"activity": "step1"}]},
        ])
        path = tmp_path / "pipeline.json"
        path.write_text(json.dumps(pipeline))
        findings = validate_pipeline_integrity(str(path), "pipe")
        assert len(findings) == 1
        assert findings[0].severity == ValidationSeverity.INFO

    def test_missing_file(self):
        findings = validate_pipeline_integrity("/no/pipeline.json", "pipe")
        assert findings[0].severity == ValidationSeverity.ERROR

    def test_invalid_json(self, tmp_path):
        path = tmp_path / "bad.json"
        path.write_text("not json!")
        findings = validate_pipeline_integrity(str(path), "pipe")
        assert findings[0].severity == ValidationSeverity.ERROR

    def test_no_activities(self, tmp_path):
        pipeline = self._make_pipeline([])
        path = tmp_path / "empty.json"
        path.write_text(json.dumps(pipeline))
        findings = validate_pipeline_integrity(str(path), "pipe")
        warnings = [f for f in findings if f.severity == ValidationSeverity.WARNING]
        assert any("no activities" in w.message.lower() for w in warnings)

    def test_missing_activity_name(self, tmp_path):
        pipeline = self._make_pipeline([{"type": "Script"}])
        path = tmp_path / "noname.json"
        path.write_text(json.dumps(pipeline))
        findings = validate_pipeline_integrity(str(path), "pipe")
        errors = [f for f in findings if f.severity == ValidationSeverity.ERROR]
        assert any("missing 'name'" in e.message for e in errors)

    def test_missing_activity_type(self, tmp_path):
        pipeline = self._make_pipeline([{"name": "step1"}])
        path = tmp_path / "notype.json"
        path.write_text(json.dumps(pipeline))
        findings = validate_pipeline_integrity(str(path), "pipe")
        errors = [f for f in findings if f.severity == ValidationSeverity.ERROR]
        assert any("missing 'type'" in e.message for e in errors)

    def test_duplicate_activity_name(self, tmp_path):
        pipeline = self._make_pipeline([
            {"name": "step1", "type": "Script"},
            {"name": "step1", "type": "Script"},
        ])
        path = tmp_path / "dup.json"
        path.write_text(json.dumps(pipeline))
        findings = validate_pipeline_integrity(str(path), "pipe")
        errors = [f for f in findings if f.severity == ValidationSeverity.ERROR]
        assert any("duplicate" in e.message.lower() for e in errors)

    def test_placeholder_activity_warning(self, tmp_path):
        pipeline = self._make_pipeline([
            {"name": "unknown_step", "type": "Placeholder"},
        ])
        path = tmp_path / "placeholder.json"
        path.write_text(json.dumps(pipeline))
        findings = validate_pipeline_integrity(str(path), "pipe")
        warnings = [f for f in findings if f.severity == ValidationSeverity.WARNING]
        assert any("placeholder" in w.message.lower() for w in warnings)

    def test_bad_dependency_reference(self, tmp_path):
        pipeline = self._make_pipeline([
            {"name": "step1", "type": "Script", "dependsOn": [{"activity": "nonexistent"}]},
        ])
        path = tmp_path / "baddep.json"
        path.write_text(json.dumps(pipeline))
        findings = validate_pipeline_integrity(str(path), "pipe")
        errors = [f for f in findings if f.severity == ValidationSeverity.ERROR]
        assert any("nonexistent" in e.message for e in errors)

    def test_valid_forward_dependency(self, tmp_path):
        # step2 depends on step1, listed in order — should pass
        pipeline = self._make_pipeline([
            {"name": "step1", "type": "Script"},
            {"name": "step2", "type": "Script", "dependsOn": [{"activity": "step1"}]},
        ])
        path = tmp_path / "fwd.json"
        path.write_text(json.dumps(pipeline))
        findings = validate_pipeline_integrity(str(path), "pipe")
        assert findings[0].severity == ValidationSeverity.INFO

    def test_category_is_pipeline_integrity(self, tmp_path):
        pipeline = self._make_pipeline([{"name": "s", "type": "Script"}])
        path = tmp_path / "cat.json"
        path.write_text(json.dumps(pipeline))
        findings = validate_pipeline_integrity(str(path), "pipe")
        assert all(f.category == ValidationCategory.PIPELINE_INTEGRITY for f in findings)


# ── Connection Mapping Validation ────────────────────────────


class TestValidateConnectionMapping:
    def test_valid_mapping(self, tmp_path):
        mapping = {"fabric_type": "Lakehouse", "credentials": {"type": "managed_identity"}}
        path = tmp_path / "conn.json"
        path.write_text(json.dumps(mapping))
        findings = validate_connection_mapping(str(path), "myconn")
        assert len(findings) == 1
        assert findings[0].severity == ValidationSeverity.INFO

    def test_missing_file(self):
        findings = validate_connection_mapping("/no/file.json", "conn")
        assert findings[0].severity == ValidationSeverity.ERROR

    def test_missing_fabric_type(self, tmp_path):
        mapping = {"credentials": {}}
        path = tmp_path / "notype.json"
        path.write_text(json.dumps(mapping))
        findings = validate_connection_mapping(str(path), "conn")
        errors = [f for f in findings if f.severity == ValidationSeverity.ERROR]
        assert any("fabric_type" in e.message for e in errors)

    def test_manual_steps_warning(self, tmp_path):
        mapping = {
            "fabric_type": "Gateway",
            "manual_steps": ["Install gateway", "Configure credentials"],
        }
        path = tmp_path / "manual.json"
        path.write_text(json.dumps(mapping))
        findings = validate_connection_mapping(str(path), "conn")
        warnings = [f for f in findings if f.severity == ValidationSeverity.WARNING]
        assert len(warnings) == 1
        assert "2 manual" in warnings[0].message

    def test_invalid_json(self, tmp_path):
        path = tmp_path / "bad.json"
        path.write_text("{{bad}}")
        findings = validate_connection_mapping(str(path), "conn")
        assert findings[0].severity == ValidationSeverity.ERROR

    def test_category_is_connection(self, tmp_path):
        mapping = {"fabric_type": "Lakehouse"}
        path = tmp_path / "c.json"
        path.write_text(json.dumps(mapping))
        findings = validate_connection_mapping(str(path), "c")
        assert all(f.category == ValidationCategory.CONNECTION for f in findings)


# ── Build Report ─────────────────────────────────────────────


class TestBuildReport:
    def test_basic_report(self):
        ar1 = AssetValidationResult(
            asset_id="a1", asset_name="recipe1", asset_type="recipe.sql", passed=True,
            findings=[ValidationFinding(
                category=ValidationCategory.SQL_SYNTAX,
                severity=ValidationSeverity.INFO,
                asset_name="recipe1",
                message="SQL valid",
            )],
        )
        ar2 = AssetValidationResult(
            asset_id="a2", asset_name="recipe2", asset_type="recipe.sql", passed=False,
            findings=[ValidationFinding(
                category=ValidationCategory.SQL_SYNTAX,
                severity=ValidationSeverity.ERROR,
                asset_name="recipe2",
                message="Syntax error",
            )],
        )
        report = build_report("TEST_PROJECT", [ar1, ar2], ["flag1"], ["err1"])
        assert report.project_name == "TEST_PROJECT"
        assert report.total_assets == 2
        assert report.assets_passed == 1
        assert report.assets_failed == 1
        assert len(report.review_flags) == 1
        assert len(report.errors) == 1

    def test_category_summaries(self):
        findings = [
            ValidationFinding(category=ValidationCategory.SCHEMA, severity=ValidationSeverity.INFO,
                              asset_name="a", message="ok"),
            ValidationFinding(category=ValidationCategory.SCHEMA, severity=ValidationSeverity.WARNING,
                              asset_name="a", message="warn"),
            ValidationFinding(category=ValidationCategory.SQL_SYNTAX, severity=ValidationSeverity.ERROR,
                              asset_name="a", message="bad"),
        ]
        ar = AssetValidationResult(asset_id="a", asset_name="a", asset_type="sql", passed=False, findings=findings)
        report = build_report("P", [ar], [], [])
        cats = {cs.category: cs for cs in report.category_summaries}
        assert ValidationCategory.SCHEMA in cats
        assert cats[ValidationCategory.SCHEMA].passed == 1
        assert cats[ValidationCategory.SCHEMA].warnings == 1
        assert ValidationCategory.SQL_SYNTAX in cats
        assert cats[ValidationCategory.SQL_SYNTAX].failed == 1

    def test_empty_report(self):
        report = build_report("P", [], [], [])
        assert report.total_assets == 0
        assert report.success_rate == 0.0

    def test_all_passed(self):
        ar = AssetValidationResult(asset_id="a", asset_name="a", asset_type="sql", passed=True, findings=[])
        report = build_report("P", [ar], [], [])
        assert report.success_rate == 100.0


# ── Report Model ─────────────────────────────────────────────


class TestReportModel:
    def test_success_rate_100(self):
        r = ValidationReport(project_name="P", assets_validated=10, assets_passed=10)
        assert r.success_rate == 100.0

    def test_success_rate_partial(self):
        r = ValidationReport(project_name="P", assets_validated=10, assets_passed=7)
        assert abs(r.success_rate - 70.0) < 0.01

    def test_success_rate_zero_validated(self):
        r = ValidationReport(project_name="P")
        assert r.success_rate == 0.0

    def test_to_summary(self):
        r = ValidationReport(
            project_name="PROJ",
            total_assets=5,
            assets_validated=5,
            assets_passed=4,
            assets_failed=1,
            review_flags=["flag1", "flag2"],
            errors=["err1"],
        )
        s = r.to_summary()
        assert s["project"] == "PROJ"
        assert s["total_assets"] == 5
        assert s["passed"] == 4
        assert s["failed"] == 1
        assert s["success_rate"] == "80.0%"
        assert s["review_flags"] == 2
        assert s["errors"] == 1

    def test_model_dump_roundtrip(self):
        r = ValidationReport(project_name="P", total_assets=3, assets_validated=3, assets_passed=2, assets_failed=1)
        d = r.model_dump(mode="json")
        r2 = ValidationReport.model_validate(d)
        assert r2.project_name == "P"
        assert r2.assets_passed == 2


# ── JSON Report ──────────────────────────────────────────────


class TestJsonReport:
    def test_generate_json(self):
        r = ValidationReport(project_name="P", total_assets=1, assets_validated=1, assets_passed=1)
        out = generate_json_report(r)
        parsed = json.loads(out)
        assert parsed["project_name"] == "P"
        assert parsed["total_assets"] == 1

    def test_save_json_report(self, tmp_path):
        r = ValidationReport(project_name="P", total_assets=2)
        p = save_json_report(r, tmp_path / "report.json")
        assert p.exists()
        data = json.loads(p.read_text())
        assert data["project_name"] == "P"
        assert data["total_assets"] == 2

    def test_json_includes_findings(self):
        ar = AssetValidationResult(
            asset_id="a1", asset_name="r1", asset_type="sql", passed=True,
            findings=[ValidationFinding(
                category=ValidationCategory.SQL_SYNTAX,
                severity=ValidationSeverity.INFO,
                asset_name="r1",
                message="ok",
            )],
        )
        r = ValidationReport(project_name="P", asset_results=[ar])
        out = generate_json_report(r)
        parsed = json.loads(out)
        assert len(parsed["asset_results"]) == 1
        assert parsed["asset_results"][0]["findings"][0]["message"] == "ok"


# ── HTML Report ──────────────────────────────────────────────


class TestHtmlReport:
    def test_generate_html_basic(self):
        r = ValidationReport(project_name="TestProject", total_assets=3,
                             assets_validated=3, assets_passed=2, assets_failed=1)
        html = generate_html_report(r)
        assert "<!DOCTYPE html>" in html
        assert "TestProject" in html
        assert "100.0%" not in html  # 2/3 ≈ 66.7%

    def test_html_contains_findings(self):
        ar = AssetValidationResult(
            asset_id="a1", asset_name="my_recipe", asset_type="recipe.sql", passed=False,
            findings=[ValidationFinding(
                category=ValidationCategory.SQL_SYNTAX,
                severity=ValidationSeverity.ERROR,
                asset_name="my_recipe",
                message="Syntax error at line 5",
            )],
        )
        r = ValidationReport(project_name="P", asset_results=[ar])
        html = generate_html_report(r)
        assert "my_recipe" in html
        assert "Syntax error at line 5" in html
        assert "ERROR" in html

    def test_html_contains_review_flags(self):
        r = ValidationReport(project_name="P", review_flags=["Check manually"])
        html = generate_html_report(r)
        assert "Check manually" in html
        assert "Review Flags" in html

    def test_html_contains_errors(self):
        r = ValidationReport(project_name="P", errors=["Something broke"])
        html = generate_html_report(r)
        assert "Something broke" in html

    def test_html_escapes_content(self):
        r = ValidationReport(project_name="<script>alert('xss')</script>")
        html = generate_html_report(r)
        assert "<script>" not in html
        assert "&lt;script&gt;" in html

    def test_save_html_report(self, tmp_path):
        r = ValidationReport(project_name="P", total_assets=1)
        p = save_html_report(r, tmp_path / "report.html")
        assert p.exists()
        content = p.read_text()
        assert "<!DOCTYPE html>" in content

    def test_html_category_summary_table(self):
        r = ValidationReport(
            project_name="P",
            category_summaries=[
                CategorySummary(category=ValidationCategory.SCHEMA, total=5, passed=3, failed=1, warnings=1),
            ],
        )
        html = generate_html_report(r)
        assert "schema" in html.lower()

    def test_html_success_rate_color(self):
        # High rate → green
        r = ValidationReport(project_name="P", assets_validated=10, assets_passed=10)
        html = generate_html_report(r)
        assert "#28a745" in html  # green for 100%


# ── Validation Agent Execute ─────────────────────────────────


class TestValidationAgentExecute:
    def _setup_context(self, tmp_path, assets):
        registry = AssetRegistry(project_key="TEST", registry_path=tmp_path / "reg.json")
        for a in assets:
            registry.add_asset(a)
        config = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path)))
        ctx = StubContext(registry=registry, config=config)
        return ctx

    def test_sql_asset_passes(self, tmp_path):
        sql_file = tmp_path / "recipe.sql"
        sql_file.write_text("SELECT 1")
        asset = _make_asset("a1", "recipe1", target={"type": "sql_script", "path": str(sql_file), "dialect": "tsql"})
        ctx = self._setup_context(tmp_path, [asset])

        agent = ValidationAgent()
        result = asyncio.run(agent.execute(ctx))

        assert result.status.value == "completed"
        assert result.assets_converted == 1  # passes
        assert ctx.registry.get_asset("a1").state == MigrationState.VALIDATED

    def test_sql_asset_fails_syntax(self, tmp_path):
        sql_file = tmp_path / "bad.sql"
        sql_file.write_text("SELEC broken FROM")
        asset = _make_asset("a1", "badrec", target={"type": "sql_script", "path": str(sql_file), "dialect": "tsql"})
        ctx = self._setup_context(tmp_path, [asset])

        agent = ValidationAgent()
        result = asyncio.run(agent.execute(ctx))

        assert result.assets_failed >= 1
        assert ctx.registry.get_asset("a1").state == MigrationState.VALIDATION_FAILED

    def test_notebook_asset_passes(self, tmp_path):
        nb = {
            "nbformat": 4, "nbformat_minor": 5,
            "metadata": {"kernelspec": {"name": "python3"}},
            "cells": [{"cell_type": "code", "source": ["print(1)"]}],
        }
        nb_file = tmp_path / "nb.ipynb"
        nb_file.write_text(json.dumps(nb))
        asset = _make_asset("a1", "nb1", asset_type=AssetType.RECIPE_PYTHON,
                            target={"type": "notebook", "path": str(nb_file)})
        ctx = self._setup_context(tmp_path, [asset])

        agent = ValidationAgent()
        result = asyncio.run(agent.execute(ctx))

        assert result.assets_converted == 1
        assert ctx.registry.get_asset("a1").state == MigrationState.VALIDATED

    def test_pipeline_asset_passes(self, tmp_path):
        pipeline = {"properties": {"activities": [{"name": "s1", "type": "Script"}]}}
        p_file = tmp_path / "pipe.json"
        p_file.write_text(json.dumps(pipeline))
        asset = _make_asset("a1", "flow1", asset_type=AssetType.FLOW,
                            target={"type": "data_pipeline", "path": str(p_file)})
        ctx = self._setup_context(tmp_path, [asset])

        agent = ValidationAgent()
        result = asyncio.run(agent.execute(ctx))

        assert result.assets_converted == 1

    def test_connection_mapping_passes(self, tmp_path):
        mapping = {"fabric_type": "Lakehouse"}
        c_file = tmp_path / "conn.json"
        c_file.write_text(json.dumps(mapping))
        asset = _make_asset("a1", "conn1", asset_type=AssetType.CONNECTION,
                            target={"type": "connection_mapping", "path": str(c_file)})
        ctx = self._setup_context(tmp_path, [asset])

        agent = ValidationAgent()
        result = asyncio.run(agent.execute(ctx))

        assert result.assets_converted == 1

    def test_discovered_only_flagged(self, tmp_path):
        asset = _make_asset("a1", "unprocessed", state=MigrationState.DISCOVERED)
        ctx = self._setup_context(tmp_path, [asset])

        agent = ValidationAgent()
        result = asyncio.run(agent.execute(ctx))

        assert len(result.review_flags) >= 1
        assert any("not converted" in f.lower() for f in result.review_flags)

    def test_prior_failures_in_errors(self, tmp_path):
        asset = _make_asset("a1", "broken", state=MigrationState.FAILED, errors=["conversion error"])
        ctx = self._setup_context(tmp_path, [asset])

        agent = ValidationAgent()
        result = asyncio.run(agent.execute(ctx))

        assert len(result.errors) >= 1
        assert any("conversion error" in e for e in result.errors)

    def test_review_flags_aggregated(self, tmp_path):
        sql_file = tmp_path / "r.sql"
        sql_file.write_text("SELECT 1")
        asset = _make_asset("a1", "flagged", target={"type": "sql_script", "path": str(sql_file), "dialect": "tsql"},
                            review_flags=["needs manual check"])
        ctx = self._setup_context(tmp_path, [asset])

        agent = ValidationAgent()
        result = asyncio.run(agent.execute(ctx))

        assert any("manual check" in f for f in result.review_flags)

    def test_no_target_info_fails(self, tmp_path):
        asset = _make_asset("a1", "notarget", target=None)
        ctx = self._setup_context(tmp_path, [asset])

        agent = ValidationAgent()
        result = asyncio.run(agent.execute(ctx))

        assert result.assets_failed >= 1

    def test_report_in_details(self, tmp_path):
        sql_file = tmp_path / "ok.sql"
        sql_file.write_text("SELECT 1")
        asset = _make_asset("a1", "r1", target={"type": "sql_script", "path": str(sql_file), "dialect": "tsql"})
        ctx = self._setup_context(tmp_path, [asset])

        agent = ValidationAgent()
        result = asyncio.run(agent.execute(ctx))

        assert "report" in result.details
        rpt = result.details["report"]
        assert rpt["project_name"] == "TEST"
        assert rpt["assets_passed"] >= 1

    def test_mixed_assets(self, tmp_path):
        """Test with a mix of passing SQL, failing notebook, and discovered asset."""
        sql_file = tmp_path / "good.sql"
        sql_file.write_text("SELECT id FROM users")
        nb_file = tmp_path / "bad.ipynb"
        nb_file.write_text("not valid json")

        a1 = _make_asset("a1", "good_sql", target={"type": "sql_script", "path": str(sql_file), "dialect": "tsql"})
        a2 = _make_asset("a2", "bad_nb", asset_type=AssetType.RECIPE_PYTHON,
                          target={"type": "notebook", "path": str(nb_file)})
        a3 = _make_asset("a3", "unprocessed", state=MigrationState.DISCOVERED)

        ctx = self._setup_context(tmp_path, [a1, a2, a3])
        agent = ValidationAgent()
        result = asyncio.run(agent.execute(ctx))

        assert result.assets_processed == 3
        assert result.assets_converted >= 1  # at least good_sql passes
        assert result.assets_failed >= 1    # bad_nb fails
        assert any("not converted" in f.lower() for f in result.review_flags)

    def test_dataset_ddl_validated(self, tmp_path):
        ddl_file = tmp_path / "create.sql"
        ddl_file.write_text("CREATE TABLE t (id INT)")
        asset = _make_asset("a1", "ds1", asset_type=AssetType.DATASET,
                            target={"type": "lakehouse_table", "ddl_path": str(ddl_file)})
        ctx = self._setup_context(tmp_path, [asset])

        agent = ValidationAgent()
        result = asyncio.run(agent.execute(ctx))

        assert result.assets_converted >= 1

    def test_schema_comparison_when_available(self, tmp_path):
        sql_file = tmp_path / "r.sql"
        sql_file.write_text("SELECT 1")
        asset = _make_asset(
            "a1", "schema_test",
            target={
                "type": "sql_script", "path": str(sql_file), "dialect": "tsql",
                "target_columns": [{"name": "id", "type": "INT"}],
            },
            metadata={
                "schema": [{"name": "id", "type": "INT"}, {"name": "email", "type": "VARCHAR"}],
            },
        )
        ctx = self._setup_context(tmp_path, [asset])

        agent = ValidationAgent()
        result = asyncio.run(agent.execute(ctx))

        # Schema mismatch: email missing in target
        assert result.assets_failed >= 1

    def test_row_count_validation_when_available(self, tmp_path):
        sql_file = tmp_path / "r.sql"
        sql_file.write_text("SELECT 1")
        asset = _make_asset(
            "a1", "rowcount_test",
            target={"type": "sql_script", "path": str(sql_file), "dialect": "tsql", "row_count": 100},
            metadata={"row_count": 100},
        )
        ctx = self._setup_context(tmp_path, [asset])

        agent = ValidationAgent()
        result = asyncio.run(agent.execute(ctx))

        assert result.assets_converted >= 1

    def test_trigger_file_validated(self, tmp_path):
        trigger = {"triggers": [{"type": "ScheduleTrigger"}]}
        t_file = tmp_path / "trigger.json"
        t_file.write_text(json.dumps(trigger))
        asset = _make_asset(
            "a1", "scenario1", asset_type=AssetType.SCENARIO,
            target={"type": "pipeline_trigger", "path": str(t_file)},
        )
        ctx = self._setup_context(tmp_path, [asset])

        agent = ValidationAgent()
        result = asyncio.run(agent.execute(ctx))

        assert result.assets_converted >= 1


# ── Validation Agent Meta-Validate ───────────────────────────


class TestValidationAgentValidate:
    def test_passes_when_all_terminal(self, tmp_path):
        registry = AssetRegistry(project_key="TEST", registry_path=tmp_path / "reg.json")
        asset = _make_asset("a1", "r1", state=MigrationState.VALIDATED)
        registry.add_asset(asset)
        ctx = StubContext(registry=registry, config=StubConfig())

        agent = ValidationAgent()
        vr = asyncio.run(agent.validate(ctx))

        assert vr.passed is True

    def test_fails_with_stuck_converting(self, tmp_path):
        registry = AssetRegistry(project_key="TEST", registry_path=tmp_path / "reg.json")
        asset = _make_asset("a1", "r1", state=MigrationState.CONVERTING)
        registry.add_asset(asset)
        ctx = StubContext(registry=registry, config=StubConfig())

        agent = ValidationAgent()
        vr = asyncio.run(agent.validate(ctx))

        assert vr.passed is False
        assert "non-terminal" in vr.failures[0].lower()
