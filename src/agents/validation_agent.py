"""Validation agent — verifies correctness of all migrated assets.

Performs deep validation across all converted assets:
- Schema comparison (source vs target column names + types)
- SQL syntax validation via sqlglot
- Notebook structure validation (cell count, kernel, metadata)
- Pipeline integrity (activities, dependencies, triggers)
- Connection completeness
- Completeness & review-flag aggregation

Produces a ValidationReport model suitable for HTML/JSON export.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.agents.base_agent import AgentResult, AgentStatus, BaseAgent, ValidationResult
from src.core.logger import get_logger
from src.models.asset import AssetType, MigrationState
from src.models.report import (
    AssetValidationResult,
    CategorySummary,
    ValidationCategory,
    ValidationFinding,
    ValidationReport,
    ValidationSeverity,
)
from src.translators.sql_translator import validate_sql

logger = get_logger(__name__)


# ── Standalone validation functions (pure, testable) ─────────


def validate_schema(
    source_columns: list[dict[str, str]],
    target_columns: list[dict[str, str]],
    asset_name: str,
) -> list[ValidationFinding]:
    """Compare source and target schemas, return mismatches as findings.

    Each column dict must have at least ``name``; ``type`` is optional.
    """
    findings: list[ValidationFinding] = []

    src_names = [c["name"].lower() for c in source_columns]
    tgt_names = [c["name"].lower() for c in target_columns]

    # Missing columns
    for name in src_names:
        if name not in tgt_names:
            findings.append(ValidationFinding(
                category=ValidationCategory.SCHEMA,
                severity=ValidationSeverity.ERROR,
                asset_name=asset_name,
                message=f"Column '{name}' missing in target",
            ))

    # Extra columns in target
    for name in tgt_names:
        if name not in src_names:
            findings.append(ValidationFinding(
                category=ValidationCategory.SCHEMA,
                severity=ValidationSeverity.WARNING,
                asset_name=asset_name,
                message=f"Extra column '{name}' in target (not in source)",
            ))

    # Type mismatches for matched columns
    src_map = {c["name"].lower(): c.get("type", "") for c in source_columns}
    tgt_map = {c["name"].lower(): c.get("type", "") for c in target_columns}
    for name in src_names:
        if name in tgt_map and src_map[name] and tgt_map[name]:
            if src_map[name].lower() != tgt_map[name].lower():
                findings.append(ValidationFinding(
                    category=ValidationCategory.SCHEMA,
                    severity=ValidationSeverity.WARNING,
                    asset_name=asset_name,
                    message=f"Type mismatch for '{name}': source={src_map[name]}, target={tgt_map[name]}",
                    details={"column": name, "source_type": src_map[name], "target_type": tgt_map[name]},
                ))

    # If no issues, record a passing finding
    if not findings:
        findings.append(ValidationFinding(
            category=ValidationCategory.SCHEMA,
            severity=ValidationSeverity.INFO,
            asset_name=asset_name,
            message="Schema matches",
        ))

    return findings


def validate_row_counts(
    source_count: int,
    target_count: int,
    asset_name: str,
    tolerance: float = 0.0,
) -> list[ValidationFinding]:
    """Compare row counts with optional tolerance (0.0 = exact match)."""
    findings: list[ValidationFinding] = []

    if source_count == target_count:
        findings.append(ValidationFinding(
            category=ValidationCategory.COMPLETENESS,
            severity=ValidationSeverity.INFO,
            asset_name=asset_name,
            message=f"Row counts match: {source_count}",
        ))
    else:
        diff = abs(source_count - target_count)
        if source_count > 0 and tolerance > 0 and diff / source_count <= tolerance:
            findings.append(ValidationFinding(
                category=ValidationCategory.COMPLETENESS,
                severity=ValidationSeverity.WARNING,
                asset_name=asset_name,
                message=f"Row counts within tolerance: source={source_count}, target={target_count}",
                details={"source": source_count, "target": target_count, "diff": diff},
            ))
        else:
            findings.append(ValidationFinding(
                category=ValidationCategory.COMPLETENESS,
                severity=ValidationSeverity.ERROR,
                asset_name=asset_name,
                message=f"Row count mismatch: source={source_count}, target={target_count}",
                details={"source": source_count, "target": target_count, "diff": diff},
            ))

    return findings


def validate_sql_syntax(
    sql_path: str | Path,
    asset_name: str,
    dialect: str = "tsql",
) -> list[ValidationFinding]:
    """Validate SQL file syntax using sqlglot. Reads file, returns findings."""
    findings: list[ValidationFinding] = []
    path = Path(sql_path)

    if not path.exists():
        findings.append(ValidationFinding(
            category=ValidationCategory.SQL_SYNTAX,
            severity=ValidationSeverity.ERROR,
            asset_name=asset_name,
            message=f"SQL file not found: {path}",
        ))
        return findings

    sql = path.read_text(encoding="utf-8")
    if not sql.strip():
        findings.append(ValidationFinding(
            category=ValidationCategory.SQL_SYNTAX,
            severity=ValidationSeverity.ERROR,
            asset_name=asset_name,
            message="SQL file is empty",
        ))
        return findings

    errors = validate_sql(sql, dialect=dialect)
    if errors:
        for err in errors:
            findings.append(ValidationFinding(
                category=ValidationCategory.SQL_SYNTAX,
                severity=ValidationSeverity.ERROR,
                asset_name=asset_name,
                message=f"SQL syntax error: {err}",
            ))
    else:
        findings.append(ValidationFinding(
            category=ValidationCategory.SQL_SYNTAX,
            severity=ValidationSeverity.INFO,
            asset_name=asset_name,
            message="SQL syntax valid",
        ))

    return findings


def validate_notebook_structure(
    notebook_path: str | Path,
    asset_name: str,
) -> list[ValidationFinding]:
    """Validate Jupyter notebook JSON structure."""
    findings: list[ValidationFinding] = []
    path = Path(notebook_path)

    if not path.exists():
        findings.append(ValidationFinding(
            category=ValidationCategory.NOTEBOOK_STRUCTURE,
            severity=ValidationSeverity.ERROR,
            asset_name=asset_name,
            message=f"Notebook file not found: {path}",
        ))
        return findings

    try:
        nb = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        findings.append(ValidationFinding(
            category=ValidationCategory.NOTEBOOK_STRUCTURE,
            severity=ValidationSeverity.ERROR,
            asset_name=asset_name,
            message=f"Invalid notebook JSON: {e}",
        ))
        return findings

    # Must be nbformat v4
    if nb.get("nbformat") != 4:
        findings.append(ValidationFinding(
            category=ValidationCategory.NOTEBOOK_STRUCTURE,
            severity=ValidationSeverity.ERROR,
            asset_name=asset_name,
            message=f"Unexpected nbformat version: {nb.get('nbformat')}",
        ))

    # Must have cells
    cells = nb.get("cells", [])
    if not cells:
        findings.append(ValidationFinding(
            category=ValidationCategory.NOTEBOOK_STRUCTURE,
            severity=ValidationSeverity.WARNING,
            asset_name=asset_name,
            message="Notebook has no cells",
        ))
    else:
        # Each cell must have cell_type and source
        for i, cell in enumerate(cells):
            if "cell_type" not in cell:
                findings.append(ValidationFinding(
                    category=ValidationCategory.NOTEBOOK_STRUCTURE,
                    severity=ValidationSeverity.ERROR,
                    asset_name=asset_name,
                    message=f"Cell {i} missing cell_type",
                ))
            if "source" not in cell:
                findings.append(ValidationFinding(
                    category=ValidationCategory.NOTEBOOK_STRUCTURE,
                    severity=ValidationSeverity.ERROR,
                    asset_name=asset_name,
                    message=f"Cell {i} missing source",
                ))

    # Must have kernel metadata
    kernelspec = nb.get("metadata", {}).get("kernelspec", {})
    if not kernelspec:
        findings.append(ValidationFinding(
            category=ValidationCategory.NOTEBOOK_STRUCTURE,
            severity=ValidationSeverity.WARNING,
            asset_name=asset_name,
            message="Notebook missing kernelspec metadata",
        ))

    if not findings:
        findings.append(ValidationFinding(
            category=ValidationCategory.NOTEBOOK_STRUCTURE,
            severity=ValidationSeverity.INFO,
            asset_name=asset_name,
            message=f"Notebook structure valid ({len(cells)} cells)",
        ))

    return findings


def validate_pipeline_integrity(
    pipeline_path: str | Path,
    asset_name: str,
) -> list[ValidationFinding]:
    """Validate Fabric Data Pipeline JSON structure."""
    findings: list[ValidationFinding] = []
    path = Path(pipeline_path)

    if not path.exists():
        findings.append(ValidationFinding(
            category=ValidationCategory.PIPELINE_INTEGRITY,
            severity=ValidationSeverity.ERROR,
            asset_name=asset_name,
            message=f"Pipeline file not found: {path}",
        ))
        return findings

    try:
        pipeline = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        findings.append(ValidationFinding(
            category=ValidationCategory.PIPELINE_INTEGRITY,
            severity=ValidationSeverity.ERROR,
            asset_name=asset_name,
            message=f"Invalid pipeline JSON: {e}",
        ))
        return findings

    props = pipeline.get("properties", {})
    activities = props.get("activities", [])

    # Must have at least one activity
    if not activities:
        findings.append(ValidationFinding(
            category=ValidationCategory.PIPELINE_INTEGRITY,
            severity=ValidationSeverity.WARNING,
            asset_name=asset_name,
            message="Pipeline has no activities",
        ))

    # Validate each activity
    activity_names: set[str] = set()
    for act in activities:
        aname = act.get("name", "")
        if not aname:
            findings.append(ValidationFinding(
                category=ValidationCategory.PIPELINE_INTEGRITY,
                severity=ValidationSeverity.ERROR,
                asset_name=asset_name,
                message="Activity missing 'name'",
            ))
        elif aname in activity_names:
            findings.append(ValidationFinding(
                category=ValidationCategory.PIPELINE_INTEGRITY,
                severity=ValidationSeverity.ERROR,
                asset_name=asset_name,
                message=f"Duplicate activity name: '{aname}'",
            ))
        else:
            activity_names.add(aname)

        if not act.get("type"):
            findings.append(ValidationFinding(
                category=ValidationCategory.PIPELINE_INTEGRITY,
                severity=ValidationSeverity.ERROR,
                asset_name=asset_name,
                message=f"Activity '{aname}' missing 'type'",
            ))

        # Check dependency references point to valid activities
        for dep in act.get("dependsOn", []):
            dep_name = dep.get("activity")
            if dep_name and dep_name not in activity_names and dep_name != aname:
                # Forward references are okay in the JSON — we just flag truly missing ones
                # We'll check after collecting all names
                pass

    # Second pass: validate all dependency references exist
    for act in activities:
        aname = act.get("name", "")
        for dep in act.get("dependsOn", []):
            dep_name = dep.get("activity")
            if dep_name and dep_name not in activity_names:
                findings.append(ValidationFinding(
                    category=ValidationCategory.PIPELINE_INTEGRITY,
                    severity=ValidationSeverity.ERROR,
                    asset_name=asset_name,
                    message=f"Activity '{aname}' depends on unknown activity '{dep_name}'",
                ))

    # Check for Placeholder activities (need manual review)
    placeholders = [a for a in activities if a.get("type") == "Placeholder"]
    if placeholders:
        for p in placeholders:
            findings.append(ValidationFinding(
                category=ValidationCategory.PIPELINE_INTEGRITY,
                severity=ValidationSeverity.WARNING,
                asset_name=asset_name,
                message=f"Activity '{p.get('name', '?')}' is Placeholder — requires manual review",
            ))

    if not findings:
        findings.append(ValidationFinding(
            category=ValidationCategory.PIPELINE_INTEGRITY,
            severity=ValidationSeverity.INFO,
            asset_name=asset_name,
            message=f"Pipeline structure valid ({len(activities)} activities)",
        ))

    return findings


def validate_connection_mapping(
    connection_path: str | Path,
    asset_name: str,
) -> list[ValidationFinding]:
    """Validate a connection mapping JSON file."""
    findings: list[ValidationFinding] = []
    path = Path(connection_path)

    if not path.exists():
        findings.append(ValidationFinding(
            category=ValidationCategory.CONNECTION,
            severity=ValidationSeverity.ERROR,
            asset_name=asset_name,
            message=f"Connection mapping file not found: {path}",
        ))
        return findings

    try:
        mapping = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        findings.append(ValidationFinding(
            category=ValidationCategory.CONNECTION,
            severity=ValidationSeverity.ERROR,
            asset_name=asset_name,
            message=f"Invalid connection JSON: {e}",
        ))
        return findings

    # Must have fabric_type
    if not mapping.get("fabric_type"):
        findings.append(ValidationFinding(
            category=ValidationCategory.CONNECTION,
            severity=ValidationSeverity.ERROR,
            asset_name=asset_name,
            message="Connection mapping missing 'fabric_type'",
        ))

    # Check manual steps
    manual_steps = mapping.get("manual_steps", [])
    if manual_steps:
        findings.append(ValidationFinding(
            category=ValidationCategory.CONNECTION,
            severity=ValidationSeverity.WARNING,
            asset_name=asset_name,
            message=f"{len(manual_steps)} manual step(s) required for connection setup",
            details={"manual_steps": manual_steps},
        ))

    if not findings:
        findings.append(ValidationFinding(
            category=ValidationCategory.CONNECTION,
            severity=ValidationSeverity.INFO,
            asset_name=asset_name,
            message="Connection mapping valid",
        ))

    return findings


def build_report(
    project_name: str,
    asset_results: list[AssetValidationResult],
    review_flags: list[str],
    errors: list[str],
) -> ValidationReport:
    """Build a ValidationReport from per-asset results."""
    total = len(asset_results)
    passed = sum(1 for r in asset_results if r.passed)
    failed = sum(1 for r in asset_results if not r.passed)

    # Build category summaries
    cat_map: dict[ValidationCategory, CategorySummary] = {}
    for ar in asset_results:
        for f in ar.findings:
            if f.category not in cat_map:
                cat_map[f.category] = CategorySummary(category=f.category)
            cs = cat_map[f.category]
            cs.total += 1
            if f.severity == ValidationSeverity.INFO:
                cs.passed += 1
            elif f.severity == ValidationSeverity.WARNING:
                cs.warnings += 1
            else:
                cs.failed += 1

    return ValidationReport(
        project_name=project_name,
        total_assets=total,
        assets_validated=total,
        assets_passed=passed,
        assets_failed=failed,
        asset_results=asset_results,
        category_summaries=list(cat_map.values()),
        review_flags=review_flags,
        errors=errors,
    )


# ── Validation Agent ─────────────────────────────────────────


class ValidationAgent(BaseAgent):
    """Runs deep validation checks across all migrated assets."""

    @property
    def name(self) -> str:
        return "validator"

    @property
    def description(self) -> str:
        return "Validate all migrated assets for correctness and completeness"

    def _validate_asset(self, asset, output_dir: Path) -> AssetValidationResult:
        """Run category-specific validations for a single asset."""
        findings: list[ValidationFinding] = []
        target = asset.target_fabric_asset or {}

        target_type = target.get("type", "")

        # SQL syntax validation
        if target_type == "sql_script":
            path = target.get("path", "")
            dialect = target.get("dialect", "tsql")
            if path:
                findings.extend(validate_sql_syntax(path, asset.name, dialect))

        # Notebook structure validation
        elif target_type == "notebook":
            path = target.get("path", "")
            if path:
                findings.extend(validate_notebook_structure(path, asset.name))

        # Pipeline integrity validation
        elif target_type == "data_pipeline":
            path = target.get("path", "")
            if path:
                findings.extend(validate_pipeline_integrity(path, asset.name))

        # Connection mapping validation
        elif target_type == "connection_mapping":
            path = target.get("path", "")
            if path:
                findings.extend(validate_connection_mapping(path, asset.name))

        # Dataset DDL validation (validate the DDL SQL file)
        elif target_type in ("lakehouse_table", "warehouse_table"):
            ddl_path = target.get("ddl_path", "")
            if ddl_path:
                findings.extend(validate_sql_syntax(ddl_path, asset.name, "tsql"))

        # Pipeline trigger validation
        elif target_type == "pipeline_trigger":
            path = target.get("path", "")
            if path:
                p = Path(path)
                if not p.exists():
                    findings.append(ValidationFinding(
                        category=ValidationCategory.PIPELINE_INTEGRITY,
                        severity=ValidationSeverity.ERROR,
                        asset_name=asset.name,
                        message=f"Trigger file not found: {p}",
                    ))
                else:
                    try:
                        json.loads(p.read_text(encoding="utf-8"))
                        findings.append(ValidationFinding(
                            category=ValidationCategory.PIPELINE_INTEGRITY,
                            severity=ValidationSeverity.INFO,
                            asset_name=asset.name,
                            message="Trigger file valid JSON",
                        ))
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        findings.append(ValidationFinding(
                            category=ValidationCategory.PIPELINE_INTEGRITY,
                            severity=ValidationSeverity.ERROR,
                            asset_name=asset.name,
                            message=f"Invalid trigger JSON: {e}",
                        ))

        # Schema comparison: if source metadata has columns and target has column_count
        source_cols = asset.metadata.get("schema", asset.metadata.get("columns", []))
        target_cols = target.get("target_columns", [])
        if source_cols and target_cols:
            findings.extend(validate_schema(source_cols, target_cols, asset.name))

        # Row count comparison: if both source and target counts are available
        source_rows = asset.metadata.get("row_count")
        target_rows = target.get("row_count")
        if source_rows is not None and target_rows is not None:
            findings.extend(validate_row_counts(source_rows, target_rows, asset.name))

        # Review flags → findings
        for flag in asset.review_flags:
            findings.append(ValidationFinding(
                category=ValidationCategory.REVIEW_FLAG,
                severity=ValidationSeverity.WARNING,
                asset_name=asset.name,
                message=flag,
            ))

        # Completeness: assets without target info
        if not target:
            findings.append(ValidationFinding(
                category=ValidationCategory.COMPLETENESS,
                severity=ValidationSeverity.ERROR,
                asset_name=asset.name,
                message="No target Fabric asset info set",
            ))

        asset_passed = all(
            f.severity in (ValidationSeverity.INFO, ValidationSeverity.WARNING)
            for f in findings
        )

        return AssetValidationResult(
            asset_id=asset.id,
            asset_name=asset.name,
            asset_type=asset.type.value,
            passed=asset_passed,
            findings=findings,
        )

    async def execute(self, context: Any) -> AgentResult:
        """Run deep validation across all converted assets."""
        registry = context.registry
        config = context.config
        output_dir = Path(config.migration.output_dir)
        all_assets = registry.get_all()

        total = len(all_assets)
        converted = [a for a in all_assets if a.state in (
            MigrationState.CONVERTED, MigrationState.DEPLOYED, MigrationState.VALIDATED
        )]
        failed_assets = [a for a in all_assets if a.state in (
            MigrationState.FAILED, MigrationState.DEPLOY_FAILED, MigrationState.VALIDATION_FAILED
        )]
        discovered_only = [a for a in all_assets if a.state == MigrationState.DISCOVERED]

        review_flags: list[str] = []
        errors: list[str] = []
        asset_results: list[AssetValidationResult] = []

        # Check 1: unconverted assets
        for asset in discovered_only:
            review_flags.append(f"Asset not converted: {asset.name} ({asset.type.value})")

        # Check 2: prior failures
        for asset in failed_assets:
            for err in asset.errors:
                errors.append(f"{asset.name}: {err}")

        # Deep validation per converted asset
        for asset in converted:
            result = self._validate_asset(asset, output_dir)
            asset_results.append(result)

            if result.passed:
                registry.update_state(asset.id, MigrationState.VALIDATED)
            else:
                registry.update_state(asset.id, MigrationState.VALIDATION_FAILED)
                for f in result.findings:
                    if f.severity in (ValidationSeverity.ERROR, ValidationSeverity.CRITICAL):
                        errors.append(f"{asset.name}: {f.message}")

        # Aggregate review flags from findings
        for ar in asset_results:
            for f in ar.findings:
                if f.category == ValidationCategory.REVIEW_FLAG:
                    review_flags.append(f"{f.asset_name}: {f.message}")

        # Build report
        report = build_report(
            project_name=registry.project_key,
            asset_results=asset_results,
            review_flags=review_flags,
            errors=errors,
        )

        # Store report as agent detail
        report_dict = report.model_dump(mode="json")

        registry.save()

        passed_count = sum(1 for r in asset_results if r.passed)
        failed_count = sum(1 for r in asset_results if not r.passed)

        return AgentResult(
            agent_name=self.name,
            status=AgentStatus.COMPLETED if not errors else AgentStatus.COMPLETED,
            assets_processed=total,
            assets_converted=passed_count,
            assets_failed=failed_count + len(failed_assets),
            review_flags=review_flags,
            errors=errors,
            details={
                "total_assets": total,
                "validated": len(asset_results),
                "passed": passed_count,
                "failed": failed_count,
                "not_processed": len(discovered_only),
                "prior_failures": len(failed_assets),
                "report": report_dict,
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
