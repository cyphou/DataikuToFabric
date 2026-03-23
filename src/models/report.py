"""Validation report data models and export functions (HTML + JSON)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from enum import Enum
from html import escape
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class ValidationSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ValidationCategory(str, Enum):
    SCHEMA = "schema"
    SQL_SYNTAX = "sql_syntax"
    NOTEBOOK_STRUCTURE = "notebook_structure"
    PIPELINE_INTEGRITY = "pipeline_integrity"
    CONNECTION = "connection"
    DATA_LINEAGE = "data_lineage"
    REVIEW_FLAG = "review_flag"
    COMPLETENESS = "completeness"


class ValidationFinding(BaseModel):
    """A single validation finding (pass or fail)."""

    category: ValidationCategory
    severity: ValidationSeverity
    asset_name: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


class AssetValidationResult(BaseModel):
    """Validation result for a single asset."""

    asset_id: str
    asset_name: str
    asset_type: str
    passed: bool
    findings: list[ValidationFinding] = Field(default_factory=list)


class CategorySummary(BaseModel):
    """Summary of findings in one category."""

    category: ValidationCategory
    total: int = 0
    passed: int = 0
    failed: int = 0
    warnings: int = 0


class ValidationReport(BaseModel):
    """Full migration validation report."""

    project_name: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    total_assets: int = 0
    assets_validated: int = 0
    assets_passed: int = 0
    assets_failed: int = 0
    assets_skipped: int = 0
    asset_results: list[AssetValidationResult] = Field(default_factory=list)
    category_summaries: list[CategorySummary] = Field(default_factory=list)
    review_flags: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)

    @property
    def success_rate(self) -> float:
        if self.assets_validated == 0:
            return 0.0
        return self.assets_passed / self.assets_validated * 100

    def to_summary(self) -> dict[str, Any]:
        return {
            "project": self.project_name,
            "timestamp": self.timestamp.isoformat(),
            "total_assets": self.total_assets,
            "validated": self.assets_validated,
            "passed": self.assets_passed,
            "failed": self.assets_failed,
            "skipped": self.assets_skipped,
            "success_rate": f"{self.success_rate:.1f}%",
            "review_flags": len(self.review_flags),
            "errors": len(self.errors),
        }


# ── JSON export ──────────────────────────────────────────────


def generate_json_report(report: ValidationReport) -> str:
    """Serialize a ValidationReport to a pretty-printed JSON string."""
    return json.dumps(report.model_dump(mode="json"), indent=2, default=str)


def save_json_report(report: ValidationReport, path: str | Path) -> Path:
    """Save a ValidationReport as JSON to a file. Returns the path written."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(generate_json_report(report), encoding="utf-8")
    return p


# ── HTML export ──────────────────────────────────────────────

_SEVERITY_COLOR = {
    "info": "#28a745",
    "warning": "#ffc107",
    "error": "#dc3545",
    "critical": "#721c24",
}

_SEVERITY_BADGE = {
    "info": "&#10003;",     # checkmark
    "warning": "&#9888;",   # warning sign
    "error": "&#10007;",    # cross
    "critical": "&#10007;", # cross
}


def generate_html_report(report: ValidationReport) -> str:
    """Render a ValidationReport as a self-contained HTML document."""
    h = escape  # alias for html.escape

    # Build category summary rows
    cat_rows = ""
    for cs in report.category_summaries:
        cat_rows += (
            f"<tr><td>{h(cs.category.value)}</td>"
            f"<td>{cs.total}</td>"
            f"<td style='color:#28a745'>{cs.passed}</td>"
            f"<td style='color:#ffc107'>{cs.warnings}</td>"
            f"<td style='color:#dc3545'>{cs.failed}</td></tr>\n"
        )

    # Build asset detail rows
    asset_rows = ""
    for ar in report.asset_results:
        status_icon = "&#10003;" if ar.passed else "&#10007;"
        status_color = "#28a745" if ar.passed else "#dc3545"
        finding_items = ""
        for f in ar.findings:
            sev = f.severity.value
            color = _SEVERITY_COLOR.get(sev, "#333")
            badge = _SEVERITY_BADGE.get(sev, "")
            finding_items += (
                f"<li style='color:{color}'>"
                f"<strong>[{h(sev.upper())}]</strong> {badge} "
                f"<em>{h(f.category.value)}</em>: {h(f.message)}</li>\n"
            )
        asset_rows += (
            f"<tr><td><strong>{h(ar.asset_name)}</strong></td>"
            f"<td>{h(ar.asset_type)}</td>"
            f"<td style='color:{status_color}'>{status_icon}</td>"
            f"<td><ul style='margin:0;padding-left:1.5em'>{finding_items}</ul></td></tr>\n"
        )

    # Review flags section
    flags_html = ""
    if report.review_flags:
        flag_items = "".join(f"<li>{h(f)}</li>" for f in report.review_flags)
        flags_html = f"<h2>Review Flags ({len(report.review_flags)})</h2><ul>{flag_items}</ul>"

    # Errors section
    errors_html = ""
    if report.errors:
        err_items = "".join(f"<li style='color:#dc3545'>{h(e)}</li>" for e in report.errors)
        errors_html = f"<h2>Errors ({len(report.errors)})</h2><ul>{err_items}</ul>"

    rate = report.success_rate
    rate_color = "#28a745" if rate >= 90 else "#ffc107" if rate >= 70 else "#dc3545"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Migration Report — {h(report.project_name)}</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         margin: 2em; color: #333; background: #f8f9fa; }}
  h1 {{ color: #0078d4; }}
  table {{ border-collapse: collapse; width: 100%; margin-bottom: 1.5em; background: #fff; }}
  th, td {{ border: 1px solid #dee2e6; padding: 8px 12px; text-align: left; }}
  th {{ background: #e9ecef; }}
  .summary-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                   gap: 1em; margin-bottom: 2em; }}
  .summary-card {{ background: #fff; border: 1px solid #dee2e6; border-radius: 8px;
                   padding: 1em; text-align: center; }}
  .summary-card .label {{ font-size: 0.85em; color: #6c757d; }}
  .summary-card .value {{ font-size: 1.8em; font-weight: bold; }}
</style>
</head>
<body>
<h1>Migration Validation Report</h1>
<p>Project: <strong>{h(report.project_name)}</strong> &mdash;
   Generated: {h(report.timestamp.isoformat())}</p>

<div class="summary-grid">
  <div class="summary-card">
    <div class="label">Total Assets</div>
    <div class="value">{report.total_assets}</div>
  </div>
  <div class="summary-card">
    <div class="label">Validated</div>
    <div class="value">{report.assets_validated}</div>
  </div>
  <div class="summary-card">
    <div class="label">Passed</div>
    <div class="value" style="color:#28a745">{report.assets_passed}</div>
  </div>
  <div class="summary-card">
    <div class="label">Failed</div>
    <div class="value" style="color:#dc3545">{report.assets_failed}</div>
  </div>
  <div class="summary-card">
    <div class="label">Success Rate</div>
    <div class="value" style="color:{rate_color}">{rate:.1f}%</div>
  </div>
</div>

<h2>Category Summary</h2>
<table>
<tr><th>Category</th><th>Total</th><th>Passed</th><th>Warnings</th><th>Failed</th></tr>
{cat_rows}
</table>

<h2>Asset Details</h2>
<table>
<tr><th>Asset</th><th>Type</th><th>Status</th><th>Findings</th></tr>
{asset_rows}
</table>

{flags_html}
{errors_html}

</body>
</html>"""

    return html


def save_html_report(report: ValidationReport, path: str | Path) -> Path:
    """Save a ValidationReport as HTML to a file. Returns the path written."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(generate_html_report(report), encoding="utf-8")
    return p
