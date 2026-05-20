"""Drift report — HTML visualization of schema drift."""

from __future__ import annotations

from html import escape
from pathlib import Path

from src.drift.drift_detector import DriftReport, DriftSeverity


def generate_drift_html(report: DriftReport) -> str:
    """Generate an HTML report for drift detection results."""
    h = escape

    severity_colors = {"info": "#17a2b8", "warning": "#ffc107", "breaking": "#dc3545"}
    breaking = report.get_by_severity(DriftSeverity.BREAKING)
    warnings = report.get_by_severity(DriftSeverity.WARNING)
    infos = report.get_by_severity(DriftSeverity.INFO)

    rows = ""
    for d in report.drifts:
        sc = severity_colors.get(d.severity.value, "#6c757d")
        rows += f"""<tr>
            <td><span style="color:{sc};font-weight:bold">{h(d.severity.value.upper())}</span></td>
            <td>{h(d.drift_type.value.replace('_', ' ').title())}</td>
            <td>{h(d.asset_name)}</td>
            <td>{h(d.description)}</td>
        </tr>"""

    header_color = "#dc3545" if report.has_breaking_changes else "#28a745"
    header_text = "BREAKING CHANGES DETECTED" if report.has_breaking_changes else "No Breaking Changes"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Schema Drift Report — {h(report.project_key)}</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; padding: 20px; background: #f8f9fa; }}
.container {{ max-width: 1000px; margin: 0 auto; }}
h1 {{ color: #343a40; }}
.cards {{ display: flex; gap: 20px; flex-wrap: wrap; margin: 20px 0; }}
.card {{ background: white; border-radius: 8px; padding: 20px; box-shadow: 0 2px 4px rgba(0,0,0,.1); text-align: center; min-width: 120px; }}
.card .value {{ font-size: 2em; font-weight: bold; }}
.card .label {{ color: #6c757d; }}
table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,.1); margin: 20px 0; }}
th {{ background: #343a40; color: white; padding: 12px; text-align: left; }}
td {{ padding: 10px 12px; border-bottom: 1px solid #dee2e6; }}
</style>
</head>
<body>
<div class="container">
<h1>Schema Drift Report</h1>
<p>Project: <strong>{h(report.project_key)}</strong></p>
<p>Baseline: {h(report.baseline_id)} → Current: {h(report.current_id)}</p>
<p style="color:{header_color};font-weight:bold;font-size:1.2em">{header_text}</p>

<div class="cards">
    <div class="card"><div class="value">{report.drift_count}</div><div class="label">Total Drifts</div></div>
    <div class="card"><div class="value" style="color:#dc3545">{len(breaking)}</div><div class="label">Breaking</div></div>
    <div class="card"><div class="value" style="color:#ffc107">{len(warnings)}</div><div class="label">Warnings</div></div>
    <div class="card"><div class="value" style="color:#17a2b8">{len(infos)}</div><div class="label">Info</div></div>
</div>

<h2>Drift Details</h2>
{'<table><tr><th>Severity</th><th>Type</th><th>Asset</th><th>Description</th></tr>' + rows + '</table>' if rows else '<p>No drifts detected.</p>'}
</div>
</body>
</html>"""


def save_drift_report(report: DriftReport, path: str | Path) -> Path:
    """Save drift report as HTML."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(generate_drift_html(report), encoding="utf-8")
    return p
