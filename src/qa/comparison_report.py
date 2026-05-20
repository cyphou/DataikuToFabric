"""Comparison report — QA suite HTML report."""

from __future__ import annotations

import json
from html import escape
from pathlib import Path
from typing import Any

from src.qa.qa_suite import QAResult


def generate_qa_html(result: QAResult) -> str:
    """Generate an HTML report for QA suite results."""
    h = escape

    stage_rows = ""
    status_colors = {"passed": "#28a745", "failed": "#dc3545", "skipped": "#6c757d", "warning": "#ffc107"}
    for sr in result.stage_results:
        sc = status_colors.get(sr.status.value, "#6c757d")
        findings_html = "<br>".join(h(f) for f in sr.findings[:10])
        if len(sr.findings) > 10:
            findings_html += f"<br>... and {len(sr.findings) - 10} more"
        stage_rows += f"""<tr>
            <td>{h(sr.stage.value.replace('_', ' ').title())}</td>
            <td><span style="color:{sc};font-weight:bold">{h(sr.status.value.upper())}</span></td>
            <td>{len(sr.findings)}</td>
            <td>{sr.fixes_applied}</td>
            <td>{findings_html}</td>
        </tr>"""

    fidelity_section = ""
    if result.fidelity:
        fidelity_section = f"""
        <h2>Fidelity Score</h2>
        <div class="card" style="max-width:200px">
            <div class="value">{result.fidelity.overall_score:.1f}%</div>
            <div class="label">Overall Fidelity</div>
        </div>
        <p>{result.fidelity.scored_assets} assets scored, {len(result.fidelity.penalties)} penalties</p>
        """

    overall_color = "#28a745" if result.overall_passed else "#dc3545"
    overall_text = "PASSED" if result.overall_passed else "FAILED"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>QA Report — {h(result.project_key)}</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; padding: 20px; background: #f8f9fa; }}
.container {{ max-width: 1000px; margin: 0 auto; }}
h1 {{ color: #343a40; }}
.card {{ background: white; border-radius: 8px; padding: 20px; box-shadow: 0 2px 4px rgba(0,0,0,.1); text-align: center; display: inline-block; margin: 10px; }}
.card .value {{ font-size: 2em; font-weight: bold; }}
.card .label {{ color: #6c757d; }}
table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,.1); margin: 20px 0; }}
th {{ background: #343a40; color: white; padding: 12px; text-align: left; }}
td {{ padding: 10px 12px; border-bottom: 1px solid #dee2e6; vertical-align: top; }}
</style>
</head>
<body>
<div class="container">
<h1>QA Suite Report</h1>
<p>Project: <strong>{h(result.project_key)}</strong></p>
<div class="card">
    <div class="value" style="color:{overall_color}">{overall_text}</div>
    <div class="label">Overall</div>
</div>
<div class="card">
    <div class="value">{result.total_findings}</div>
    <div class="label">Findings</div>
</div>
<div class="card">
    <div class="value">{result.total_fixes}</div>
    <div class="label">Fixes Applied</div>
</div>

<h2>Stage Results</h2>
<table>
<tr><th>Stage</th><th>Status</th><th>Findings</th><th>Fixes</th><th>Details</th></tr>
{stage_rows}
</table>

{fidelity_section}
</div>
</body>
</html>"""


def save_qa_report(result: QAResult, path: str | Path) -> Path:
    """Save QA report as HTML."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(generate_qa_html(result), encoding="utf-8")
    return p
