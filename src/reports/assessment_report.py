"""Assessment HTML report generator — radar chart, stat cards, risk table."""

from __future__ import annotations

import json
from html import escape
from pathlib import Path
from typing import Any

from src.analyzers.project_analyzer import AssessmentResult

_GRADE_COLORS = {
    "A": "#28a745",
    "B": "#17a2b8",
    "C": "#ffc107",
    "D": "#fd7e14",
    "F": "#dc3545",
}


def generate_assessment_html(result: AssessmentResult) -> str:
    """Render an assessment result as a self-contained HTML report."""
    h = escape
    grade_color = _GRADE_COLORS.get(result.grade.value, "#6c757d")

    # Radar chart data
    labels = [cs.category.value.replace("_", " ").title() for cs in result.category_scores]
    values = [cs.score for cs in result.category_scores]
    radar_labels_json = json.dumps(labels)
    radar_values_json = json.dumps(values)

    # Category score rows
    cat_rows = ""
    for cs in result.category_scores:
        bar_color = "#28a745" if cs.score >= 75 else "#ffc107" if cs.score >= 50 else "#dc3545"
        cat_rows += f"""<tr>
            <td>{h(cs.category.value.replace('_', ' ').title())}</td>
            <td>{cs.asset_count}</td>
            <td>
                <div style="background:#e9ecef;border-radius:4px;height:20px;width:200px;display:inline-block">
                    <div style="background:{bar_color};height:20px;width:{cs.score * 2}px;border-radius:4px"></div>
                </div>
                {cs.score:.0f}/100
            </td>
            <td>{'<br>'.join(h(f) for f in cs.findings[:5])}{' ...' if len(cs.findings) > 5 else ''}</td>
        </tr>"""

    # Risk rows
    risk_rows = ""
    risk_colors = {"critical": "#721c24", "high": "#dc3545", "medium": "#ffc107", "low": "#28a745"}
    for r in result.risks:
        rc = risk_colors.get(r.level.value, "#6c757d")
        risk_rows += f"""<tr>
            <td><span style="color:{rc};font-weight:bold">{h(r.level.value.upper())}</span></td>
            <td>{h(r.category.value.replace('_', ' ').title())}</td>
            <td>{h(r.description)}</td>
            <td>{h(r.mitigation)}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Migration Assessment — {h(result.project_key)}</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 20px; background: #f8f9fa; color: #212529; }}
.container {{ max-width: 1200px; margin: 0 auto; }}
h1 {{ color: #343a40; }}
.cards {{ display: flex; gap: 20px; flex-wrap: wrap; margin: 20px 0; }}
.card {{ background: white; border-radius: 8px; padding: 20px; box-shadow: 0 2px 4px rgba(0,0,0,.1); min-width: 150px; text-align: center; }}
.card .value {{ font-size: 2em; font-weight: bold; }}
.card .label {{ color: #6c757d; font-size: 0.9em; }}
table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,.1); margin: 20px 0; }}
th {{ background: #343a40; color: white; padding: 12px; text-align: left; }}
td {{ padding: 10px 12px; border-bottom: 1px solid #dee2e6; vertical-align: top; }}
tr:hover {{ background: #f1f3f5; }}
canvas {{ max-width: 400px; margin: 20px auto; display: block; }}
</style>
</head>
<body>
<div class="container">
<h1>Migration Assessment Report</h1>
<p>Project: <strong>{h(result.project_key)}</strong></p>

<div class="cards">
    <div class="card">
        <div class="value" style="color:{grade_color}">{result.grade.value}</div>
        <div class="label">Grade</div>
    </div>
    <div class="card">
        <div class="value">{result.overall_score:.1f}</div>
        <div class="label">Score / 100</div>
    </div>
    <div class="card">
        <div class="value">{result.total_assets}</div>
        <div class="label">Total Assets</div>
    </div>
    <div class="card">
        <div class="value">{len(result.risks)}</div>
        <div class="label">Risks</div>
    </div>
</div>

<h2>Category Scores</h2>
<table>
<tr><th>Category</th><th>Assets</th><th>Score</th><th>Findings</th></tr>
{cat_rows}
</table>

<h2>Readiness Radar</h2>
<canvas id="radar" width="400" height="400"></canvas>

<h2>Risks</h2>
{'<table><tr><th>Level</th><th>Category</th><th>Description</th><th>Mitigation</th></tr>' + risk_rows + '</table>' if risk_rows else '<p>No significant risks identified.</p>'}

<p style="color:#6c757d;margin-top:40px;font-size:0.85em">{h(result.summary)}</p>
</div>

<script>
// Simple radar chart (no external deps)
(function() {{
    const canvas = document.getElementById('radar');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    const labels = {radar_labels_json};
    const values = {radar_values_json};
    const cx = 200, cy = 200, r = 150, n = labels.length;

    function angle(i) {{ return (Math.PI * 2 * i / n) - Math.PI / 2; }}
    function point(i, v) {{ return [cx + r * (v/100) * Math.cos(angle(i)), cy + r * (v/100) * Math.sin(angle(i))]; }}

    // Grid
    ctx.strokeStyle = '#dee2e6';
    for (let ring = 25; ring <= 100; ring += 25) {{
        ctx.beginPath();
        for (let i = 0; i <= n; i++) {{ const [x, y] = point(i % n, ring); i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y); }}
        ctx.stroke();
    }}

    // Axes + labels
    ctx.fillStyle = '#495057';
    ctx.font = '12px sans-serif';
    ctx.textAlign = 'center';
    for (let i = 0; i < n; i++) {{
        const [x, y] = point(i, 100);
        ctx.beginPath(); ctx.moveTo(cx, cy); ctx.lineTo(x, y); ctx.strokeStyle = '#dee2e6'; ctx.stroke();
        const [lx, ly] = point(i, 115);
        ctx.fillText(labels[i], lx, ly + 4);
    }}

    // Data polygon
    ctx.beginPath();
    for (let i = 0; i <= n; i++) {{ const [x, y] = point(i % n, values[i % n]); i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y); }}
    ctx.fillStyle = 'rgba(54, 162, 235, 0.3)';
    ctx.fill();
    ctx.strokeStyle = 'rgba(54, 162, 235, 1)';
    ctx.lineWidth = 2;
    ctx.stroke();

    // Data points
    for (let i = 0; i < n; i++) {{
        const [x, y] = point(i, values[i]);
        ctx.beginPath(); ctx.arc(x, y, 4, 0, Math.PI * 2); ctx.fillStyle = 'rgba(54, 162, 235, 1)'; ctx.fill();
    }}
}})();
</script>
</body>
</html>"""
    return html


def save_assessment_report(result: AssessmentResult, path: str | Path) -> Path:
    """Save an assessment report as HTML."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(generate_assessment_html(result), encoding="utf-8")
    return p
