"""Merge report — HTML heatmap and overlap analysis report."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.merge.merge_assessment import MergeAssessment
from src.merge.deduplicator import DeduplicationResult


def generate_merge_html(
    assessment: MergeAssessment,
    dedup: DeduplicationResult | None = None,
) -> str:
    """Generate an HTML merge assessment report with overlap heatmap."""

    # Build heatmap table
    projects = assessment.project_keys
    heatmap_rows = ""
    for pk_a in projects:
        cells = ""
        for pk_b in projects:
            count = assessment.overlap_matrix.get(pk_a, {}).get(pk_b, 0)
            if pk_a == pk_b:
                bg = "#e9ecef"
                cells += f'<td style="background:{bg}; text-align:center">—</td>'
            else:
                intensity = min(255, count * 30)
                bg = f"rgba(220, 53, 69, {intensity / 255:.2f})"
                cells += f'<td style="background:{bg}; text-align:center; color:#fff">{count}</td>'
        heatmap_rows += f"<tr><td><strong>{pk_a}</strong></td>{cells}</tr>"

    header_cells = "".join(f"<th>{pk}</th>" for pk in projects)

    # Build overlap table
    overlap_rows = ""
    for o in assessment.overlaps[:50]:
        overlap_rows += (
            f"<tr>"
            f"<td>{o.asset_a.project_key}/{o.asset_a.asset_name}</td>"
            f"<td>{o.asset_b.project_key}/{o.asset_b.asset_name}</td>"
            f"<td>{o.similarity:.1%}</td>"
            f"<td>{o.overlap_type}</td>"
            f"</tr>"
        )

    # Dedup summary
    dedup_section = ""
    if dedup:
        dedup_section = f"""
        <h2>Deduplication Summary</h2>
        <div style="display:flex; gap:20px; flex-wrap:wrap; margin-bottom:20px">
            <div class="card"><h3>Input Assets</h3><p class="big">{dedup.total_input_assets}</p></div>
            <div class="card"><h3>Output Assets</h3><p class="big">{dedup.total_output_assets}</p></div>
            <div class="card"><h3>Resolved</h3><p class="big">{dedup.conflicts_resolved}</p></div>
            <div class="card"><h3>Pending</h3><p class="big">{dedup.conflicts_pending}</p></div>
        </div>
        """

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Multi-Project Merge Report</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; background: #f8f9fa; }}
h1 {{ color: #212529; }}
h2 {{ color: #495057; margin-top: 30px; }}
.card {{ background: #fff; border-radius: 8px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,.1); min-width: 150px; text-align: center; }}
.card h3 {{ margin: 0 0 10px; color: #6c757d; font-size: 14px; }}
.card .big {{ font-size: 32px; font-weight: bold; margin: 0; color: #212529; }}
table {{ border-collapse: collapse; width: 100%; margin: 15px 0; background: #fff; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,.1); }}
th, td {{ padding: 10px 14px; text-align: left; border-bottom: 1px solid #dee2e6; }}
th {{ background: #212529; color: #fff; }}
</style>
</head>
<body>
<h1>Multi-Project Merge Report</h1>
<div style="display:flex; gap:20px; flex-wrap:wrap; margin-bottom:20px">
    <div class="card"><h3>Projects</h3><p class="big">{len(projects)}</p></div>
    <div class="card"><h3>Total Assets</h3><p class="big">{assessment.total_assets}</p></div>
    <div class="card"><h3>Unique</h3><p class="big">{assessment.unique_asset_count}</p></div>
    <div class="card"><h3>Overlaps</h3><p class="big">{len(assessment.overlaps)}</p></div>
    <div class="card"><h3>Merge Score</h3><p class="big">{assessment.merge_score:.0f}</p></div>
</div>

<h2>Overlap Heatmap</h2>
<table>
<tr><th>Project</th>{header_cells}</tr>
{heatmap_rows}
</table>

<h2>Overlapping Assets</h2>
<table>
<tr><th>Asset A</th><th>Asset B</th><th>Similarity</th><th>Type</th></tr>
{overlap_rows}
</table>

{dedup_section}
</body>
</html>"""
    return html


def save_merge_report(
    assessment: MergeAssessment,
    path: str | Path,
    dedup: DeduplicationResult | None = None,
) -> Path:
    """Generate and save a merge assessment HTML report."""
    html = generate_merge_html(assessment, dedup)
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(html, encoding="utf-8")
    return p
