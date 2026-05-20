"""Lineage report — Mermaid-based HTML visualization."""

from __future__ import annotations

from html import escape
from pathlib import Path

from src.lineage.lineage_model import LineageGraph


def generate_lineage_html(graph: LineageGraph) -> str:
    """Generate an HTML report with interactive Mermaid lineage diagram."""
    h = escape
    mermaid_code = graph.to_mermaid()

    node_rows = ""
    for node in graph.nodes.values():
        up = len(graph.get_upstream(node.node_id))
        down = len(graph.get_downstream(node.node_id))
        node_rows += f"""<tr>
            <td>{h(node.name)}</td>
            <td>{h(node.node_type.value)}</td>
            <td>{up}</td>
            <td>{down}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Data Lineage — {h(graph.project_key)}</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; padding: 20px; background: #f8f9fa; }}
.container {{ max-width: 1200px; margin: 0 auto; }}
h1 {{ color: #343a40; }}
.cards {{ display: flex; gap: 20px; flex-wrap: wrap; margin: 20px 0; }}
.card {{ background: white; border-radius: 8px; padding: 20px; box-shadow: 0 2px 4px rgba(0,0,0,.1); text-align: center; min-width: 120px; }}
.card .value {{ font-size: 2em; font-weight: bold; }}
.card .label {{ color: #6c757d; }}
.mermaid {{ background: white; border-radius: 8px; padding: 20px; box-shadow: 0 2px 4px rgba(0,0,0,.1); margin: 20px 0; overflow-x: auto; }}
table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,.1); margin: 20px 0; }}
th {{ background: #343a40; color: white; padding: 12px; text-align: left; }}
td {{ padding: 10px 12px; border-bottom: 1px solid #dee2e6; }}
</style>
</head>
<body>
<div class="container">
<h1>Data Lineage Map</h1>
<p>Project: <strong>{h(graph.project_key)}</strong></p>

<div class="cards">
    <div class="card"><div class="value">{len(graph.nodes)}</div><div class="label">Nodes</div></div>
    <div class="card"><div class="value">{len(graph.edges)}</div><div class="label">Edges</div></div>
</div>

<h2>Lineage Diagram</h2>
<div class="mermaid">
{h(mermaid_code)}
</div>

<h2>Node Summary</h2>
<table>
<tr><th>Name</th><th>Type</th><th>Upstream</th><th>Downstream</th></tr>
{node_rows}
</table>
</div>

<script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
<script>mermaid.initialize({{startOnLoad:true}});</script>
</body>
</html>"""


def save_lineage_report(graph: LineageGraph, path: str | Path) -> Path:
    """Save lineage report as HTML."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(generate_lineage_html(graph), encoding="utf-8")
    return p
