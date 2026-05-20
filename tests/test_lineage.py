"""Tests for Phase 23 — Lineage Map & Visualization."""

from __future__ import annotations

import pytest

from src.lineage.lineage_model import LineageEdge, LineageGraph, LineageNode, NodeType
from src.lineage.lineage_builder import build_lineage
from src.reports.lineage_report import generate_lineage_html, save_lineage_report
from src.models.asset import Asset, AssetType, MigrationState


# ── LineageGraph ──────────────────────────────────────────────

class TestLineageGraph:
    def test_add_node(self):
        g = LineageGraph()
        node = LineageNode(node_id="n1", name="orders", node_type=NodeType.DATASET)
        g.add_node(node)
        assert g.get_node("n1") == node

    def test_add_edge(self):
        g = LineageGraph()
        g.add_node(LineageNode(node_id="n1", name="src", node_type=NodeType.DATASET))
        g.add_node(LineageNode(node_id="n2", name="tgt", node_type=NodeType.RECIPE))
        g.add_edge(LineageEdge(source_id="n1", target_id="n2", edge_type="input"))
        assert "n2" in [e.target_id for e in g.edges]

    def test_get_upstream(self):
        g = LineageGraph()
        g.add_node(LineageNode(node_id="a", name="a", node_type=NodeType.DATASET))
        g.add_node(LineageNode(node_id="b", name="b", node_type=NodeType.RECIPE))
        g.add_edge(LineageEdge(source_id="a", target_id="b", edge_type="input"))
        upstream = g.get_upstream("b")
        assert any(n.node_id == "a" for n in upstream)

    def test_get_downstream(self):
        g = LineageGraph()
        g.add_node(LineageNode(node_id="a", name="a", node_type=NodeType.DATASET))
        g.add_node(LineageNode(node_id="b", name="b", node_type=NodeType.RECIPE))
        g.add_edge(LineageEdge(source_id="a", target_id="b", edge_type="input"))
        downstream = g.get_downstream("a")
        assert any(n.node_id == "b" for n in downstream)

    def test_get_all_upstream(self):
        g = LineageGraph()
        g.add_node(LineageNode(node_id="a", name="a", node_type=NodeType.DATASET))
        g.add_node(LineageNode(node_id="b", name="b", node_type=NodeType.RECIPE))
        g.add_node(LineageNode(node_id="c", name="c", node_type=NodeType.DATASET))
        g.add_edge(LineageEdge(source_id="a", target_id="b", edge_type="input"))
        g.add_edge(LineageEdge(source_id="b", target_id="c", edge_type="output"))
        all_up = g.get_all_upstream("c")
        assert "a" in all_up
        assert "b" in all_up

    def test_get_all_downstream(self):
        g = LineageGraph()
        g.add_node(LineageNode(node_id="a", name="a", node_type=NodeType.DATASET))
        g.add_node(LineageNode(node_id="b", name="b", node_type=NodeType.RECIPE))
        g.add_node(LineageNode(node_id="c", name="c", node_type=NodeType.DATASET))
        g.add_edge(LineageEdge(source_id="a", target_id="b", edge_type="input"))
        g.add_edge(LineageEdge(source_id="b", target_id="c", edge_type="output"))
        all_down = g.get_all_downstream("a")
        assert "b" in all_down
        assert "c" in all_down

    def test_impact_analysis(self):
        g = LineageGraph()
        g.add_node(LineageNode(node_id="a", name="a", node_type=NodeType.DATASET))
        g.add_node(LineageNode(node_id="b", name="b", node_type=NodeType.RECIPE))
        g.add_node(LineageNode(node_id="c", name="c", node_type=NodeType.DATASET))
        g.add_edge(LineageEdge(source_id="a", target_id="b", edge_type="input"))
        g.add_edge(LineageEdge(source_id="b", target_id="c", edge_type="output"))
        impact = g.impact_analysis("a")
        assert "b" in impact["downstream_nodes"]
        assert "c" in impact["downstream_nodes"]

    def test_to_dict(self):
        g = LineageGraph()
        g.add_node(LineageNode(node_id="a", name="a", node_type=NodeType.DATASET))
        d = g.to_dict()
        assert "nodes" in d
        assert "edges" in d
        assert len(d["nodes"]) == 1

    def test_to_mermaid(self):
        g = LineageGraph()
        g.add_node(LineageNode(node_id="a", name="a", node_type=NodeType.DATASET))
        g.add_node(LineageNode(node_id="b", name="b", node_type=NodeType.RECIPE))
        g.add_edge(LineageEdge(source_id="a", target_id="b", edge_type="input"))
        mermaid = g.to_mermaid()
        assert "graph" in mermaid.lower() or "flowchart" in mermaid.lower()

    def test_empty_graph(self):
        g = LineageGraph()
        assert g.get_upstream("nonexistent") == []
        assert g.get_downstream("nonexistent") == []

    def test_node_to_dict(self):
        n = LineageNode(node_id="a", name="test", node_type=NodeType.DATASET)
        d = n.to_dict()
        assert d["id"] == "a"
        assert d["name"] == "test"

    def test_edge_to_dict(self):
        e = LineageEdge(source_id="a", target_id="b", edge_type="input")
        d = e.to_dict()
        assert d["source"] == "a"
        assert d["target"] == "b"


# ── Lineage Builder ──────────────────────────────────────────

class TestLineageBuilder:
    def test_build_empty(self, registry):
        g = build_lineage(registry)
        assert isinstance(g, LineageGraph)
        assert len(g.nodes) == 0

    def test_build_with_assets(self, populated_registry):
        g = build_lineage(populated_registry)
        assert len(g.nodes) > 0

    def test_build_with_dependencies(self, registry):
        a = Asset(id="ds1", name="orders", type=AssetType.DATASET, source_project="P")
        b = Asset(id="r1", name="compute", type=AssetType.RECIPE_SQL, source_project="P",
                  dependencies=["ds1"])
        registry.add_asset(a)
        registry.add_asset(b)
        g = build_lineage(registry)
        upstream = g.get_upstream("r1")
        assert any(n.node_id == "ds1" for n in upstream)


# ── Lineage Report ────────────────────────────────────────────

class TestLineageReport:
    def test_generate_html(self, populated_registry):
        g = build_lineage(populated_registry)
        html = generate_lineage_html(g)
        assert "<html" in html

    def test_save_report(self, tmp_path, populated_registry):
        g = build_lineage(populated_registry)
        path = save_lineage_report(g, tmp_path / "lineage.html")
        assert path.exists()
