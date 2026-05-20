"""Lineage model — graph data structures for data lineage."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class NodeType(str, Enum):
    DATASET = "dataset"
    RECIPE = "recipe"
    CONNECTION = "connection"
    NOTEBOOK = "notebook"
    PIPELINE = "pipeline"
    FOLDER = "folder"


@dataclass
class LineageNode:
    """A node in the lineage graph."""
    node_id: str
    name: str
    node_type: NodeType
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.node_id,
            "name": self.name,
            "type": self.node_type.value,
            "metadata": self.metadata,
        }


@dataclass
class LineageEdge:
    """A directed edge in the lineage graph."""
    source_id: str
    target_id: str
    edge_type: str = "data_flow"
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source_id,
            "target": self.target_id,
            "type": self.edge_type,
            "metadata": self.metadata,
        }


@dataclass
class LineageGraph:
    """Complete lineage graph for a project."""
    project_key: str = ""
    nodes: dict[str, LineageNode] = field(default_factory=dict)
    edges: list[LineageEdge] = field(default_factory=list)

    def add_node(self, node: LineageNode) -> None:
        self.nodes[node.node_id] = node

    def add_edge(self, edge: LineageEdge) -> None:
        self.edges.append(edge)

    def get_node(self, node_id: str) -> LineageNode | None:
        return self.nodes.get(node_id)

    def get_upstream(self, node_id: str) -> list[LineageNode]:
        """Get all direct upstream nodes."""
        upstream_ids = {e.source_id for e in self.edges if e.target_id == node_id}
        return [self.nodes[nid] for nid in upstream_ids if nid in self.nodes]

    def get_downstream(self, node_id: str) -> list[LineageNode]:
        """Get all direct downstream nodes."""
        downstream_ids = {e.target_id for e in self.edges if e.source_id == node_id}
        return [self.nodes[nid] for nid in downstream_ids if nid in self.nodes]

    def get_all_upstream(self, node_id: str) -> set[str]:
        """Recursively get all upstream node IDs (transitive closure)."""
        visited: set[str] = set()
        stack = [node_id]
        while stack:
            current = stack.pop()
            for e in self.edges:
                if e.target_id == current and e.source_id not in visited:
                    visited.add(e.source_id)
                    stack.append(e.source_id)
        return visited

    def get_all_downstream(self, node_id: str) -> set[str]:
        """Recursively get all downstream node IDs (transitive closure)."""
        visited: set[str] = set()
        stack = [node_id]
        while stack:
            current = stack.pop()
            for e in self.edges:
                if e.source_id == current and e.target_id not in visited:
                    visited.add(e.target_id)
                    stack.append(e.target_id)
        return visited

    def impact_analysis(self, node_id: str) -> dict[str, Any]:
        """Analyze the impact of changing a specific node."""
        downstream = self.get_all_downstream(node_id)
        upstream = self.get_all_upstream(node_id)
        node = self.get_node(node_id)

        return {
            "node": node.to_dict() if node else {"id": node_id},
            "upstream_count": len(upstream),
            "downstream_count": len(downstream),
            "upstream_nodes": sorted(upstream),
            "downstream_nodes": sorted(downstream),
            "total_impacted": len(downstream),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "project_key": self.project_key,
            "node_count": len(self.nodes),
            "edge_count": len(self.edges),
            "nodes": [n.to_dict() for n in self.nodes.values()],
            "edges": [e.to_dict() for e in self.edges],
        }

    def to_mermaid(self) -> str:
        """Generate Mermaid flowchart from the lineage graph."""
        lines = ["graph LR"]
        type_shapes = {
            NodeType.DATASET: ("[({name})]", "dataset"),
            NodeType.RECIPE: ("[{name}]", "recipe"),
            NodeType.CONNECTION: ("({name})", "connection"),
            NodeType.NOTEBOOK: ("[/{name}/]", "notebook"),
            NodeType.PIPELINE: ("[[{name}]]", "pipeline"),
            NodeType.FOLDER: ("[\\{name}\\]", "folder"),
        }

        for node in self.nodes.values():
            shape_tpl, _ = type_shapes.get(node.node_type, ("[{name}]", "default"))
            shape = shape_tpl.format(name=node.name)
            safe_id = node.node_id.replace("-", "_").replace(" ", "_")
            lines.append(f"    {safe_id}{shape}")

        for edge in self.edges:
            src = edge.source_id.replace("-", "_").replace(" ", "_")
            tgt = edge.target_id.replace("-", "_").replace(" ", "_")
            label = edge.edge_type.replace("_", " ")
            lines.append(f"    {src} -->|{label}| {tgt}")

        return "\n".join(lines)
