"""Lineage builder — construct lineage graph from AssetRegistry."""

from __future__ import annotations

from src.core.registry import AssetRegistry
from src.lineage.lineage_model import LineageEdge, LineageGraph, LineageNode, NodeType
from src.models.asset import AssetType

_ASSET_TYPE_TO_NODE_TYPE = {
    AssetType.DATASET: NodeType.DATASET,
    AssetType.RECIPE_SQL: NodeType.RECIPE,
    AssetType.RECIPE_PYTHON: NodeType.RECIPE,
    AssetType.RECIPE_VISUAL: NodeType.RECIPE,
    AssetType.CONNECTION: NodeType.CONNECTION,
    AssetType.MANAGED_FOLDER: NodeType.FOLDER,
    AssetType.FLOW: NodeType.PIPELINE,
    AssetType.SCENARIO: NodeType.PIPELINE,
    AssetType.SAVED_MODEL: NodeType.DATASET,
    AssetType.DASHBOARD: NodeType.DATASET,
}


def build_lineage(registry: AssetRegistry) -> LineageGraph:
    """Build a lineage graph from the asset registry.

    Creates nodes for each asset and edges from dependency relationships.

    Args:
        registry: Populated AssetRegistry.

    Returns:
        LineageGraph with all nodes and edges.
    """
    graph = LineageGraph(project_key=registry.project_key)

    # Create nodes
    for asset in registry.get_all():
        node_type = _ASSET_TYPE_TO_NODE_TYPE.get(asset.type, NodeType.DATASET)
        node = LineageNode(
            node_id=asset.id,
            name=asset.name,
            node_type=node_type,
            metadata={
                "asset_type": asset.type.value,
                "state": asset.state.value,
                "has_target": asset.target_fabric_asset is not None,
            },
        )
        graph.add_node(node)

    # Create edges from dependencies
    all_ids = {a.id for a in registry.get_all()}
    for asset in registry.get_all():
        for dep_id in asset.dependencies:
            if dep_id in all_ids:
                graph.add_edge(LineageEdge(
                    source_id=dep_id,
                    target_id=asset.id,
                    edge_type="depends_on",
                ))

    # Create edges from recipe inputs/outputs in metadata
    for asset in registry.get_all():
        if asset.type in (AssetType.RECIPE_SQL, AssetType.RECIPE_PYTHON, AssetType.RECIPE_VISUAL):
            inputs = asset.metadata.get("inputs", [])
            outputs = asset.metadata.get("outputs", [])

            for inp in inputs:
                inp_id = inp if isinstance(inp, str) else inp.get("id", "")
                if inp_id in all_ids:
                    graph.add_edge(LineageEdge(
                        source_id=inp_id,
                        target_id=asset.id,
                        edge_type="data_flow",
                    ))

            for out in outputs:
                out_id = out if isinstance(out, str) else out.get("id", "")
                if out_id in all_ids:
                    graph.add_edge(LineageEdge(
                        source_id=asset.id,
                        target_id=out_id,
                        edge_type="data_flow",
                    ))

    return graph
