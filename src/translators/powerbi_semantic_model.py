"""Dataiku dataset schemas → Power BI/Fabric SemanticModel (TMDL, DirectLake).

Generates a minimal DirectLake semantic model — tables and columns only, no
measures/relationships/RLS — over datasets already migrated to a Fabric
Lakehouse, so they're immediately reportable in Power BI without importing
another copy of the data.

Structurally inspired by the DirectLake TMDL layout used in the sibling
TableauToPowerBI project (``database.tmdl`` / ``model.tmdl`` /
``expressions.tmdl`` / ``tables/*.tmdl``), reimplemented here since that
project's generator is coupled to Tableau-specific extraction internals
(calculated fields, worksheets) that don't apply to Dataiku dataset schemas.
"""

from __future__ import annotations

# ── Type mapping: Dataiku → TMDL/DAX dataType ─────────────────

_DAX_TYPE_MAP: dict[str, str] = {
    "string": "string",
    "tinyint": "int64",
    "smallint": "int64",
    "int": "int64",
    "bigint": "int64",
    "float": "double",
    "double": "double",
    "decimal": "decimal",
    "boolean": "boolean",
    "date": "dateTime",
    "timestamp": "dateTime",
    "binary": "binary",
    "array": "string",
    "map": "string",
    "object": "string",
}


def _map_dax_type(column_type: str) -> str:
    """Map a Dataiku column type (e.g. ``decimal(18,2)``) to a TMDL dataType."""
    base = (column_type or "string").lower().split("(")[0]
    return _DAX_TYPE_MAP.get(base, "string")


def generate_table_tmdl(table_name: str, columns: list[dict]) -> str:
    """Generate the TMDL definition for one DirectLake table."""
    lines: list[str] = [f"table {table_name}", ""]

    for col in columns:
        name = col.get("name", "")
        dax_type = _map_dax_type(col.get("type", "string"))
        lines += [
            f"\tcolumn {name}",
            f"\t\tdataType: {dax_type}",
            f"\t\tsourceColumn: {name}",
            "\t\tsummarizeBy: none",
            "",
        ]

    lines += [
        f"\tpartition {table_name} = entity",
        "\t\tmode: directLake",
        "\t\tsource",
        f"\t\t\tentityName: {table_name}",
        "\t\t\texpressionSource: DatabaseQuery",
        "",
    ]
    return "\n".join(lines)


def generate_semantic_model_tmdl(
    model_name: str,
    tables: list[dict],
    *,
    workspace_id: str,
    lakehouse_id: str,
    compatibility_level: int = 1604,
) -> dict[str, str]:
    """Build the full TMDL file set for a DirectLake Fabric SemanticModel.

    Args:
        model_name: Display name of the semantic model.
        tables: One entry per migrated dataset: ``{"name": str, "columns": [...]}``,
            matching the schema shape already stored on ``DATASET`` assets.
        workspace_id: Fabric workspace GUID hosting the Lakehouse.
        lakehouse_id: Fabric Lakehouse item GUID backing this model.
        compatibility_level: TMDL model compatibility level.

    Returns:
        Mapping of relative file path to file content, ready to pass to
        ``FabricClient.create_semantic_model()``.
    """
    files: dict[str, str] = {
        "definition/database.tmdl": f"database\n\tcompatibilityLevel: {compatibility_level}\n",
    }

    model_lines = [
        "model Model",
        "\tculture: en-US",
        "\tdefaultPowerBIDataSourceVersion: powerBI_V3",
        "\tsourceQueryCulture: en-US",
    ]
    model_lines += [f"\ttable {t['name']}" for t in tables]
    files["definition/model.tmdl"] = "\n".join(model_lines) + "\n"

    onelake_url = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}"
    files["definition/expressions.tmdl"] = (
        "expression DatabaseQuery = let\n"
        f'\t\tSource = AzureStorage.DataLake("{onelake_url}")\n'
        "\tin\n"
        "\t\tSource\n"
        "\tlineageTag: database-query\n"
        "\tqueryGroup: 'Database'\n"
        "\tannotation PBI_IncludeFutureArtifacts = False\n"
    )

    for table in tables:
        files[f"definition/tables/{table['name']}.tmdl"] = generate_table_tmdl(
            table["name"], table.get("columns", [])
        )

    return files
