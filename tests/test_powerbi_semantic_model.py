"""Tests for the Power BI/Fabric DirectLake semantic model (TMDL) generator."""

from __future__ import annotations

from src.translators.powerbi_semantic_model import (
    _map_dax_type,
    generate_semantic_model_tmdl,
    generate_table_tmdl,
)

SAMPLE_COLUMNS = [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"},
    {"name": "amount", "type": "decimal(18,2)"},
    {"name": "created_at", "type": "timestamp"},
    {"name": "active", "type": "boolean"},
]


class TestMapDaxType:
    def test_basic_types(self):
        assert _map_dax_type("int") == "int64"
        assert _map_dax_type("string") == "string"
        assert _map_dax_type("double") == "double"
        assert _map_dax_type("boolean") == "boolean"

    def test_decimal_with_precision_maps_to_decimal(self):
        assert _map_dax_type("decimal(18,2)") == "decimal"

    def test_timestamp_and_date_map_to_datetime(self):
        assert _map_dax_type("timestamp") == "dateTime"
        assert _map_dax_type("date") == "dateTime"

    def test_unknown_type_falls_back_to_string(self):
        assert _map_dax_type("exotic") == "string"

    def test_empty_defaults_to_string(self):
        assert _map_dax_type("") == "string"


class TestGenerateTableTmdl:
    def test_contains_table_header(self):
        tmdl = generate_table_tmdl("orders", SAMPLE_COLUMNS)
        assert "table orders" in tmdl

    def test_contains_all_columns(self):
        tmdl = generate_table_tmdl("orders", SAMPLE_COLUMNS)
        for col in SAMPLE_COLUMNS:
            assert f"column {col['name']}" in tmdl

    def test_direct_lake_partition_present(self):
        tmdl = generate_table_tmdl("orders", SAMPLE_COLUMNS)
        assert "mode: directLake" in tmdl
        assert "entityName: orders" in tmdl
        assert "expressionSource: DatabaseQuery" in tmdl

    def test_column_data_types_mapped(self):
        tmdl = generate_table_tmdl("orders", SAMPLE_COLUMNS)
        assert "dataType: int64" in tmdl
        assert "dataType: decimal" in tmdl
        assert "dataType: dateTime" in tmdl
        assert "dataType: boolean" in tmdl

    def test_empty_columns_still_produces_valid_partition(self):
        tmdl = generate_table_tmdl("empty_table", [])
        assert "table empty_table" in tmdl
        assert "mode: directLake" in tmdl


class TestGenerateSemanticModelTmdl:
    def test_produces_expected_files(self):
        tables = [{"name": "orders", "columns": SAMPLE_COLUMNS}]
        files = generate_semantic_model_tmdl(
            "Sales_Model", tables, workspace_id="ws-1", lakehouse_id="lh-1",
        )
        assert "definition/database.tmdl" in files
        assert "definition/model.tmdl" in files
        assert "definition/expressions.tmdl" in files
        assert "definition/tables/orders.tmdl" in files

    def test_model_tmdl_references_every_table(self):
        tables = [
            {"name": "orders", "columns": SAMPLE_COLUMNS},
            {"name": "customers", "columns": SAMPLE_COLUMNS},
        ]
        files = generate_semantic_model_tmdl(
            "Sales_Model", tables, workspace_id="ws-1", lakehouse_id="lh-1",
        )
        model_tmdl = files["definition/model.tmdl"]
        assert "table orders" in model_tmdl
        assert "table customers" in model_tmdl

    def test_expressions_tmdl_references_onelake_workspace_and_lakehouse(self):
        files = generate_semantic_model_tmdl(
            "Sales_Model", [], workspace_id="ws-123", lakehouse_id="lh-456",
        )
        expr = files["definition/expressions.tmdl"]
        assert "ws-123" in expr
        assert "lh-456" in expr
        assert "onelake.dfs.fabric.microsoft.com" in expr

    def test_one_tmdl_file_per_table(self):
        tables = [{"name": f"t{i}", "columns": SAMPLE_COLUMNS} for i in range(3)]
        files = generate_semantic_model_tmdl(
            "Model", tables, workspace_id="ws-1", lakehouse_id="lh-1",
        )
        table_files = [p for p in files if p.startswith("definition/tables/")]
        assert len(table_files) == 3

    def test_compatibility_level_configurable(self):
        files = generate_semantic_model_tmdl(
            "Model", [], workspace_id="ws-1", lakehouse_id="lh-1",
            compatibility_level=1550,
        )
        assert "compatibilityLevel: 1550" in files["definition/database.tmdl"]

    def test_no_tables_still_produces_valid_shell(self):
        files = generate_semantic_model_tmdl(
            "Empty_Model", [], workspace_id="ws-1", lakehouse_id="lh-1",
        )
        assert "definition/database.tmdl" in files
        assert "definition/model.tmdl" in files
        assert not any(p.startswith("definition/tables/") for p in files)
