"""Tests for Phase 21 — Self-Healing Engine."""

from __future__ import annotations

import pytest

from src.healers.base_healer import BaseHealer, HealerCategory, HealerRegistry, HealResult
from src.healers.sql_healers import (
    NvlToIsNullHealer,
    SysdateToGetdateHealer,
    DecodeToCaseHealer,
    PgCastHealer,
    IlikeHealer,
    StringConcatHealer,
    RownumToRowNumberHealer,
    Nvl2ToCaseHealer,
    get_sql_healers,
)
from src.healers.notebook_healers import (
    DataikuImportHealer,
    DataikuDatasetReadHealer,
    DataikuDatasetWriteHealer,
    DataikuFolderHealer,
    SparkSessionHealer,
    get_notebook_healers,
)
from src.healers.pipeline_healers import (
    MissingTimeoutHealer,
    MissingRetryHealer,
    DependencyOrderHealer,
    get_pipeline_healers,
)
from src.healers.schema_healers import (
    OracleTypeHealer,
    PgTypeHealer,
    NullableDefaultHealer,
    get_schema_healers,
)
from src.models.asset import Asset, AssetType


_EMPTY: dict = {}


# ── Healer Registry ──────────────────────────────────────────

class TestHealerRegistry:
    def test_register_and_get(self):
        reg = HealerRegistry()
        healers = get_sql_healers()
        for h in healers:
            reg.register(h)
        sql_healers = reg.get_by_category(HealerCategory.SQL)
        assert len(sql_healers) == len(healers)

    def test_get_by_category_empty(self):
        reg = HealerRegistry()
        assert reg.get_by_category(HealerCategory.PIPELINE) == []

    def test_heal_content(self):
        reg = HealerRegistry()
        reg.register(NvlToIsNullHealer())
        fixed, results = reg.heal_content("SELECT NVL(a, b) FROM t", {"name": "test_asset"}, HealerCategory.SQL)
        assert isinstance(results, list)
        assert "ISNULL" in fixed

    def test_heal_all(self):
        reg = HealerRegistry()
        for h in get_sql_healers():
            reg.register(h)
        assets = [
            Asset(
                id="r1", name="compute", type=AssetType.RECIPE_SQL,
                source_project="P",
                metadata={"payload": "SELECT NVL(a, b), SYSDATE FROM t WHERE ROWNUM <= 10"},
            ),
        ]
        results = reg.heal_all(assets)
        assert isinstance(results, list)
        assert len(results) > 0


# ── SQL Healers ───────────────────────────────────────────────

class TestSQLHealers:
    def test_nvl_to_isnull(self):
        h = NvlToIsNullHealer()
        assert h.can_heal("SELECT NVL(a, b) FROM t", _EMPTY)
        result = h.heal("SELECT NVL(a, b) FROM t", {"name": "asset1"})
        assert result.applied
        assert "ISNULL" in result.after

    def test_nvl_no_match(self):
        h = NvlToIsNullHealer()
        assert not h.can_heal("SELECT a FROM t", _EMPTY)

    def test_sysdate_to_getdate(self):
        h = SysdateToGetdateHealer()
        assert h.can_heal("SELECT SYSDATE FROM dual", _EMPTY)
        result = h.heal("SELECT SYSDATE FROM dual", {"name": "asset1"})
        assert result.applied
        assert "GETDATE()" in result.after

    def test_decode_to_case(self):
        h = DecodeToCaseHealer()
        assert h.can_heal("SELECT DECODE(status, 1, 'A', 'B') FROM t", _EMPTY)
        result = h.heal("SELECT DECODE(status, 1, 'A', 'B') FROM t", {"name": "asset1"})
        assert result.applied

    def test_pg_cast(self):
        h = PgCastHealer()
        assert h.can_heal("SELECT x::int FROM t", _EMPTY)
        result = h.heal("SELECT x::int FROM t", {"name": "asset1"})
        assert result.applied
        assert "CAST" in result.after

    def test_ilike(self):
        h = IlikeHealer()
        assert h.can_heal("SELECT * FROM t WHERE name ILIKE '%abc%'", _EMPTY)
        result = h.heal("SELECT * FROM t WHERE name ILIKE '%abc%'", {"name": "asset1"})
        assert result.applied

    def test_string_concat(self):
        h = StringConcatHealer()
        assert h.can_heal("SELECT a || b FROM t", _EMPTY)
        result = h.heal("SELECT a || b FROM t", {"name": "asset1"})
        assert result.applied

    def test_rownum_to_rownumber(self):
        h = RownumToRowNumberHealer()
        assert h.can_heal("SELECT * FROM t WHERE ROWNUM <= 10", _EMPTY)
        result = h.heal("SELECT * FROM t WHERE ROWNUM <= 10", {"name": "asset1"})
        assert result.applied

    def test_nvl2_to_case(self):
        h = Nvl2ToCaseHealer()
        assert h.can_heal("SELECT NVL2(a, b, c) FROM t", _EMPTY)
        result = h.heal("SELECT NVL2(a, b, c) FROM t", {"name": "asset1"})
        assert result.applied

    def test_get_sql_healers_returns_all(self):
        healers = get_sql_healers()
        assert len(healers) == 8
        assert all(isinstance(h, BaseHealer) for h in healers)


# ── Notebook Healers ──────────────────────────────────────────

class TestNotebookHealers:
    def test_dataiku_import_healer(self):
        h = DataikuImportHealer()
        assert h.can_heal("import dataiku", _EMPTY)
        result = h.heal("import dataiku\ndf = something", {"name": "nb1"})
        assert result.applied

    def test_dataset_read_healer(self):
        h = DataikuDatasetReadHealer()
        code = 'df = dataiku.Dataset("orders").get_dataframe()'
        assert h.can_heal(code, _EMPTY)
        result = h.heal(code, {"name": "nb1"})
        assert result.applied
        assert "spark.read" in result.after

    def test_dataset_write_healer(self):
        h = DataikuDatasetWriteHealer()
        code = 'dataiku.Dataset("output").write_dataframe(df)'
        assert h.can_heal(code, _EMPTY)
        result = h.heal(code, {"name": "nb1"})
        assert result.applied
        assert "write" in result.after

    def test_folder_healer(self):
        h = DataikuFolderHealer()
        code = 'path = dataiku.Folder("data").get_path()'
        assert h.can_heal(code, _EMPTY)
        result = h.heal(code, {"name": "nb1"})
        assert result.applied

    def test_spark_session_healer(self):
        h = SparkSessionHealer()
        code = "spark.read.format('delta')"
        assert h.can_heal(code, _EMPTY)
        result = h.heal(code, {"name": "nb1"})
        assert result.applied

    def test_get_notebook_healers_count(self):
        assert len(get_notebook_healers()) == 5


# ── Pipeline Healers ─────────────────────────────────────────

class TestPipelineHealers:
    def test_missing_timeout_healer(self):
        h = MissingTimeoutHealer()
        pipeline = '{"properties": {"activities": [{"name": "run_notebook", "policy": {}}]}}'
        assert h.can_heal(pipeline, _EMPTY)
        result = h.heal(pipeline, {"name": "pipe1"})
        assert result.applied

    def test_missing_retry_healer(self):
        h = MissingRetryHealer()
        pipeline = '{"properties": {"activities": [{"name": "run"}]}}'
        assert h.can_heal(pipeline, _EMPTY)
        result = h.heal(pipeline, {"name": "pipe1"})
        assert result.applied

    def test_dependency_order_healer(self):
        h = DependencyOrderHealer()
        pipeline = '{"properties": {"activities": [{"name": "a", "dependsOn": [{"activity": "nonexistent"}]}, {"name": "b"}]}}'
        assert h.can_heal(pipeline, _EMPTY)

    def test_get_pipeline_healers_count(self):
        assert len(get_pipeline_healers()) == 3


# ── Schema Healers ───────────────────────────────────────────

class TestSchemaHealers:
    def test_oracle_type_healer(self):
        h = OracleTypeHealer()
        ddl = 'CREATE TABLE t (col1 NUMBER NOT NULL)'
        assert h.can_heal(ddl, _EMPTY)
        result = h.heal(ddl, {"name": "ds1"})
        assert result.applied
        assert "DECIMAL" in result.after

    def test_pg_type_healer(self):
        h = PgTypeHealer()
        ddl = 'CREATE TABLE t (col1 SERIAL NOT NULL)'
        assert h.can_heal(ddl, _EMPTY)
        result = h.heal(ddl, {"name": "ds1"})
        assert result.applied

    def test_nullable_default_healer(self):
        h = NullableDefaultHealer()
        ddl = 'CREATE TABLE t (\n  col1 int,\n  col2 varchar\n)'
        assert h.can_heal(ddl, _EMPTY)
        result = h.heal(ddl, {"name": "ds1"})
        assert result.applied

    def test_get_schema_healers_count(self):
        assert len(get_schema_healers()) == 3
