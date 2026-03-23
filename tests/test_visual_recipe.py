"""Tests for the visual recipe agent SQL generation."""

from src.agents.visual_recipe_agent import (
    RECIPE_GENERATORS,
    _generate_filter_sql,
    _generate_group_sql,
    _generate_join_sql,
    _generate_sort_sql,
    _generate_window_sql,
)


class TestJoinGenerator:
    def test_inner_join(self):
        params = {
            "joinType": "INNER",
            "leftInput": "orders",
            "rightInput": "customers",
            "conditions": [{"leftColumn": "customer_id", "rightColumn": "id"}],
        }
        sql = _generate_join_sql(params)
        assert "INNER JOIN" in sql.upper()
        assert "orders" in sql
        assert "customers" in sql
        assert "customer_id" in sql

    def test_left_join(self):
        params = {
            "joinType": "LEFT",
            "leftInput": "orders",
            "rightInput": "products",
            "conditions": [{"leftColumn": "product_id", "rightColumn": "id"}],
        }
        sql = _generate_join_sql(params)
        assert "LEFT JOIN" in sql.upper()

    def test_multiple_conditions(self):
        params = {
            "joinType": "INNER",
            "leftInput": "t1",
            "rightInput": "t2",
            "conditions": [
                {"leftColumn": "a", "rightColumn": "x"},
                {"leftColumn": "b", "rightColumn": "y"},
            ],
        }
        sql = _generate_join_sql(params)
        assert "AND" in sql.upper()

    def test_no_conditions_uses_fallback(self):
        params = {"joinType": "CROSS", "leftInput": "t1", "rightInput": "t2", "conditions": []}
        sql = _generate_join_sql(params)
        assert "1=1" in sql


class TestGroupGenerator:
    def test_basic_group_by(self):
        params = {
            "input": "orders",
            "groupColumns": ["customer_id"],
            "aggregations": [{"column": "amount", "function": "SUM", "alias": "total"}],
        }
        sql = _generate_group_sql(params)
        assert "GROUP BY" in sql.upper()
        assert "SUM" in sql.upper()
        assert "customer_id" in sql

    def test_multiple_aggregations(self):
        params = {
            "input": "sales",
            "groupColumns": ["region"],
            "aggregations": [
                {"column": "amount", "function": "SUM", "alias": "total"},
                {"column": "id", "function": "COUNT", "alias": "cnt"},
            ],
        }
        sql = _generate_group_sql(params)
        assert "SUM" in sql.upper()
        assert "COUNT" in sql.upper()


class TestFilterGenerator:
    def test_single_condition(self):
        params = {
            "input": "orders",
            "conditions": [{"column": "amount", "operator": ">", "value": "100"}],
        }
        sql = _generate_filter_sql(params)
        assert "WHERE" in sql.upper()
        assert "amount" in sql

    def test_multiple_conditions(self):
        params = {
            "input": "orders",
            "conditions": [
                {"column": "amount", "operator": ">", "value": "100"},
                {"column": "status", "operator": "=", "value": "active"},
            ],
        }
        sql = _generate_filter_sql(params)
        assert "AND" in sql.upper()


class TestWindowGenerator:
    def test_row_number(self):
        params = {
            "input": "orders",
            "function": "ROW_NUMBER",
            "partitionColumns": ["customer_id"],
            "orderColumns": ["order_date"],
            "outputColumn": "rn",
        }
        sql = _generate_window_sql(params)
        assert "ROW_NUMBER" in sql.upper()
        assert "PARTITION BY" in sql.upper()
        assert "ORDER BY" in sql.upper()

    def test_rank_without_partition(self):
        params = {
            "input": "scores",
            "function": "RANK",
            "partitionColumns": [],
            "orderColumns": ["score"],
            "outputColumn": "rank",
        }
        sql = _generate_window_sql(params)
        assert "RANK" in sql.upper()
        assert "PARTITION BY" not in sql.upper()


class TestSortGenerator:
    def test_single_order(self):
        params = {
            "input": "orders",
            "orders": [{"column": "created_at", "direction": "DESC"}],
        }
        sql = _generate_sort_sql(params)
        assert "ORDER BY" in sql.upper()
        assert "DESC" in sql.upper()

    def test_multiple_orders(self):
        params = {
            "input": "t",
            "orders": [
                {"column": "a", "direction": "ASC"},
                {"column": "b", "direction": "DESC"},
            ],
        }
        sql = _generate_sort_sql(params)
        assert "ASC" in sql.upper()
        assert "DESC" in sql.upper()


class TestRecipeGenerators:
    def test_vstack_union(self):
        gen = RECIPE_GENERATORS["vstack"]
        sql = gen({"input1": "t1", "input2": "t2"})
        assert "UNION ALL" in sql.upper()

    def test_distinct(self):
        gen = RECIPE_GENERATORS["distinct"]
        sql = gen({"input": "orders"})
        assert "DISTINCT" in sql.upper()

    def test_topn(self):
        gen = RECIPE_GENERATORS["topn"]
        sql = gen({"input": "orders", "n": 5, "orderColumn": "amount", "direction": "DESC"})
        assert "TOP 5" in sql.upper()
        assert "ORDER BY" in sql.upper()

    def test_all_generators_callable(self):
        for name, gen in RECIPE_GENERATORS.items():
            assert callable(gen), f"Generator '{name}' is not callable"
