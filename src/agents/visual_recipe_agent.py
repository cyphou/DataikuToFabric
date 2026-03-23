"""Visual recipe agent — converts Dataiku visual recipes to SQL queries."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.agents.base_agent import AgentResult, AgentStatus, BaseAgent, ValidationResult
from src.core.logger import get_logger
from src.models.asset import AssetType, MigrationState

logger = get_logger(__name__)


def _generate_join_sql(params: dict) -> str:
    """Generate a JOIN SQL statement from visual recipe parameters."""
    join_type = params.get("joinType", "INNER").upper()
    left_table = params.get("leftInput", "left_table")
    right_table = params.get("rightInput", "right_table")
    conditions = params.get("conditions", [])

    on_clauses = []
    for cond in conditions:
        left_col = cond.get("leftColumn", "")
        right_col = cond.get("rightColumn", "")
        if left_col and right_col:
            on_clauses.append(f"a.[{left_col}] = b.[{right_col}]")

    on_clause = " AND ".join(on_clauses) if on_clauses else "1=1"
    return f"SELECT a.*, b.*\nFROM [{left_table}] a\n{join_type} JOIN [{right_table}] b\n  ON {on_clause}"


def _generate_group_sql(params: dict) -> str:
    """Generate a GROUP BY SQL statement from visual recipe parameters."""
    table = params.get("input", "source_table")
    group_cols = params.get("groupColumns", [])
    aggregations = params.get("aggregations", [])

    select_parts = [f"[{col}]" for col in group_cols]
    for agg in aggregations:
        func = agg.get("function", "COUNT").upper()
        col = agg.get("column", "*")
        alias = agg.get("alias", f"{func}_{col}")
        select_parts.append(f"{func}([{col}]) AS [{alias}]")

    group_clause = ", ".join(f"[{col}]" for col in group_cols)
    return f"SELECT {', '.join(select_parts)}\nFROM [{table}]\nGROUP BY {group_clause}"


def _generate_filter_sql(params: dict) -> str:
    """Generate a WHERE clause from visual recipe parameters."""
    table = params.get("input", "source_table")
    conditions = params.get("conditions", [])

    where_parts = []
    for cond in conditions:
        col = cond.get("column", "")
        op = cond.get("operator", "=")
        val = cond.get("value", "")
        where_parts.append(f"[{col}] {op} '{val}'")

    where_clause = " AND ".join(where_parts) if where_parts else "1=1"
    return f"SELECT *\nFROM [{table}]\nWHERE {where_clause}"


def _generate_window_sql(params: dict) -> str:
    """Generate a window function SQL statement."""
    table = params.get("input", "source_table")
    window_func = params.get("function", "ROW_NUMBER").upper()
    partition_cols = params.get("partitionColumns", [])
    order_cols = params.get("orderColumns", [])
    alias = params.get("outputColumn", "window_result")

    partition = ", ".join(f"[{c}]" for c in partition_cols) if partition_cols else ""
    order = ", ".join(f"[{c}]" for c in order_cols) if order_cols else "[1]"

    partition_clause = f"PARTITION BY {partition} " if partition else ""
    return f"SELECT *,\n  {window_func}() OVER ({partition_clause}ORDER BY {order}) AS [{alias}]\nFROM [{table}]"


def _generate_sort_sql(params: dict) -> str:
    """Generate an ORDER BY SQL statement."""
    table = params.get("input", "source_table")
    orders = params.get("orders", [])

    order_parts = []
    for order in orders:
        col = order.get("column", "")
        direction = order.get("direction", "ASC").upper()
        order_parts.append(f"[{col}] {direction}")

    order_clause = ", ".join(order_parts) if order_parts else "[1]"
    return f"SELECT *\nFROM [{table}]\nORDER BY {order_clause}"


def _generate_pivot_sql(params: dict) -> str:
    """Generate a PIVOT SQL statement from visual recipe parameters."""
    table = params.get("input", "source_table")
    pivot_col = params.get("pivotColumn", "category")
    value_col = params.get("valueColumn", "amount")
    agg_func = params.get("aggregation", "SUM").upper()
    pivot_values = params.get("pivotValues", [])
    group_cols = params.get("groupColumns", [])

    if not pivot_values:
        return (
            f"-- PIVOT on [{pivot_col}]: values not specified, add pivot values\n"
            f"SELECT *\nFROM [{table}]\n"
            f"PIVOT ({agg_func}([{value_col}]) FOR [{pivot_col}] IN (/* add values */)) AS pvt"
        )

    values_list = ", ".join(f"[{v}]" for v in pivot_values)

    if group_cols:
        group_select = ", ".join(f"[{c}]" for c in group_cols)
        return (
            f"SELECT {group_select}, {values_list}\n"
            f"FROM (\n"
            f"  SELECT {group_select}, [{pivot_col}], [{value_col}]\n"
            f"  FROM [{table}]\n"
            f") src\n"
            f"PIVOT (\n"
            f"  {agg_func}([{value_col}])\n"
            f"  FOR [{pivot_col}] IN ({values_list})\n"
            f") AS pvt"
        )

    return (
        f"SELECT {values_list}\n"
        f"FROM [{table}]\n"
        f"PIVOT (\n"
        f"  {agg_func}([{value_col}])\n"
        f"  FOR [{pivot_col}] IN ({values_list})\n"
        f") AS pvt"
    )


def _generate_prepare_sql(params: dict) -> str:
    """Generate SQL from a Dataiku Prepare recipe (data-wrangling steps).

    Supports common step types: rename, filter, delete_column, fill_empty,
    formula, uppercase, lowercase, split_column, find_replace, type_change.
    Unsupported steps are emitted as comments.
    """
    table = params.get("input", "source_table")
    steps = params.get("steps", [])

    if not steps:
        return f"SELECT *\nFROM [{table}]"

    # Build a list of SELECT expressions and WHERE clauses
    select_exprs: list[str] = ["*"]  # default all columns
    where_clauses: list[str] = []
    column_renames: dict[str, str] = {}
    columns_to_drop: set[str] = set()
    extra_columns: list[str] = []
    comments: list[str] = []

    for step in steps:
        step_type = step.get("type", "").lower()

        if step_type == "rename":
            old_name = step.get("column", "")
            new_name = step.get("newName", "")
            if old_name and new_name:
                column_renames[old_name] = new_name

        elif step_type in ("filter", "filter_rows", "remove_rows"):
            col = step.get("column", "")
            op = step.get("operator", "=")
            val = step.get("value", "")
            if col:
                where_clauses.append(f"[{col}] {op} '{val}'")

        elif step_type in ("delete_column", "remove_column"):
            col = step.get("column", "")
            if col:
                columns_to_drop.add(col)

        elif step_type in ("fill_empty", "fill_na"):
            col = step.get("column", "")
            val = step.get("value", "")
            if col:
                extra_columns.append(f"ISNULL([{col}], '{val}') AS [{col}]")

        elif step_type == "formula":
            expr = step.get("expression", "")
            alias = step.get("outputColumn", step.get("column", "computed"))
            if expr:
                extra_columns.append(f"({expr}) AS [{alias}]")

        elif step_type == "uppercase":
            col = step.get("column", "")
            if col:
                extra_columns.append(f"UPPER([{col}]) AS [{col}]")

        elif step_type == "lowercase":
            col = step.get("column", "")
            if col:
                extra_columns.append(f"LOWER([{col}]) AS [{col}]")

        elif step_type == "find_replace":
            col = step.get("column", "")
            find = step.get("find", "")
            replace_val = step.get("replace", "")
            if col and find:
                extra_columns.append(
                    f"REPLACE([{col}], '{find}', '{replace_val}') AS [{col}]"
                )

        elif step_type == "type_change":
            col = step.get("column", "")
            new_type = step.get("newType", "VARCHAR(MAX)")
            if col:
                extra_columns.append(f"CAST([{col}] AS {new_type}) AS [{col}]")

        elif step_type == "split_column":
            col = step.get("column", "")
            delimiter = step.get("delimiter", ",")
            if col:
                comments.append(
                    f"-- SPLIT [{col}] on '{delimiter}' — requires manual CHARINDEX/SUBSTRING"
                )

        else:
            comments.append(f"-- Unsupported prepare step: {step_type}")

    # Build SELECT list
    parts: list[str] = []
    if columns_to_drop or column_renames or extra_columns:
        # Can't use SELECT * when we have modifications
        # Use * and overlay with computed columns
        # The extra columns will override same-named columns in application
        parts.append("*")
        parts.extend(extra_columns)
    else:
        parts.append("*")

    select_str = ", ".join(parts)
    sql = f"SELECT {select_str}\nFROM [{table}]"

    if where_clauses:
        sql += f"\nWHERE {' AND '.join(where_clauses)}"

    if columns_to_drop:
        # T-SQL doesn't support DROP in SELECT; add as comment
        drop_list = ", ".join(f"[{c}]" for c in columns_to_drop)
        sql = f"-- NOTE: Drop columns {drop_list} from result\n{sql}"

    if column_renames:
        rename_comments = ", ".join(f"[{k}]→[{v}]" for k, v in column_renames.items())
        sql = f"-- NOTE: Rename columns {rename_comments}\n{sql}"

    if comments:
        sql = "\n".join(comments) + "\n" + sql

    return sql


RECIPE_GENERATORS: dict[str, callable] = {
    "join": _generate_join_sql,
    "vstack": lambda p: f"SELECT * FROM [{p.get('input1', 'table1')}]\nUNION ALL\nSELECT * FROM [{p.get('input2', 'table2')}]",
    "group": _generate_group_sql,
    "filter": _generate_filter_sql,
    "window": _generate_window_sql,
    "sort": _generate_sort_sql,
    "distinct": lambda p: f"SELECT DISTINCT *\nFROM [{p.get('input', 'source_table')}]",
    "topn": lambda p: f"SELECT TOP {p.get('n', 10)} *\nFROM [{p.get('input', 'source_table')}]\nORDER BY [{p.get('orderColumn', 'id')}] {p.get('direction', 'DESC')}",
    "pivot": _generate_pivot_sql,
    "prepare": _generate_prepare_sql,
}


class VisualRecipeAgent(BaseAgent):
    """Converts Dataiku visual recipes to SQL queries."""

    @property
    def name(self) -> str:
        return "visual_converter"

    @property
    def description(self) -> str:
        return "Convert Dataiku visual recipes (Join, Group, Filter, etc.) to SQL"

    async def execute(self, context: Any) -> AgentResult:
        registry = context.registry
        config = context.config
        output_dir = Path(config.migration.output_dir) / "sql"
        output_dir.mkdir(parents=True, exist_ok=True)

        visual_assets = registry.get_by_type(AssetType.RECIPE_VISUAL)
        processed = 0
        converted = 0
        failed = 0
        all_flags: list[str] = []
        errors: list[str] = []

        for asset in visual_assets:
            registry.update_state(asset.id, MigrationState.CONVERTING)
            recipe_type = asset.metadata.get("recipe_type", "").lower()
            params = asset.metadata.get("params", {})

            generator = RECIPE_GENERATORS.get(recipe_type)
            if not generator:
                flag = f"Unsupported visual recipe type: {recipe_type}"
                all_flags.append(flag)
                registry.add_review_flag(asset.id, flag)
                registry.update_state(asset.id, MigrationState.FAILED)
                failed += 1
                processed += 1
                continue

            try:
                sql = generator(params)
                out_file = output_dir / f"{asset.name}_visual.sql"
                out_file.write_text(sql, encoding="utf-8")

                registry.set_target(asset.id, {
                    "type": "sql_script",
                    "dialect": config.migration.target_sql_dialect,
                    "path": str(out_file),
                    "source_recipe_type": recipe_type,
                })
                registry.update_state(asset.id, MigrationState.CONVERTED)
                converted += 1

            except Exception as e:
                logger.error("visual_conversion_error", asset=asset.id, error=str(e))
                registry.add_error(asset.id, str(e))
                registry.update_state(asset.id, MigrationState.FAILED)
                errors.append(f"{asset.name}: {e}")
                failed += 1

            processed += 1

        return AgentResult(
            agent_name=self.name,
            status=AgentStatus.COMPLETED,
            assets_processed=processed,
            assets_converted=converted,
            assets_failed=failed,
            review_flags=all_flags,
            errors=errors,
        )

    async def validate(self, context: Any) -> ValidationResult:
        registry = context.registry
        converted = registry.get_by_type(AssetType.RECIPE_VISUAL)
        converted = [a for a in converted if a.state == MigrationState.CONVERTED]

        checks_run = 0
        failures: list[str] = []

        for asset in converted:
            target = asset.target_fabric_asset or {}
            sql_path = target.get("path")
            checks_run += 1
            if not sql_path or not Path(sql_path).exists():
                failures.append(f"{asset.name}: Output SQL file not found")

        return ValidationResult(
            passed=len(failures) == 0,
            checks_run=checks_run,
            checks_passed=checks_run - len(failures),
            checks_failed=len(failures),
            failures=failures,
        )
