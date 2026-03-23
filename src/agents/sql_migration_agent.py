"""SQL migration agent — converts Dataiku SQL recipes to T-SQL or Spark SQL."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.agents.base_agent import AgentResult, AgentStatus, BaseAgent, ValidationResult
from src.core.logger import get_logger
from src.models.asset import AssetType, MigrationState
from src.translators.oracle_to_tsql import apply_oracle_rules
from src.translators.postgres_to_tsql import apply_postgres_rules
from src.translators.sql_translator import translate_multi_statement, translate_sql, validate_sql

logger = get_logger(__name__)

# Maps Dataiku SQL dialect identifiers to sqlglot dialect names.
DIALECT_MAP: dict[str, str] = {
    "oracle": "oracle",
    "postgresql": "postgres",
    "postgres": "postgres",
    "mysql": "mysql",
    "hive": "hive",
    "impala": "hive",
    "sqlserver": "tsql",
    "mssql": "tsql",
}


def _detect_dialect(asset_metadata: dict) -> str:
    """Detect source SQL dialect from recipe metadata or connection info.

    Priority:
    1. Explicit ``dialect`` key in metadata
    2. ``connection`` key matched against known connection types
    3. ``recipe_type`` key (legacy)
    """
    # Explicit dialect
    dialect = asset_metadata.get("dialect", "").lower()
    if dialect and dialect in DIALECT_MAP:
        return dialect

    # Infer from connection name / type
    conn = asset_metadata.get("connection", "").lower()
    for keyword, mapped in [
        ("oracle", "oracle"),
        ("postgres", "postgres"),
        ("postgresql", "postgres"),
        ("mysql", "mysql"),
        ("hive", "hive"),
        ("mssql", "tsql"),
        ("sqlserver", "tsql"),
    ]:
        if keyword in conn:
            return mapped

    # Fallback to recipe_type
    return asset_metadata.get("recipe_type", "").lower()


class SQLMigrationAgent(BaseAgent):
    """Converts SQL recipes from Oracle/PostgreSQL to T-SQL or Spark SQL."""

    @property
    def name(self) -> str:
        return "sql_converter"

    @property
    def description(self) -> str:
        return "Convert Dataiku SQL recipes to Fabric-compatible T-SQL or Spark SQL"

    async def execute(self, context: Any) -> AgentResult:
        """Convert all SQL recipe assets in the registry."""
        registry = context.registry
        config = context.config
        target_dialect = config.migration.target_sql_dialect
        output_dir = Path(config.migration.output_dir) / "sql"
        output_dir.mkdir(parents=True, exist_ok=True)

        sql_assets = registry.get_by_type(AssetType.RECIPE_SQL)
        processed = 0
        converted = 0
        failed = 0
        all_flags: list[str] = []
        errors: list[str] = []

        for asset in sql_assets:
            registry.update_state(asset.id, MigrationState.CONVERTING)

            # Extract SQL code (try multiple keys)
            source_sql = (
                asset.metadata.get("payload")
                or asset.metadata.get("code")
                or asset.metadata.get("sql")
                or ""
            ).strip()

            if not source_sql:
                registry.add_error(asset.id, "No SQL code found in recipe metadata")
                registry.update_state(asset.id, MigrationState.FAILED)
                failed += 1
                processed += 1
                continue

            # Detect dialect
            raw_dialect = _detect_dialect(asset.metadata)
            sqlglot_dialect = DIALECT_MAP.get(raw_dialect, raw_dialect)

            try:
                # Step 1: sqlglot transpilation (auto multi-statement)
                if ";" in source_sql and source_sql.count(";") > 1:
                    result = translate_multi_statement(
                        source_sql, source=sqlglot_dialect, target=target_dialect
                    )
                else:
                    result = translate_sql(
                        source_sql, source=sqlglot_dialect, target=target_dialect
                    )
                translated = result.translated_sql
                review_flags = list(result.review_flags)

                # Step 2: Apply dialect-specific custom rules
                if raw_dialect in ("oracle",):
                    translated, extra_flags = apply_oracle_rules(translated)
                    review_flags.extend(extra_flags)
                elif raw_dialect in ("postgresql", "postgres"):
                    translated, extra_flags = apply_postgres_rules(translated)
                    review_flags.extend(extra_flags)

                # Step 3: Write output
                out_file = output_dir / f"{asset.name}.sql"
                out_file.write_text(translated, encoding="utf-8")

                # Step 4: Update registry
                registry.set_target(asset.id, {
                    "type": "sql_script",
                    "dialect": target_dialect,
                    "path": str(out_file),
                    "source_dialect": raw_dialect,
                })
                for flag in review_flags:
                    registry.add_review_flag(asset.id, flag)

                if result.success:
                    registry.update_state(asset.id, MigrationState.CONVERTED)
                    converted += 1
                else:
                    registry.update_state(asset.id, MigrationState.FAILED)
                    for err in result.errors:
                        registry.add_error(asset.id, err)
                    failed += 1

                all_flags.extend(review_flags)

            except Exception as e:
                logger.error("sql_conversion_error", asset=asset.id, error=str(e))
                registry.add_error(asset.id, str(e))
                registry.update_state(asset.id, MigrationState.FAILED)
                errors.append(f"{asset.name}: {e}")
                failed += 1

            processed += 1

        return AgentResult(
            agent_name=self.name,
            status=AgentStatus.COMPLETED if failed == 0 else AgentStatus.COMPLETED,
            assets_processed=processed,
            assets_converted=converted,
            assets_failed=failed,
            review_flags=all_flags,
            errors=errors,
        )

    async def validate(self, context: Any) -> ValidationResult:
        """Validate all converted SQL scripts parse cleanly."""
        registry = context.registry
        target_dialect = context.config.migration.target_sql_dialect

        converted = registry.get_by_type(AssetType.RECIPE_SQL)
        converted = [a for a in converted if a.state == MigrationState.CONVERTED]

        checks_run = 0
        failures: list[str] = []

        for asset in converted:
            target = asset.target_fabric_asset or {}
            sql_path = target.get("path")
            if not sql_path or not Path(sql_path).exists():
                failures.append(f"{asset.name}: Output SQL file not found")
                checks_run += 1
                continue

            sql_content = Path(sql_path).read_text(encoding="utf-8")
            errors = validate_sql(sql_content, dialect=target_dialect)
            checks_run += 1
            if errors:
                failures.append(f"{asset.name}: {'; '.join(errors)}")

        return ValidationResult(
            passed=len(failures) == 0,
            checks_run=checks_run,
            checks_passed=checks_run - len(failures),
            checks_failed=len(failures),
            failures=failures,
        )
