"""Python migration agent — converts Dataiku Python recipes to Fabric Notebooks."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.agents.base_agent import AgentResult, AgentStatus, BaseAgent, ValidationResult
from src.core.logger import get_logger
from src.models.asset import AssetType, MigrationState
from src.translators.python_to_notebook import convert_python_to_pyspark, generate_notebook

logger = get_logger(__name__)


class PythonMigrationAgent(BaseAgent):
    """Converts Python recipes to Fabric Notebooks (.ipynb)."""

    @property
    def name(self) -> str:
        return "python_converter"

    @property
    def description(self) -> str:
        return "Convert Dataiku Python recipes to Fabric PySpark Notebooks"

    async def execute(self, context: Any) -> AgentResult:
        """Convert all Python recipe assets to notebooks."""
        registry = context.registry
        config = context.config
        output_dir = Path(config.migration.output_dir) / "notebooks"
        output_dir.mkdir(parents=True, exist_ok=True)

        python_assets = registry.get_by_type(AssetType.RECIPE_PYTHON)
        processed = 0
        converted = 0
        failed = 0
        all_flags: list[str] = []
        errors: list[str] = []

        for asset in python_assets:
            registry.update_state(asset.id, MigrationState.CONVERTING)
            source_code = asset.metadata.get("payload", asset.metadata.get("code", ""))

            if not source_code:
                registry.add_error(asset.id, "No Python code found in recipe metadata")
                registry.update_state(asset.id, MigrationState.FAILED)
                failed += 1
                processed += 1
                continue

            try:
                # Convert SDK calls
                converted_code, review_flags, replacement_count = convert_python_to_pyspark(source_code)
                all_flags.extend(review_flags)

                # Extract input/output dataset references from metadata
                inputs = asset.metadata.get("inputs", [])
                outputs = asset.metadata.get("outputs", [])

                # Generate notebook
                notebook_json = generate_notebook(
                    code=converted_code,
                    recipe_name=asset.name,
                    project_key=config.dataiku.project_key,
                    input_datasets=inputs,
                    output_datasets=outputs,
                    review_flags=review_flags,
                )

                # Write output
                out_file = output_dir / f"{asset.name}.ipynb"
                out_file.write_text(notebook_json, encoding="utf-8")

                # Update registry
                registry.set_target(asset.id, {
                    "type": "notebook",
                    "path": str(out_file),
                    "replacements_made": replacement_count,
                })
                for flag in review_flags:
                    registry.add_review_flag(asset.id, flag)

                registry.update_state(asset.id, MigrationState.CONVERTED)
                converted += 1

            except Exception as e:
                logger.error("python_conversion_error", asset=asset.id, error=str(e))
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
        """Validate generated notebooks are valid .ipynb format."""
        import nbformat as nbf

        registry = context.registry
        converted = registry.get_by_type(AssetType.RECIPE_PYTHON)
        converted = [a for a in converted if a.state == MigrationState.CONVERTED]

        checks_run = 0
        failures: list[str] = []

        for asset in converted:
            target = asset.target_fabric_asset or {}
            nb_path = target.get("path")
            if not nb_path or not Path(nb_path).exists():
                failures.append(f"{asset.name}: Output notebook not found")
                checks_run += 1
                continue

            try:
                nb_content = Path(nb_path).read_text(encoding="utf-8")
                nbf.reads(nb_content, as_version=4)
                checks_run += 1
            except Exception as e:
                failures.append(f"{asset.name}: Invalid notebook format — {e}")
                checks_run += 1

        return ValidationResult(
            passed=len(failures) == 0,
            checks_run=checks_run,
            checks_passed=checks_run - len(failures),
            checks_failed=len(failures),
            failures=failures,
        )
