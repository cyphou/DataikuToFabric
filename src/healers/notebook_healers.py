"""Notebook healers — auto-fix common Fabric notebook issues."""

from __future__ import annotations

import re
from typing import Any

from src.healers.base_healer import BaseHealer, HealerCategory, HealResult


class DataikuImportHealer(BaseHealer):
    """Remove or replace 'import dataiku' statements."""

    @property
    def name(self) -> str:
        return "remove_dataiku_import"

    @property
    def category(self) -> HealerCategory:
        return HealerCategory.NOTEBOOK

    @property
    def description(self) -> str:
        return "Replace 'import dataiku' with PySpark imports"

    def can_heal(self, content: str, metadata: dict[str, Any]) -> bool:
        return "import dataiku" in content

    def heal(self, content: str, metadata: dict[str, Any]) -> HealResult:
        replacement = "from pyspark.sql import SparkSession\nfrom pyspark.sql import functions as F"
        fixed = re.sub(
            r"^\s*import\s+dataiku\s*$",
            replacement,
            content,
            flags=re.MULTILINE,
        )
        return HealResult(
            healer_name=self.name,
            asset_name=metadata.get("name", "unknown"),
            applied=fixed != content,
            description="Replaced 'import dataiku' with PySpark imports",
            before=content,
            after=fixed,
        )


class DataikuDatasetReadHealer(BaseHealer):
    """Replace dataiku.Dataset(...).get_dataframe() with spark.read."""

    @property
    def name(self) -> str:
        return "dataset_read_to_spark"

    @property
    def category(self) -> HealerCategory:
        return HealerCategory.NOTEBOOK

    @property
    def description(self) -> str:
        return "Replace dataiku.Dataset().get_dataframe() with spark.read.format('delta')"

    def can_heal(self, content: str, metadata: dict[str, Any]) -> bool:
        return "get_dataframe()" in content

    def heal(self, content: str, metadata: dict[str, Any]) -> HealResult:
        pattern = r'dataiku\.Dataset\(\s*["\'](\w+)["\']\s*\)\.get_dataframe\(\)'
        fixed = re.sub(
            pattern,
            r'spark.read.format("delta").load("Tables/\1")',
            content,
        )
        return HealResult(
            healer_name=self.name,
            asset_name=metadata.get("name", "unknown"),
            applied=fixed != content,
            description="Replaced Dataset().get_dataframe() with spark.read",
            before=content,
            after=fixed,
        )


class DataikuDatasetWriteHealer(BaseHealer):
    """Replace dataiku.Dataset(...).write_dataframe(df) with df.write."""

    @property
    def name(self) -> str:
        return "dataset_write_to_spark"

    @property
    def category(self) -> HealerCategory:
        return HealerCategory.NOTEBOOK

    @property
    def description(self) -> str:
        return "Replace dataiku.Dataset().write_dataframe() with df.write"

    def can_heal(self, content: str, metadata: dict[str, Any]) -> bool:
        return "write_dataframe(" in content

    def heal(self, content: str, metadata: dict[str, Any]) -> HealResult:
        pattern = r'dataiku\.Dataset\(\s*["\'](\w+)["\']\s*\)\.write_dataframe\(\s*(\w+)\s*\)'
        fixed = re.sub(
            pattern,
            r'\2.write.format("delta").mode("overwrite").save("Tables/\1")',
            content,
        )
        return HealResult(
            healer_name=self.name,
            asset_name=metadata.get("name", "unknown"),
            applied=fixed != content,
            description="Replaced Dataset().write_dataframe() with df.write",
            before=content,
            after=fixed,
        )


class DataikuFolderHealer(BaseHealer):
    """Replace dataiku.Folder(...).get_path() with OneLake path."""

    @property
    def name(self) -> str:
        return "folder_to_onelake"

    @property
    def category(self) -> HealerCategory:
        return HealerCategory.NOTEBOOK

    @property
    def description(self) -> str:
        return "Replace dataiku.Folder().get_path() with OneLake path"

    def can_heal(self, content: str, metadata: dict[str, Any]) -> bool:
        return "dataiku.Folder" in content

    def heal(self, content: str, metadata: dict[str, Any]) -> HealResult:
        pattern = r'dataiku\.Folder\(\s*["\'](\w+)["\']\s*\)\.get_path\(\)'
        fixed = re.sub(
            pattern,
            r'"/lakehouse/default/Files/\1"',
            content,
        )
        return HealResult(
            healer_name=self.name,
            asset_name=metadata.get("name", "unknown"),
            applied=fixed != content,
            description="Replaced Folder().get_path() with OneLake path",
            before=content,
            after=fixed,
        )


class SparkSessionHealer(BaseHealer):
    """Ensure SparkSession is initialized in notebook."""

    @property
    def name(self) -> str:
        return "ensure_spark_session"

    @property
    def category(self) -> HealerCategory:
        return HealerCategory.NOTEBOOK

    @property
    def description(self) -> str:
        return "Ensure SparkSession initialization is present"

    def can_heal(self, content: str, metadata: dict[str, Any]) -> bool:
        return "spark.read" in content and "SparkSession" not in content

    def heal(self, content: str, metadata: dict[str, Any]) -> HealResult:
        init_code = 'spark = SparkSession.builder.getOrCreate()\n'
        fixed = init_code + content
        return HealResult(
            healer_name=self.name,
            asset_name=metadata.get("name", "unknown"),
            applied=True,
            description="Added SparkSession initialization",
            before=content,
            after=fixed,
        )


def get_notebook_healers() -> list[BaseHealer]:
    """Return all notebook healers."""
    return [
        DataikuImportHealer(),
        DataikuDatasetReadHealer(),
        DataikuDatasetWriteHealer(),
        DataikuFolderHealer(),
        SparkSessionHealer(),
    ]
