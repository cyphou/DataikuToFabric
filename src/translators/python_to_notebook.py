"""Python → Fabric Notebook converter — rewrites Dataiku SDK calls to PySpark."""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

import nbformat

from src.core.logger import get_logger

logger = get_logger(__name__)

# ── Dataiku SDK → PySpark regex replacements ───────────────────────────────

SDK_REPLACEMENTS: list[tuple[str, str, str]] = [
    # (regex_pattern, replacement, description)
    # --- Dataset read/write ---
    (
        r'dataiku\.Dataset\(\s*["\'](\w+)["\']\s*\)\.get_dataframe\(\)',
        r'spark.read.format("delta").load("Tables/\1")',
        "Dataset.get_dataframe → spark.read",
    ),
    (
        r'dataiku\.Dataset\(\s*["\'](\w+)["\']\s*\)\.write_dataframe\((\w+)\)',
        r'\2.write.format("delta").mode("overwrite").save("Tables/\1")',
        "Dataset.write_dataframe → df.write",
    ),
    (
        r'dataiku\.Dataset\(\s*["\'](\w+)["\']\s*\)\.get_dataframe\(\s*columns\s*=\s*\[([^\]]+)\]\s*\)',
        r'spark.read.format("delta").load("Tables/\1").select(\2)',
        "Dataset.get_dataframe(columns=) → spark.read.select",
    ),
    (
        r'dataiku\.Dataset\(\s*["\'](\w+)["\']\s*\)\.iter_dataframes\(chunksize=(\d+)\)',
        r'spark.read.format("delta").load("Tables/\1")  # Was chunked read with chunksize=\2',
        "Dataset.iter_dataframes → spark.read (Spark handles partitioning)",
    ),
    (
        r'dataiku\.Dataset\(\s*["\'](\w+)["\']\s*\)\.get_metadata\(\)',
        r'spark.read.format("delta").load("Tables/\1").schema  # Was .get_metadata()',
        "Dataset.get_metadata → schema",
    ),
    (
        r'dataiku\.Dataset\(\s*["\'](\w+)["\']\s*\)\.read_schema\(\)',
        r'spark.read.format("delta").load("Tables/\1").schema  # Was .read_schema()',
        "Dataset.read_schema → schema",
    ),
    # --- Folders ---
    (
        r'dataiku\.Folder\(\s*["\'](\w+)["\']\s*\)\.get_path\(\)',
        r'"/lakehouse/default/Files/\1"',
        "Folder.get_path → Lakehouse Files path",
    ),
    (
        r'dataiku\.Folder\(\s*["\'](\w+)["\']\s*\)\.list_paths_in_partition\(\)',
        r'mssparkutils.fs.ls("/lakehouse/default/Files/\1")',
        "Folder.list_paths → mssparkutils.fs.ls",
    ),
    (
        r'dataiku\.Folder\(\s*["\'](\w+)["\']\s*\)\.get_download_stream\(\s*["\']([^"\']+)["\']\s*\)',
        r'open("/lakehouse/default/Files/\1/\2", "rb")',
        "Folder.get_download_stream → open",
    ),
    (
        r'dataiku\.Folder\(\s*["\'](\w+)["\']\s*\)\.upload_stream\(\s*["\']([^"\']+)["\']\s*,\s*(\w+)\s*\)',
        r'mssparkutils.fs.put("/lakehouse/default/Files/\1/\2", \3.read())',
        "Folder.upload_stream → mssparkutils.fs.put",
    ),
    # --- Variables & env ---
    (
        r'dataiku\.get_custom_variables\(\)',
        'spark.conf.get("spark.custom_variables", "{}")  # Was dataiku.get_custom_variables()',
        "get_custom_variables → spark.conf",
    ),
    (
        r'dataiku\.default_project_key\(\)',
        '"${PROJECT_KEY}"  # Was dataiku.default_project_key()',
        "default_project_key → env var",
    ),
    # --- SQL executor ---
    (
        r'dataiku\.SQLExecutor2\(\s*["\']?(\w+)["\']?\s*\)',
        r'spark  # Was SQLExecutor2("\1") — use spark.sql() for queries',
        "SQLExecutor2 → spark.sql",
    ),
    (
        r'\.query_to_df\(\s*["\']([^"\']+)["\']\s*\)',
        r'.sql("\1").toPandas()  # Was .query_to_df()',
        "query_to_df → spark.sql",
    ),
    # --- Imports ---
    (
        r'import\s+dataiku\b',
        '# import dataiku  # Removed — using PySpark native APIs',
        "Remove dataiku import",
    ),
    (
        r'from\s+dataiku\s+import\b.*',
        '# Dataiku import removed — using PySpark native APIs',
        "Remove from dataiku import",
    ),
    (
        r'from\s+dataiku\.core\b.*',
        '# Dataiku core import removed',
        "Remove from dataiku.core import",
    ),
]

# ── Pandas → PySpark suggestion patterns ───────────────────────────────────

PANDAS_TO_PYSPARK: list[tuple[str, str, str]] = [
    # (regex_pattern, replacement, description)
    (
        r'(\w+)\.to_csv\(\s*["\']([^"\']+)["\']\s*(?:,\s*index\s*=\s*False)?\s*\)',
        r'\1.write.mode("overwrite").csv("/lakehouse/default/Files/\2")',
        "df.to_csv → df.write.csv",
    ),
    (
        r'pd\.read_csv\(\s*["\']([^"\']+)["\']\s*\)',
        r'spark.read.csv("\1", header=True, inferSchema=True)',
        "pd.read_csv → spark.read.csv",
    ),
    (
        r'pd\.read_parquet\(\s*["\']([^"\']+)["\']\s*\)',
        r'spark.read.parquet("\1")',
        "pd.read_parquet → spark.read.parquet",
    ),
    (
        r'pd\.read_json\(\s*["\']([^"\']+)["\']\s*\)',
        r'spark.read.json("\1")',
        "pd.read_json → spark.read.json",
    ),
    (
        r'(\w+)\.to_parquet\(\s*["\']([^"\']+)["\']\s*\)',
        r'\1.write.mode("overwrite").parquet("\2")',
        "df.to_parquet → df.write.parquet",
    ),
    (
        r'pd\.merge\(\s*(\w+)\s*,\s*(\w+)\s*,\s*on\s*=\s*["\'](\w+)["\']\s*(?:,\s*how\s*=\s*["\'](\w+)["\'])?\s*\)',
        r'\1.join(\2, on="\3", how="\4")',
        "pd.merge → df.join",
    ),
    (
        r'(\w+)\.groupby\(\s*["\'](\w+)["\']\s*\)\.agg\(',
        r'\1.groupBy("\2").agg(',
        "df.groupby → df.groupBy",
    ),
    (
        r'(\w+)\.fillna\(',
        r'\1.fillna(',
        "df.fillna (compatible)",
    ),
    (
        r'(\w+)\.drop_duplicates\(\)',
        r'\1.dropDuplicates()',
        "df.drop_duplicates → df.dropDuplicates",
    ),
    (
        r'(\w+)\.rename\(columns\s*=\s*\{([^}]+)\}\)',
        r'\1  # TODO: rename columns — use .withColumnRenamed() calls',
        "df.rename(columns=) → withColumnRenamed",
    ),
]


@dataclass
class NotebookConversionResult:
    """Result of converting Python code to a Fabric Notebook."""

    notebook_json: str
    review_flags: list[str] = field(default_factory=list)
    replacements_made: int = 0
    errors: list[str] = field(default_factory=list)


# ── AST-based detection ────────────────────────────────────────────────────


def detect_dataiku_sdk_usage(code: str) -> list[str]:
    """Identify all Dataiku SDK calls using AST parsing with regex fallback."""
    found: list[str] = []

    # Try AST-based detection first
    try:
        tree = ast.parse(code)
        for node in ast.walk(tree):
            # Detect: import dataiku / from dataiku import ...
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("dataiku"):
                        found.append(f"import {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                if node.module and node.module.startswith("dataiku"):
                    found.append(f"from {node.module} import ...")
            # Detect: dataiku.Xxx(...) calls
            elif isinstance(node, ast.Attribute):
                if isinstance(node.value, ast.Name) and node.value.id == "dataiku":
                    found.append(f"dataiku.{node.attr}")
    except SyntaxError:
        pass  # Fall back to regex

    # Regex fallback — catches patterns AST might miss (e.g. in strings, comments)
    regex_patterns = [
        (r'dataiku\.Dataset\(', "dataiku.Dataset("),
        (r'dataiku\.Folder\(', "dataiku.Folder("),
        (r'dataiku\.get_custom_variables\(', "dataiku.get_custom_variables("),
        (r'dataiku\.SQLExecutor2\(', "dataiku.SQLExecutor2("),
        (r'dataiku\.Model\(', "dataiku.Model("),
        (r'dataiku\.api_client\(', "dataiku.api_client("),
        (r'dataiku\.customrecipe\.', "dataiku.customrecipe."),
        (r'dataiku\.default_project_key\(', "dataiku.default_project_key("),
    ]
    for pattern, label in regex_patterns:
        if re.search(pattern, code):
            if label not in found and not any(label in f for f in found):
                found.append(label)

    return found


def detect_pandas_usage(code: str) -> list[str]:
    """Identify pandas calls that can be converted to PySpark."""
    found: list[str] = []
    patterns = [
        (r'pd\.read_csv\(', "pd.read_csv"),
        (r'pd\.read_parquet\(', "pd.read_parquet"),
        (r'pd\.read_json\(', "pd.read_json"),
        (r'pd\.merge\(', "pd.merge"),
        (r'\.to_csv\(', "df.to_csv"),
        (r'\.to_parquet\(', "df.to_parquet"),
        (r'\.groupby\(', "df.groupby"),
        (r'\.drop_duplicates\(', "df.drop_duplicates"),
        (r'import\s+pandas', "import pandas"),
    ]
    for pattern, label in patterns:
        if re.search(pattern, code):
            found.append(label)
    return found


def convert_python_to_pyspark(code: str) -> tuple[str, list[str], int]:
    """
    Replace Dataiku SDK calls with PySpark equivalents.

    Returns:
        Tuple of (converted_code, review_flags, replacement_count).
    """
    review_flags: list[str] = []
    result = code
    total_replacements = 0

    # Apply Dataiku SDK replacements
    for pattern, replacement, desc in SDK_REPLACEMENTS:
        new_result, count = re.subn(pattern, replacement, result)
        if count > 0:
            total_replacements += count
            logger.info("sdk_replacement", description=desc, count=count)
        result = new_result

    # Apply pandas → PySpark replacements
    for pattern, replacement, desc in PANDAS_TO_PYSPARK:
        new_result, count = re.subn(pattern, replacement, result)
        if count > 0:
            total_replacements += count
            review_flags.append(f"Pandas→PySpark: {desc}")
            logger.info("pandas_replacement", description=desc, count=count)
        result = new_result

    # Flag complex SDK usage that needs manual review
    if 'dataiku.api_client' in code:
        review_flags.append("Uses dataiku.api_client() — requires manual migration")
    if 'dataiku.customrecipe' in code:
        review_flags.append("Uses dataiku.customrecipe — custom recipe plugin needs manual migration")
    if 'dataiku.Model' in code:
        review_flags.append("Uses dataiku.Model — consider MLflow for Fabric ML models")
    if 'rpy2' in code or 'import rpy2' in code:
        review_flags.append("Contains R code (rpy2) — requires manual conversion")
    if 'sklearn' in code or 'import sklearn' in code:
        review_flags.append("Uses scikit-learn — consider MLlib or keep with pip install")
    if 'tensorflow' in code or 'import tensorflow' in code:
        review_flags.append("Uses TensorFlow — install via pip in Fabric environment")
    if 'torch' in code or 'import torch' in code:
        review_flags.append("Uses PyTorch — install via pip in Fabric environment")

    return result, review_flags, total_replacements


def generate_notebook(
    code: str,
    recipe_name: str,
    project_key: str,
    input_datasets: list[str] | None = None,
    output_datasets: list[str] | None = None,
    review_flags: list[str] | None = None,
) -> str:
    """
    Generate a Fabric Notebook (.ipynb JSON) from converted Python code.

    Args:
        code: The converted PySpark code.
        recipe_name: Original Dataiku recipe name.
        project_key: Dataiku project key.
        input_datasets: List of input dataset names.
        output_datasets: List of output dataset names.
        review_flags: Any review flags from conversion.

    Returns:
        JSON string of the .ipynb notebook.
    """
    nb = nbformat.v4.new_notebook()
    nb.metadata["kernelspec"] = {
        "display_name": "PySpark",
        "language": "python",
        "name": "synapse_pyspark",
    }

    # Cell 1: Header
    flags_text = ", ".join(review_flags) if review_flags else "None"
    header = (
        f"# Migrated from Dataiku\n\n"
        f"**Original recipe:** `{recipe_name}`\n\n"
        f"**Source project:** `{project_key}`\n\n"
        f"**Migration date:** `{datetime.now(timezone.utc).strftime('%Y-%m-%d')}`\n\n"
        f"**Review flags:** {flags_text}"
    )
    nb.cells.append(nbformat.v4.new_markdown_cell(header))

    # Cell 2: Spark setup
    setup = (
        "from pyspark.sql import SparkSession\n"
        "spark = SparkSession.builder.getOrCreate()"
    )
    nb.cells.append(nbformat.v4.new_code_cell(setup))

    # Cell 3: Input datasets
    if input_datasets:
        inputs_code = "# Input: Load datasets\n"
        for ds in input_datasets:
            var_name = re.sub(r'[^a-zA-Z0-9_]', '_', ds)
            inputs_code += f'df_{var_name} = spark.read.format("delta").load("Tables/{ds}")\n'
        nb.cells.append(nbformat.v4.new_code_cell(inputs_code.strip()))

    # Cell 4: Migrated logic
    nb.cells.append(nbformat.v4.new_code_cell(f"# Migrated logic\n{code}"))

    # Cell 5: Output datasets
    if output_datasets:
        outputs_code = "# Output: Write results\n"
        for ds in output_datasets:
            var_name = re.sub(r'[^a-zA-Z0-9_]', '_', ds)
            outputs_code += (
                f'df_{var_name}.write.format("delta")'
                f'.mode("overwrite").save("Tables/{ds}")\n'
            )
        nb.cells.append(nbformat.v4.new_code_cell(outputs_code.strip()))

    return nbformat.writes(nb)
