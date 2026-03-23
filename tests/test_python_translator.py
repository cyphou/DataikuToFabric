"""Tests for the Python-to-notebook translator."""

import json

from src.translators.python_to_notebook import (
    convert_python_to_pyspark,
    detect_dataiku_sdk_usage,
    generate_notebook,
)


class TestDetectDataikuSDK:
    def test_detects_dataset_call(self):
        code = 'import dataiku\ndf = dataiku.Dataset("x").get_dataframe()'
        found = detect_dataiku_sdk_usage(code)
        assert len(found) > 0
        assert any("Dataset" in f for f in found)

    def test_detects_folder_call(self):
        code = 'path = dataiku.Folder("x").get_path()'
        found = detect_dataiku_sdk_usage(code)
        assert len(found) > 0

    def test_no_dataiku_returns_empty(self):
        code = "import pandas as pd\ndf = pd.DataFrame()"
        found = detect_dataiku_sdk_usage(code)
        assert found == []

    def test_detects_api_client(self):
        code = "client = dataiku.api_client()"
        found = detect_dataiku_sdk_usage(code)
        assert len(found) > 0

    def test_detects_custom_recipe(self):
        code = "params = dataiku.customrecipe.get_recipe_config()"
        found = detect_dataiku_sdk_usage(code)
        assert len(found) > 0


class TestConvertPythonToPySpark:
    def test_replaces_dataset_read(self):
        code = 'df = dataiku.Dataset("orders").get_dataframe()'
        result, flags, count = convert_python_to_pyspark(code)
        assert "spark.read" in result
        assert "delta" in result
        assert count >= 1

    def test_replaces_dataset_write(self):
        code = 'dataiku.Dataset("orders").write_dataframe(df)'
        result, flags, count = convert_python_to_pyspark(code)
        assert "write" in result
        assert "delta" in result
        assert count >= 1

    def test_replaces_folder_path(self):
        code = 'path = dataiku.Folder("uploads").get_path()'
        result, flags, count = convert_python_to_pyspark(code)
        assert "/lakehouse/default/Files/" in result
        assert count >= 1

    def test_removes_import_dataiku(self):
        code = "import dataiku\nprint('hello')"
        result, _, count = convert_python_to_pyspark(code)
        assert "import dataiku" not in result or "# import dataiku" in result
        assert count >= 1

    def test_flags_api_client(self):
        code = "client = dataiku.api_client()\nclient.list_projects()"
        _, flags, _ = convert_python_to_pyspark(code)
        assert any("api_client" in f for f in flags)

    def test_flags_rpy2(self):
        code = "import rpy2\nfrom rpy2 import robjects"
        _, flags, _ = convert_python_to_pyspark(code)
        assert any("R code" in f or "rpy2" in f for f in flags)

    def test_no_replacements_on_clean_code(self):
        code = "import numpy as np\nx = np.array([1, 2, 3])"
        result, flags, count = convert_python_to_pyspark(code)
        assert count == 0
        assert result == code


class TestGenerateNotebook:
    def test_generates_valid_notebook_json(self):
        code = "df = spark.read.format('delta').load('Tables/x')"
        nb_json = generate_notebook(code, "test_recipe", "TEST_PROJECT")
        nb = json.loads(nb_json)
        assert nb["nbformat"] == 4
        assert len(nb["cells"]) >= 2  # header + setup + logic at minimum

    def test_notebook_has_pyspark_kernel(self):
        code = "print('hello')"
        nb_json = generate_notebook(code, "recipe1", "PROJ")
        nb = json.loads(nb_json)
        assert nb["metadata"]["kernelspec"]["name"] == "synapse_pyspark"

    def test_notebook_includes_input_datasets(self):
        code = "# logic"
        nb_json = generate_notebook(code, "r1", "P", input_datasets=["orders", "customers"])
        nb_content = json.loads(nb_json)
        code_cells = [c for c in nb_content["cells"] if c["cell_type"] == "code"]
        all_code = "\n".join("".join(c["source"]) for c in code_cells)
        assert "orders" in all_code
        assert "customers" in all_code

    def test_notebook_includes_output_datasets(self):
        code = "# logic"
        nb_json = generate_notebook(code, "r1", "P", output_datasets=["result"])
        nb_content = json.loads(nb_json)
        code_cells = [c for c in nb_content["cells"] if c["cell_type"] == "code"]
        all_code = "\n".join("".join(c["source"]) for c in code_cells)
        assert "result" in all_code

    def test_notebook_includes_review_flags(self):
        code = "# logic"
        nb_json = generate_notebook(code, "r1", "P", review_flags=["Need manual review"])
        nb_content = json.loads(nb_json)
        md_cells = [c for c in nb_content["cells"] if c["cell_type"] == "markdown"]
        all_md = "\n".join("".join(c["source"]) for c in md_cells)
        assert "manual review" in all_md.lower()
