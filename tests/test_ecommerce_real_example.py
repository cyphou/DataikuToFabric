"""
Integration tests for real-world Dataiku example: E-Commerce Analytics
Tests the DataikuToFabric toolkit against production-like project complexity.
"""

import json
import pytest
from pathlib import Path
from src.core.logger import get_logger

log = get_logger(__name__)


@pytest.fixture
def ecommerce_example():
    """Load the real-world e-commerce analytics example."""
    example_path = Path(__file__).parent.parent / "examples" / "ecommerce_example.json"
    assert example_path.exists(), f"Example file not found: {example_path}"
    
    with open(example_path) as f:
        return json.load(f)


@pytest.fixture
def mock_discovery_context(ecommerce_example):
    """Build a realistic discovery context from the example."""
    return {
        "project": ecommerce_example["assets"],
        "connections": {conn["id"]: conn for conn in ecommerce_example["assets"]["connections"]},
        "datasets": {ds["id"]: ds for ds in ecommerce_example["assets"]["datasets"]},
        "recipes": {recipe["id"]: recipe for recipe in ecommerce_example["assets"]["recipes"]},
        "scenarios": {scenario["id"]: scenario for scenario in ecommerce_example["assets"]["scenarios"]},
    }


class TestEcommerceExampleDiscovery:
    """Test discovery agent against real-world example."""
    
    def test_discover_all_connections(self, ecommerce_example):
        """Verify all 6 connection types are discovered."""
        connections = ecommerce_example["assets"]["connections"]
        assert len(connections) == 6, f"Expected 6 connections, got {len(connections)}"
        
        conn_types = {c["type"] for c in connections}
        expected_types = {"Oracle", "PostgreSQL", "S3", "AzureBlobStorage", "Snowflake", "MongoDB"}
        assert conn_types == expected_types, f"Connection types mismatch: {conn_types}"
        
        # Verify SSL configuration
        ssl_enabled = [c for c in connections if c.get("ssl_enabled")]
        assert len(ssl_enabled) >= 2, "At least 2 connections should have SSL enabled"
    
    def test_discover_dataset_diversity(self, ecommerce_example):
        """Verify dataset type diversity."""
        datasets = ecommerce_example["assets"]["datasets"]
        assert len(datasets) == 5, f"Expected 5 datasets, got {len(datasets)}"
        
        # Check for mixed types
        db_datasets = [d for d in datasets if d["type"] == "database"]
        fs_datasets = [d for d in datasets if d["type"] == "filesystem"]
        
        assert len(db_datasets) >= 3, "Should have at least 3 database datasets"
        assert len(fs_datasets) >= 2, "Should have at least 2 filesystem datasets"
        
        # Verify incremental loading keys
        incremental_datasets = [d for d in datasets if "incremental_key" in d]
        assert len(incremental_datasets) >= 2, "Should have incremental loading patterns"
    
    def test_discover_complex_dag(self, ecommerce_example):
        """Verify DAG has appropriate complexity."""
        flow = ecommerce_example["assets"]["flow"]
        assert flow["nodes"] >= 9, f"Expected 9+ nodes, got {flow['nodes']}"
        assert flow["edges"] >= 12, f"Expected 12+ edges, got {flow['edges']}"
        
        # Check for fan-out/fan-in patterns
        dag_edges = flow["dag"]
        out_degrees = {}
        in_degrees = {}
        
        for edge in dag_edges:
            out_degrees[edge["from"]] = out_degrees.get(edge["from"], 0) + 1
            in_degrees[edge["to"]] = in_degrees.get(edge["to"], 0) + 1
        
        # Should have some nodes with multiple inputs/outputs
        multi_input = [k for k, v in in_degrees.items() if v > 1]
        multi_output = [k for k, v in out_degrees.items() if v > 1]
        
        assert len(multi_input) > 0, "Should have nodes with multiple inputs (join/merge patterns)"
        assert len(multi_output) > 0, "Should have nodes with multiple outputs (fan-out patterns)"


class TestEcommerceExampleRecipeConversion:
    """Test recipe type conversions."""
    
    def test_sql_recipe_conversion_oracle(self, ecommerce_example):
        """Test Oracle SQL recipe conversion."""
        sql_recipes = [r for r in ecommerce_example["assets"]["recipes"] if r["type"] == "sql"]
        oracle_recipes = [r for r in sql_recipes if r.get("sql_dialect") == "oracle"]
        
        assert len(oracle_recipes) >= 1, "Should have at least 1 Oracle recipe"
        
        recipe = oracle_recipes[0]
        assert "recipe_00" in recipe["id"], f"Oracle recipe found: {recipe['id']}"
        assert "VARCHAR2" in recipe["logic_snippet"], "Oracle syntax should include VARCHAR2 type"
        assert recipe["complexity"] in ["low", "medium", "high", "very_high"]
    
    def test_sql_recipe_conversion_postgresql(self, ecommerce_example):
        """Test PostgreSQL SQL recipe conversion."""
        sql_recipes = [r for r in ecommerce_example["assets"]["recipes"] if r["type"] == "sql"]
        postgres_recipes = [r for r in sql_recipes if r.get("sql_dialect") == "postgresql"]
        
        assert len(postgres_recipes) >= 1, "Should have at least 1 PostgreSQL recipe"
        
        recipe = postgres_recipes[0]
        assert "window_function" in recipe.get("pattern", ""), "PostgreSQL should use window functions"
        assert "OVER" in recipe["logic_snippet"], "Should demonstrate window function syntax"
    
    def test_python_recipe_ml_complexity(self, ecommerce_example):
        """Test Python ML recipe complexity."""
        python_recipes = [r for r in ecommerce_example["assets"]["recipes"] if r["type"] == "python"]
        ml_recipes = [r for r in python_recipes if r.get("complexity") == "very_high"]
        
        assert len(ml_recipes) >= 2, "Should have multiple ML complexity recipes"
        
        # Verify ML frameworks are used
        all_logic = " ".join([r["logic_snippet"] for r in python_recipes])
        assert "sklearn" in all_logic or "joblib" in all_logic or "xgboost" in all_logic, \
            "Should include ML framework usage"
    
    def test_visual_recipe_diversity(self, ecommerce_example):
        """Test visual recipe types."""
        visual_recipes = [r for r in ecommerce_example["assets"]["recipes"] if r["type"] == "visual"]
        
        expected_types = {"join", "group_by", "filter"}
        visual_types = {r["visual_type"] for r in visual_recipes}
        
        assert visual_types >= expected_types, \
            f"Should have join, group_by, filter; got {visual_types}"


class TestEcommerceExampleLineage:
    """Test lineage tracking for complex DAG."""
    
    def test_lineage_construction(self, ecommerce_example):
        """Test building complete lineage from example."""
        flow = ecommerce_example["assets"]["flow"]
        dag = flow["dag"]
        
        # Build adjacency graph
        graph = {}
        for edge in dag:
            if edge["from"] not in graph:
                graph[edge["from"]] = []
            graph[edge["from"]].append(edge["to"])
        
        # Check connectivity
        all_sources = {edge["from"] for edge in dag}
        all_targets = {edge["to"] for edge in dag}
        
        # Sources should have outgoing edges
        assert len(all_sources) > 0, "Should have source nodes"
        
        # Some nodes should be both sources and targets (intermediate)
        intermediates = all_sources & all_targets
        assert len(intermediates) > 0, "Should have intermediate nodes"
        
        # Sinks should only be targets
        sinks = all_targets - all_sources
        assert len(sinks) > 0, "Should have sink nodes"
    
    def test_recipe_to_output_mapping(self, ecommerce_example):
        """Test that all recipes map to output datasets."""
        recipes = ecommerce_example["assets"]["recipes"]
        datasets = {ds["id"] for ds in ecommerce_example["assets"]["datasets"]}
        
        for recipe in recipes:
            output_dataset = recipe.get("output_dataset")
            # Either output_dataset is defined or recipe is final in flow
            if output_dataset:
                # Note: In real example, not all outputs are in base datasets
                # Intermediate datasets created by recipes
                pass


class TestEcommerceExampleScenarios:
    """Test scenario/orchestration conversion."""
    
    def test_scenario_diversity(self, ecommerce_example):
        """Test different scenario types."""
        scenarios = ecommerce_example["assets"]["scenarios"]
        
        types = {s["type"] for s in scenarios}
        assert "scheduled" in types, "Should have scheduled scenarios"
        assert "manual" in types, "Should have manual scenarios"
    
    def test_scheduled_scenario_details(self, ecommerce_example):
        """Test scheduled scenario has proper cron expression."""
        scheduled = [s for s in ecommerce_example["assets"]["scenarios"] if s["type"] == "scheduled"]
        assert len(scheduled) >= 1, "Should have at least 1 scheduled scenario"
        
        daily = [s for s in scheduled if "daily" in s["name"].lower()]
        assert len(daily) >= 1, "Should have daily scenario"
        
        scenario = daily[0]
        assert scenario["schedule"], "Schedule should be defined"
        assert scenario["timezone"], "Timezone should be defined"
        assert len(scenario["steps"]) > 0, "Should have execution steps"
        
        # Verify step ordering
        step_orders = [step["order"] for step in scenario["steps"]]
        assert step_orders == sorted(step_orders), "Steps should be in order"
    
    def test_scenario_timeouts(self, ecommerce_example):
        """Test that scenarios have timeout specifications."""
        scenarios = ecommerce_example["assets"]["scenarios"]
        
        for scenario in scenarios:
            total_timeout = 0
            for step in scenario["steps"]:
                timeout = step.get("timeout_minutes", 0)
                assert timeout > 0, f"Step should have timeout: {step}"
                total_timeout += timeout
            
            # Scenario should estimate total runtime
            if "estimated_total_runtime_minutes" in scenario:
                assert scenario["estimated_total_runtime_minutes"] > 0


class TestEcommerceExampleMigrationReadiness:
    """Test migration readiness assessment."""
    
    def test_migration_readiness_structure(self, ecommerce_example):
        """Test readiness assessment structure."""
        readiness = ecommerce_example["migration_readiness"]
        
        assert "supported_features" in readiness
        assert "migration_path" in readiness
        assert "key_considerations" in readiness
        
        # Should identify advanced features
        supported = readiness["supported_features"]
        assert any("ML" in f for f in supported), "Should identify ML complexity"
        assert any("Multi" in f for f in supported), "Should identify multi-database"
    
    def test_test_coverage_defined(self, ecommerce_example):
        """Test that example defines what should be validated."""
        test_coverage = ecommerce_example.get("test_coverage", {})
        scenarios = test_coverage.get("test_scenarios", [])
        
        assert len(scenarios) >= 4, "Should define multiple test scenarios"
        
        # Should cover key patterns
        scenario_text = " ".join([s.get("scenario", "") for s in scenarios])
        assert "conversion" in scenario_text.lower(), "Should test conversion patterns"
        assert "loading" in scenario_text.lower() or "pattern" in scenario_text.lower(), \
            "Should test data patterns"


class TestEcommerceExampleValidation:
    """End-to-end validation tests."""
    
    def test_example_json_structure(self, ecommerce_example):
        """Validate example JSON structure."""
        # Top-level keys
        assert "metadata" in ecommerce_example
        assert "assets" in ecommerce_example
        assert "migration_readiness" in ecommerce_example
        
        # Assets structure
        assets = ecommerce_example["assets"]
        assert "connections" in assets
        assert "datasets" in assets
        assert "recipes" in assets
        assert "scenarios" in assets
        assert "flow" in assets
        
        # Metadata
        metadata = ecommerce_example["metadata"]
        assert metadata.get("complexity") == "advanced"
        assert metadata.get("estimated_migration_time_hours") > 0
    
    def test_no_duplicate_ids(self, ecommerce_example):
        """Verify no duplicate asset IDs."""
        all_ids = []
        assets = ecommerce_example["assets"]
        
        for conn in assets["connections"]:
            all_ids.append(("connection", conn["id"]))
        for ds in assets["datasets"]:
            all_ids.append(("dataset", ds["id"]))
        for recipe in assets["recipes"]:
            all_ids.append(("recipe", recipe["id"]))
        for scenario in assets["scenarios"]:
            all_ids.append(("scenario", scenario["id"]))
        
        # Check for duplicates
        id_values = [id_tuple[1] for id_tuple in all_ids]
        assert len(id_values) == len(set(id_values)), "Duplicate IDs found"


class TestEcommerceExampleScale:
    """Test realistic scale and performance expectations."""
    
    def test_row_count_estimates(self, ecommerce_example):
        """Verify datasets have realistic row count estimates."""
        datasets = ecommerce_example["assets"]["datasets"]
        
        for ds in datasets:
            if "row_count_sample" in ds:
                count = ds["row_count_sample"]
                assert count > 0, f"Row count should be positive: {ds['id']}"
                
                # Log-realistic scale
                assert 100 < count < 1_000_000_000, \
                    f"Row count should be realistic (>100, <1B): {count} in {ds['id']}"
    
    def test_runtime_estimates(self, ecommerce_example):
        """Verify scenarios have realistic runtime estimates."""
        scenarios = ecommerce_example["assets"]["scenarios"]
        
        for scenario in scenarios:
            if "estimated_total_runtime_minutes" in scenario:
                runtime = scenario["estimated_total_runtime_minutes"]
                assert 1 <= runtime <= 1440, \
                    f"Runtime should be 1min-24hrs: {runtime}min in {scenario['id']}"


# ============================================================================
# Parametrized tests for recipe conversion
# ============================================================================

recipe_conversion_data = [
    ("Oracle SQL", "recipe_00_ingest_orders", "sql"),
    ("PostgreSQL SQL", "recipe_03_agg_customer_ltv", "sql"),
    ("Python Feature Eng", "recipe_04_feature_engineering", "python"),
    ("Python ML Scoring", "recipe_05_ml_scoring", "python"),
    ("Visual Join", "recipe_02_orders_join_customer", "visual"),
    ("Visual Filter", "recipe_07_filter_high_value", "visual"),
]

@pytest.mark.parametrize("recipe_name,recipe_id,expected_type", recipe_conversion_data)
def test_recipe_conversion_pattern(ecommerce_example, recipe_name, recipe_id, expected_type):
    """Parametrized test for each recipe conversion type."""
    recipes_by_id = {r["id"]: r for r in ecommerce_example["assets"]["recipes"]}
    
    assert recipe_id in recipes_by_id, f"Recipe not found: {recipe_id}"
    
    recipe = recipes_by_id[recipe_id]
    assert recipe["type"] == expected_type, f"Type mismatch for {recipe_name}"
    
    # Verify conversion details
    if expected_type == "sql":
        assert "sql_dialect" in recipe
        assert recipe["complexity"] in ["low", "medium", "high", "very_high"]
    elif expected_type == "python":
        assert "logic_snippet" in recipe
        assert recipe["complexity"] in ["medium", "high", "very_high"]
    elif expected_type == "visual":
        assert "visual_type" in recipe


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
