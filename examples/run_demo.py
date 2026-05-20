#!/usr/bin/env python
"""
End-to-End Migration Demo — runs all migration steps offline.

No live Dataiku server or Fabric workspace needed.
Uses the bundled example fixtures to exercise every migration path.

Usage:
    py examples/run_demo.py                   # Run everything
    py examples/run_demo.py --step assess     # Assessment only
    py examples/run_demo.py --step sql        # SQL translation only
    py examples/run_demo.py --step lineage    # Lineage graph only
    py examples/run_demo.py --step all        # All steps (default)
"""

from __future__ import annotations

import json
import sys
import shutil
from pathlib import Path

# Ensure repo root is importable
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

EXAMPLES = ROOT / "examples"
INPUT_DIR = EXAMPLES / "input"
OUTPUT_DIR = EXAMPLES / "output" / "demo"


# ── Helpers ───────────────────────────────────────────────────

def _load(path: Path) -> dict | list:
    return json.loads(path.read_text(encoding="utf-8"))


def _save_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _save_json(path: Path, data) -> None:
    _save_text(path, json.dumps(data, indent=2, default=str))


def _banner(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


# ── Step 1: Build registry from example fixtures ─────────────

def build_registry():
    """Load all example input files and register assets."""
    from src.core.registry import AssetRegistry
    from src.models.asset import Asset, AssetType, MigrationState

    _banner("Step 1 — Discovery (build registry from fixtures)")

    registry = AssetRegistry(
        project_key="CUSTOMER_ANALYTICS",
        registry_path=OUTPUT_DIR / "registry.json",
    )

    # Datasets
    datasets = _load(INPUT_DIR / "datasets" / "datasets.json")
    for ds in datasets:
        name = ds["name"]
        conn = ds.get("params", {}).get("connection", "filesystem_managed")
        schema = ds.get("schema", {}).get("columns", [])
        asset = Asset(
            id=f"dataset_{name}",
            name=name,
            type=AssetType.DATASET,
            source_project="CUSTOMER_ANALYTICS",
            state=MigrationState.DISCOVERED,
            metadata={
                "connection": conn,
                "format_type": ds.get("formatType", "csv"),
                "schema": schema,
                "row_count": ds.get("metrics", {}).get("records"),
                "partitioning": ds.get("partitioning"),
            },
        )
        registry.add_asset(asset)
        print(f"  + Dataset: {name} ({len(schema)} cols, {conn})")

    # Recipes
    recipe_dir = INPUT_DIR / "recipes"
    for recipe_file in sorted(recipe_dir.glob("*.json")):
        recipe = _load(recipe_file)
        name = recipe["name"]
        rtype = recipe["type"]

        if rtype == "sql":
            asset_type = AssetType.RECIPE_SQL
        elif rtype == "python":
            asset_type = AssetType.RECIPE_PYTHON
        else:
            asset_type = AssetType.RECIPE_VISUAL

        deps = []
        for item in recipe.get("inputs", {}).get("main", {}).get("items", []):
            ref = item.get("ref", item) if isinstance(item, dict) else item
            deps.append(f"dataset_{ref}")
        asset = Asset(
            id=f"recipe_{name}",
            name=name,
            type=asset_type,
            source_project="CUSTOMER_ANALYTICS",
            state=MigrationState.DISCOVERED,
            metadata=recipe,
            dependencies=deps,
        )
        registry.add_asset(asset)
        print(f"  + Recipe: {name} ({rtype})")

    # Connections
    connections_data = _load(INPUT_DIR / "connections" / "connections.json")
    # Handle both dict-of-connections and list-of-connections formats
    if isinstance(connections_data, dict):
        connections = list(connections_data.values())
    else:
        connections = connections_data
    for conn in connections:
        name = conn["name"]
        asset = Asset(
            id=f"connection_{name}",
            name=name,
            type=AssetType.CONNECTION,
            source_project="CUSTOMER_ANALYTICS",
            state=MigrationState.DISCOVERED,
            metadata=conn,
        )
        registry.add_asset(asset)
        print(f"  + Connection: {name} ({conn['type']})")

    # Flow
    flow = _load(INPUT_DIR / "flow.json")
    asset = Asset(
        id="flow_CUSTOMER_ANALYTICS",
        name="CUSTOMER_ANALYTICS_flow",
        type=AssetType.FLOW,
        source_project="CUSTOMER_ANALYTICS",
        state=MigrationState.DISCOVERED,
        metadata=flow,
    )
    registry.add_asset(asset)
    print(f"  + Flow: {len(flow.get('nodes', []))} nodes, {len(flow.get('edges', []))} edges")

    # Scenarios
    scenarios = _load(INPUT_DIR / "scenarios" / "scenarios.json")
    for scen in scenarios:
        scen_id = scen.get("id", scen["name"])
        name = scen["name"]
        asset = Asset(
            id=f"scenario_{scen_id}",
            name=name,
            type=AssetType.SCENARIO,
            source_project="CUSTOMER_ANALYTICS",
            state=MigrationState.DISCOVERED,
            metadata=scen,
        )
        registry.add_asset(asset)
        print(f"  + Scenario: {name}")

    registry.save()
    stats = registry.get_statistics()
    print(f"\n  Registry: {stats['total']} assets registered")
    return registry


# ── Step 2: Pre-migration assessment ─────────────────────────

def run_assessment(registry):
    """Run project assessment and generate readiness report."""
    _banner("Step 2 — Pre-Migration Assessment")

    from src.analyzers.project_analyzer import assess_project
    from src.analyzers.strategy_advisor import recommend_strategy
    from src.reports.assessment_report import save_assessment_report

    result = assess_project(registry)
    print(f"  Overall Score : {result.overall_score:.1f} / 100")
    print(f"  Grade         : {result.grade.value}")
    print(f"  Total Assets  : {result.total_assets}")
    print(f"  Risks         : {len(result.risks)}")

    for cat in result.category_scores:
        print(f"    {cat.category.value:<20s} {cat.score:5.1f}")

    strategy = recommend_strategy(result, registry)
    print(f"\n  Recommended Strategy : {strategy.strategy.value}")
    print(f"  Estimated Complexity : {strategy.estimated_complexity}")
    print(f"  Confidence           : {strategy.confidence:.0f}%")

    report_path = OUTPUT_DIR / "assessment.html"
    save_assessment_report(result, report_path)
    print(f"  Report saved  : {report_path}")

    _save_json(OUTPUT_DIR / "assessment.json", result.to_dict())
    return result


# ── Step 3: SQL Translation ──────────────────────────────────

def run_sql_translation(registry):
    """Translate SQL recipes from Oracle/PostgreSQL to T-SQL."""
    _banner("Step 3 — SQL Translation (Oracle/PostgreSQL → T-SQL)")

    from src.translators.sql_translator import translate_sql
    from src.translators.oracle_to_tsql import apply_oracle_rules
    from src.translators.postgres_to_tsql import apply_postgres_rules
    from src.models.asset import AssetType, MigrationState

    sql_dir = OUTPUT_DIR / "sql"
    sql_dir.mkdir(parents=True, exist_ok=True)

    for asset in registry.get_by_type(AssetType.RECIPE_SQL):
        meta = asset.metadata
        payload = meta.get("payload", "")
        conn = meta.get("params", {}).get("connection", meta.get("connection", ""))

        pre_flags: list[str] = []
        if "oracle" in conn.lower():
            dialect = "oracle"
            payload, pre_flags = apply_oracle_rules(payload)
        elif "postgres" in conn.lower():
            dialect = "postgres"
            payload, pre_flags = apply_postgres_rules(payload)
        else:
            dialect = "oracle"

        result = translate_sql(payload, source=dialect, target="tsql")
        all_flags = pre_flags + result.review_flags

        out_path = sql_dir / f"{asset.name}.sql"
        header = f"-- Source: {asset.name} ({dialect} → T-SQL)\n"
        header += f"-- Status: {'OK' if result.success else 'ERRORS'}\n"
        if all_flags:
            header += f"-- Review flags: {', '.join(all_flags)}\n"
        header += "\n"

        _save_text(out_path, header + result.translated_sql)

        status = "✓" if result.success else "✗"
        flags = f" [{len(all_flags)} flags]" if all_flags else ""
        print(f"  {status} {asset.name}: {dialect} → T-SQL{flags}")

        registry.update_state(asset.id, MigrationState.CONVERTED)
        registry.set_target(asset.id, {
            "type": "sql_script",
            "path": str(out_path),
            "dialect": "tsql",
            "review_flags": all_flags,
        })

    registry.save()


# ── Step 4: Python → Notebook Migration ──────────────────────

def run_python_migration(registry):
    """Convert Python recipes to Fabric notebooks."""
    _banner("Step 4 — Python Recipe → Fabric Notebook")

    from src.translators.python_to_notebook import (
        convert_python_to_pyspark,
        generate_notebook,
    )
    from src.models.asset import AssetType, MigrationState

    nb_dir = OUTPUT_DIR / "notebooks"
    nb_dir.mkdir(parents=True, exist_ok=True)

    for asset in registry.get_by_type(AssetType.RECIPE_PYTHON):
        meta = asset.metadata
        payload = meta.get("payload", "")

        converted_code, review_flags, replacements = convert_python_to_pyspark(payload)

        inputs = [
            item.get("ref", item) if isinstance(item, dict) else item
            for item in meta.get("inputs", {}).get("main", {}).get("items", [])
        ]
        outputs = [
            item.get("ref", item) if isinstance(item, dict) else item
            for item in meta.get("outputs", {}).get("main", {}).get("items", [])
        ]
        nb_json = generate_notebook(
            converted_code, asset.name, "CUSTOMER_ANALYTICS",
            input_datasets=inputs, output_datasets=outputs,
            review_flags=review_flags,
        )

        out_path = nb_dir / f"{asset.name}.ipynb"
        _save_text(out_path, nb_json)

        status = "✓" if replacements > 0 else "~"
        flags = f" [{len(review_flags)} flags]" if review_flags else ""
        print(f"  {status} {asset.name} → {out_path.name} ({replacements} SDK replacements){flags}")

        registry.update_state(asset.id, MigrationState.CONVERTED)
        registry.set_target(asset.id, {
            "type": "notebook",
            "path": str(out_path),
            "review_flags": review_flags,
            "replacements": replacements,
        })

    registry.save()


# ── Step 5: Visual Recipe → SQL ──────────────────────────────

def run_visual_recipes(registry):
    """Convert visual recipes to SQL."""
    _banner("Step 5 — Visual Recipe → SQL")

    from src.agents.visual_recipe_agent import RECIPE_GENERATORS
    from src.models.asset import AssetType, MigrationState

    sql_dir = OUTPUT_DIR / "sql"
    sql_dir.mkdir(parents=True, exist_ok=True)

    for asset in registry.get_by_type(AssetType.RECIPE_VISUAL):
        meta = asset.metadata
        recipe_type = meta.get("type", "")

        generator = RECIPE_GENERATORS.get(recipe_type)
        if not generator:
            print(f"  ? {asset.name}: no generator for '{recipe_type}'")
            continue

        try:
            sql = generator(meta)
            out_path = sql_dir / f"{asset.name}_visual.sql"
            header = f"-- Source: {asset.name} (visual {recipe_type} → SQL)\n\n"
            _save_text(out_path, header + sql)
            print(f"  ✓ {asset.name} ({recipe_type}) → SQL")

            registry.update_state(asset.id, MigrationState.CONVERTED)
            registry.set_target(asset.id, {
                "type": "sql_script",
                "path": str(out_path),
                "source_type": recipe_type,
            })
        except Exception as e:
            print(f"  ✗ {asset.name}: {e}")
            registry.add_error(asset.id, str(e))

    registry.save()


# ── Step 6: Dataset DDL ──────────────────────────────────────

def run_dataset_ddl(registry):
    """Generate DDL for datasets."""
    _banner("Step 6 — Dataset DDL Generation")

    from src.models.asset import AssetType, MigrationState

    ddl_dir = OUTPUT_DIR / "ddl"
    ddl_dir.mkdir(parents=True, exist_ok=True)

    TYPE_MAP = {
        "string": "NVARCHAR(255)", "varchar": "NVARCHAR(255)",
        "bigint": "BIGINT", "int": "INT", "integer": "INT",
        "double": "FLOAT", "float": "FLOAT",
        "boolean": "BIT", "date": "DATE",
        "timestamp": "DATETIME2", "decimal": "DECIMAL(18,2)",
        "array": "NVARCHAR(MAX)", "map": "NVARCHAR(MAX)",
    }

    for asset in registry.get_by_type(AssetType.DATASET):
        meta = asset.metadata
        schema = meta.get("schema", [])
        if not schema:
            print(f"  ? {asset.name}: no schema, skipping DDL")
            continue

        conn = meta.get("connection", "")
        is_lakehouse = "filesystem" in conn.lower() or "s3" in conn.lower()

        cols = []
        for col in schema:
            col_type = TYPE_MAP.get(col["type"].lower(), "NVARCHAR(MAX)")
            cols.append(f"  [{col['name']}] {col_type} NULL")

        col_defs = ",\n".join(cols)

        if is_lakehouse:
            partition = meta.get("partitioning")
            ddl = f"-- Lakehouse (Delta)\nCREATE TABLE IF NOT EXISTS {asset.name} (\n{col_defs}\n) USING DELTA"
            if partition:
                pcols = partition.get("columns", [])
                if pcols:
                    ddl += f"\nPARTITIONED BY ({', '.join(pcols)})"
            target_type = "lakehouse"
        else:
            ddl = f"-- Warehouse (T-SQL)\nCREATE TABLE [{asset.name}] (\n{col_defs}\n)"
            target_type = "warehouse"

        out_path = ddl_dir / f"{asset.name}.sql"
        _save_text(out_path, ddl + "\n")
        print(f"  ✓ {asset.name} → {target_type} DDL ({len(schema)} columns)")

        registry.update_state(asset.id, MigrationState.CONVERTED)
        registry.set_target(asset.id, {
            "type": target_type,
            "ddl_path": str(out_path),
            "schema": schema,
            "row_count": meta.get("row_count"),
        })

    registry.save()


# ── Step 7: Connection Mapping ────────────────────────────────

def run_connection_mapping(registry):
    """Map connections to Fabric equivalents."""
    _banner("Step 7 — Connection Mapping")

    from src.models.asset import AssetType, MigrationState

    conn_dir = OUTPUT_DIR / "connections"
    conn_dir.mkdir(parents=True, exist_ok=True)

    CONN_MAP = {
        "Oracle": ("OnPremisesDataGateway", "Install & configure on-premises data gateway"),
        "PostgreSQL": ("OnPremisesDataGateway", "Install & configure on-premises data gateway"),
        "EC2": ("OneLake_Shortcut", "Create S3 shortcut in OneLake"),
        "Azure": ("OneLake_Shortcut", "Create Azure shortcut in OneLake"),
        "HDFS": ("OneLake_FileCopy", "Export data and upload to OneLake Files"),
        "Filesystem": ("Lakehouse_Files", "Upload to Lakehouse Files section"),
    }

    for asset in registry.get_by_type(AssetType.CONNECTION):
        meta = asset.metadata
        conn_type = meta.get("type", "Unknown")
        fabric_type, action = CONN_MAP.get(conn_type, ("Manual", "Manual configuration required"))

        mapping = {
            "source_connection": asset.name,
            "source_type": conn_type,
            "fabric_type": fabric_type,
            "action_required": action,
            "original_config": {
                k: v for k, v in meta.get("params", {}).items()
                if k not in ("password", "secretKey", "clientSecret")
            },
        }

        out_path = conn_dir / f"{asset.name}.json"
        _save_json(out_path, mapping)
        print(f"  ✓ {asset.name}: {conn_type} → {fabric_type}")

        registry.update_state(asset.id, MigrationState.CONVERTED)
        registry.set_target(asset.id, {
            "type": fabric_type,
            "mapping_path": str(out_path),
        })

    registry.save()


# ── Step 8: QA Suite ──────────────────────────────────────────

def run_qa(registry):
    """Run governance, fidelity, and cross-validation checks."""
    _banner("Step 8 — QA Suite")

    from src.qa.qa_suite import run_qa_suite
    from src.qa.comparison_report import save_qa_report

    result = run_qa_suite(registry)
    print(f"  Overall Passed : {result.overall_passed}")
    print(f"  Total Findings : {result.total_findings}")
    if result.fidelity:
        print(f"  Fidelity Score : {result.fidelity.overall_score:.1f}%")

    for stage in result.stage_results:
        print(f"    {stage.stage:<15s} {stage.status}")

    report_path = OUTPUT_DIR / "qa_report.html"
    save_qa_report(result, report_path)
    print(f"  Report saved   : {report_path}")

    _save_json(OUTPUT_DIR / "qa_result.json", result.to_dict())


# ── Step 9: Self-Healing ──────────────────────────────────────

def run_self_healing(registry):
    """Run self-healing on converted assets."""
    _banner("Step 9 — Self-Healing Engine")

    from src.healers.base_healer import HealerRegistry as HR
    from src.healers.sql_healers import get_sql_healers
    from src.healers.notebook_healers import get_notebook_healers
    from src.healers.schema_healers import get_schema_healers
    from src.models.asset import AssetType

    healer_reg = HR()
    for h in get_sql_healers():
        healer_reg.register(h)
    for h in get_notebook_healers():
        healer_reg.register(h)
    for h in get_schema_healers():
        healer_reg.register(h)

    print(f"  Registered {len(healer_reg.healers)} healers")

    fixes = healer_reg.heal_all(registry.get_all())
    if fixes:
        print(f"  Applied {len(fixes)} fixes:")
        for f in fixes:
            print(f"    - {f}")
    else:
        print("  No issues detected — all assets clean")


# ── Step 10: Lineage ─────────────────────────────────────────

def run_lineage(registry):
    """Build lineage graph and generate visualization."""
    _banner("Step 10 — Lineage Map & Impact Analysis")

    from src.lineage.lineage_builder import build_lineage
    from src.reports.lineage_report import save_lineage_report

    graph = build_lineage(registry)
    d = graph.to_dict()
    print(f"  Nodes   : {len(d['nodes'])}")
    print(f"  Edges   : {len(d['edges'])}")

    # Mermaid diagram
    mermaid = graph.to_mermaid()
    mermaid_path = OUTPUT_DIR / "lineage.mmd"
    _save_text(mermaid_path, mermaid)
    print(f"  Mermaid : {mermaid_path}")

    # HTML report
    report_path = OUTPUT_DIR / "lineage.html"
    save_lineage_report(graph, report_path)
    print(f"  Report  : {report_path}")

    # Impact analysis for each dataset
    for node in graph.nodes.values():
        if node.node_type.value == "dataset":
            impact = graph.impact_analysis(node.node_id)
            if impact["total_impacted"] > 0:
                print(f"    {node.name}: impacts {impact['total_impacted']} downstream assets")

    _save_json(OUTPUT_DIR / "lineage.json", d)


# ── Step 11: Drift Detection ────────────────────────────────

def run_drift(registry):
    """Take a baseline snapshot, mutate, detect drift."""
    _banner("Step 11 — Schema Drift Detection")

    from src.drift.snapshot import take_snapshot
    from src.drift.drift_detector import detect_drift
    from src.reports.drift_report import save_drift_report
    from src.models.asset import MigrationState

    baseline = take_snapshot(registry)
    baseline.save(OUTPUT_DIR / "baseline_snapshot.json")
    print(f"  Baseline snapshot: {len(baseline.assets)} assets")

    # Simulate a drift: change one asset's state
    all_assets = registry.get_all()
    if all_assets:
        first = all_assets[0]
        original_state = first.state
        registry.update_state(first.id, MigrationState.VALIDATED)

    current = take_snapshot(registry)
    report = detect_drift(baseline, current)

    print(f"  Drift items     : {report.drift_count}")
    print(f"  Breaking changes: {report.has_breaking_changes}")

    for item in report.drifts:
        print(f"    [{item.severity.value}] {item.drift_type.value}: {item.description}")

    if report.drift_count > 0:
        report_path = OUTPUT_DIR / "drift_report.html"
        save_drift_report(report, report_path)
        print(f"  Report saved    : {report_path}")

    # Restore original state
    if all_assets:
        registry.update_state(first.id, original_state)

    _save_json(OUTPUT_DIR / "drift_report.json", report.to_dict())


# ── Step 12: Equivalence Testing ─────────────────────────────

def run_equivalence(registry):
    """Run equivalence checks on converted datasets."""
    _banner("Step 12 — Equivalence Testing")

    from src.testing.equivalence_tester import run_equivalence_suite

    assets = registry.get_all()
    suite = run_equivalence_suite(assets)

    print(f"  Total tested    : {suite.total_tested}")
    print(f"  Equivalent      : {suite.total_equivalent}")
    print(f"  Different       : {suite.total_different}")
    print(f"  Untestable      : {suite.total_untestable}")

    _save_json(OUTPUT_DIR / "equivalence.json", suite.to_dict())


# ── Step 13: Pipeline Generation (summary) ───────────────────

def run_pipeline_summary(registry):
    """Summarize flow and scenario assets for pipeline generation."""
    _banner("Step 13 — Flow → Pipeline Summary")

    from src.models.asset import AssetType

    flows = registry.get_by_type(AssetType.FLOW)
    scenarios = registry.get_by_type(AssetType.SCENARIO)

    for flow in flows:
        nodes = flow.metadata.get("nodes", [])
        edges = flow.metadata.get("edges", [])
        print(f"  Flow: {flow.name} ({len(nodes)} nodes, {len(edges)} edges)")

    for scen in scenarios:
        triggers = scen.metadata.get("triggers", [])
        steps = scen.metadata.get("steps", [])
        print(f"  Scenario: {scen.name} ({len(triggers)} triggers, {len(steps)} steps)")


# ── Main ──────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Dataiku → Fabric Migration Demo")
    parser.add_argument("--step", default="all",
                        choices=["all", "discover", "assess", "sql", "python",
                                 "visual", "ddl", "connections", "qa", "heal",
                                 "lineage", "drift", "equivalence", "pipeline"],
                        help="Run a specific step or 'all'")
    parser.add_argument("--clean", action="store_true",
                        help="Remove output/demo/ before running")
    args = parser.parse_args()

    if args.clean and OUTPUT_DIR.exists():
        try:
            shutil.rmtree(OUTPUT_DIR)
            print(f"Cleaned {OUTPUT_DIR}")
        except PermissionError:
            # OneDrive or antivirus may lock files; delete contents instead
            for child in OUTPUT_DIR.rglob("*"):
                if child.is_file():
                    child.unlink(missing_ok=True)
            print(f"Cleaned files in {OUTPUT_DIR}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("  Dataiku → Fabric Migration Demo")
    print("  Project: CUSTOMER_ANALYTICS")
    print(f"  Output:  {OUTPUT_DIR}")
    print("=" * 60)

    # Always build the registry first
    registry = build_registry()

    step = args.step
    steps = {
        "assess": run_assessment,
        "sql": run_sql_translation,
        "python": run_python_migration,
        "visual": run_visual_recipes,
        "ddl": run_dataset_ddl,
        "connections": run_connection_mapping,
        "qa": run_qa,
        "heal": run_self_healing,
        "lineage": run_lineage,
        "drift": run_drift,
        "equivalence": run_equivalence,
        "pipeline": run_pipeline_summary,
    }

    if step == "all" or step == "discover":
        if step == "discover":
            return  # Registry already built

    if step == "all":
        for name, fn in steps.items():
            try:
                fn(registry)
            except Exception as e:
                print(f"\n  ⚠ Step '{name}' failed: {e}")
    elif step in steps:
        steps[step](registry)
    else:
        print(f"Unknown step: {step}")
        sys.exit(1)

    # Final summary
    _banner("Migration Demo Complete")
    stats = registry.get_statistics()
    print(f"  Total assets : {stats['total']}")
    print(f"  By state     :")
    for state, count in stats.get("by_state", {}).items():
        if count > 0:
            print(f"    {state:<20s} {count}")
    print(f"\n  Output dir   : {OUTPUT_DIR}")

    # List generated files
    files = sorted(OUTPUT_DIR.rglob("*"))
    files = [f for f in files if f.is_file()]
    print(f"  Files generated: {len(files)}")
    for f in files:
        size = f.stat().st_size
        rel = f.relative_to(OUTPUT_DIR)
        print(f"    {rel} ({size:,} bytes)")


if __name__ == "__main__":
    main()
