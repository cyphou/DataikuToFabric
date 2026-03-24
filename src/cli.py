"""CLI entry point for the Dataiku → Fabric Migration Toolkit."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import click

from src.core.config import load_config, validate_config
from src.core.logger import setup_logging
from src.core.orchestrator import Orchestrator
from src.core.registry import AssetRegistry

from src.connectors.dataiku_client import DataikuClient
from src.connectors.fabric_client import FabricClient, _acquire_token

# -- Agent imports --
from src.agents.connection_agent import ConnectionMapperAgent
from src.agents.dataset_agent import DatasetMigrationAgent
from src.agents.discovery_agent import DiscoveryAgent
from src.agents.flow_pipeline_agent import FlowPipelineAgent
from src.agents.python_migration_agent import PythonMigrationAgent
from src.agents.sql_migration_agent import SQLMigrationAgent
from src.agents.validation_agent import ValidationAgent
from src.agents.visual_recipe_agent import VisualRecipeAgent


# ── Output formatting helpers ─────────────────────────────────

def _format_output(data: Any, fmt: str) -> str:
    """Format data as json, table, or yaml."""
    if fmt == "json":
        return json.dumps(data, indent=2, default=str)
    elif fmt == "yaml":
        import yaml
        return yaml.dump(data, default_flow_style=False, sort_keys=False)
    else:
        return _format_table(data)


def _format_table(data: Any) -> str:
    """Format a dict or list as a simple ASCII table."""
    if isinstance(data, dict):
        lines = []
        max_key = max((len(str(k)) for k in data), default=0)
        for k, v in data.items():
            if isinstance(v, (dict, list)):
                v = json.dumps(v, default=str)
            lines.append(f"  {str(k):<{max_key}}  {v}")
        return "\n".join(lines)
    elif isinstance(data, list) and data and isinstance(data[0], dict):
        keys = list(data[0].keys())
        widths = {k: max(len(k), *(len(str(row.get(k, ""))) for row in data)) for k in keys}
        header = "  ".join(f"{k:<{widths[k]}}" for k in keys)
        sep = "  ".join("-" * widths[k] for k in keys)
        rows = []
        for row in data:
            rows.append("  ".join(f"{str(row.get(k, '')):<{widths[k]}}" for k in keys))
        return "\n".join([header, sep] + rows)
    return str(data)


# ── Rich progress helpers ─────────────────────────────────────

def _make_progress():
    """Create a Rich progress bar for agent execution."""
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
    )


def _build_orchestrator(config_path: str) -> Orchestrator:
    """Load config, registry, and register all agents."""
    cfg = load_config(config_path)
    setup_logging(level=cfg.logging.level, log_format=cfg.logging.format, log_file=cfg.logging.file)
    registry = AssetRegistry(
        project_key=cfg.dataiku.project_key,
        registry_path=Path(cfg.migration.output_dir) / "registry.json",
    )
    orch = Orchestrator(config=cfg, registry=registry)

    # Wire connectors into orchestration context
    try:
        dataiku_client = DataikuClient(
            base_url=cfg.dataiku.url,
            api_key=cfg.dataiku.api_key,
            timeout=cfg.dataiku.timeout_seconds,
            max_retries=cfg.dataiku.max_retries,
        )
        orch.context.connectors["dataiku"] = dataiku_client
    except ValueError:
        pass  # API key not set — discovery will fail gracefully

    try:
        token = _acquire_token(cfg.fabric)
        fabric_client = FabricClient(
            workspace_id=cfg.fabric.workspace_id,
            access_token=token,
        )
        orch.context.connectors["fabric"] = fabric_client
    except Exception:
        pass  # Azure auth not configured — deploy steps will fail gracefully

    orch.register_agent(DiscoveryAgent())
    orch.register_agent(ConnectionMapperAgent())
    orch.register_agent(DatasetMigrationAgent())
    orch.register_agent(SQLMigrationAgent())
    orch.register_agent(PythonMigrationAgent())
    orch.register_agent(VisualRecipeAgent())
    orch.register_agent(FlowPipelineAgent())
    orch.register_agent(ValidationAgent())

    return orch


@click.group()
@click.version_option(version="0.1.0")
def cli():
    """Dataiku to Fabric Migration Toolkit

    Automated migration of Dataiku projects to Microsoft Fabric.
    """


# ── discover ─────────────────────────────────────────────────

@cli.command()
@click.option("--project", "-p", required=True, help="Dataiku project key")
@click.option("--config", "-c", default="config/config.yaml", help="Config file path")
@click.option("--output-format", "-f", "fmt", type=click.Choice(["table", "json", "yaml"]), default="table", help="Output format")
def discover(project: str, config: str, fmt: str):
    """Discover all assets in a Dataiku project."""
    click.echo(f"Discovering assets in project: {project}")
    orch = _build_orchestrator(config)
    orch.config.dataiku.project_key = project
    result = asyncio.run(orch.run_agent("discovery"))

    data = {
        "status": result.status.value,
        "assets_discovered": result.assets_processed,
        "review_flags": len(result.review_flags),
        "errors": result.errors,
    }
    click.echo(_format_output(data, fmt))
    if result.errors:
        sys.exit(1)


# ── migrate ──────────────────────────────────────────────────

@cli.command()
@click.option("--project", "-p", required=True, help="Dataiku project key")
@click.option("--target", "-t", required=True, help="Fabric workspace name or ID")
@click.option("--config", "-c", default="config/config.yaml", help="Config file path")
@click.option("--agents", "-a", multiple=True, help="Specific agents to run (default: all)")
@click.option("--resume", is_flag=True, default=False, help="Resume from last checkpoint, skip completed agents")
@click.option("--rerun", multiple=True, help="Force re-run specific agents (resets their assets)")
@click.option("--asset-ids", default=None, help="Comma-separated asset IDs to process")
@click.option("--keep-checkpoints", is_flag=True, default=False, help="Keep checkpoint files after completion")
@click.option("--dry-run", is_flag=True, default=False, help="Print execution plan without running")
@click.option("--output-format", "-f", "fmt", type=click.Choice(["table", "json", "yaml"]), default="table", help="Output format")
@click.option("--quiet", "-q", is_flag=True, default=False, help="Suppress progress bars")
def migrate(project: str, target: str, config: str, agents: tuple,
            resume: bool, rerun: tuple, asset_ids: str | None,
            keep_checkpoints: bool, dry_run: bool, fmt: str, quiet: bool):
    """Run full migration from Dataiku to Fabric."""
    orch = _build_orchestrator(config)
    orch.config.dataiku.project_key = project
    orch.config.fabric.workspace_id = target

    if resume:
        orch.registry.load()

    agent_names = list(agents) if agents else None

    # ── Dry-run: print plan and exit ──
    if dry_run:
        plan = orch.get_execution_plan(agent_names, resume=resume)
        click.echo(_format_output(plan, fmt))
        return

    click.echo(f"Migrating project: {project} -> {target}")
    if resume:
        click.echo("Resuming from checkpoint...")

    rerun_list = list(rerun) if rerun else None
    asset_id_set = set(asset_ids.split(",")) if asset_ids else None

    # ── Run with progress bars ──
    if not quiet:
        try:
            progress = _make_progress()
            agent_tasks: dict[str, Any] = {}

            def on_start(name: str, wave: int) -> None:
                agent_tasks[name] = progress.add_task(f"Wave {wave}: {name}", total=1)

            def on_done(name: str, result: Any) -> None:
                tid = agent_tasks.get(name)
                if tid is not None:
                    progress.update(tid, completed=1)

            with progress:
                results = asyncio.run(orch.run_pipeline(
                    agent_names,
                    resume=resume,
                    rerun_agents=rerun_list,
                    asset_ids=asset_id_set,
                    keep_checkpoints=keep_checkpoints,
                    on_agent_start=on_start,
                    on_agent_done=on_done,
                ))
        except ImportError:
            # Rich not available — fall back to no progress
            results = asyncio.run(orch.run_pipeline(
                agent_names,
                resume=resume,
                rerun_agents=rerun_list,
                asset_ids=asset_id_set,
                keep_checkpoints=keep_checkpoints,
            ))
    else:
        results = asyncio.run(orch.run_pipeline(
            agent_names,
            resume=resume,
            rerun_agents=rerun_list,
            asset_ids=asset_id_set,
            keep_checkpoints=keep_checkpoints,
        ))

    # ── Output results ──
    result_data = [
        {
            "agent": name,
            "status": r.status.value,
            "processed": r.assets_processed,
            "converted": r.assets_converted,
            "failed": r.assets_failed,
        }
        for name, r in results.items()
    ]
    click.echo(_format_output(result_data, fmt))

    failed = [r for r in results.values() if r.status.value == "failed"]
    if failed:
        click.echo(f"\n{len(failed)} agent(s) failed.")
        sys.exit(1)
    click.echo("\nMigration complete.")


# ── validate ─────────────────────────────────────────────────

@cli.command()
@click.option("--project", "-p", required=True, help="Dataiku project key")
@click.option("--config", "-c", default="config/config.yaml", help="Config file path")
@click.option("--output-format", "-f", "fmt", type=click.Choice(["table", "json", "yaml"]), default="table", help="Output format")
def validate(project: str, config: str, fmt: str):
    """Validate migrated assets."""
    click.echo(f"Validating migration for project: {project}")
    orch = _build_orchestrator(config)
    orch.config.dataiku.project_key = project
    result = asyncio.run(orch.run_agent("validator"))

    data = {
        "status": result.status.value,
        "total": result.assets_processed,
        "passed": result.assets_converted,
        "failed": result.assets_failed,
        "review_flags": result.review_flags,
        "errors": result.errors,
    }
    click.echo(_format_output(data, fmt))
    if result.errors:
        sys.exit(1)


# ── report ───────────────────────────────────────────────────

@cli.command()
@click.option("--project", "-p", required=True, help="Dataiku project key")
@click.option("--format", "-f", "fmt", type=click.Choice(["html", "json"]), default="json")
@click.option("--output", "-o", "output_path", default=None, help="Output file path (default: output/<project>_report.<fmt>)")
@click.option("--config", "-c", default="config/config.yaml", help="Config file path")
def report(project: str, fmt: str, output_path: str | None, config: str):
    """Generate migration validation report."""
    from src.models.report import save_html_report, save_json_report

    orch = _build_orchestrator(config)
    orch.config.dataiku.project_key = project

    # Run validation agent to produce detailed report
    result = asyncio.run(orch.run_agent("validator"))
    report_data = result.details.get("report")

    if report_data:
        from src.models.report import ValidationReport
        rpt = ValidationReport.model_validate(report_data)
    else:
        # Fallback: lightweight report from agent results
        click.echo(json.dumps({
            "status": result.status.value,
            "processed": result.assets_processed,
            "converted": result.assets_converted,
            "failed": result.assets_failed,
        }, indent=2))
        return

    output_dir = Path(orch.config.migration.output_dir)
    if output_path is None:
        output_path = str(output_dir / f"{project}_report.{fmt}")

    if fmt == "json":
        save_json_report(rpt, output_path)
    else:
        save_html_report(rpt, output_path)

    click.echo(f"Report saved: {output_path}")
    summary = rpt.to_summary()
    click.echo(f"  Assets: {summary['total_assets']} total, "
               f"{summary['passed']} passed, {summary['failed']} failed — "
               f"{summary['success_rate']} success rate")


# ── config (validate) ────────────────────────────────────────

@cli.group(name="config")
def config_group():
    """Configuration management commands."""


@config_group.command(name="validate")
@click.argument("config_path", default="config/config.yaml")
@click.option("--output-format", "-f", "fmt", type=click.Choice(["table", "json", "yaml"]), default="table", help="Output format")
def config_validate(config_path: str, fmt: str):
    """Validate a configuration file.

    Checks YAML syntax, schema, and environment variable availability.
    """
    issues = validate_config(config_path)

    if fmt != "table":
        click.echo(_format_output({"config_path": config_path, "issues": issues, "valid": len([i for i in issues if i["level"] == "error"]) == 0}, fmt))
    else:
        if not issues:
            click.echo(f"✓ {config_path} — valid, no issues found")
        else:
            for issue in issues:
                icon = "✗" if issue["level"] == "error" else "⚠"
                click.echo(f"  {icon} [{issue['level'].upper()}] {issue['message']}")
            errors = [i for i in issues if i["level"] == "error"]
            warnings = [i for i in issues if i["level"] == "warning"]
            click.echo(f"\n{len(errors)} error(s), {len(warnings)} warning(s)")

    if any(i["level"] == "error" for i in issues):
        sys.exit(1)


# ── status ────────────────────────────────────────────────────

@cli.command()
@click.option("--config", "-c", default="config/config.yaml", help="Config file path")
@click.option("--output-format", "-f", "fmt", type=click.Choice(["table", "json", "yaml"]), default="table", help="Output format")
def status(config: str, fmt: str):
    """Show current migration status from the registry."""
    orch = _build_orchestrator(config)
    orch.registry.load()
    status_data = orch.get_status()
    click.echo(_format_output(status_data, fmt))


# ── interactive ───────────────────────────────────────────────

@cli.command()
def interactive():
    """Launch interactive migration wizard.

    Prompts for project key, workspace, agent selection, and
    confirmation before running the migration.
    """
    click.echo("=== Dataiku → Fabric Migration Wizard ===\n")

    project = click.prompt("Dataiku project key")
    target = click.prompt("Fabric workspace ID")
    config_path = click.prompt("Config file path", default="config/config.yaml")

    # Validate config first
    issues = validate_config(config_path)
    errors = [i for i in issues if i["level"] == "error"]
    if errors:
        click.echo("\nConfig validation failed:")
        for e in errors:
            click.echo(f"  ✗ {e['message']}")
        sys.exit(1)

    warnings = [i for i in issues if i["level"] == "warning"]
    if warnings:
        click.echo("\nWarnings:")
        for w in warnings:
            click.echo(f"  ⚠ {w['message']}")

    # Agent selection
    all_agents = [
        "discovery", "connection_mapper", "dataset_migrator",
        "sql_converter", "python_converter", "visual_converter",
        "pipeline_builder", "validator",
    ]
    click.echo(f"\nAvailable agents: {', '.join(all_agents)}")
    agent_input = click.prompt(
        "Agents to run (comma-separated, or 'all')", default="all"
    )
    agent_names = None if agent_input.strip().lower() == "all" else [
        a.strip() for a in agent_input.split(",")
    ]

    resume = click.confirm("Resume from checkpoint?", default=False)
    dry_run = click.confirm("Dry run first?", default=True)

    orch = _build_orchestrator(config_path)
    orch.config.dataiku.project_key = project
    orch.config.fabric.workspace_id = target

    if resume:
        orch.registry.load()

    if dry_run:
        plan = orch.get_execution_plan(agent_names, resume=resume)
        click.echo("\n--- Execution Plan ---")
        click.echo(_format_output(plan, "table"))

        if not click.confirm("\nProceed with migration?", default=True):
            click.echo("Aborted.")
            return

    click.echo(f"\nStarting migration: {project} → {target}")
    results = asyncio.run(orch.run_pipeline(agent_names, resume=resume))

    for name, result in results.items():
        icon = "✓" if result.status.value == "completed" else "✗"
        click.echo(f"  {icon} {name}: {result.status.value} "
                    f"(processed={result.assets_processed}, converted={result.assets_converted})")

    failed = [r for r in results.values() if r.status.value == "failed"]
    if failed:
        click.echo(f"\n{len(failed)} agent(s) failed.")
        sys.exit(1)
    click.echo("\nMigration complete.")


if __name__ == "__main__":
    cli()
