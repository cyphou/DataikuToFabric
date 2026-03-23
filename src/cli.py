"""CLI entry point for the Dataiku → Fabric Migration Toolkit."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import click

from src.core.config import load_config
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


@cli.command()
@click.option("--project", "-p", required=True, help="Dataiku project key")
@click.option("--config", "-c", default="config/config.yaml", help="Config file path")
def discover(project: str, config: str):
    """Discover all assets in a Dataiku project."""
    click.echo(f"Discovering assets in project: {project}")
    orch = _build_orchestrator(config)
    orch.config.dataiku.project_key = project
    result = asyncio.run(orch.run_agent("discovery"))
    click.echo(f"Status: {result.status.value}")
    click.echo(f"Assets discovered: {result.assets_processed}")
    if result.review_flags:
        click.echo(f"Review flags: {len(result.review_flags)}")
    if result.errors:
        click.echo(f"Errors: {result.errors}")
        sys.exit(1)


@cli.command()
@click.option("--project", "-p", required=True, help="Dataiku project key")
@click.option("--target", "-t", required=True, help="Fabric workspace name or ID")
@click.option("--config", "-c", default="config/config.yaml", help="Config file path")
@click.option("--agents", "-a", multiple=True, help="Specific agents to run (default: all)")
def migrate(project: str, target: str, config: str, agents: tuple):
    """Run full migration from Dataiku to Fabric."""
    click.echo(f"Migrating project: {project} -> {target}")
    orch = _build_orchestrator(config)
    orch.config.dataiku.project_key = project
    orch.config.fabric.workspace_id = target

    agent_names = list(agents) if agents else None
    results = asyncio.run(orch.run_pipeline(agent_names))

    for name, result in results.items():
        click.echo(f"  {name}: {result.status.value} "
                    f"(processed={result.assets_processed}, converted={result.assets_converted})")
    failed = [r for r in results.values() if r.status.value == "failed"]
    if failed:
        click.echo(f"\n{len(failed)} agent(s) failed.")
        sys.exit(1)
    click.echo("\nMigration complete.")


@cli.command()
@click.option("--project", "-p", required=True, help="Dataiku project key")
@click.option("--config", "-c", default="config/config.yaml", help="Config file path")
def validate(project: str, config: str):
    """Validate migrated assets."""
    click.echo(f"Validating migration for project: {project}")
    orch = _build_orchestrator(config)
    orch.config.dataiku.project_key = project
    result = asyncio.run(orch.run_agent("validator"))
    click.echo(f"Status: {result.status.value}")
    click.echo(f"Total: {result.assets_processed}, Passed: {result.assets_converted}, Failed: {result.assets_failed}")
    if result.review_flags:
        for flag in result.review_flags:
            click.echo(f"  [FLAG] {flag}")
    if result.errors:
        for err in result.errors:
            click.echo(f"  [ERROR] {err}")
        sys.exit(1)


@cli.command()
@click.option("--project", "-p", required=True, help="Dataiku project key")
@click.option("--format", "-f", "fmt", type=click.Choice(["html", "json"]), default="json")
@click.option("--config", "-c", default="config/config.yaml", help="Config file path")
def report(project: str, fmt: str, config: str):
    """Generate migration report."""
    orch = _build_orchestrator(config)
    results = orch.get_results()
    report_data = {
        name: {
            "status": r.status.value,
            "processed": r.assets_processed,
            "converted": r.assets_converted,
            "failed": r.assets_failed,
            "review_flags": r.review_flags,
            "errors": r.errors,
        }
        for name, r in results.items()
    }
    if fmt == "json":
        click.echo(json.dumps(report_data, indent=2))
    else:
        click.echo(f"HTML report generation not yet implemented for project: {project}")


if __name__ == "__main__":
    cli()
