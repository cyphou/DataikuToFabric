# Troubleshooting

## Common Errors

### `ModuleNotFoundError: No module named 'src'`

You need to install the package in development mode:

```bash
pip install -e ".[all]"
```

Or ensure you're running from the project root:

```bash
python -m src.cli --help
```

### `ConnectionRefusedError` when connecting to Dataiku

- Verify the `dataiku.host` URL in `config/config.yaml` is correct and reachable.
- Check that your Dataiku instance is running and the API is enabled.
- If behind a VPN or firewall, ensure network access is allowed.

### `401 Unauthorized` from Dataiku API

- Verify your `dataiku.api_key` is valid and has not expired.
- Ensure the API key has **read access** to the target project.
- Generate a new key from Dataiku → Administration → API Keys.

### `403 Forbidden` from Fabric REST API

- Verify your Azure AD token has the `Fabric.ReadWrite.All` scope.
- Ensure the service principal or user has **Contributor** access to the target Fabric workspace.
- Re-authenticate with Azure CLI:

```bash
az login
az account set --subscription <SUBSCRIPTION_ID>
```

### `429 Request Rate Too Large`

The Fabric API throttled your request. The SDK retries automatically with backoff. If persistent:

- Reduce `orchestrator.parallel_agents` in config.
- Wait a few minutes and retry.

### `TimeoutError: Agent 'agent_name' timed out`

An agent exceeded its execution timeout. To increase:

```yaml
orchestrator:
  agent_timeout_seconds: 600  # default is 300
```

### `Circuit breaker open — skipping agent`

After consecutive agent failures, the circuit breaker prevents further execution. To adjust:

```yaml
orchestrator:
  circuit_breaker_threshold: 5  # default is 3
```

Fix the underlying agent failures and re-run.

## SQL Translation Issues

### Oracle syntax not fully converted

If Oracle-specific SQL is not translated:

- Check the error details in `output/report.json` under the SQL migration agent results.
- Common unsupported patterns: dynamic SQL, PL/SQL blocks, `DBMS_*` package calls.
- Manually translate unsupported constructs and add them as Fabric SQL scripts.

### PostgreSQL `::` cast syntax in output

If PostgreSQL cast syntax leaks into T-SQL output:

- Verify the source recipe's `engine` is set to `postgresql` in the Dataiku metadata.
- The SQL translator auto-detects dialects; misidentified dialects may cause partial translation.

## Notebook Issues

### Generated notebook has import errors

If the generated `.ipynb` notebook fails to run:

- Ensure PySpark and Fabric runtime libraries are available in the target Fabric workspace.
- Check that `import dataiku` calls were all replaced with PySpark equivalents.
- Review the notebook cells for any remaining `dataiku.*` references.

## Pipeline Issues

### Pipeline JSON fails to deploy

- Validate the pipeline JSON with `dataiku-to-fabric validate`.
- Check that all referenced notebook and SQL activity names exist.
- Ensure the pipeline activities match the expected Fabric Data Pipeline schema.

## Performance

### Migration too slow for large projects

- Enable parallel agent execution by increasing `parallel_agents`.
- Use the `--agents` flag to run only specific agents for iterative debugging.
- Profile with the integration performance test: `pytest tests/integration/test_perf.py -v`.

## Checkpoint & Resume

### Migration interrupted — how to resume

If a migration was interrupted (crash, timeout, Ctrl+C), resume from the last checkpoint:

```bash
dataiku-to-fabric migrate --project MY_PROJECT --target MY_WORKSPACE --resume
```

The `--resume` flag loads the most recent registry state and skips agents that already completed successfully.

### Re-running a specific agent after a fix

If an agent failed and you've fixed the underlying issue, re-run just that agent (and any downstream dependents):

```bash
dataiku-to-fabric migrate --project MY_PROJECT --target MY_WORKSPACE --rerun sql_migration
```

Multiple agents can be re-run:

```bash
dataiku-to-fabric migrate --project MY_PROJECT --target MY_WORKSPACE --rerun sql_migration --rerun validation
```

### Checkpoint files accumulating

Checkpoint files are saved in the output directory during migration. By default, they are cleaned up after a successful run. If you used `--keep-checkpoints`, you can manually delete them:

```bash
rm output/checkpoint_wave_*.json
```

### Migrating only specific assets

To re-process a subset of assets without running the full pipeline:

```bash
dataiku-to-fabric migrate --project MY_PROJECT --target MY_WORKSPACE --asset-ids "recipe_sql_1,dataset_customers"
```

This filters the registry to only the specified asset IDs and skips the discovery phase.

## Getting Help

- Check [docs/SETUP.md](SETUP.md) for installation instructions.
- Review [docs/ARCHITECTURE.md](ARCHITECTURE.md) for system design details.
- Open an issue at [GitHub Issues](https://github.com/cyphou/DataikuToFabric/issues).
