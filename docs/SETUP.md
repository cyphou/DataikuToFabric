# Setup Guide

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.10+ | Core runtime |
| Azure CLI | Latest | Fabric API authentication |
| Dataiku API key | — | Access to source Dataiku instance |
| Fabric workspace | — | Target deployment workspace |
| Git | 2.x | Version control |

## Installation

### From PyPI (recommended)

```bash
pip install dataiku-to-fabric
```

To include Azure deployment support and data extras:

```bash
pip install "dataiku-to-fabric[all]"
```

### From source

```bash
git clone https://github.com/cyphou/DataikuToFabric.git
cd DataikuToFabric
pip install -e ".[all]"
```

### Docker

```bash
docker build -t dataiku-to-fabric .
docker run --rm dataiku-to-fabric --version
```

## Configuration

1. Copy the template:

```bash
cp config/config.template.yaml config/config.yaml
```

2. Edit `config/config.yaml` with your credentials:

```yaml
dataiku:
  host: "https://your-dataiku-instance.com"
  api_key: "<DATAIKU_API_KEY>"
  project_key: "MY_PROJECT"

fabric:
  workspace_id: "<FABRIC_WORKSPACE_GUID>"
  tenant_id: "<AZURE_TENANT_ID>"
  lakehouse_name: "MigratedLakehouse"
  warehouse_name: "MigratedWarehouse"

orchestrator:
  output_dir: "output"
  parallel_agents: 4
  agent_timeout_seconds: 300
  circuit_breaker_threshold: 3
```

> **Security:** Never commit `config/config.yaml` to version control. It is listed in `.gitignore`.

## First Run

### 1. Discover assets

Scan the Dataiku project and build the asset registry:

```bash
dataiku-to-fabric discover --project MY_PROJECT
```

This creates `output/registry.json` with all discovered assets.

### 2. Convert assets

Run the full migration pipeline (discovery → conversion → validation):

```bash
dataiku-to-fabric migrate --project MY_PROJECT --target MY_WORKSPACE
```

#### Resume from checkpoint

If a migration was interrupted, resume from the last checkpoint:

```bash
dataiku-to-fabric migrate --project MY_PROJECT --target MY_WORKSPACE --resume
```

#### Re-run specific agents

Re-run one or more agents (and their downstream dependents):

```bash
dataiku-to-fabric migrate --project MY_PROJECT --target MY_WORKSPACE --rerun sql_migration --rerun validation
```

#### Filter by asset IDs

Migrate only specific assets:

```bash
dataiku-to-fabric migrate --project MY_PROJECT --target MY_WORKSPACE --asset-ids "recipe_sql_1,dataset_customers"
```

#### Keep checkpoints

By default, checkpoints are cleaned up after a successful run. To keep them:

```bash
dataiku-to-fabric migrate --project MY_PROJECT --target MY_WORKSPACE --keep-checkpoints
```

### 3. Validate

Run validation only (assumes assets are already converted):

```bash
dataiku-to-fabric validate --project MY_PROJECT
```

### 4. Check the report

After migration or validation, reports are generated in `output/`:

- `output/report.html` — Human-readable HTML report
- `output/report.json` — Machine-readable JSON report

## Running via Docker

Mount your config and output directory:

```bash
docker run --rm \
  -v $(pwd)/config:/app/config:ro \
  -v $(pwd)/output:/app/output \
  dataiku-to-fabric migrate --project MY_PROJECT
```

## Development Setup

```bash
git clone https://github.com/cyphou/DataikuToFabric.git
cd DataikuToFabric
pip install -e ".[all]"

# Run tests
pytest tests/ tests/integration/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=term-missing
```
