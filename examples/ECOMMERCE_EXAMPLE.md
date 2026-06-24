# 🚀 Real-World Dataiku Example: E-Commerce Analytics

This example simulates a **production-grade e-commerce analytics pipeline** with real complexity that enterprises encounter when migrating from Dataiku to Fabric.

## 📊 Project Overview

**Project**: `ECOMMERCE_ANALYTICS`  
**Complexity**: Advanced  
**Estimated Migration Time**: 16 hours  
**Data Scale**: 50M+ daily clickstream events, 2.5M+ order records

### What This Example Includes

| Component | Count | Details |
|-----------|-------|---------|
| **Data Sources** | 6 connections | Oracle ERP, PostgreSQL DW, S3, Azure Blob, Snowflake, MongoDB |
| **Datasets** | 5 core datasets | Raw orders, clickstream (Parquet), customer dimensions, aggregates, ML features |
| **Recipes** | 9 recipes | SQL (Oracle + PostgreSQL), Python (ML feature engineering), Visual (Join, Filter, GroupBy) |
| **Scenarios** | 3 automated pipelines | Daily ETL, weekly export, manual ML retraining |
| **DAG Complexity** | 9 nodes, 12 edges | Multi-level transformations with fan-out/fan-in pattern |

---

## 🔄 Data Flow

```
Oracle ERP (raw_orders)  ─┐
                          ├─→ recipe_00: Cleanse Orders ─→ stg_orders_cleaned
                          │
PostgreSQL DW (dim_customer) ┐
                          ├─→ recipe_02: Join (Orders × Customer) ─→ orders_with_customer_360
                          │                                          │
S3 Parquet (raw_clickstream)  │                                     ├─→ recipe_03: Aggregate LTV ─→ customer_ltv_360
     └─→ recipe_01: Parse Events ─→ stg_clickstream_parsed ────────┘                                │
                                                                     ├─→ recipe_04: Feature Engineering
                                                                     │
                                                                     ├─→ recipe_05: ML Scoring (Churn)
                                                                     │
                                                                     ├─→ recipe_06: Daily KPIs
                                                                     │
                                                                     └─→ recipe_07: Segment High-Value
```

---

## 🛠️ Migration Complexity Breakdown

### SQL Recipes (2 total)
- **recipe_00**: Oracle-specific syntax (TRUNC, CASE, VARCHAR2)
- **recipe_03**: PostgreSQL window functions (ROW_NUMBER, SUM OVER), CTEs

### Python Recipes (3 total)
- **recipe_01**: JSON parsing, GeoIP enrichment (dataiku SDK, external API calls)
- **recipe_04**: Feature engineering with scikit-learn, StandardScaler, PolynomialFeatures
- **recipe_05**: ML model scoring with XGBoost (joblib, pickle artifacts)

### Visual Recipes (4 total)
- **recipe_02**: INNER JOIN on customer_id
- **recipe_03**: GROUP BY with SUM, COUNT, AVG
- **recipe_07**: Filter with compound conditions
- **recipe_06**: GROUP BY on multiple columns

### Incremental Loading (2 patterns)
- **raw_orders**: Daily incremental by `order_date` (Oracle table)
- **raw_clickstream**: Partitioned Parquet with date-based retention

---

## 🧪 How to Test This Example

### 1. **Inspect the Example**
```bash
# View the complete example definition
cat examples/ecommerce_example.json

# Or query specific sections
python3 -c "
import json
with open('examples/ecommerce_example.json') as f:
    data = json.load(f)
    print('Connections:', [c['id'] for c in data['assets']['connections']])
    print('Recipes:', [r['id'] for r in data['assets']['recipes']])
"
```

### 2. **Run Migration Analysis**
```bash
# Analyze the project for migration readiness
python3 -m src.cli analyze examples/ecommerce_example.json \
  --output examples/output/ecommerce_analysis.html
```

### 3. **Validate Conversion Patterns**
```bash
# Test individual recipe conversions
pytest tests/test_realistic_fixtures.py::test_ecommerce_example -v
```

### 4. **Full End-to-End Migration** (Requires Dataiku + Fabric instances)
```bash
# Configure your instances
cat > examples/ecommerce_config.yaml << EOF
dataiku:
  url: "https://your-dataiku-instance:port"
  api_key: "${DATAIKU_API_KEY}"
  verify_ssl: true  # or provide CA bundle path

fabric:
  workspace_id: "your-fabric-workspace-id"
  tenant_id: "${AZURE_TENANT_ID}"
  client_id: "${AZURE_CLIENT_ID}"
  client_secret: "${AZURE_CLIENT_SECRET}"
EOF

# Run the full discovery → assessment → migration workflow
python3 -m src.cli migrate examples/ecommerce_config.yaml \
  --project ECOMMERCE_ANALYTICS \
  --output examples/output/ecommerce_migration/
```

---

## 🎯 What Gets Validated

This example exercises the toolkit's ability to handle:

### ✅ Database Connectivity
- Oracle ERP with SSL/TLS (connection security)
- PostgreSQL analytics with RDS (IAM auth pattern)
- Multiple database dialects in single project

### ✅ Recipe Type Diversity
- Oracle PL/SQL → T-SQL conversion (TRUNC, CASE)
- PostgreSQL window functions → Spark SQL (ROW_NUMBER, SUM OVER)
- Python dataiku SDK calls → PySpark equivalents
- Visual recipes → SQL query generation

### ✅ Data Patterns
- Incremental loading (date-based, partition-aware)
- Large-scale data (50M+ events, partitioned Parquet)
- ML model artifacts (XGBoost, pickle)
- Streaming-adjacent (real-time events collection)

### ✅ Orchestration
- Complex DAG with 12 edges (multi-level dependencies)
- Scheduled scenarios (daily, weekly, manual triggers)
- Timeout specifications and error handling

### ✅ Data Quality
- Null checks and distribution validation
- Churn score bounds verification (0.0 ≤ score ≤ 1.0)
- Incremental loading integrity

---

## 📝 Expected Migration Output

### Generated Files
```
examples/output/ecommerce_migration/
├── connections/
│   ├── oracle_erp.json              # → Fabric connection mapping
│   ├── postgres_warehouse.json       # → PostgreSQL linked service
│   ├── s3_events.json                # → OneLake Shortcut
│   └── ...
│
├── sql/
│   ├── recipe_00_oracle_to_tsql.sql  # Oracle SQL → T-SQL
│   ├── recipe_03_postgres_to_spark.sql # PostgreSQL → Spark SQL
│   └── ...
│
├── notebooks/
│   ├── recipe_01_ingest_clickstream.ipynb  # JSON parsing → PySpark
│   ├── recipe_04_feature_engineering.ipynb # sklearn → PySpark ML
│   ├── recipe_05_ml_scoring.ipynb          # Model scoring notebook
│   └── ...
│
├── pipelines/
│   ├── daily_pipeline.json           # Scenario → Fabric Data Pipeline
│   ├── weekly_export.json            # Weekly → Fabric trigger
│   └── ...
│
├── ddl/
│   ├── lakehouse_schemas.sql         # Lake schemas for datasets
│   ├── warehouse_tables.sql          # Warehouse materialized views
│   └── ...
│
└── reports/
    ├── migration_assessment.html     # Readiness report
    ├── lineage_graph.html            # DAG visualization
    └── validation_results.json       # Conversion quality scores
```

### Fidelity Metrics
- **SQL Conversion**: Oracle functions translated → PostgreSQL compatibility validated
- **Python Semantics**: dataiku.Dataset calls → spark.read/write equivalents
- **Lineage Accuracy**: 12-edge DAG fully traversed → Fabric pipeline dependency order
- **Data Types**: Schema mappings validated (Oracle NUMBER → T-SQL decimal, PostgreSQL bigint → PySpark long)

---

## 🔗 Related Examples

- **Customer Analytics** (`examples/input/`): Simpler multi-database example
- **Real Dataiku Project**: Add your own exported Dataiku project to `examples/` and follow this pattern

---

## 🤝 Contributing

Found a gap or new pattern? Add it here:
```bash
# Export your Dataiku project
dataiku export-project MYPROJECT > examples/myproject_example.json

# Document the complexity
# Run toolkit migration
# Add test case to tests/test_realistic_fixtures.py
```

---

**Last Updated**: 2026-06-24  
**Example Complexity**: Advanced (ML, multi-dialect, incremental loading)  
**Use This For**: Capacity planning, assessment demos, validation testing
