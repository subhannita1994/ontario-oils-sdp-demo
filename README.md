# Ontario Oils SDP Demo Pipeline

A Databricks Spark Declarative Pipeline (SDP) demonstrating streaming ingestion from simulated PostgreSQL CDC files through Bronze and Silver medallion layers.

## Overview

This demo showcases:
- **Streaming ingestion** using Auto Loader from Unity Catalog Volumes
- **Change Data Capture (CDC)** with SCD Type 1 and Type 2 handling
- **Data quality expectations** with different severities
- **Databricks Asset Bundles** for Git-backed deployment
- **Environment partitioning** at schema level (arc_dev/arc_test/arc_prod)

### Architecture

```
PostgreSQL (Simulated)
       │
       │ CDC (Debezium-style JSON)
       ▼
┌─────────────────────────────────────┐
│  Unity Catalog Volume               │
│  /Volumes/erp-demonstrations/       │
│    arc_dev/landing_zone/            │
│      ├── dim_location/              │
│      ├── dim_well_type/             │
│      ├── dim_date/                  │
│      ├── dim_formation/             │
│      └── fact_well_construction/    │
└─────────────────────────────────────┘
       │
       │ Auto Loader (Streaming)
       ▼
┌─────────────────────────────────────┐
│  Bronze Layer (Raw)                 │
│  ├── dim_location_bronze            │
│  ├── dim_well_type_bronze           │
│  ├── dim_date_bronze                │
│  ├── dim_formation_bronze           │
│  └── fact_well_construction_bronze  │
└─────────────────────────────────────┘
       │
       │ apply_changes() + Expectations
       ▼
┌─────────────────────────────────────┐
│  Silver Layer (Cleaned)             │
│  ├── dim_location_silver (SCD2)     │
│  ├── dim_well_type_silver (SCD2)    │
│  ├── dim_date_silver (SCD1)         │
│  ├── dim_formation_silver (SCD2)    │
│  └── fact_well_construction (SCD1)  │
│                                     │
│  + Validated views with expectations│
└─────────────────────────────────────┘
```

---

## Part 1: Workspace Setup (Prerequisites)

### 1.1 Unity Catalog Setup

In your Databricks workspace (https://fevm-serverless-nv91sa.cloud.databricks.com/):

1. **Open a SQL Warehouse or Notebook** and run:

```sql
-- Verify catalog exists (or create if you have permissions)
-- CREATE CATALOG IF NOT EXISTS `erp-demonstrations`;

-- Create environment schemas
CREATE SCHEMA IF NOT EXISTS `erp-demonstrations`.arc_dev;
CREATE SCHEMA IF NOT EXISTS `erp-demonstrations`.arc_test;
CREATE SCHEMA IF NOT EXISTS `erp-demonstrations`.arc_prod;

-- Create landing zone volumes for CDC files
CREATE VOLUME IF NOT EXISTS `erp-demonstrations`.arc_dev.landing_zone;
CREATE VOLUME IF NOT EXISTS `erp-demonstrations`.arc_test.landing_zone;
CREATE VOLUME IF NOT EXISTS `erp-demonstrations`.arc_prod.landing_zone;
```

### 1.2 GitHub Repository Setup

1. **Create repository** on GitHub: `ontario-oils-sdp-demo`
2. **Initialize** with this project structure
3. **Connect Databricks to GitHub**:
   - Navigate to: **Settings** > **Developer** > **Git integration**
   - Add GitHub provider and authenticate

### 1.3 Databricks CLI Setup

```bash
# Install Databricks CLI v2 (if not installed)
pip install databricks-cli

# Configure with your workspace
databricks configure --host https://fevm-serverless-nv91sa.cloud.databricks.com

# Verify connection
databricks workspace list /
```

---

## Part 2: Generate Sample Data

Before running the pipeline, generate CDC files:

```bash
# Generate initial load
python sample_data/generate_cdc_files.py \
    --output-dir ./landing_zone_data \
    --initial

# Upload to Unity Catalog Volume
databricks fs cp -r ./landing_zone_data/* \
    dbfs:/Volumes/erp-demonstrations/arc_dev/landing_zone/ \
    --overwrite
```

Or run directly in Databricks notebook:

```python
%run ./sample_data/generate_cdc_files

# Generate files directly to volume
generator = CDCGenerator("/Volumes/erp-demonstrations/arc_dev/landing_zone", batch_id=1)
generator.generate_initial_load()
```

---

## Part 3: Deploy and Run Pipeline

### Using Databricks Asset Bundles

```bash
# Validate the bundle configuration
databricks bundle validate

# Deploy to arc_dev environment
databricks bundle deploy -t arc_dev

# Run the pipeline
databricks bundle run ontario_oils_pipeline -t arc_dev

# Check pipeline status
databricks pipelines list
```

### Manual Deployment (Alternative)

1. **Create Pipeline** in Databricks UI:
   - Navigate to **Workflows** > **Delta Live Tables**
   - Click **Create Pipeline**
   - Configure:
     - Name: `Ontario Oils SDP Pipeline - arc_dev`
     - Catalog: `erp-demonstrations`
     - Target Schema: `arc_dev`
     - Source: Upload notebooks from `src/ontario_oils/`

---

## Part 4: Demo Walkthrough

### Demo 1: Pipeline Definition

**Location**: `src/ontario_oils/bronze/ingest.py` and `src/ontario_oils/silver/transform.py`

**Talking Points**:

1. **Key Coding Rules** (Editor validates automatically):
   ```python
   @dlt.table(name="my_table")
   def my_table():
       # RULE 1: Must return a DataFrame
       return spark.readStream.format("cloudFiles")...
       
       # RULE 2: No imperative side effects allowed
       # BAD: spark.sql("DROP TABLE x")  # Editor will flag this!
       # BAD: print("debug")             # Side effect not allowed
   ```

2. **AUTO CDC with apply_changes()**:
   ```python
   # SCD Type 2 - Preserves history with __START_AT, __END_AT
   dlt.apply_changes(
       target="dim_location_silver",
       source="dim_location_bronze",
       keys=["location_id"],
       sequence_by="_ts",              # Handles out-of-order events!
       apply_as_deletes=F.expr("_op = 'D'"),
       stored_as_scd_type=2            # 1=upsert, 2=history
   )
   ```

3. **SQL Streaming Tables Parity** (for SQL-only teams):
   ```sql
   -- Equivalent SQL syntax
   CREATE OR REFRESH STREAMING TABLE dim_location_bronze
   AS SELECT * FROM STREAM read_files('/Volumes/.../landing_zone/dim_location');
   
   APPLY CHANGES INTO LIVE.dim_location_silver
   FROM STREAM(LIVE.dim_location_bronze)
   KEYS (location_id)
   SEQUENCE BY _ts
   STORED AS SCD TYPE 2;
   ```

### Demo 2: Visual DAG and Lineage

**Location**: Pipeline UI after running

**What to Show**:
1. **DAG View**: Notice how dimension tables process in parallel
2. **Inferred Ordering**: Facts depend on dimensions (shown by edges)
3. **Data Previews**: Click any table node to see sample data
4. **Compiled Plan**: Expand to see Spark execution plan

**Screenshot Areas**:
- The visual graph showing Bronze → Silver flow
- Parallel execution of dimension tables
- Table details panel with row counts

### Demo 3: Event Logs

**Location**: Pipeline UI > Event Log tab, or query directly

**Queries for Anomaly Detection**:

```sql
-- Flow progress and throughput
SELECT 
    timestamp,
    details:flow_name AS flow,
    details:flow_progress:num_output_rows AS output_rows,
    details:flow_progress:metrics AS metrics
FROM event_log(TABLE(`erp-demonstrations`.arc_dev.ontario_oils_pipeline))
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC
LIMIT 50;

-- Stream progress metrics (look for spikes in num_output_rows)
SELECT 
    timestamp,
    details:flow_name,
    details:flow_progress:num_output_rows,
    details:flow_progress:status
FROM event_log(TABLE(`erp-demonstrations`.arc_dev.ontario_oils_pipeline))
WHERE event_type = 'flow_progress'
  AND details:flow_progress:num_output_rows > 0;

-- Failures over time (ops dashboard query)
SELECT 
    DATE(timestamp) as date,
    COUNT(*) as failure_count,
    COLLECT_SET(details:flow_name) as failed_flows
FROM event_log(TABLE(`erp-demonstrations`.arc_dev.ontario_oils_pipeline))
WHERE event_type IN ('flow_failure', 'update_failure')
GROUP BY DATE(timestamp)
ORDER BY date DESC;

-- Throughput trends
SELECT 
    DATE_TRUNC('hour', timestamp) as hour,
    SUM(details:flow_progress:num_output_rows) as total_rows,
    COUNT(DISTINCT details:flow_name) as active_flows
FROM event_log(TABLE(`erp-demonstrations`.arc_dev.ontario_oils_pipeline))
WHERE event_type = 'flow_progress'
GROUP BY DATE_TRUNC('hour', timestamp)
ORDER BY hour DESC;
```

### Demo 4: Expectations

**Location**: `src/ontario_oils/silver/transform.py`

**Expectation Types**:

| Decorator | Behavior | Use Case |
|-----------|----------|----------|
| `@dlt.expect("name", "condition")` | Track only | Monitoring, metrics |
| `@dlt.expect_or_warn("name", "condition")` | Track + warn | Soft constraints |
| `@dlt.expect_or_drop("name", "condition")` | Drop bad rows | Data cleansing |
| `@dlt.expect_or_fail("name", "condition")` | Fail pipeline | Critical constraints |

**Query Expectation Results**:

```sql
-- Expectation summary
SELECT
    details:flow_name,
    details:expectation:name,
    details:expectation:dataset,
    details:expectation:passed_records,
    details:expectation:failed_records
FROM event_log(TABLE(`erp-demonstrations`.arc_dev.ontario_oils_pipeline))
WHERE event_type = 'expectation'
ORDER BY timestamp DESC;
```

### Demo 5: Intentional Break Scenarios

#### 5.1 Schema Evolution Mismatch

```bash
# Generate schema break files
python sample_data/generate_cdc_files.py \
    --output-dir ./landing_zone_data \
    --break-schema

# Upload to trigger error
databricks fs cp -r ./landing_zone_data/* \
    dbfs:/Volumes/erp-demonstrations/arc_dev/landing_zone/
```

**What happens**: Pipeline shows schema mismatch error in DAG
**Remediation**: 
- Adjust `cloudFiles.schemaEvolutionMode` to `rescue` or `failOnNewColumns`
- Or update expectations to handle missing columns

#### 5.2 Disallowed Imperative Operation

Add this to `transform.py` to demonstrate editor validation:

```python
@dlt.table(name="bad_example")
def bad_example():
    # This will be flagged by the editor!
    spark.sql("SELECT 1")  # ERROR: Imperative operation
    return dlt.read("some_table")
```

**What happens**: Editor immediately highlights the error
**Talking Point**: "This is why 'just writing code' doesn't work in dataset definitions"

#### 5.3 Expectation Failure

```bash
# Generate records that violate expectations
python sample_data/generate_cdc_files.py \
    --output-dir ./landing_zone_data \
    --expect-violations
```

**What happens**: 
- `expect_or_fail` causes pipeline failure (invalid year/month)
- `expect_or_warn` logs warning but continues
- `expect_or_drop` silently drops bad rows

**Remediation**:
1. Change `expect_or_fail` to `expect_or_warn` for graceful handling
2. Adjust the condition bounds
3. Re-run only affected datasets

#### 5.4 Data Engineering Agent

In the Pipeline Editor:
1. Click on a table definition
2. Open **AI Assistant** / **Data Engineering Agent**
3. Prompt: "Add an expectation to ensure casing_diameter is between 50 and 500 mm"
4. Review generated code
5. Accept/modify and validate
6. Re-run changed datasets only

---

## Part 5: Git Integration

### Databricks Asset Bundle Structure

```
ontario-oils-sdp-demo/
├── databricks.yml           # Bundle config with targets
├── resources/
│   └── ontario_oils_pipeline.yml
├── src/
│   └── ontario_oils/
│       ├── bronze/ingest.py
│       └── silver/transform.py
└── sample_data/
    └── generate_cdc_files.py
```

### CI/CD Quick Setup

Add to `.github/workflows/deploy.yml`:

```yaml
name: Deploy SDP Pipeline

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: databricks/setup-cli@main
      - run: databricks bundle validate
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}

  deploy-dev:
    needs: validate
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy -t arc_dev
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
```

---

## Quick Reference

### Deployment Commands

```bash
# Validate bundle
databricks bundle validate

# Deploy to environment
databricks bundle deploy -t arc_dev
databricks bundle deploy -t arc_test
databricks bundle deploy -t arc_prod

# Run pipeline
databricks bundle run ontario_oils_pipeline -t arc_dev

# Destroy deployment
databricks bundle destroy -t arc_dev
```

### Useful SQL Queries

```sql
-- List all tables in schema
SHOW TABLES IN `erp-demonstrations`.arc_dev;

-- Check SCD Type 2 history
SELECT * FROM `erp-demonstrations`.arc_dev.dim_location_silver
WHERE location_id = 1
ORDER BY __START_AT;

-- View expectation metrics
SELECT * FROM `erp-demonstrations`.arc_dev.__event_log
WHERE event_type = 'expectation';
```

---

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| No data flowing | Empty landing zone | Run `generate_cdc_files.py --initial` |
| Schema error | Missing column in CDC | Use `schemaEvolutionMode: rescue` |
| Expectation failure | Data quality issue | Check event log, adjust expectation severity |
| Permission denied | Missing catalog grants | Grant USE CATALOG, USE SCHEMA, CREATE TABLE |

---

## Contact

- **Demo Owner**: Subhannita Sarcar (subhannita.sarcar@databricks.com)
- **Workspace**: https://fevm-serverless-nv91sa.cloud.databricks.com/
- **Catalog**: erp-demonstrations
- **Schemas**: arc_dev, arc_test, arc_prod
