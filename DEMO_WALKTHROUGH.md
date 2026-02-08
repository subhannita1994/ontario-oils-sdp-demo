# Ontario Oils SDP Demo Walkthrough

## Demo Flow Overview

```
1. Pipeline Definition (Code)     →  Show in Workspace/IDE
2. Visual DAG & Lineage          →  Show in Pipeline UI
3. Data Previews & Plan          →  Show in Pipeline UI
4. Compare to Traditional Jobs   →  Discussion points
```

---

## Part 1: Pipeline Definition (Show Code)

### Where to Show
- **Option A**: Open notebooks in Databricks Workspace (`/Workspace/Shared/ontario-oils-sdp-demo/arc_dev/files/src/ontario_oils/`)
- **Option B**: Show from your local IDE/GitHub

### 1.1 Key Coding Rules

**Open `bronze/ingest.py` and highlight:**

```python
@dlt.table(
    name="dim_location_bronze",
    comment="Raw location dimension from CDC files"
)
def dim_location_bronze():
    return (                          # ← RULE 1: Must return a DataFrame
        spark.readStream              # ← Streaming source
        .format("cloudFiles")         # ← Auto Loader
        .option("cloudFiles.format", "json")
        .load(f"{LANDING_BASE}/dim_location")
        .withColumn("_ingested_at", F.current_timestamp())
    )
```

**Talking Points:**
> "Notice two key rules that SDP enforces:
> 
> 1. **Functions must return a DataFrame** - The `@dlt.table` decorator expects the function to return a DataFrame. This is declarative - you describe WHAT data you want, not HOW to process it.
> 
> 2. **No imperative side effects** - You can't do things like `spark.sql("DROP TABLE")` or `print()` statements inside these functions. The editor will flag these immediately.
> 
> This is fundamentally different from traditional Spark jobs where you might write imperative code that reads, transforms, and writes in sequence."

**Demo the Editor Validation (Optional):**

Add this bad code temporarily to show the error:
```python
@dlt.table(name="bad_example")
def bad_example():
    spark.sql("SELECT 1")  # ← Editor flags this as disallowed!
    return spark.read.table("some_table")
```

---

### 1.2 AUTO CDC for SCD Type 1/2

**Open `silver/transform.py` and highlight:**

```python
# SCD Type 2 - Preserves history
dlt.create_streaming_table(
    name="dim_location_silver",
    comment="Cleaned location dimension with SCD Type 2"
)

dlt.apply_changes(
    target="dim_location_silver",
    source="dim_location_bronze",
    keys=["location_id"],           # ← Primary key for matching
    sequence_by="_ts",              # ← Handles out-of-order events!
    apply_as_deletes=F.expr("_op = 'D'"),
    stored_as_scd_type=2            # ← 1=upsert, 2=history
)
```

**Talking Points:**
> "The `apply_changes()` API handles CDC complexity with a single call:
> 
> **SCD Type 1 vs Type 2:**
> - Type 1 (`stored_as_scd_type=1`): Overwrites records - just the latest version
> - Type 2 (`stored_as_scd_type=2`): Preserves history with `__START_AT` and `__END_AT` columns
> 
> **Out-of-Order Event Handling:**
> The `sequence_by="_ts"` parameter is key. If events arrive out of order (common in distributed systems), DLT uses this timestamp to determine the correct sequence. You don't need to write complex windowing logic.
> 
> **Compare to Traditional Approach:**
> Without this, you'd write 50+ lines of MERGE statements with window functions to handle late-arriving data. Here it's one API call."

**Show the SCD Type 2 History:**
```sql
-- Run in SQL Editor to show history tracking
SELECT location_id, township_con_lot, __START_AT, __END_AT 
FROM `erp-demonstrations`.arc_dev.dim_location_silver 
WHERE location_id = 1 
ORDER BY __START_AT;
```

---

### 1.3 SQL Streaming Tables Parity

**Talking Points:**
> "For SQL-only teams, there's complete parity. Everything we just showed in Python can be written in SQL:
> 
> ```sql
> -- Streaming Table (equivalent to @dlt.table with streaming source)
> CREATE OR REFRESH STREAMING TABLE dim_location_bronze
> AS SELECT * FROM STREAM read_files('/Volumes/.../landing_zone/dim_location');
> 
> -- Apply Changes (equivalent to dlt.apply_changes)
> APPLY CHANGES INTO LIVE.dim_location_silver
> FROM STREAM(LIVE.dim_location_bronze)
> KEYS (location_id)
> SEQUENCE BY _ts
> STORED AS SCD TYPE 2;
> 
> -- Materialized View (for aggregations)
> CREATE MATERIALIZED VIEW location_stats AS
> SELECT COUNT(*) as total FROM LIVE.dim_location_silver;
> ```
> 
> This means your data engineering team can use Python while your analytics team uses SQL - same pipeline, same runtime, same UI."

---

## Part 2: Visual DAG and Lineage

### Where to Show
**Workflows** → **Delta Live Tables** → **Ontario Oils SDP Pipeline - arc_dev**

### 2.1 The Pipeline DAG

**What to Point Out:**

```
┌─────────────────────────────────────────────────────────────┐
│                        PIPELINE DAG                          │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│   dim_location_bronze ──→ dim_location_silver ──→ dim_location_validated
│                              (SCD Type 2)                    │
│   dim_well_type_bronze ──→ dim_well_type_silver             │
│                              (SCD Type 2)                    │
│   dim_date_bronze ──────→ dim_date_silver ──────→ dim_date_validated
│                              (SCD Type 1)                    │
│   dim_formation_bronze ──→ dim_formation_silver             │
│                              (SCD Type 2)                    │
│                                                              │
│   fact_well_construction_bronze ──→ fact_well_construction_silver
│                                      (SCD Type 1)            │
│                                           │                  │
│                                           ▼                  │
│                              fact_well_construction_validated │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

**Talking Points:**
> "Notice how DLT automatically infers the execution order from data dependencies. I didn't specify that bronze runs before silver - it figured that out.
> 
> **Inferred Parallelism:**
> Look at the dimension tables - they're all processed in parallel because they're independent. The fact table waits for nothing because we're not doing referential integrity joins in this layer.
> 
> **Compare to Traditional Jobs:**
> In a traditional Spark job, you'd manually orchestrate this with Airflow/Workflows DAGs, managing dependencies explicitly. Here, dependencies are inferred from the code."

### 2.2 Data Lineage

**How to Access:**
1. Click on any table node (e.g., `dim_location_silver`)
2. Click the **Lineage** icon (or find it in the side panel)

**Talking Points:**
> "The lineage shows the complete data flow - where data comes from (landing zone files), transformations applied, and downstream consumers.
> 
> This is automatically captured - no additional configuration. It integrates with Unity Catalog lineage, so you can trace data across multiple pipelines and jobs."

---

## Part 3: Data Previews & Compiled Plan

### 3.1 Data Previews

**How to Access:**
1. Click on any table node in the DAG
2. Look at the right panel - you'll see **row counts** and **sample data**
3. Click **"View data"** for full preview

**What to Show:**
- Bronze table: Raw JSON fields, `_ingested_at` timestamp
- Silver SCD2 table: Same data plus `__START_AT`, `__END_AT` columns
- Validated table: Data that passed expectations

**Talking Points:**
> "I can preview data at any stage without running queries. This is especially useful for debugging - if something looks wrong in silver, I can check bronze to see if it's a source issue or transformation issue."

### 3.2 Compiled Plan

**How to Access:**
1. Click on a table node
2. Look for **"Compiled plan"** or **"Execution details"** in the panel

**Talking Points:**
> "The compiled plan shows the actual Spark execution plan. This is useful for performance tuning - you can see if there are expensive shuffles or suboptimal joins.
> 
> But notice: in declarative pipelines, you often don't need to look at this because DLT optimizes automatically. It handles things like:
> - Adaptive query execution
> - Automatic caching of intermediate results
> - Optimal partitioning"

---

## Part 4: Comparison with Traditional Jobs/Pipelines

### Key Differences to Highlight

| Aspect | Traditional Spark Job | SDP/DLT |
|--------|----------------------|---------|
| **Orchestration** | External (Airflow, Workflows) | Built-in, inferred from code |
| **Dependencies** | Manually defined | Auto-inferred from data flow |
| **Error Handling** | Custom retry logic | Automatic retries, checkpointing |
| **Schema Evolution** | Manual ALTER TABLE | Automatic with options |
| **CDC/SCD** | 50+ lines of MERGE | One `apply_changes()` call |
| **Data Quality** | Separate framework (Great Expectations) | Built-in expectations |
| **Monitoring** | Custom dashboards | Built-in event logs |
| **Incremental Processing** | Manual watermarking | Automatic with streaming |

### Talking Points for Traditional Job Comparison

> "Let me show you what this would look like as a traditional Spark job:
> 
> **Traditional Approach:**
> ```python
> # Job 1: Bronze ingestion
> df = spark.read.json("/landing/dim_location")
> df.write.mode("append").saveAsTable("bronze.dim_location")
> 
> # Job 2: Silver transformation (separate job, orchestrated by Airflow)
> bronze_df = spark.read.table("bronze.dim_location")
> # 50 lines of MERGE logic for SCD Type 2...
> silver_df.write.mode("overwrite").saveAsTable("silver.dim_location")
> 
> # Job 3: Validation (another separate job)
> # Custom data quality checks...
> ```
> 
> You'd need:
> - 3+ separate jobs
> - Airflow DAG to orchestrate them
> - Custom code for CDC handling
> - Separate Great Expectations setup
> - Manual checkpoint management
> 
> **SDP Approach:**
> - Single pipeline definition
> - Dependencies auto-inferred
> - CDC in one line
> - Built-in expectations
> - Automatic checkpointing
> 
> The key insight: **SDP is declarative**. You describe the end state you want, and the system figures out how to get there efficiently."

---

## Quick Reference: Where to Find Things

| Feature | Location |
|---------|----------|
| Pipeline code | Workspace → Shared → ontario-oils-sdp-demo → files → src |
| Pipeline DAG | Workflows → Delta Live Tables → [Pipeline Name] |
| Data preview | Click table node → Right panel |
| Lineage | Click table node → Lineage icon |
| Event logs | Pipeline UI → Event Log tab |
| Expectations | Pipeline UI → Data Quality tab |
| Run history | Pipeline UI → History tab |
| Settings | Pipeline UI → Settings tab |

---

## Demo Script (5-minute version)

1. **[30 sec]** "Let me show you a streaming data pipeline..." → Open Pipeline DAG
2. **[60 sec]** "Notice the Bronze-Silver architecture..." → Point out parallel execution
3. **[60 sec]** "Let's look at the code..." → Show `@dlt.table` and `apply_changes()`
4. **[45 sec]** "CDC handling is one line..." → Highlight SCD Type 2, sequence_by
5. **[45 sec]** "Data quality is built-in..." → Show expectations on validated tables
6. **[30 sec]** "Click any table for preview..." → Show data preview panel
7. **[30 sec]** "All this is Git-backed..." → Mention CI/CD workflow
