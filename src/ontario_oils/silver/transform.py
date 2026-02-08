# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Ontario Oils Cleaned & Transformed Data
# MAGIC 
# MAGIC This notebook defines streaming tables with data quality expectations and CDC handling.
# MAGIC 
# MAGIC **Key Features Demonstrated:**
# MAGIC 1. **Expectations** - Data quality rules with different severities
# MAGIC 2. **AUTO CDC** - SCD Type 1 and Type 2 with `apply_changes()`
# MAGIC 3. **Out-of-order event handling** - Automatic with sequence_by column
# MAGIC 4. **Schema evolution** - Handled gracefully with merge schema
# MAGIC 
# MAGIC **Expectation Severities:**
# MAGIC - `@dlt.expect("rule", "condition")` - Track violations, keep all rows
# MAGIC - `@dlt.expect_or_warn("rule", "condition")` - Warn on violations, keep rows
# MAGIC - `@dlt.expect_or_drop("rule", "condition")` - Drop violating rows
# MAGIC - `@dlt.expect_or_fail("rule", "condition")` - Fail pipeline on violations

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## AUTO CDC Overview
# MAGIC 
# MAGIC `dlt.apply_changes()` provides automatic Change Data Capture handling:
# MAGIC 
# MAGIC ```python
# MAGIC dlt.apply_changes(
# MAGIC     target="silver_table",           # Target table name
# MAGIC     source="bronze_table",           # Source streaming table
# MAGIC     keys=["id"],                     # Primary key(s) for matching
# MAGIC     sequence_by="timestamp",         # Column for ordering (handles out-of-order)
# MAGIC     apply_as_deletes=expr("op='D'"), # Delete condition
# MAGIC     apply_as_truncates=None,         # Truncate condition (optional)
# MAGIC     column_list=None,                # Columns to include (None = all)
# MAGIC     except_column_list=["op","ts"],  # Columns to exclude
# MAGIC     stored_as_scd_type=1,            # 1=upsert, 2=history tracking
# MAGIC     track_history_column_list=None,  # Columns to track for SCD2 (None = all)
# MAGIC     track_history_except_column_list=None
# MAGIC )
# MAGIC ```
# MAGIC 
# MAGIC **SCD Type 1 vs Type 2:**
# MAGIC - **Type 1**: Overwrites existing records (no history)
# MAGIC - **Type 2**: Preserves history with `__START_AT`, `__END_AT` columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Tables - Silver Layer (SCD Type 2)
# MAGIC 
# MAGIC Dimensions use SCD Type 2 to preserve historical changes.

# COMMAND ----------

# Create the target table for dim_location with expectations
dlt.create_streaming_table(
    name="dim_location_silver",
    comment="Cleaned location dimension with SCD Type 2 history tracking",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)

# Apply CDC changes with SCD Type 2
dlt.apply_changes(
    target="dim_location_silver",
    source="dim_location_bronze",
    keys=["location_id"],
    sequence_by="_ts",
    apply_as_deletes=F.expr("_op = 'D'"),
    except_column_list=["_op", "_ts", "_ingested_at", "_source_file", "_rescued_data"],
    stored_as_scd_type=2,
    track_history_column_list=["township_con_lot", "utm_coordinates", "utm_zone", "utm_easting", "utm_northing"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Location Dimension with Expectations
# MAGIC 
# MAGIC We also create a view with expectations for data quality monitoring.

# COMMAND ----------

@dlt.table(
    name="dim_location_validated",
    comment="Location dimension with data quality expectations applied"
)
@dlt.expect("location_id_not_null", "location_id IS NOT NULL")
@dlt.expect_or_warn("valid_utm_easting", "utm_easting > 0 OR utm_easting IS NULL")
@dlt.expect_or_warn("valid_utm_northing", "utm_northing > 0 OR utm_northing IS NULL")
@dlt.expect("valid_utm_zone", "utm_zone IS NULL OR (utm_zone >= 1 AND utm_zone <= 60)")
def dim_location_validated():
    """
    Apply expectations to the location dimension.
    Demonstrates different expectation severities.
    """
    return (
        dlt.read_stream("dim_location_silver")
        .withColumn("township_con_lot", F.trim(F.col("township_con_lot")))
        .withColumn("utm_coordinates", F.trim(F.col("utm_coordinates")))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Well Type Dimension (SCD Type 2)

# COMMAND ----------

dlt.create_streaming_table(
    name="dim_well_type_silver",
    comment="Cleaned well type dimension with SCD Type 2 history tracking",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)

dlt.apply_changes(
    target="dim_well_type_silver",
    source="dim_well_type_bronze",
    keys=["well_type_id"],
    sequence_by="_ts",
    apply_as_deletes=F.expr("_op = 'D'"),
    except_column_list=["_op", "_ts", "_ingested_at", "_source_file", "_rescued_data"],
    stored_as_scd_type=2,
    track_history_column_list=["well_use_code", "screen_info", "well_use_desc"]
)

# COMMAND ----------

@dlt.table(
    name="dim_well_type_validated",
    comment="Well type dimension with data quality expectations"
)
@dlt.expect("well_type_id_not_null", "well_type_id IS NOT NULL")
@dlt.expect_or_warn("well_use_code_not_null", "well_use_code IS NOT NULL")
@dlt.expect("well_use_code_uppercase", "well_use_code = UPPER(well_use_code) OR well_use_code IS NULL")
def dim_well_type_validated():
    """
    Apply expectations and normalize well type data.
    """
    return (
        dlt.read_stream("dim_well_type_silver")
        .withColumn("well_use_code", F.upper(F.trim(F.col("well_use_code"))))
        .withColumn("well_use_desc", F.trim(F.col("well_use_desc")))
        .withColumn("screen_info", F.trim(F.col("screen_info")))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date Dimension (SCD Type 1)
# MAGIC 
# MAGIC Date dimension uses SCD Type 1 as it's typically a static reference table.

# COMMAND ----------

dlt.create_streaming_table(
    name="dim_date_silver",
    comment="Cleaned date dimension with SCD Type 1 (upsert only)",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)

dlt.apply_changes(
    target="dim_date_silver",
    source="dim_date_bronze",
    keys=["date_id"],
    sequence_by="_ts",
    apply_as_deletes=F.expr("_op = 'D'"),
    except_column_list=["_op", "_ts", "_ingested_at", "_source_file", "_rescued_data"],
    stored_as_scd_type=1  # Type 1: No history, just upsert
)

# COMMAND ----------

@dlt.table(
    name="dim_date_validated",
    comment="Date dimension with data quality expectations"
)
@dlt.expect("date_id_not_null", "date_id IS NOT NULL")
@dlt.expect_or_fail("valid_year", "year BETWEEN 1800 AND 2100")
@dlt.expect_or_fail("valid_month", "month BETWEEN 1 AND 12")
@dlt.expect_or_drop("contractor_code_not_empty", "contractor_code IS NULL OR LENGTH(TRIM(contractor_code)) > 0")
def dim_date_validated():
    """
    Apply strict expectations to date dimension.
    Uses expect_or_fail for critical date validation.
    """
    return (
        dlt.read_stream("dim_date_silver")
        .withColumn("contractor_code", F.trim(F.col("contractor_code")))
        .withColumn("year", F.col("year").cast("int"))
        .withColumn("month", F.col("month").cast("int"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Formation Dimension (SCD Type 2)

# COMMAND ----------

dlt.create_streaming_table(
    name="dim_formation_silver",
    comment="Cleaned geological formation dimension with SCD Type 2",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)

dlt.apply_changes(
    target="dim_formation_silver",
    source="dim_formation_bronze",
    keys=["formation_id"],
    sequence_by="_ts",
    apply_as_deletes=F.expr("_op = 'D'"),
    except_column_list=["_op", "_ts", "_ingested_at", "_source_file", "_rescued_data"],
    stored_as_scd_type=2,
    track_history_column_list=["formation_desc", "formation_type"]
)

# COMMAND ----------

@dlt.table(
    name="dim_formation_validated",
    comment="Formation dimension with data quality expectations"
)
@dlt.expect("formation_id_not_null", "formation_id IS NOT NULL")
@dlt.expect_or_warn("formation_desc_not_null", "formation_desc IS NOT NULL")
def dim_formation_validated():
    """
    Apply expectations to formation dimension.
    """
    return (
        dlt.read_stream("dim_formation_silver")
        .withColumn("formation_desc", F.trim(F.col("formation_desc")))
        .withColumn("formation_type", F.trim(F.col("formation_type")))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table - Silver Layer (SCD Type 1)
# MAGIC 
# MAGIC Fact table uses SCD Type 1 - new records are inserted, existing records are updated.

# COMMAND ----------

dlt.create_streaming_table(
    name="fact_well_construction_silver",
    comment="Cleaned well construction fact table with SCD Type 1",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)

dlt.apply_changes(
    target="fact_well_construction_silver",
    source="fact_well_construction_bronze",
    keys=["well_construction_id"],
    sequence_by="_ts",
    apply_as_deletes=F.expr("_op = 'D'"),
    except_column_list=["_op", "_ts", "_ingested_at", "_source_file", "_rescued_data"],
    stored_as_scd_type=1
)

# COMMAND ----------

@dlt.table(
    name="fact_well_construction_validated",
    comment="Well construction fact with comprehensive data quality expectations"
)
# Primary key expectation - CRITICAL
@dlt.expect_or_fail("pk_not_null", "well_construction_id IS NOT NULL")
# Foreign key expectations - IMPORTANT
@dlt.expect_or_warn("fk_location_not_null", "location_id IS NOT NULL")
@dlt.expect_or_warn("fk_well_type_not_null", "well_type_id IS NOT NULL")
@dlt.expect_or_warn("fk_date_not_null", "date_id IS NOT NULL")
@dlt.expect_or_warn("fk_formation_not_null", "formation_id IS NOT NULL")
# Data quality expectations
@dlt.expect("valid_casing_diameter", "casing_diameter IS NULL OR casing_diameter > 0")
@dlt.expect_or_drop("valid_well_identifier", "well_identifier IS NOT NULL AND LENGTH(TRIM(well_identifier)) > 0")
# Boolean field expectations
@dlt.expect("valid_has_water_data", "has_water_data IN (true, false) OR has_water_data IS NULL")
@dlt.expect("valid_has_pump_test", "has_pump_test IN (true, false) OR has_pump_test IS NULL")
def fact_well_construction_validated():
    """
    Apply comprehensive expectations to the fact table.
    
    Demonstrates:
    - expect_or_fail for critical constraints (PK not null)
    - expect_or_warn for foreign key warnings
    - expect_or_drop for data cleansing
    - expect for tracking metrics
    """
    return (
        dlt.read_stream("fact_well_construction_silver")
        .withColumn("well_identifier", F.trim(F.col("well_identifier")))
        .withColumn("water_level_info", F.trim(F.col("water_level_info")))
        .withColumn("pump_test_results", F.trim(F.col("pump_test_results")))
        # Cast numeric fields
        .withColumn("casing_diameter", F.col("casing_diameter").cast("double"))
        # Cast boolean fields
        .withColumn("has_water_data", F.col("has_water_data").cast("boolean"))
        .withColumn("has_pump_test", F.col("has_pump_test").cast("boolean"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Streaming Tables & Materialized Views Parity
# MAGIC 
# MAGIC For SQL-only teams, equivalent Databricks SQL syntax:
# MAGIC 
# MAGIC ```sql
# MAGIC -- Streaming Table with Expectations (similar to @dlt.table)
# MAGIC CREATE OR REFRESH STREAMING TABLE dim_location_silver (
# MAGIC     CONSTRAINT location_id_not_null EXPECT (location_id IS NOT NULL),
# MAGIC     CONSTRAINT valid_utm_easting EXPECT (utm_easting > 0) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT 'Cleaned location dimension'
# MAGIC AS SELECT 
# MAGIC     location_id,
# MAGIC     TRIM(township_con_lot) as township_con_lot,
# MAGIC     utm_coordinates,
# MAGIC     utm_zone,
# MAGIC     utm_easting,
# MAGIC     utm_northing
# MAGIC FROM STREAM(LIVE.dim_location_bronze);
# MAGIC 
# MAGIC -- Materialized View for aggregations
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW well_construction_summary
# MAGIC AS SELECT 
# MAGIC     date_id,
# MAGIC     COUNT(*) as total_wells,
# MAGIC     AVG(casing_diameter) as avg_casing_diameter
# MAGIC FROM LIVE.fact_well_construction_silver
# MAGIC GROUP BY date_id;
# MAGIC 
# MAGIC -- Apply Changes for CDC (SQL syntax)
# MAGIC CREATE OR REFRESH STREAMING TABLE dim_location_cdc;
# MAGIC 
# MAGIC APPLY CHANGES INTO LIVE.dim_location_cdc
# MAGIC FROM STREAM(LIVE.dim_location_bronze)
# MAGIC KEYS (location_id)
# MAGIC SEQUENCE BY _ts
# MAGIC STORED AS SCD TYPE 2;
# MAGIC ```
# MAGIC 
# MAGIC These SQL constructs integrate with the same SDP runtime and UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intentional Break Examples (For Demo)
# MAGIC 
# MAGIC ### Example 1: Disallowed Imperative Operation
# MAGIC 
# MAGIC Uncomment the code below to see the editor validation catch the error:
# MAGIC 
# MAGIC ```python
# MAGIC @dlt.table(name="bad_example")
# MAGIC def bad_example():
# MAGIC     # ERROR: spark.sql() is an imperative operation not allowed in @dlt.table
# MAGIC     spark.sql("SELECT 1")  # This will be flagged by the editor!
# MAGIC     return spark.read.table("some_table")
# MAGIC ```
# MAGIC 
# MAGIC ### Example 2: Schema Evolution Mismatch
# MAGIC 
# MAGIC To demonstrate schema mismatch, remove a required column from the source CDC file:
# MAGIC 1. Generate CDC file without `utm_coordinates` column
# MAGIC 2. Pipeline will show the schema evolution error in the DAG
# MAGIC 3. Remediate by: adjusting the expectation or using `mergeSchema`
# MAGIC 
# MAGIC ### Example 3: Expectation Failure
# MAGIC 
# MAGIC Insert a record that violates `expect_or_fail`:
# MAGIC ```json
# MAGIC {"date_id": 999, "year": 3000, "month": 13, "_op": "I", "_ts": "2024-01-01T00:00:00Z"}
# MAGIC ```
# MAGIC This will cause the pipeline to fail due to invalid year/month values.
