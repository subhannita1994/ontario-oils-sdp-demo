# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Contract Tables - Reconciliation & Observability
# MAGIC 
# MAGIC This notebook creates metrics tables that are materialized as first-class pipeline outputs.
# MAGIC These tables provide:
# MAGIC - **Ingestion counts** (bronze layer)
# MAGIC - **Applied change counts** (silver layer)
# MAGIC - **Dropped/filtered counts** (quality metrics)
# MAGIC - **Expectation violations** (schema and business rule violations)
# MAGIC - **Watermark positions** (for tracking pipeline progress)
# MAGIC 
# MAGIC Updated on every pipeline run - downstream consumers can use these as contract tables.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from functools import reduce

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Metrics
# MAGIC 
# MAGIC Captures for each bronze table:
# MAGIC - Total record count
# MAGIC - Records with schema issues (`_rescued_data IS NOT NULL`)
# MAGIC - Operation breakdown (Insert/Update/Delete counts)
# MAGIC - Watermark: `MAX(_ts)` and `MAX(_ingested_at)`

# COMMAND ----------

@dlt.table(
    name="pipeline_bronze_metrics",
    comment="Bronze layer ingestion metrics - updated every pipeline run"
)
def pipeline_bronze_metrics():
    """
    Aggregates metrics from all bronze tables into a single contract table.
    Tracks ingestion counts, operation types, schema violations, and watermarks.
    """
    bronze_tables = [
        ("dim_location_bronze", "location_id"),
        ("dim_date_bronze", "date_id"),
        ("dim_well_type_bronze", "well_type_id"),
        ("dim_formation_bronze", "formation_id"),
        ("fact_well_construction_bronze", "well_construction_id"),
    ]
    
    metrics_dfs = []
    
    for table_name, key_column in bronze_tables:
        df = dlt.read(table_name)
        
        metrics = df.agg(
            F.lit(table_name).alias("table_name"),
            F.lit("bronze").alias("layer"),
            F.count("*").alias("total_records"),
            F.countDistinct(key_column).alias("unique_keys"),
            F.sum(F.when(F.col("_rescued_data").isNotNull(), 1).otherwise(0)).alias("schema_violations"),
            F.sum(F.when(F.col("_op") == "I", 1).otherwise(0)).alias("insert_count"),
            F.sum(F.when(F.col("_op") == "U", 1).otherwise(0)).alias("update_count"),
            F.sum(F.when(F.col("_op") == "D", 1).otherwise(0)).alias("delete_count"),
            F.max("_ts").alias("watermark_ts"),
            F.max("_ingested_at").alias("watermark_ingested"),
            F.current_timestamp().alias("snapshot_at")
        )
        metrics_dfs.append(metrics)
    
    # Union all metrics dataframes
    return reduce(lambda a, b: a.unionByName(b), metrics_dfs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Metrics
# MAGIC 
# MAGIC Captures for each silver table:
# MAGIC - Total record count
# MAGIC - Current records (for SCD Type 2: `__END_AT IS NULL`)
# MAGIC - Historical records (for SCD Type 2: `__END_AT IS NOT NULL`)
# MAGIC - Unique keys count
# MAGIC - SCD type indicator
# MAGIC - Watermark: `MAX(__START_AT)`

# COMMAND ----------

@dlt.table(
    name="pipeline_silver_metrics",
    comment="Silver layer metrics with SCD tracking - updated every pipeline run"
)
def pipeline_silver_metrics():
    """
    Aggregates metrics from all silver tables into a single contract table.
    Tracks record counts, SCD Type 2 history, and watermarks.
    """
    # SCD Type 2 tables have __START_AT and __END_AT columns
    scd_type2_tables = [
        ("dim_location_silver", "location_id", 2),
        ("dim_well_type_silver", "well_type_id", 2),
        ("dim_formation_silver", "formation_id", 2),
    ]
    
    # SCD Type 1 tables don't have history columns
    scd_type1_tables = [
        ("dim_date_silver", "date_id", 1),
        ("fact_well_construction_silver", "well_construction_id", 1),
    ]
    
    metrics_dfs = []
    
    # Process SCD Type 2 tables
    for table_name, key_column, scd_type in scd_type2_tables:
        df = dlt.read(table_name)
        
        metrics = df.agg(
            F.lit(table_name).alias("table_name"),
            F.lit("silver").alias("layer"),
            F.lit(scd_type).alias("scd_type"),
            F.count("*").alias("total_records"),
            F.countDistinct(key_column).alias("unique_keys"),
            # Current records: __END_AT is NULL
            F.sum(F.when(F.col("__END_AT").isNull(), 1).otherwise(0)).alias("current_records"),
            # Historical records: __END_AT is NOT NULL
            F.sum(F.when(F.col("__END_AT").isNotNull(), 1).otherwise(0)).alias("historical_records"),
            F.max("__START_AT").alias("watermark_start_at"),
            F.current_timestamp().alias("snapshot_at")
        )
        metrics_dfs.append(metrics)
    
    # Process SCD Type 1 tables
    for table_name, key_column, scd_type in scd_type1_tables:
        df = dlt.read(table_name)
        
        metrics = df.agg(
            F.lit(table_name).alias("table_name"),
            F.lit("silver").alias("layer"),
            F.lit(scd_type).alias("scd_type"),
            F.count("*").alias("total_records"),
            F.countDistinct(key_column).alias("unique_keys"),
            # For SCD Type 1, all records are "current"
            F.count("*").alias("current_records"),
            F.lit(0).cast("long").alias("historical_records"),
            # SCD Type 1 tables may not have __START_AT, use NULL
            F.lit(None).cast("timestamp").alias("watermark_start_at"),
            F.current_timestamp().alias("snapshot_at")
        )
        metrics_dfs.append(metrics)
    
    return reduce(lambda a, b: a.unionByName(b), metrics_dfs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Metrics
# MAGIC 
# MAGIC Captures quality indicators:
# MAGIC - Records with rescued data (schema violations)
# MAGIC - NULL key violations
# MAGIC - Bronze to Silver record flow analysis
# MAGIC - Quality score calculation

# COMMAND ----------

@dlt.table(
    name="pipeline_quality_metrics",
    comment="Data quality metrics across pipeline - updated every pipeline run"
)
def pipeline_quality_metrics():
    """
    Tracks data quality indicators per table pair (bronze -> silver).
    Calculates quality scores and identifies potential issues.
    """
    table_pairs = [
        ("dim_location_bronze", "dim_location_silver", "location_id"),
        ("dim_date_bronze", "dim_date_silver", "date_id"),
        ("dim_well_type_bronze", "dim_well_type_silver", "well_type_id"),
        ("dim_formation_bronze", "dim_formation_silver", "formation_id"),
        ("fact_well_construction_bronze", "fact_well_construction_silver", "well_construction_id"),
    ]
    
    metrics_dfs = []
    
    for bronze_table, silver_table, key_column in table_pairs:
        bronze_df = dlt.read(bronze_table)
        silver_df = dlt.read(silver_table)
        
        # Bronze metrics
        bronze_metrics = bronze_df.agg(
            F.count("*").alias("bronze_total"),
            F.countDistinct(key_column).alias("bronze_unique_keys"),
            F.sum(F.when(F.col("_rescued_data").isNotNull(), 1).otherwise(0)).alias("schema_violations"),
            F.sum(F.when(F.col(key_column).isNull(), 1).otherwise(0)).alias("null_key_violations"),
            F.sum(F.when(F.col("_op") == "D", 1).otherwise(0)).alias("delete_operations"),
        ).collect()[0]
        
        # Silver metrics (current records only for comparison)
        silver_metrics = silver_df.agg(
            F.count("*").alias("silver_total"),
            F.countDistinct(key_column).alias("silver_unique_keys"),
        ).collect()[0]
        
        # Calculate quality score
        # Quality score = (records without issues) / (total records)
        total_issues = bronze_metrics["schema_violations"] + bronze_metrics["null_key_violations"]
        total_records = bronze_metrics["bronze_total"]
        quality_score = 1.0 if total_records == 0 else (total_records - total_issues) / total_records
        
        # Create metrics row
        metrics = spark.createDataFrame([{
            "table_pair": f"{bronze_table} -> {silver_table}",
            "bronze_table": bronze_table,
            "silver_table": silver_table,
            "bronze_total_records": bronze_metrics["bronze_total"],
            "bronze_unique_keys": bronze_metrics["bronze_unique_keys"],
            "silver_total_records": silver_metrics["silver_total"],
            "silver_unique_keys": silver_metrics["silver_unique_keys"],
            "schema_violations": bronze_metrics["schema_violations"],
            "null_key_violations": bronze_metrics["null_key_violations"],
            "delete_operations": bronze_metrics["delete_operations"],
            "quality_score": round(quality_score, 4),
            "snapshot_at": F.current_timestamp(),
        }])
        
        metrics_dfs.append(metrics)
    
    result = reduce(lambda a, b: a.unionByName(b), metrics_dfs)
    return result.withColumn("snapshot_at", F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Contract Summary
# MAGIC 
# MAGIC Unified view combining all metrics for downstream consumers.
# MAGIC This is the primary table that downstream applications should query
# MAGIC to verify data quality and completeness before consuming data.

# COMMAND ----------

@dlt.table(
    name="pipeline_contract_summary",
    comment="Unified pipeline contract table for downstream consumers - updated every pipeline run"
)
def pipeline_contract_summary():
    """
    Combines bronze metrics, silver metrics, and quality metrics into a single
    unified view. Downstream consumers can query this table to verify:
    - Data completeness (record counts)
    - Data quality (quality scores, violation counts)
    - Pipeline freshness (watermarks, snapshot timestamps)
    """
    bronze = dlt.read("pipeline_bronze_metrics")
    silver = dlt.read("pipeline_silver_metrics")
    quality = dlt.read("pipeline_quality_metrics")
    
    # Prepare bronze summary
    bronze_summary = bronze.select(
        F.col("table_name"),
        F.col("layer"),
        F.col("total_records"),
        F.col("unique_keys"),
        F.lit(None).cast("long").alias("current_records"),
        F.lit(None).cast("long").alias("historical_records"),
        F.lit(None).cast("int").alias("scd_type"),
        F.col("schema_violations"),
        F.col("insert_count"),
        F.col("update_count"),
        F.col("delete_count"),
        F.col("watermark_ts"),
        F.col("watermark_ingested"),
        F.lit(None).cast("timestamp").alias("watermark_start_at"),
        F.col("snapshot_at")
    )
    
    # Prepare silver summary
    silver_summary = silver.select(
        F.col("table_name"),
        F.col("layer"),
        F.col("total_records"),
        F.col("unique_keys"),
        F.col("current_records"),
        F.col("historical_records"),
        F.col("scd_type"),
        F.lit(None).cast("long").alias("schema_violations"),
        F.lit(None).cast("long").alias("insert_count"),
        F.lit(None).cast("long").alias("update_count"),
        F.lit(None).cast("long").alias("delete_count"),
        F.lit(None).cast("timestamp").alias("watermark_ts"),
        F.lit(None).cast("timestamp").alias("watermark_ingested"),
        F.col("watermark_start_at"),
        F.col("snapshot_at")
    )
    
    # Union bronze and silver summaries
    combined = bronze_summary.unionByName(silver_summary)
    
    # Add quality scores from quality metrics
    quality_scores = quality.select(
        F.col("bronze_table").alias("table_name"),
        F.col("quality_score")
    ).union(
        quality.select(
            F.col("silver_table").alias("table_name"),
            F.col("quality_score")
        )
    )
    
    # Join with quality scores
    result = combined.join(
        quality_scores,
        on="table_name",
        how="left"
    )
    
    return result.select(
        "table_name",
        "layer",
        "scd_type",
        "total_records",
        "unique_keys",
        "current_records",
        "historical_records",
        "schema_violations",
        "insert_count",
        "update_count", 
        "delete_count",
        "quality_score",
        "watermark_ts",
        "watermark_ingested",
        "watermark_start_at",
        "snapshot_at"
    )
