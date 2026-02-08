# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Ontario Oils Raw Data Ingestion

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# Landing zone paths - UPDATE THESE FOR YOUR ENVIRONMENT
LANDING_BASE = "/Volumes/erp-demonstrations/arc_dev/landing_zone"

# COMMAND ----------

@dlt.table(
    name="dim_location_bronze",
    comment="Raw location dimension from CDC files"
)
def dim_location_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{LANDING_BASE}/dim_location/_schema")
        .load(f"{LANDING_BASE}/dim_location")
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

@dlt.table(
    name="dim_well_type_bronze",
    comment="Raw well type dimension from CDC files"
)
def dim_well_type_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{LANDING_BASE}/dim_well_type/_schema")
        .load(f"{LANDING_BASE}/dim_well_type")
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

@dlt.table(
    name="dim_date_bronze",
    comment="Raw date dimension from CDC files"
)
def dim_date_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{LANDING_BASE}/dim_date/_schema")
        .load(f"{LANDING_BASE}/dim_date")
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

@dlt.table(
    name="dim_formation_bronze",
    comment="Raw formation dimension from CDC files"
)
def dim_formation_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{LANDING_BASE}/dim_formation/_schema")
        .load(f"{LANDING_BASE}/dim_formation")
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

@dlt.table(
    name="fact_well_construction_bronze",
    comment="Raw well construction fact from CDC files"
)
def fact_well_construction_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{LANDING_BASE}/fact_well_construction/_schema")
        .load(f"{LANDING_BASE}/fact_well_construction")
        .withColumn("_ingested_at", F.current_timestamp())
    )
