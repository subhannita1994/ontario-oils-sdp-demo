# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Ontario Oils Raw Data Ingestion
# MAGIC 
# MAGIC This notebook defines streaming tables for raw CDC data ingestion using Auto Loader.
# MAGIC 
# MAGIC **Key SDP Coding Rules:**
# MAGIC 1. Functions decorated with `@dlt.table` must return a DataFrame
# MAGIC 2. Avoid imperative/non-DataFrame side effects (e.g., spark.sql(), print())
# MAGIC 3. Use spark.readStream for streaming sources
# MAGIC 4. The editor validates these rules automatically
# MAGIC 
# MAGIC **Tables:**
# MAGIC - dim_location_bronze
# MAGIC - dim_well_type_bronze
# MAGIC - dim_date_bronze
# MAGIC - dim_formation_bronze
# MAGIC - fact_well_construction_bronze

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Pipeline configuration is passed from databricks.yml via spark conf

# COMMAND ----------

def get_config(key: str) -> str:
    """Get configuration value from pipeline settings."""
    return spark.conf.get(key)

def get_landing_path(table_name: str) -> str:
    """Construct the landing zone path for a table's CDC files."""
    catalog = get_config("catalog")
    schema = get_config("schema")
    volume = get_config("landing_volume")
    return f"/Volumes/{catalog}/{schema}/{volume}/{table_name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader Configuration
# MAGIC 
# MAGIC Auto Loader (cloudFiles) provides:
# MAGIC - Automatic schema inference
# MAGIC - Exactly-once file processing with checkpointing
# MAGIC - Scalable file discovery using notifications or directory listing
# MAGIC - Support for JSON, CSV, Parquet, Avro, and more

# COMMAND ----------

def create_autoloader_stream(table_name: str, file_format: str = "json"):
    """
    Create an Auto Loader streaming DataFrame for CDC files.
    
    Args:
        table_name: Name of the source table (used for path construction)
        file_format: File format (json, parquet, csv, avro)
    
    Returns:
        Streaming DataFrame with raw CDC data plus metadata columns
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{get_landing_path(table_name)}/_schema")
        # Schema evolution options
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("mergeSchema", "true")
        # Include metadata for lineage
        .option("cloudFiles.includeExistingFiles", "true")
        .load(get_landing_path(table_name))
        # Add ingestion metadata
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Tables - Bronze Layer

# COMMAND ----------

@dlt.table(
    name="dim_location_bronze",
    comment="Raw location dimension data from CDC files. Contains geographic coordinates and township information.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def dim_location_bronze():
    """
    Ingest raw location dimension CDC data.
    
    Expected columns: location_id, township_con_lot, utm_coordinates, 
                     utm_zone, utm_easting, utm_northing
    CDC columns: _op (I/U/D), _ts (timestamp)
    """
    return create_autoloader_stream("dim_location")

# COMMAND ----------

@dlt.table(
    name="dim_well_type_bronze",
    comment="Raw well type dimension data from CDC files. Contains well usage codes and descriptions.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def dim_well_type_bronze():
    """
    Ingest raw well type dimension CDC data.
    
    Expected columns: well_type_id, well_use_code, screen_info, well_use_desc
    CDC columns: _op (I/U/D), _ts (timestamp)
    """
    return create_autoloader_stream("dim_well_type")

# COMMAND ----------

@dlt.table(
    name="dim_date_bronze",
    comment="Raw date dimension data from CDC files. Contains year, month, and contractor information.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def dim_date_bronze():
    """
    Ingest raw date dimension CDC data.
    
    Expected columns: date_id, year, month, contractor_code
    CDC columns: _op (I/U/D), _ts (timestamp)
    """
    return create_autoloader_stream("dim_date")

# COMMAND ----------

@dlt.table(
    name="dim_formation_bronze",
    comment="Raw geological formation dimension data from CDC files.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def dim_formation_bronze():
    """
    Ingest raw formation dimension CDC data.
    
    Expected columns: formation_id, formation_desc, formation_type
    CDC columns: _op (I/U/D), _ts (timestamp)
    """
    return create_autoloader_stream("dim_formation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table - Bronze Layer

# COMMAND ----------

@dlt.table(
    name="fact_well_construction_bronze",
    comment="Raw well construction fact data from CDC files. Contains measurements and foreign keys to dimensions.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def fact_well_construction_bronze():
    """
    Ingest raw well construction fact CDC data.
    
    Expected columns: well_construction_id, location_id, well_type_id, date_id,
                     formation_id, well_identifier, casing_diameter, water_level_info,
                     pump_test_results, has_water_data, has_pump_test
    CDC columns: _op (I/U/D), _ts (timestamp)
    """
    return create_autoloader_stream("fact_well_construction")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Streaming Tables Parity (Reference)
# MAGIC 
# MAGIC For SQL-only teams, the equivalent Databricks SQL syntax would be:
# MAGIC 
# MAGIC ```sql
# MAGIC -- Create streaming table with Auto Loader
# MAGIC CREATE OR REFRESH STREAMING TABLE dim_location_bronze
# MAGIC COMMENT 'Raw location dimension data from CDC files'
# MAGIC TBLPROPERTIES ('quality' = 'bronze')
# MAGIC AS SELECT 
# MAGIC     *,
# MAGIC     current_timestamp() AS _ingested_at,
# MAGIC     _metadata.file_path AS _source_file
# MAGIC FROM STREAM read_files(
# MAGIC     '/Volumes/erp-demonstrations/arc_dev/landing_zone/dim_location',
# MAGIC     format => 'json',
# MAGIC     inferColumnTypes => true
# MAGIC );
# MAGIC 
# MAGIC -- Materialized View example (for aggregations)
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW dim_location_count
# MAGIC AS SELECT COUNT(*) as total_locations FROM dim_location_bronze;
# MAGIC ```
# MAGIC 
# MAGIC This SQL syntax integrates seamlessly with SDP Python pipelines.
