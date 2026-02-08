# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Ontario Oils Cleaned & Transformed Data

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Tables - Silver Layer with SCD Type 2

# COMMAND ----------

# dim_location - SCD Type 2
dlt.create_streaming_table(
    name="dim_location_silver",
    comment="Cleaned location dimension with SCD Type 2"
)

dlt.apply_changes(
    target="dim_location_silver",
    source="dim_location_bronze",
    keys=["location_id"],
    sequence_by="_ts",
    apply_as_deletes=F.expr("_op = 'D'"),
    except_column_list=["_op", "_ts", "_ingested_at"],
    stored_as_scd_type=2
)

# COMMAND ----------

# dim_well_type - SCD Type 2
dlt.create_streaming_table(
    name="dim_well_type_silver",
    comment="Cleaned well type dimension with SCD Type 2"
)

dlt.apply_changes(
    target="dim_well_type_silver",
    source="dim_well_type_bronze",
    keys=["well_type_id"],
    sequence_by="_ts",
    apply_as_deletes=F.expr("_op = 'D'"),
    except_column_list=["_op", "_ts", "_ingested_at"],
    stored_as_scd_type=2
)

# COMMAND ----------

# dim_date - SCD Type 1
dlt.create_streaming_table(
    name="dim_date_silver",
    comment="Cleaned date dimension with SCD Type 1"
)

dlt.apply_changes(
    target="dim_date_silver",
    source="dim_date_bronze",
    keys=["date_id"],
    sequence_by="_ts",
    apply_as_deletes=F.expr("_op = 'D'"),
    except_column_list=["_op", "_ts", "_ingested_at"],
    stored_as_scd_type=1
)

# COMMAND ----------

# dim_formation - SCD Type 2
dlt.create_streaming_table(
    name="dim_formation_silver",
    comment="Cleaned formation dimension with SCD Type 2"
)

dlt.apply_changes(
    target="dim_formation_silver",
    source="dim_formation_bronze",
    keys=["formation_id"],
    sequence_by="_ts",
    apply_as_deletes=F.expr("_op = 'D'"),
    except_column_list=["_op", "_ts", "_ingested_at"],
    stored_as_scd_type=2
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table - Silver Layer with SCD Type 1

# COMMAND ----------

dlt.create_streaming_table(
    name="fact_well_construction_silver",
    comment="Cleaned well construction fact with SCD Type 1"
)

dlt.apply_changes(
    target="fact_well_construction_silver",
    source="fact_well_construction_bronze",
    keys=["well_construction_id"],
    sequence_by="_ts",
    apply_as_deletes=F.expr("_op = 'D'"),
    except_column_list=["_op", "_ts", "_ingested_at"],
    stored_as_scd_type=1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validated Tables with Expectations
# MAGIC 
# MAGIC Using Materialized Views (not streaming tables) because SCD Type 2 tables 
# MAGIC perform MERGE operations which are not append-only.

# COMMAND ----------

@dlt.view(
    name="dim_location_validated",
    comment="Location dimension with expectations (Materialized View)"
)
@dlt.expect("location_id_not_null", "location_id IS NOT NULL")
@dlt.expect("valid_utm_easting", "utm_easting > 0 OR utm_easting IS NULL")
def dim_location_validated():
    # Use dlt.read() for batch read from SCD Type 2 table
    return dlt.read("dim_location_silver")

# COMMAND ----------

@dlt.view(
    name="dim_date_validated",
    comment="Date dimension with expectations (Materialized View)"
)
@dlt.expect("date_id_not_null", "date_id IS NOT NULL")
@dlt.expect_or_fail("valid_year", "year BETWEEN 1800 AND 2100")
@dlt.expect_or_fail("valid_month", "month BETWEEN 1 AND 12")
def dim_date_validated():
    return dlt.read("dim_date_silver")

# COMMAND ----------

@dlt.view(
    name="fact_well_construction_validated",
    comment="Fact table with expectations (Materialized View)"
)
@dlt.expect_or_fail("pk_not_null", "well_construction_id IS NOT NULL")
@dlt.expect("fk_location_not_null", "location_id IS NOT NULL")
@dlt.expect_or_drop("valid_well_identifier", "well_identifier IS NOT NULL")
def fact_well_construction_validated():
    return dlt.read("fact_well_construction_silver")
