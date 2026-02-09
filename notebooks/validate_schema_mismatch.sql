-- Schema Mismatch Validation Queries
-- Run these in Databricks SQL Editor after running generate_schema_mismatch() and the pipeline

-- =============================================================================
-- QUERY 1: Check Bronze Table for Schema Evolution
-- =============================================================================
-- Auto Loader adds a _rescued_data column for data that doesn't match the schema

SELECT 
    location_id,
    township_con_lot,
    utm_coordinates,
    utm_easting,
    _rescued_data,
    _ingested_at
FROM `erp-demonstrations`.arc_dev.dim_location_bronze
WHERE location_id >= 20 OR _rescued_data IS NOT NULL
ORDER BY _ingested_at DESC;

-- Expected observations:
-- • location_id=20: utm_coordinates is NULL (missing from source)
-- • location_id=21: Record exists, extra_info may be in _rescued_data
-- • location_id=22: utm_easting may be NULL or wrong, string value in _rescued_data
-- • location_id=NULL: Wrong schema record, most data in _rescued_data


-- =============================================================================
-- QUERY 2: Check for Rescued Data (Schema Mismatches)
-- =============================================================================
-- _rescued_data contains JSON of fields that couldn't be parsed into the schema

SELECT 
    location_id,
    township_con_lot,
    _rescued_data,
    _ingested_at
FROM `erp-demonstrations`.arc_dev.dim_location_bronze
WHERE _rescued_data IS NOT NULL
ORDER BY _ingested_at DESC;

-- If you see data here, it means Auto Loader "rescued" fields that didn't fit the schema


-- =============================================================================
-- QUERY 3: Check Schema Evolution History
-- =============================================================================
-- List the schema checkpoint files to see schema evolution

-- In a Python cell, run:
-- display(dbutils.fs.ls("/Volumes/erp-demonstrations/arc_dev/landing_zone/dim_location/_schema"))


-- =============================================================================
-- QUERY 4: Compare Bronze vs Silver (Schema Issues Filtered Out)
-- =============================================================================
-- Silver table should have clean data; bad records filtered by apply_changes

SELECT 
    'Bronze' as layer,
    COUNT(*) as total_records,
    COUNT(CASE WHEN location_id IS NULL THEN 1 END) as null_ids,
    COUNT(CASE WHEN _rescued_data IS NOT NULL THEN 1 END) as rescued_records
FROM `erp-demonstrations`.arc_dev.dim_location_bronze
WHERE location_id >= 20 OR _rescued_data IS NOT NULL

UNION ALL

SELECT 
    'Silver' as layer,
    COUNT(*) as total_records,
    0 as null_ids,
    0 as rescued_records
FROM `erp-demonstrations`.arc_dev.dim_location_silver
WHERE location_id >= 20;


-- =============================================================================
-- QUERY 5: Check Actual Schema of Bronze Table
-- =============================================================================
-- See all columns including any that were added by schema evolution

DESCRIBE `erp-demonstrations`.arc_dev.dim_location_bronze;


-- =============================================================================
-- QUERY 6: View the Type Mismatch Record
-- =============================================================================
-- The utm_easting="five-hundred-thousand" record

SELECT 
    location_id,
    township_con_lot,
    utm_easting,
    typeof(utm_easting) as utm_easting_type,
    _rescued_data
FROM `erp-demonstrations`.arc_dev.dim_location_bronze
WHERE location_id = 22 OR township_con_lot LIKE '%Township C%';


-- =============================================================================
-- DEMO TALKING POINTS
-- =============================================================================
/*
1. AUTO LOADER SCHEMA HANDLING:
   - With schemaEvolutionMode='addNewColumns': New columns are added automatically
   - With inferColumnTypes=true: Types are inferred from data
   - _rescued_data column catches anything that doesn't fit

2. COMPARE TO TRADITIONAL:
   Traditional Spark:
   ```python
   try:
       df = spark.read.json(path)
       expected_schema = StructType([...])
       if df.schema != expected_schema:
           # Complex error handling
           # Manual schema migration
           # Risk of data loss
   except:
       # Error handling code
   ```
   
   DLT + Auto Loader:
   ```python
   .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
   .option("cloudFiles.inferColumnTypes", "true")
   # Done! Schema changes handled automatically
   ```

3. RECOVERY OPTIONS:
   - "rescue" mode: Bad data goes to _rescued_data, nothing lost
   - "addNewColumns" mode: New columns added to schema
   - "failOnNewColumns" mode: Strict - fails on any schema change
   
4. AUDIT TRAIL:
   - Schema history stored in _schema checkpoint folder
   - Can see exactly when schema changed
   - Full lineage in Unity Catalog
*/
