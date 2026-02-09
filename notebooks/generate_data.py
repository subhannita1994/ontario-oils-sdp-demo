# Databricks notebook source
# MAGIC %md
# MAGIC # Generate CDC Test Data
# MAGIC 
# MAGIC Run this notebook to generate sample CDC data directly to the Unity Catalog Volume.
# MAGIC 
# MAGIC **Usage:**
# MAGIC - Run "Initial Load" first to populate base data
# MAGIC - Then run other sections to test specific scenarios

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import json
import os
from datetime import datetime, timedelta
import random

# Target volume path
VOLUME_PATH = "/Volumes/erp-demonstrations/arc_dev/landing_zone"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data Definitions

# COMMAND ----------

LOCATIONS = [
    {"location_id": 1, "township_con_lot": "Chatham-Kent Lot 5", "utm_coordinates": "17T 456789 4712345", "utm_zone": 17, "utm_easting": 456789, "utm_northing": 4712345},
    {"location_id": 2, "township_con_lot": "Lambton County Lot 12", "utm_coordinates": "17T 423456 4756789", "utm_zone": 17, "utm_easting": 423456, "utm_northing": 4756789},
    {"location_id": 3, "township_con_lot": "Essex County Lot 8", "utm_coordinates": "17T 398765 4698765", "utm_zone": 17, "utm_easting": 398765, "utm_northing": 4698765},
    {"location_id": 4, "township_con_lot": "Middlesex Lot 22", "utm_coordinates": "17T 478901 4789012", "utm_zone": 17, "utm_easting": 478901, "utm_northing": 4789012},
    {"location_id": 5, "township_con_lot": "Elgin County Lot 3", "utm_coordinates": "17T 501234 4723456", "utm_zone": 17, "utm_easting": 501234, "utm_northing": 4723456},
]

WELL_TYPES = [
    {"well_type_id": 1, "well_use_code": "OIL", "screen_info": "Perforated casing 100-150m", "well_use_desc": "Oil production well"},
    {"well_type_id": 2, "well_use_code": "GAS", "screen_info": "Open hole completion", "well_use_desc": "Natural gas production"},
    {"well_type_id": 3, "well_use_code": "INJ", "screen_info": "Slotted liner 200-250m", "well_use_desc": "Injection well for enhanced recovery"},
    {"well_type_id": 4, "well_use_code": "OBS", "screen_info": "Open hole monitoring", "well_use_desc": "Observation/monitoring well"},
    {"well_type_id": 5, "well_use_code": "DIS", "screen_info": "Cased and abandoned", "well_use_desc": "Disposal well"},
]

DATES = [
    {"date_id": 1, "year": 2020, "month": 1, "contractor_code": "ABC123"},
    {"date_id": 2, "year": 2020, "month": 6, "contractor_code": "XYZ789"},
    {"date_id": 3, "year": 2021, "month": 3, "contractor_code": "DEF456"},
    {"date_id": 4, "year": 2022, "month": 9, "contractor_code": "GHI012"},
    {"date_id": 5, "year": 2023, "month": 12, "contractor_code": "JKL345"},
]

FORMATIONS = [
    {"formation_id": 1, "formation_desc": "Dundee Formation", "formation_type": "Limestone"},
    {"formation_id": 2, "formation_desc": "Bois Blanc Formation", "formation_type": "Dolomite"},
    {"formation_id": 3, "formation_desc": "Bass Islands Formation", "formation_type": "Dolomite"},
    {"formation_id": 4, "formation_desc": "Guelph Formation", "formation_type": "Reef dolomite"},
    {"formation_id": 5, "formation_desc": "Niagara Formation", "formation_type": "Limestone"},
]

WELL_CONSTRUCTIONS = [
    {"well_construction_id": 1, "location_id": 1, "well_type_id": 1, "date_id": 1, "formation_id": 1, 
     "well_identifier": "ONT-2020-0001", "casing_diameter": 139.7, "water_level_info": "Static: 45m", 
     "pump_test_results": "Q=50 m3/d", "has_water_data": True, "has_pump_test": True},
    {"well_construction_id": 2, "location_id": 2, "well_type_id": 2, "date_id": 2, "formation_id": 2,
     "well_identifier": "ONT-2020-0002", "casing_diameter": 177.8, "water_level_info": "N/A",
     "pump_test_results": "N/A", "has_water_data": False, "has_pump_test": False},
    {"well_construction_id": 3, "location_id": 3, "well_type_id": 3, "date_id": 3, "formation_id": 3,
     "well_identifier": "ONT-2021-0001", "casing_diameter": 244.5, "water_level_info": "Static: 62m",
     "pump_test_results": "Q=120 m3/d", "has_water_data": True, "has_pump_test": True},
    {"well_construction_id": 4, "location_id": 4, "well_type_id": 4, "date_id": 4, "formation_id": 4,
     "well_identifier": "ONT-2022-0001", "casing_diameter": 114.3, "water_level_info": "Static: 38m",
     "pump_test_results": "Monitoring only", "has_water_data": True, "has_pump_test": False},
    {"well_construction_id": 5, "location_id": 5, "well_type_id": 5, "date_id": 5, "formation_id": 5,
     "well_identifier": "ONT-2023-0001", "casing_diameter": 193.7, "water_level_info": "N/A",
     "pump_test_results": "N/A", "has_water_data": False, "has_pump_test": False},
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_timestamp(offset_minutes=0):
    """Generate ISO timestamp."""
    ts = datetime(2024, 1, 1) + timedelta(minutes=offset_minutes)
    return ts.isoformat() + "Z"

def add_cdc_fields(record, op, ts_offset):
    """Add CDC operation and timestamp fields."""
    cdc_record = record.copy()
    cdc_record["_op"] = op  # I=Insert, U=Update, D=Delete
    cdc_record["_ts"] = get_timestamp(ts_offset)
    return cdc_record

def write_to_volume(table_name, records, suffix=""):
    """Write records as newline-delimited JSON to the volume."""
    table_path = f"{VOLUME_PATH}/{table_name}"
    
    # Create directory if it doesn't exist
    dbutils.fs.mkdirs(table_path)
    
    # Generate filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"batch{suffix}_{timestamp}.json"
    filepath = f"{table_path}/{filename}"
    
    # Write as newline-delimited JSON
    content = "\n".join(json.dumps(r) for r in records)
    dbutils.fs.put(filepath, content, overwrite=True)
    
    print(f"✓ Wrote {len(records)} records to {filepath}")
    return filepath

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Initial Load (Run First!)
# MAGIC 
# MAGIC This generates the base data for all tables.

# COMMAND ----------

def generate_initial_load():
    """Generate initial data load (all inserts)."""
    print("=== Generating Initial Load ===\n")
    
    # Dimensions
    write_to_volume("dim_location", 
                    [add_cdc_fields(r, "I", i*10) for i, r in enumerate(LOCATIONS)],
                    "_initial")
    
    write_to_volume("dim_well_type", 
                    [add_cdc_fields(r, "I", i*10) for i, r in enumerate(WELL_TYPES)],
                    "_initial")
    
    write_to_volume("dim_date", 
                    [add_cdc_fields(r, "I", i*10) for i, r in enumerate(DATES)],
                    "_initial")
    
    write_to_volume("dim_formation", 
                    [add_cdc_fields(r, "I", i*10) for i, r in enumerate(FORMATIONS)],
                    "_initial")
    
    # Fact
    write_to_volume("fact_well_construction", 
                    [add_cdc_fields(r, "I", i*10) for i, r in enumerate(WELL_CONSTRUCTIONS)],
                    "_initial")
    
    print("\n✅ Initial load complete! Run the pipeline to ingest.")

# Uncomment and run:
# generate_initial_load()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate Updates (SCD Type 2 Demo)
# MAGIC 
# MAGIC This updates existing records to demonstrate SCD Type 2 history tracking.

# COMMAND ----------

def generate_updates():
    """Generate updates to demonstrate SCD Type 2."""
    print("=== Generating Updates ===\n")
    
    # Update location (will create new version in SCD Type 2)
    updated_location = LOCATIONS[0].copy()
    updated_location["utm_easting"] = 456800  # Changed!
    updated_location["township_con_lot"] = "Chatham-Kent Lot 5 (Updated)"
    write_to_volume("dim_location", 
                    [add_cdc_fields(updated_location, "U", 100)],
                    "_update")
    
    # Update well type description
    updated_well_type = WELL_TYPES[1].copy()
    updated_well_type["well_use_desc"] = "Natural gas production - Updated"
    write_to_volume("dim_well_type", 
                    [add_cdc_fields(updated_well_type, "U", 110)],
                    "_update")
    
    # New fact record
    new_well = {
        "well_construction_id": 6,
        "location_id": 1, "well_type_id": 2, "date_id": 5, "formation_id": 2,
        "well_identifier": "ONT-2024-0001", "casing_diameter": 168.3,
        "water_level_info": "Static: 55m", "pump_test_results": "Q=75 m3/d",
        "has_water_data": True, "has_pump_test": True
    }
    write_to_volume("fact_well_construction", 
                    [add_cdc_fields(new_well, "I", 120)],
                    "_new")
    
    print("\n✅ Updates generated! Run the pipeline to see SCD Type 2 history.")

# Uncomment and run:
# generate_updates()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate Out-of-Order Events (AUTO CDC Demo)
# MAGIC 
# MAGIC This generates events with timestamps that arrive out of order to demonstrate how `sequence_by` handles them.

# COMMAND ----------

def generate_out_of_order():
    """Generate out-of-order events to demo AUTO CDC handling."""
    print("=== Generating Out-of-Order Events ===\n")
    
    records = []
    
    # This update has OLD timestamp but arrives NOW
    late_update = LOCATIONS[0].copy()
    late_update["township_con_lot"] = "Chatham-Kent Lot 5 (Late Arrival)"
    records.append(add_cdc_fields(late_update, "U", 50))  # timestamp=50 (earlier than update at 100)
    
    # This is the "correct" latest update
    final_update = LOCATIONS[0].copy()
    final_update["township_con_lot"] = "Chatham-Kent Lot 5 (Final)"
    final_update["utm_easting"] = 456850
    records.append(add_cdc_fields(final_update, "U", 200))
    
    write_to_volume("dim_location", records, "_out_of_order")
    
    print("\n✅ Out-of-order events generated!")
    print("The late arrival (ts=50) will be correctly ordered BEFORE the update at ts=100")
    print("because apply_changes() uses sequence_by='_ts' to determine order.")

# Uncomment and run:
# generate_out_of_order()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generate Expectation Violations (Data Quality Demo)
# MAGIC 
# MAGIC This generates records that violate expectations to demo different behaviors.

# COMMAND ----------

def generate_expectation_violations():
    """Generate records that violate expectations."""
    print("=== Generating Expectation Violations ===\n")
    
    # Violate expect_or_fail (invalid year/month in dim_date)
    print("⚠️  dim_date: Invalid year=3000, month=13 (will FAIL pipeline)")
    invalid_date = {
        "date_id": 999,
        "year": 3000,   # Violates: year BETWEEN 1800 AND 2100
        "month": 13,    # Violates: month BETWEEN 1 AND 12
        "contractor_code": "INVALID"
    }
    write_to_volume("dim_date", 
                    [add_cdc_fields(invalid_date, "I", 500)],
                    "_expect_fail")
    
    # Violate expect (null FK in fact - will be tracked but not fail)
    print("📊 fact: NULL location_id (will be TRACKED in metrics)")
    null_fk = {
        "well_construction_id": 100,
        "location_id": None,  # Violates FK expectation
        "well_type_id": 1, "date_id": 1, "formation_id": 1,
        "well_identifier": "ONT-TEST-WARN", "casing_diameter": 150.0,
        "water_level_info": "Test", "pump_test_results": "Test",
        "has_water_data": True, "has_pump_test": False
    }
    write_to_volume("fact_well_construction", 
                    [add_cdc_fields(null_fk, "I", 510)],
                    "_expect_track")
    
    # Violate expect_or_drop (empty well_identifier - will be DROPPED)
    print("🗑️  fact: Empty well_identifier (will be DROPPED)")
    empty_id = {
        "well_construction_id": 101,
        "location_id": 1, "well_type_id": 1, "date_id": 1, "formation_id": 1,
        "well_identifier": None,  # Violates: well_identifier IS NOT NULL
        "casing_diameter": 100.0,
        "water_level_info": "Test", "pump_test_results": "Test",
        "has_water_data": False, "has_pump_test": False
    }
    write_to_volume("fact_well_construction", 
                    [add_cdc_fields(empty_id, "I", 520)],
                    "_expect_drop")
    
    print("\n✅ Expectation violation records generated!")
    print("\n⚠️  WARNING: The invalid date record will cause the pipeline to FAIL.")
    print("This is intentional for demo purposes. Delete it to continue:")
    print(f"   dbutils.fs.rm('{VOLUME_PATH}/dim_date/batch_expect_fail_*.json')")

# Uncomment and run:
# generate_expectation_violations()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generate Schema Mismatch (Schema Evolution Demo)
# MAGIC 
# MAGIC This generates records with schema variations to demonstrate:
# MAGIC - Missing columns (column dropped from source)
# MAGIC - New columns (column added to source)
# MAGIC - Type mismatches (string instead of int)
# MAGIC 
# MAGIC Auto Loader handles these based on `schemaEvolutionMode` setting.

# COMMAND ----------

def generate_schema_mismatch():
    """Generate records with schema mismatches to demo schema evolution handling."""
    print("=== Generating Schema Mismatch Records ===\n")
    
    # Scenario A: Missing column (utm_coordinates dropped)
    print("📋 Scenario A: Missing column 'utm_coordinates'")
    missing_column_record = {
        "location_id": 20,
        "township_con_lot": "Schema Test Township A",
        # "utm_coordinates" is MISSING - simulates column dropped from source
        "utm_zone": 17,
        "utm_easting": 500000,
        "utm_northing": 4800000,
        "_op": "I",
        "_ts": get_timestamp(600)
    }
    write_to_volume("dim_location", [missing_column_record], "_schema_missing_col")
    print("   → Auto Loader will set utm_coordinates to NULL (with addNewColumns mode)")
    
    # Scenario B: New column added (extra_info not in original schema)
    print("\n📋 Scenario B: New column 'extra_info' added to source")
    new_column_record = {
        "location_id": 21,
        "township_con_lot": "Schema Test Township B",
        "utm_coordinates": "17T 510000 4810000",
        "utm_zone": 17,
        "utm_easting": 510000,
        "utm_northing": 4810000,
        "extra_info": "This column was added later!",  # NEW column
        "another_new_field": 12345,                    # Another NEW column
        "_op": "I",
        "_ts": get_timestamp(610)
    }
    write_to_volume("dim_location", [new_column_record], "_schema_new_col")
    print("   → Auto Loader will add extra_info to schema (with addNewColumns mode)")
    print("   → Or rescue to _rescued_data column (with rescue mode)")
    
    # Scenario C: Type mismatch (utm_easting as string instead of int)
    print("\n📋 Scenario C: Type mismatch - utm_easting as STRING instead of INT")
    type_mismatch_record = {
        "location_id": 22,
        "township_con_lot": "Schema Test Township C",
        "utm_coordinates": "17T 520000 4820000",
        "utm_zone": 17,
        "utm_easting": "five-hundred-thousand",  # STRING instead of INT!
        "utm_northing": 4820000,
        "_op": "I",
        "_ts": get_timestamp(620)
    }
    write_to_volume("dim_location", [type_mismatch_record], "_schema_type_mismatch")
    print("   → This may cause a parsing error or be rescued to _rescued_data")
    
    # Scenario D: Completely different schema (wrong table data)
    print("\n📋 Scenario D: Completely wrong schema (simulates wrong file in folder)")
    wrong_schema_record = {
        "wrong_field_1": "This is not a location record",
        "wrong_field_2": 999,
        "random_data": True,
        "_op": "I",
        "_ts": get_timestamp(630)
    }
    write_to_volume("dim_location", [wrong_schema_record], "_schema_wrong")
    print("   → All expected columns will be NULL, data goes to _rescued_data")
    
    print("\n" + "="*60)
    print("✅ Schema mismatch records generated!")
    print("="*60)
    print("\n📖 DEMO TALKING POINTS:")
    print("─" * 40)
    print("1. Auto Loader's schemaEvolutionMode handles these scenarios:")
    print("   • 'addNewColumns' - Adds new columns, NULLs for missing")
    print("   • 'rescue' - Puts mismatched data in _rescued_data column")
    print("   • 'failOnNewColumns' - Fails if schema changes (strict)")
    print("   • 'none' - No evolution, may lose data")
    print("")
    print("2. In traditional Spark, you'd need to:")
    print("   • Manually check schema before processing")
    print("   • Write try/catch blocks for type errors")
    print("   • Manage schema registry separately")
    print("")
    print("3. With DLT + Auto Loader:")
    print("   • Schema evolution is declarative (one option)")
    print("   • Bad data is rescued, not lost")
    print("   • Schema changes are tracked in _schema folder")
    print("="*60)

# Uncomment and run:
# generate_schema_mismatch()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Clean Up Bad Data
# MAGIC 
# MAGIC Remove test files if you want the pipeline to succeed cleanly.

# COMMAND ----------

def cleanup_bad_data():
    """Remove files that cause pipeline failures."""
    print("=== Cleaning Up Bad Data ===\n")
    
    cleanup_patterns = [
        ("dim_date", "expect_fail"),
        ("dim_location", "schema_"),
        ("fact_well_construction", "expect_")
    ]
    
    for table, pattern in cleanup_patterns:
        try:
            files = dbutils.fs.ls(f"{VOLUME_PATH}/{table}/")
            for f in files:
                if pattern in f.name:
                    dbutils.fs.rm(f.path)
                    print(f"🗑️  Deleted: {table}/{f.name}")
        except Exception as e:
            print(f"No files to clean in {table}: {e}")
    
    print("\n✅ Cleanup complete!")

# Uncomment and run:
# cleanup_bad_data()

# COMMAND ----------

def cleanup_schema_files_only():
    """Remove only schema mismatch test files."""
    print("=== Cleaning Up Schema Test Files ===\n")
    
    try:
        files = dbutils.fs.ls(f"{VOLUME_PATH}/dim_location/")
        for f in files:
            if "schema_" in f.name:
                dbutils.fs.rm(f.path)
                print(f"🗑️  Deleted: {f.name}")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n✅ Schema test files cleaned!")

# Uncomment and run:
# cleanup_schema_files_only()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Commands
# MAGIC 
# MAGIC Uncomment and run the function you need:

# COMMAND ----------

# === RUN THESE ONE AT A TIME ===

# Step 1: Initial load (run first!)
generate_initial_load()

# Step 2: Generate updates (after initial pipeline run)
# generate_updates()

# Step 3: Out-of-order events demo
# generate_out_of_order()

# Step 4: Expectation violations (will fail pipeline!)
# generate_expectation_violations()

# Step 5: Schema mismatch demo
# generate_schema_mismatch()

# Step 6: Clean up bad data
# cleanup_bad_data()

# Step 6b: Clean up only schema test files
# cleanup_schema_files_only()
