#!/usr/bin/env python3
"""
Ontario Oils CDC Data Generator

Generates simulated Change Data Capture (CDC) JSON files for the Ontario Oils dataset.
These files simulate PostgreSQL CDC output and are used to test the SDP pipeline.

Features:
- Generates Insert (I), Update (U), Delete (D) operations
- Creates out-of-order timestamps to demo AUTO CDC handling
- Can generate schema variations for schema evolution testing
- Produces files compatible with Auto Loader ingestion

Usage:
    python generate_cdc_files.py --output-dir /path/to/landing_zone
    python generate_cdc_files.py --output-dir /path/to/landing_zone --batch-id 2
    python generate_cdc_files.py --output-dir /path/to/landing_zone --break-schema

For Databricks:
    Upload this script to a notebook or run via dbutils:
    %run ./sample_data/generate_cdc_files
"""

import json
import os
import random
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import uuid

# =============================================================================
# Sample Data Definitions
# =============================================================================

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

# =============================================================================
# CDC File Generator
# =============================================================================

class CDCGenerator:
    """Generates CDC JSON files simulating PostgreSQL change data capture."""
    
    def __init__(self, output_dir: str, batch_id: int = 1):
        self.output_dir = output_dir
        self.batch_id = batch_id
        self.base_timestamp = datetime(2024, 1, 1, 0, 0, 0)
    
    def _get_timestamp(self, offset_minutes: int = 0, out_of_order: bool = False) -> str:
        """Generate ISO timestamp with optional out-of-order simulation."""
        ts = self.base_timestamp + timedelta(minutes=offset_minutes)
        if out_of_order:
            # Randomly shift timestamp backwards to simulate out-of-order arrival
            ts = ts - timedelta(minutes=random.randint(1, 30))
        return ts.isoformat() + "Z"
    
    def _add_cdc_fields(self, record: Dict, op: str, ts_offset: int, out_of_order: bool = False) -> Dict:
        """Add CDC operation and timestamp fields to a record."""
        cdc_record = record.copy()
        cdc_record["_op"] = op  # I=Insert, U=Update, D=Delete
        cdc_record["_ts"] = self._get_timestamp(ts_offset, out_of_order)
        return cdc_record
    
    def _write_json_file(self, table_name: str, records: List[Dict], file_suffix: str = ""):
        """Write records to a JSON file in the table's landing directory."""
        table_dir = os.path.join(self.output_dir, table_name)
        os.makedirs(table_dir, exist_ok=True)
        
        filename = f"batch_{self.batch_id:04d}{file_suffix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.json"
        filepath = os.path.join(table_dir, filename)
        
        # Write as newline-delimited JSON (one record per line)
        with open(filepath, 'w') as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
        
        print(f"  Created: {filepath} ({len(records)} records)")
        return filepath
    
    def generate_initial_load(self):
        """Generate initial data load (all inserts)."""
        print("\n=== Generating Initial Load (Batch 1 - Inserts) ===\n")
        
        # Dimension tables
        print("dim_location:")
        records = [self._add_cdc_fields(r, "I", i*10) for i, r in enumerate(LOCATIONS)]
        self._write_json_file("dim_location", records)
        
        print("dim_well_type:")
        records = [self._add_cdc_fields(r, "I", i*10) for i, r in enumerate(WELL_TYPES)]
        self._write_json_file("dim_well_type", records)
        
        print("dim_date:")
        records = [self._add_cdc_fields(r, "I", i*10) for i, r in enumerate(DATES)]
        self._write_json_file("dim_date", records)
        
        print("dim_formation:")
        records = [self._add_cdc_fields(r, "I", i*10) for i, r in enumerate(FORMATIONS)]
        self._write_json_file("dim_formation", records)
        
        print("fact_well_construction:")
        records = [self._add_cdc_fields(r, "I", i*10) for i, r in enumerate(WELL_CONSTRUCTIONS)]
        self._write_json_file("fact_well_construction", records)
    
    def generate_updates_batch(self):
        """Generate a batch with updates and new inserts."""
        print("\n=== Generating Updates Batch ===\n")
        
        # Update a location (SCD Type 2 will create history)
        print("dim_location (update):")
        updated_location = LOCATIONS[0].copy()
        updated_location["utm_easting"] = 456800  # Changed!
        updated_location["utm_northing"] = 4712400  # Changed!
        records = [self._add_cdc_fields(updated_location, "U", 100)]
        self._write_json_file("dim_location", records, "_updates")
        
        # Update a well type description
        print("dim_well_type (update):")
        updated_well_type = WELL_TYPES[1].copy()
        updated_well_type["well_use_desc"] = "Natural gas production well - updated"
        records = [self._add_cdc_fields(updated_well_type, "U", 110)]
        self._write_json_file("dim_well_type", records, "_updates")
        
        # New well construction fact
        print("fact_well_construction (new insert):")
        new_well = {
            "well_construction_id": 6,
            "location_id": 1,
            "well_type_id": 2,
            "date_id": 5,
            "formation_id": 2,
            "well_identifier": "ONT-2024-0001",
            "casing_diameter": 168.3,
            "water_level_info": "Static: 55m",
            "pump_test_results": "Q=75 m3/d",
            "has_water_data": True,
            "has_pump_test": True
        }
        records = [self._add_cdc_fields(new_well, "I", 120)]
        self._write_json_file("fact_well_construction", records, "_new")
    
    def generate_out_of_order_batch(self):
        """Generate batch with out-of-order timestamps to demo AUTO CDC handling."""
        print("\n=== Generating Out-of-Order Events Batch ===\n")
        print("This batch simulates late-arriving CDC events to demonstrate")
        print("how apply_changes() handles out-of-order events via sequence_by.\n")
        
        # Multiple updates to same record with out-of-order timestamps
        print("dim_location (out-of-order updates):")
        records = []
        
        # This update has timestamp BEFORE the previous update but arrives AFTER
        late_update = LOCATIONS[0].copy()
        late_update["township_con_lot"] = "Chatham-Kent Lot 5 (corrected)"
        records.append(self._add_cdc_fields(late_update, "U", 50, out_of_order=True))
        
        # This is the "correct" latest update
        final_update = LOCATIONS[0].copy()
        final_update["township_con_lot"] = "Chatham-Kent Lot 5 - Final"
        final_update["utm_easting"] = 456850
        records.append(self._add_cdc_fields(final_update, "U", 200))
        
        self._write_json_file("dim_location", records, "_out_of_order")
    
    def generate_delete_batch(self):
        """Generate a batch with delete operations."""
        print("\n=== Generating Delete Batch ===\n")
        
        # Delete a well construction record
        print("fact_well_construction (delete):")
        deleted_well = {"well_construction_id": 4}  # Only need the key for delete
        records = [self._add_cdc_fields(deleted_well, "D", 300)]
        self._write_json_file("fact_well_construction", records, "_deletes")
    
    def generate_schema_break(self):
        """Generate files with schema variations to demonstrate schema evolution errors."""
        print("\n=== Generating Schema Break Files ===\n")
        print("WARNING: These files have modified schemas to trigger pipeline errors!\n")
        
        # Missing required column (utm_coordinates removed)
        print("dim_location (MISSING COLUMN - utm_coordinates):")
        broken_record = {
            "location_id": 10,
            "township_con_lot": "Test Township",
            # "utm_coordinates" is intentionally missing!
            "utm_zone": 17,
            "utm_easting": 500000,
            "utm_northing": 4800000,
            "_op": "I",
            "_ts": self._get_timestamp(400)
        }
        self._write_json_file("dim_location", [broken_record], "_schema_break")
        
        # Invalid data type (year as string instead of int)
        print("dim_date (INVALID TYPE - year as string):")
        type_break = {
            "date_id": 10,
            "year": "twenty-twenty-four",  # Should be integer!
            "month": 1,
            "contractor_code": "TEST",
            "_op": "I",
            "_ts": self._get_timestamp(410)
        }
        self._write_json_file("dim_date", [type_break], "_type_break")
    
    def generate_expectation_violations(self):
        """Generate records that violate expectations for demo purposes."""
        print("\n=== Generating Expectation Violation Records ===\n")
        print("These records will trigger different expectation behaviors.\n")
        
        # Violate expect_or_fail (invalid year/month in dim_date)
        print("dim_date (EXPECT_OR_FAIL violation - invalid year/month):")
        invalid_date = {
            "date_id": 999,
            "year": 3000,  # Violates: year BETWEEN 1800 AND 2100
            "month": 13,   # Violates: month BETWEEN 1 AND 12
            "contractor_code": "INVALID",
            "_op": "I",
            "_ts": self._get_timestamp(500)
        }
        self._write_json_file("dim_date", [invalid_date], "_expect_fail")
        
        # Violate expect (null FK in fact table - will be tracked in metrics)
        print("fact_well_construction (EXPECT_OR_WARN violation - null FK):")
        null_fk_well = {
            "well_construction_id": 100,
            "location_id": None,  # Violates: fk_location_not_null
            "well_type_id": 1,
            "date_id": 1,
            "formation_id": 1,
            "well_identifier": "ONT-TEST-WARN",
            "casing_diameter": 150.0,
            "water_level_info": "Test",
            "pump_test_results": "Test",
            "has_water_data": True,
            "has_pump_test": False,
            "_op": "I",
            "_ts": self._get_timestamp(510)
        }
        self._write_json_file("fact_well_construction", [null_fk_well], "_expect_warn")
        
        # Violate expect_or_drop (empty well_identifier)
        print("fact_well_construction (EXPECT_OR_DROP violation - empty identifier):")
        empty_id_well = {
            "well_construction_id": 101,
            "location_id": 1,
            "well_type_id": 1,
            "date_id": 1,
            "formation_id": 1,
            "well_identifier": "",  # Violates: valid_well_identifier (will be dropped)
            "casing_diameter": 100.0,
            "water_level_info": "Test",
            "pump_test_results": "Test",
            "has_water_data": False,
            "has_pump_test": False,
            "_op": "I",
            "_ts": self._get_timestamp(520)
        }
        self._write_json_file("fact_well_construction", [empty_id_well], "_expect_drop")


def main():
    parser = argparse.ArgumentParser(description="Generate CDC files for Ontario Oils SDP demo")
    parser.add_argument("--output-dir", "-o", required=True, help="Output directory for CDC files")
    parser.add_argument("--batch-id", "-b", type=int, default=1, help="Batch ID for file naming")
    parser.add_argument("--initial", action="store_true", help="Generate initial load only")
    parser.add_argument("--updates", action="store_true", help="Generate updates batch")
    parser.add_argument("--out-of-order", action="store_true", help="Generate out-of-order events")
    parser.add_argument("--deletes", action="store_true", help="Generate delete operations")
    parser.add_argument("--break-schema", action="store_true", help="Generate schema break files")
    parser.add_argument("--expect-violations", action="store_true", help="Generate expectation violations")
    parser.add_argument("--all", action="store_true", help="Generate all file types")
    
    args = parser.parse_args()
    
    generator = CDCGenerator(args.output_dir, args.batch_id)
    
    if args.all or (not any([args.initial, args.updates, args.out_of_order, 
                             args.deletes, args.break_schema, args.expect_violations])):
        # Default: generate initial load
        generator.generate_initial_load()
    
    if args.initial or args.all:
        generator.generate_initial_load()
    
    if args.updates or args.all:
        generator.generate_updates_batch()
    
    if args.out_of_order or args.all:
        generator.generate_out_of_order_batch()
    
    if args.deletes or args.all:
        generator.generate_delete_batch()
    
    if args.break_schema or args.all:
        generator.generate_schema_break()
    
    if args.expect_violations or args.all:
        generator.generate_expectation_violations()
    
    print("\n=== Generation Complete ===")
    print(f"Files written to: {args.output_dir}")
    print("\nNext steps:")
    print("1. Upload files to Unity Catalog Volume:")
    print(f"   databricks fs cp -r {args.output_dir}/* dbfs:/Volumes/erp-demonstrations/arc_dev/landing_zone/")
    print("2. Run the SDP pipeline to ingest the data")


if __name__ == "__main__":
    main()
