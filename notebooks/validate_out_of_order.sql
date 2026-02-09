-- Out-of-Order Event Handling Validation Queries
-- Run these in Databricks SQL Editor to demonstrate DLT's automatic handling

-- =============================================================================
-- QUERY 1: Show the SCD Type 2 History for location_id = 1
-- =============================================================================
-- This shows all versions of location 1, ordered by when they became effective
-- Notice: The timestamps (__START_AT) are in correct sequence even though
-- the "late arrival" event came AFTER the update

SELECT 
    location_id,
    township_con_lot,
    utm_easting,
    __START_AT,
    __END_AT,
    CASE 
        WHEN __END_AT IS NULL THEN '← CURRENT VERSION'
        ELSE ''
    END AS status
FROM `erp-demonstrations`.arc_dev.dim_location_silver
WHERE location_id = 1
ORDER BY __START_AT;

-- Expected result (approximately):
-- | location_id | township_con_lot                  | utm_easting | __START_AT          | __END_AT            | status          |
-- |-------------|-----------------------------------|-------------|---------------------|---------------------|-----------------|
-- | 1           | Chatham-Kent Lot 5                | 456789      | 2024-01-01 00:00:00 | 2024-01-01 00:50:00 |                 |
-- | 1           | Chatham-Kent Lot 5 (Late Arrival) | 456789      | 2024-01-01 00:50:00 | 2024-01-01 01:40:00 |                 |
-- | 1           | Chatham-Kent Lot 5 (Updated)      | 456800      | 2024-01-01 01:40:00 | 2024-01-01 03:20:00 |                 |
-- | 1           | Chatham-Kent Lot 5 (Final)        | 456850      | 2024-01-01 03:20:00 | NULL                | ← CURRENT       |


-- =============================================================================
-- QUERY 2: Show the Processing Order vs Logical Order
-- =============================================================================
-- The _ts field shows the LOGICAL timestamp (when the event occurred)
-- The __START_AT shows when DLT applied it to the SCD Type 2 table

SELECT 
    location_id,
    township_con_lot,
    __START_AT AS effective_from,
    __END_AT AS effective_to
FROM `erp-demonstrations`.arc_dev.dim_location_silver
WHERE location_id = 1
ORDER BY __START_AT;


-- =============================================================================
-- QUERY 3: Count versions per location
-- =============================================================================
-- Shows how many historical versions exist for each location

SELECT 
    location_id,
    COUNT(*) as version_count,
    MIN(__START_AT) as first_version,
    MAX(__START_AT) as latest_version
FROM `erp-demonstrations`.arc_dev.dim_location_silver
GROUP BY location_id
ORDER BY version_count DESC;


-- =============================================================================
-- QUERY 4: Show Bronze vs Silver comparison
-- =============================================================================
-- Bronze has ALL CDC events (raw), Silver has deduplicated SCD Type 2 records

SELECT 'Bronze (Raw CDC Events)' as layer, COUNT(*) as record_count
FROM `erp-demonstrations`.arc_dev.dim_location_bronze
UNION ALL
SELECT 'Silver (SCD Type 2)' as layer, COUNT(*) as record_count
FROM `erp-demonstrations`.arc_dev.dim_location_silver;
