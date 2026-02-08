# Silver Layer - Cleaned and Transformed Data
#
# This module contains streaming table definitions with:
#   - Data quality expectations (expect, expect_or_drop, expect_or_fail)
#   - Change Data Capture (CDC) handling with apply_changes()
#   - SCD Type 1 and Type 2 implementations
#   - String normalization and type casting
#
# Expectation Severities:
#   - expect: Log violations but keep all rows (metrics tracking)
#   - expect_or_drop: Drop rows that violate expectations
#   - expect_or_fail: Fail the pipeline if any violations occur
