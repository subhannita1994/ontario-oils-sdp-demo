# Bronze Layer - Raw Data Ingestion
#
# This module contains streaming table definitions for raw CDC data ingestion
# using Auto Loader from Unity Catalog Volumes.
#
# Key characteristics:
#   - Schema inference with cloudFiles.inferColumnTypes
#   - Automatic file discovery and checkpointing
#   - Raw data preservation (no transformations)
#   - Metadata columns for lineage (_rescued_data, _metadata)
