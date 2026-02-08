# Ontario Oils SDP Pipeline Package
# 
# This package contains the Spark Declarative Pipeline (DLT) definitions
# for the Ontario Oils well construction dataset.
#
# Structure:
#   - bronze/: Raw data ingestion from CDC files using Auto Loader
#   - silver/: Cleaned and transformed data with expectations and SCD handling
#
# Tables:
#   - dim_location: Geographic location dimension (SCD Type 2)
#   - dim_well_type: Well type and usage dimension (SCD Type 2)
#   - dim_date: Date dimension (SCD Type 1)
#   - dim_formation: Geological formation dimension (SCD Type 2)
#   - fact_well_construction: Well construction fact table (SCD Type 1)

__version__ = "1.0.0"
__author__ = "Subhannita Sarcar"
