# End-to-End AWS Data Lake (Local Simulation)
This project simulates an AWS S3 → Glue → Athena → QuickSight pipeline locally.
It includes:
- folder structure for raw/staged/curated (to represent S3 partitions)
- PySpark Glue-style ETL job (glue_etl_job.py) that runs locally with pyspark
- sample data and Athena-style SQL queries
- instructions to adapt to real AWS (upload scripts and IAM notes)
