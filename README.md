## Setup

1. Configure Airflow Connections:
   - aws_credentials
   - redshift

2. Add Airflow Variables:
   - s3_bucket
   - log_json_path

3. Trigger DAG:
   udacity_final_project

   ## Pipeline Features

- Parameterized S3 ingestion with execution date support
- Idempotent dimension loading
- Data quality validation with multiple checks
- Modular and reusable Airflow operators

## Architecture

S3 → Redshift staging → Fact & dimension tables → Data quality checks

This project was developed as part of the Udacity Data Engineering Nanodegree.

The initial project structure and requirements were provided by Udacity.  
All implementation, configuration, and enhancements were completed independently, but of course, the code and ideas can be further developedor even expanded to other clouds or tech.