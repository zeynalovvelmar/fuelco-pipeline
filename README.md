# Fuelco Data Pipeline

An ETL pipeline for a fictional fuel company, building a data mart from source systems using Python and SQL.

## Tech Stack
- **Pipeline:** Python, Airflow DAGs
- **Transformations:** SQL, Pandas
- **Database:** PostgreSQL

## Project Structure
- \uel_daily_pipeline.py\: Main Airflow DAG.
- \uel_processing.py\: Data cleansing and processing.
- \	ransform_to_mart.sql\: Star schema mart transformations.
- \init_db.sql\: Initial database definitions.