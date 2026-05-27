from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from scripts.fuel_processing import check_files_sensor, load_raw_bulk, validate_mart_data
default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}
with DAG(
    dag_id="fuel_daily_pipeline",
    schedule="30 23 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["fuelco", "etl", "daily"],
    template_searchpath=["/opt/airflow/dags/sql"],
) as dag:
    t1 = PythonSensor(
        task_id="wait_for_csv_files",
        python_callable=check_files_sensor,
        poke_interval=600,
        timeout=7200,
        mode="reschedule"
    )
    t2 = PythonOperator(
        task_id="load_raw",
        python_callable=load_raw_bulk
    )
    t3 = SQLExecuteQueryOperator(
        task_id="transform_to_mart",
        conn_id="{{ var.value.get('fuel_pg_conn', 'fuelco_postgres') }}",
        sql="transform_to_mart.sql"
    )
    t4 = PythonOperator(
        task_id="validate_mart",
        python_callable=validate_mart_data
    )
    t1 >> t2 >> t3 >> t4

