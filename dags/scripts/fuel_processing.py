import os
import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook

STATIONS = ["ST001", "ST002", "ST003", "ST004", "ST005"]
BASE_DIR = "/opt/airflow/data/incoming"

def check_files_sensor(ds, **kwargs):
    for station in STATIONS:
        if not os.path.exists(f"{BASE_DIR}/{station}_{ds}.csv"):
            return False
    return True

def load_raw_bulk(ds, **kwargs):
    hook = PostgresHook(postgres_conn_id="fuelco_postgres")
    sql = """
        INSERT INTO raw.fuel_sales (station_id, sale_date, fuel_type, liters, unit_price, total_amount, operator, source_file)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (station_id, sale_date, fuel_type) DO NOTHING;
    """
    for station in STATIONS:
        file_path = f"{BASE_DIR}/{station}_{ds}.csv"
        with open(file_path, mode='r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                params = (row[0], row[1], row[2], float(row[3]), float(row[4]), float(row[5]), row[6], f"{station}_{ds}.csv")
                hook.run(sql, parameters=params)

def validate_mart_data(ds, **kwargs):
    hook = PostgresHook(postgres_conn_id="fuelco_postgres")
    result = hook.get_first(f"SELECT COUNT(*) FROM mart.daily_station_summary WHERE sale_date = '{ds}';")
    if not result or result[0] == 0:
        raise ValueError(f"Validation failed: No data found in mart for date {ds}")
