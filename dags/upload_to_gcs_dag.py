from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.datasets import Dataset
from datetime import datetime
import os
import json
import csv

DATA_DIR = "/opt/airflow/data"
GCS_BUCKET = "jdeol003-bucket"  
GCS_PREFIX = "capstone3_gema"

default_args = {
    "owner": "gema",
    "start_date": datetime(2025, 1, 1),
    "retries": 1
}

gcs_dataset = Dataset(f"gs://{GCS_BUCKET}/{GCS_PREFIX}/taxi_data.csv")

with DAG(
    dag_id="upload_to_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    tags=["batch", "gcs"],
    description="Merge csv + json and upload to GCS"
) as dag:

    @task(outlets=[gcs_dataset])
    def merge_csv_and_json():
        csv_dir = os.path.join(DATA_DIR, "csv")
        json_dir = os.path.join(DATA_DIR, "json")
        merged_file_path = os.path.join(DATA_DIR, "taxi_data.csv")

        rows = []
        fieldnames = set()

        for file in os.listdir(csv_dir):
            if file.endswith(".csv"):
                with open(os.path.join(csv_dir, file), "r") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        rows.append(row)
                        fieldnames.update(row.keys())

        for file in os.listdir(json_dir):
            if file.endswith(".json"):
                with open(os.path.join(json_dir, file), "r") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        data = [data]
                    for row in data:
                        rows.append(row)
                        fieldnames.update(row.keys())

        fieldnames = list(fieldnames)

        with open(merged_file_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                cleaned_row = {}
                for key in fieldnames:
                    val = row.get(key)
                    if key in ['congestion_surcharge', 'ehail_fee']:
                        if val in [None, '', 'null']:
                            cleaned_row[key] = 0
                        else:
                            try:
                                cleaned_row[key] = int(val)
                            except:
                                cleaned_row[key] = 0
                    else:
                        cleaned_row[key] = val
                writer.writerow(cleaned_row)

        return merged_file_path

    merge_task = merge_csv_and_json()

    upload_payment_type = LocalFilesystemToGCSOperator(
        task_id="upload_payment_type",
        src=os.path.join(DATA_DIR, "payment_type.csv"),
        dst=f"{GCS_PREFIX}/payment_type.csv",
        bucket=GCS_BUCKET,
        mime_type="text/csv",
        gcp_conn_id="google_cloud_default"
    )

    upload_zone_lookup = LocalFilesystemToGCSOperator(
        task_id="upload_zone_lookup",
        src=os.path.join(DATA_DIR, "taxi_zone_lookup.csv"),
        dst=f"{GCS_PREFIX}/taxi_zone_lookup.csv",
        bucket=GCS_BUCKET,
        mime_type="text/csv",
        gcp_conn_id="google_cloud_default"
    )

    upload_merged_taxi_data = LocalFilesystemToGCSOperator(
        task_id="upload_merged_taxi_data",
        src=merge_task,
        dst=f"{GCS_PREFIX}/taxi_data.csv",
        bucket=GCS_BUCKET,
        mime_type="text/csv",
        gcp_conn_id="google_cloud_default"
    )

    merge_task >> [upload_payment_type, upload_zone_lookup, upload_merged_taxi_data]
