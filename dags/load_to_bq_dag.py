from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.datasets import Dataset
from datetime import datetime

GCS_BUCKET = "jdeol003-bucket"
GCS_PREFIX = "capstone3_gema"

BQ_DATASET = "jdeol003_capstone3_gema" 
GCP_CONN_ID = "google_cloud_default"

default_args = {
    "start_date": datetime(2025, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="load_to_bq_dag",
    schedule=[Dataset(f"gs://{GCS_BUCKET}/{GCS_PREFIX}/taxi_data.csv")],
    default_args=default_args,
    catchup=False,
    tags=["batch", "gcs", "bigquery"],
    description="Load raw files from GCS to BigQuery raw dataset",
) as dag:

    load_payment_type = GCSToBigQueryOperator(
        task_id="load_payment_type",
        bucket=GCS_BUCKET,
        source_objects=[f"{GCS_PREFIX}/payment_type.csv"],
        destination_project_dataset_table=f"purwadika.{BQ_DATASET}.payment_type",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        gcp_conn_id=GCP_CONN_ID
    )

    load_zone_lookup = GCSToBigQueryOperator(
        task_id="load_zone_lookup",
        bucket=GCS_BUCKET,
        source_objects=[f"{GCS_PREFIX}/taxi_zone_lookup.csv"],
        destination_project_dataset_table=f"purwadika.{BQ_DATASET}.taxi_zone_lookup",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        gcp_conn_id=GCP_CONN_ID
    )

    load_taxi_data = GCSToBigQueryOperator(
        task_id="load_taxi_data",
        bucket=GCS_BUCKET,
        source_objects=[f"{GCS_PREFIX}/taxi_data.csv"],
        destination_project_dataset_table=f"purwadika.{BQ_DATASET}.taxi_data",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        gcp_conn_id=GCP_CONN_ID
    )

    [load_payment_type, load_zone_lookup] >> load_taxi_data
