from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'gema_final_dag',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='dbt_transform_to_bq',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['dbt', 'bigquery'],
) as dag:
    
    dbt_run_staging = BashOperator(
    task_id='dbt_run_staging',
    bash_command='cd /opt/airflow/dbt_project && dbt run --select path:models/staging --profiles-dir .',
    )

    dbt_run_final_trip_data = BashOperator(
        task_id='run_final_taxi_data',
        bash_command='cd /opt/airflow/dbt_project && dbt run --select path:models/marts --profiles-dir .',
    )

    dbt_run_staging >> dbt_run_final_trip_data
