FROM apache/airflow:2.11.0

USER airflow

RUN pip install --no-cache-dir dbt-bigquery
