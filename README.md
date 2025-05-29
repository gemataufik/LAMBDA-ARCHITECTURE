===================== BATCH PIPELINE =====================
1. Buat docker-compose.yaml
    + edit file docker-compose.yaml
        - tambahkan keys dan data di volumes (mount ke container)
    + di folder keys menambahkan file untuk Google Cloud credentials
    + buat UID

2. Buat connections di web UI airflow

3. Fungsi file upload_to_gcs_dag.py (DAG)
    + menggabungkan data dari folder csv dan json (data preparation)
    + setelah merge berhasil/task merge data, upload semua data ke GCS

4. file load_to_bq_dag.py (DAG)
    + membuat dag untuk dataset
        - DAG ini dijalankan jika dataset sudah ada di gcs/taxi_data.csv berubah
    + untuk task yang tanpa dataset langsung load ke bq
    + lalu task load_taxi_data akan dijalankan setelah load_payment_type dan load_zone_lookup selesai

5. DBT (digunakan juga untuk test lokal/directrunner sebelum buat dag nya)
    + buat folder dbt_project
        - dbt init dbt_project
    + dbt debug 
    + buat folder staging
        - transformasi table taxi_data (dibuat incremental dan partition)
        - transformasi table taxi_zone_lookup (normalisai nama kolom menjadi snake case)
        - buat schema.yml untuk source table 
    + buat foldeer marts
        - menggabungkan hasil transforamasi dari hasil table taxi_data dan table taxi_zone_lookup
        - join 2 table hasil transformasi dengan 1 table source dari dataset yang tidak di transformasi
        - hasilnya di simpan sebagai table final_taxi_data
    + setelah tes berhasil menggunakan (dbt run) 
    + mount folder dbt_project ke container 
        - menambahkan di docker-compose.yaml

6. Buat DAG (load_to_bq_dag_final)
    + mount folder dbt_project ke container 
        - menambahkan di docker-compose.yaml
    + Buat dockerfile
        - untuk install dbt-bigquery
    + Tambahkan dockerfile di docker-compose.yaml
    + untuk task nya menjalankan semua logic dari folder dbt_project
    + load ke biguery


===================== STREAMING PIPELINE =====================
1. Buat Pub/Sub
    + Buat topic
    + Buat Subcriptions
    + Buat publisher.py
        - set GOOGLE_APPLICATION_CREDENTIALS (agar bisa akses/dapat izin ke GCP)
        - menggunakan data dummy (dari faker)

2. Buat dataflow.py
    + sebagai subcirber
    + Melakukan Transformasi
    + mengirim data ke biguery (load ke bigquery)
