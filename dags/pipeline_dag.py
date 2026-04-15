from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'main_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL - Scraping, GDrive, API -> Spark -> MinIO',
    schedule_interval="@daily",
    catchup=False,
    tags=['ETL', 'Spark', 'MinIO']
) as dag:

    # --- 1. EXTRACTION ---
    extract_books = BashOperator(
        task_id='extract_books',
        bash_command='python /opt/airflow/etl/extract/scraper.py',
    )
    
    extract_gdrive = BashOperator(
        task_id='extract_gdrive',
        bash_command='python /opt/airflow/etl/extract/gdrive_downloader.py',
    )

    extract_api = BashOperator(
        task_id='extract_api',
        bash_command='python /opt/airflow/etl/extract/api_client.py',
    )

    # --- 2. TRANSFORMATION (SPARK) ---
    transform_spark = BashOperator(
        task_id='transform_spark',
        bash_command='python /opt/airflow/etl/transform/spark_processor.py',
    )

    # --- 3. CHARGEMENT (MINIO) ---
    load_minio = BashOperator(
        task_id='load_minio',
        bash_command='python /opt/airflow/etl/load/minio_loader.py',
    )

    # --- ORDONNANCEMENT ---
    # Les 3 extractions en paralèlle -> Puis Transformation -> Puis Chargement
    [extract_books, extract_gdrive, extract_api] >> transform_spark >> load_minio
