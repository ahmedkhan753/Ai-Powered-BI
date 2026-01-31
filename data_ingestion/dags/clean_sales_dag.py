import sys
import os
sys.path.append(os.environ.get('AIRFLOW_HOME', '/opt/airflow'))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from data_transform.clean_ingestion import clean_ingestion_pipeline

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'sales_silver_cleaning_pipeline', 
    default_args=default_args,
    schedule_interval=None, 
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['silver', 'cleaning'],
) as dag:

    run_cleaning = PythonOperator(
        task_id='run_silver_cleaning',
        python_callable=clean_ingestion_pipeline, 
    )

    run_cleaning