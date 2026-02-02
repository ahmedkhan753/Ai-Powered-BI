import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def run_cleaning_wrapper():
    import sys
    import os
    import logging
    
    # Debugging: Print current path and directories in /opt/airflow
    logging.info(f"Current sys.path: {sys.path}")
    try:
        logging.info(f"Contents of /opt/airflow: {os.listdir('/opt/airflow')}")
        logging.info(f"Contents of /opt/airflow/data_transform: {os.listdir('/opt/airflow/data_transform')}")
    except Exception as e:
        logging.warning(f"Could not list directories: {e}")

    # Ensure root is in path
    root_dir = '/opt/airflow'
    if root_dir not in sys.path:
        sys.path.append(root_dir)

    from data_transform.clean_ingestion import clean_ingestion_pipeline
    clean_ingestion_pipeline()

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
        python_callable=run_cleaning_wrapper, 
    )

    trigger_star_schema = TriggerDagRunOperator(
        task_id='trigger_star_schema_population',
        trigger_dag_id='star_schema_pipeline', 
        wait_for_completion=False,
    )

    run_cleaning >> trigger_star_schema