import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def run_star_schema_wrapper():
    import logging
    # Ensure root is in path
    root_dir = '/opt/airflow'
    if root_dir not in sys.path:
        sys.path.append(root_dir)
    
    try:
        from data_warehouse.star_schema import run_star_schema_etl
        run_star_schema_etl()
    except Exception as e:
        logging.error(f"Error running star schema ETL: {e}")
        raise e

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'star_schema_pipeline', 
    default_args=default_args,
    schedule_interval=None, # Triggered by clean_sales_dag
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['gold', 'star_schema'],
) as dag:

    run_etl = PythonOperator(
        task_id='run_star_schema_population',
        python_callable=run_star_schema_wrapper, 
    )

    run_etl
