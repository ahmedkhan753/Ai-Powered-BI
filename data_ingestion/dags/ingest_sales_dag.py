import sys
sys.path.append('/opt/airflow/scripts')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from scripts.ingest_sales import ingest_sales_pipeline

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'sales_ingestion_pipeline',
    default_args=default_args,
    description='Incremental sales data ingestion every 5 minutes',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['bi', 'sales'],
) as dag:

    run_ingestion = PythonOperator(
        task_id='run_sales_ingestion',
        python_callable=ingest_sales_pipeline,
    )

    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_silver_cleaning',
        trigger_dag_id='sales_silver_cleaning_pipeline', 
        wait_for_completion=False,
    )
    run_ingestion >> trigger_silver 

  
    if __name__ == "__main__":
        print("DAG file loaded successfully! (This is just a syntax check)")
        print("DAG ID:", dag.dag_id)
        print("Tasks:", [task.task_id for task in dag.tasks])