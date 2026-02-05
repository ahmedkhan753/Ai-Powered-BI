import sys

sys.path.append("/opt/airflow/scripts")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from scripts.ingest_sales import ingest_sales_pipeline

default_args = {
    "owner": "you",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["ahmedk32410@gmail.com"],
}

with DAG(
    "sales_ingestion_pipeline",
    default_args=default_args,
    description="Incremental sales data ingestion every 2 minutes",
    schedule_interval=timedelta(minutes=2),
    start_date=datetime(2025, 12, 1),
    catchup=False,
    max_active_runs=1,
    doc_md="""
    ### Incremental Sales Data Ingestion
    
    This DAG performs incremental sales data ingestion every 2 minutes.
    
    **Pipeline Steps**:
    1. Extracts the full dataset from the source file.
    2. Filters new rows based on the current watermark (max loaded order_id).
    3. Validates the data schema.
    4. Loads the new rows into the raw sales table.
    5. Triggers the silver cleaning pipeline.
    
    **Error Handling**:
    - Logs errors and exits if data schema mismatch is detected.
    - Logs warnings if no new rows are found.
    
    **Tags**:
    - bi
    - sales
    """,
    tags=["bi", "sales"],
) as dag:

    run_ingestion = PythonOperator(
        task_id="run_sales_ingestion",
        python_callable=ingest_sales_pipeline,
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_cleaning",
        trigger_dag_id="sales_silver_cleaning_pipeline",
        wait_for_completion=False,
    )

    run_ingestion >> trigger_silver

    if __name__ == "__main__":
        print("DAG file loaded successfully! (This is just a syntax check)")
        print("DAG ID:", dag.dag_id)
        print("Tasks:", [task.task_id for task in dag.tasks])
