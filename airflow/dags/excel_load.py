from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from etl.etl_utils import run_full_pipeline

def run_etl():
    run_full_pipeline()


with DAG(
    dag_id="full_excel_load",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="etl_pipeline",
        python_callable=run_etl,
    )