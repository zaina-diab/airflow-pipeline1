from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from etl.event_load import fetch_and_load_new_rows


with DAG(
    dag_id="event_based_load",
    start_date=datetime(2024, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
) as dag:

    fetch_and_load_task = PythonOperator(
        task_id="fetch_and_load_new_rows",
        python_callable=fetch_and_load_new_rows,
    )