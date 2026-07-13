from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# 1. function to run inside task
def start_pipeline():
    print("Pipeline Started")


def end_pipeline():
    print(" Pipeline Finished")


# 2. DAG definition
with DAG(
    dag_id="simple_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual run only
    catchup=False
) as dag:

    # 3. Tasks
    start_task = PythonOperator(
        task_id="start_task",
        python_callable=start_pipeline
    )

    end_task = PythonOperator(
        task_id="end_task",
        python_callable=end_pipeline
    )

    # 4. Workflow (order)
    start_task >> end_task