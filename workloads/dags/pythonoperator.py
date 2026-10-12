from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime


with DAG(
    dag_id="python_operator",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
):
    PythonOperator(
        task_id="hello_task",
        python_callable=lambda: print("Hello!"),
    )
