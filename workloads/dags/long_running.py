from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain
from pendulum import datetime

with DAG(
    dag_id="long_running",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
):
    tasks = []
    for x in range(60):
        tasks.append(BashOperator(task_id=f"sleep_{x + 1}", bash_command="sleep 60"))
    chain(*tasks)
