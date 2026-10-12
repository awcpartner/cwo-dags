from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import datetime

with DAG(
    dag_id="bash_operator",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
):
    BashOperator(task_id="whoami", bash_command="whoami")
