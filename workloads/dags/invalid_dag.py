from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from pendulum import datetime

default_args = {
    "owner": "airflow",
}


@dag(
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    schedule=None,
    description="Cycle in the tasks",
)
def invalid_dag():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    a = EmptyOperator(task_id="a")
    b = EmptyOperator(task_id="b")
    c = EmptyOperator(task_id="c")
    # Cycle in tasks -> invalid DAG
    start >> a >> b >> c >> a >> end


invalid_dag()
