from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from pendulum import datetime


# top level import error. The DAG won't be parsed/registered in Airflow and therefore cannot be executed either.
import hopefullythiswontexistever  # noqa: F401


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def import_error():
    EmptyOperator(task_id="start")


import_error()
