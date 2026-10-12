from airflow.decorators import dag, task
from pendulum import datetime

default_args = {
    "owner": "airflow",
}


@dag(
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    schedule=None,
    description="TaskFlow, dynamic task mapping",
)
def dynamic_tasks():
    @task
    def add(x: int, y: int):
        return x + y

    @task
    def sum_it(values):
        total = sum(values)
        print(f"Total was {total}")

    # https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html#adding-parameters-that-do-not-expand
    added_values = add.partial(y=10).expand(x=[1, 2, 3])
    # This results in add function being expanded to
    # add(x=1, y=10)
    # add(x=2, y=10)
    # add(x=3, y=10)
    sum_it(added_values)


dynamic_tasks()
