from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def task_with_import_error():
    @task()
    def task_with_imports():
        # The DAG can be imported, but the task will have an import error
        import hopefullythiswontexistever  # noqa: F401

        print("import error")

    task_with_imports()


task_with_import_error()
