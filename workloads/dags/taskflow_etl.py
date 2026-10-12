import json

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
    description="TaskFlow, task",
)
def taskflow_etl():
    @task()
    def extract():
        return json.loads("""
{
    "class_a": [1, 2, 3, 4, 5, 6, 7, 8, 9],
    "class_b": [9, 8, 7, 6, 5, 4, 3, 2, 1],
    "class_c": [3, 3, 3, 3, 3, 3, 3, 3, 3],
    "class_d": [0, 0, 2, 2, 4, 4, 5, 5, 5]
}
""")

    @task(multiple_outputs=True)
    def transform(data: dict):
        import statistics

        processed_data = {}
        for key, value in data.items():
            processed_data[key] = {
                "min": min(value),
                "max": max(value),
                "avg": statistics.mean(value),
            }

        return processed_data

    @task()
    def load(data: dict):
        # instead of store just print
        for key, value in data.items():
            print(f"{key}:")
            print(f"  Min: {value['min']}")
            print(f"  Max: {value['max']}")
            print(f"  Avg: {value['avg']}")

    load(transform(extract()))


taskflow_etl()
