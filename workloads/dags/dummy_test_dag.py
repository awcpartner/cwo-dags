"""Small dummy DAG for testing. Manual trigger only; no external connections."""

from __future__ import annotations

import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def print_message() -> None:
    print("Hello from dummy_test_dag!")


with DAG(
    dag_id="dummy_test_dag",
    description="Dummy DAG for testing; manual trigger only.",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["test"],
) as dag:
    say_hello = PythonOperator(
        task_id="say_hello",
        python_callable=print_message,
    )

    echo_message = BashOperator(
        task_id="echo_message",
        bash_command="echo 'dummy_test_dag: bash step done'",
    )

    say_hello >> echo_message
