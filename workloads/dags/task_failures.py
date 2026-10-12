from airflow.decorators import dag, task
from airflow.models import Variable
from pendulum import datetime
from datetime import timedelta

# This is just a Demo DAG, do not use Variables for state keeping
# between tasks
VARIABLE_RETRY_COUNTER = "task_failures_retry_counter"

default_args = {"owner": "airflow", "retries": 5, "retry_delay": timedelta(minutes=1)}


@dag(
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    schedule=None,
    description="Failures and Retries",
)
def task_failures():
    @task()
    def init_variable():
        Variable.set(VARIABLE_RETRY_COUNTER, 2)

    @task()
    def retry_able_fail():
        count = int(Variable.get(VARIABLE_RETRY_COUNTER))
        if count > 0:
            Variable.set(VARIABLE_RETRY_COUNTER, count - 1)
            raise ValueError("error - retry-able")

    @task()
    def non_retry_able_fail():
        from airflow.exceptions import AirflowFailException

        raise AirflowFailException("error - not retry-able")

    @task()
    def wont_be_reached():
        print("won't be reached")

    init_variable() >> retry_able_fail() >> non_retry_able_fail() >> wont_be_reached()


task_failures()
