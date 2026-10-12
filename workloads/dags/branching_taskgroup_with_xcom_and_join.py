from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label

import random
from pendulum import datetime


@dag(
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule=None,
    description="TaskFlow, task, TaskGroup, BashOperator, xcom",
)
def branching_taskgroup_with_xcom_and_join():
    @task.branch(task_id="branching")
    def random_choice(choices):
        return random.choice(choices)

    start = EmptyOperator(task_id="start")
    branch = random_choice(choices=["bash.start", "python.start"])
    join = EmptyOperator(task_id="join", trigger_rule="none_failed_min_one_success")

    with TaskGroup("bash") as b:

        @task
        def print_host_user_from_bash_pull(ti=None):
            host = ti.xcom_pull(task_ids="bash.host")
            user = ti.xcom_pull(task_ids="bash.whoami")
            print(f"host: {host} user: {user}")

        bash_start = EmptyOperator(task_id="start")
        bash_whoami = BashOperator(
            task_id="whoami", bash_command="whoami", do_xcom_push=True
        )
        bash_host = BashOperator(
            task_id="host", bash_command="hostname", do_xcom_push=True
        )
        bash_print = print_host_user_from_bash_pull()
        bash_start >> [bash_host, bash_whoami] >> bash_print

    with TaskGroup("python") as p:

        @task
        def whoami() -> str:
            import os
            import pwd

            return pwd.getpwuid(os.getuid())[0]

        @task
        def host() -> str:
            import socket

            return socket.gethostname()

        @task
        def print_host_user(ti=None):
            host = ti.xcom_pull(task_ids="python.host")
            user = ti.xcom_pull(task_ids="python.whoami")
            print(f"host: {host} user: {user}")

        python_start = EmptyOperator(task_id="start")
        python_start >> [host(), whoami()] >> print_host_user()

    start >> branch
    branch >> Label("bash") >> b >> Label("bash") >> join
    branch >> Label("python") >> p >> Label("python") >> join


branching_taskgroup_with_xcom_and_join()
