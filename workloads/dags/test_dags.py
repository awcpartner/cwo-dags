"""
On loading the file, it will create 100 DAGs, each with two available task types
running in parallel: cpu_task and sleep_task.
The cpu_task will compute if a number is prime, while the sleep task will sleep
for the provided number of seconds.
The number of tasks to run of each type within a DAG can be set by the cpu_task_num and
sleep_task_num and parameters. The sleep time (in seconds) can be set by the sleep_sec parameter.

Another DAG is also created, called trigger_test_dags, which can trigger 1-100 number of the
previously created DAGs, set by the dag_num parameter.

**Sync / revision marker (optional):** ``caf-perf`` ``sync.git_publish`` rewrites the constant block
below so every DAG gets ``description`` with ``caf_perf rev=…`` and optional ``pushed_at=…``.
Alternatively set env ``CAF_PERF_SYNC_MARKER`` at deploy. ``caf-perf`` uses ``sync.revision_marker``
in JSON (or values filled by ``git_publish``) for the API sync wait; ``git_publish`` also records
push time for ``sync_lag_since_push_sec`` in the report.
"""

import os
import time

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task
from airflow.sdk import Param, chain_linear
from airflow.operators.python import get_current_context, PythonOperator


def t_div(n: int):
    """
    https://www.geeksforgeeks.org/trial-division-algorithm-for-prime-factorization/
    """
    i = 2
    k = int(n**0.5)
    a = []
    while i <= k:
        if n % i == 0:
            a.append(i)
        i += 1
    return a


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "wait_for_downstream": False,
    "start_date": pendulum.datetime(2024, 1, 1, tz="UTC"),
}

# --- caf-perf sync marker block ---
_CAF_PERF_SYNC_MARKER_VALUE = "e353dc1be34b0cc4754987f1eeb23ad3"
_CAF_PERF_PUSH_TIME_ISO = "2026-05-14T10:34:24.123048Z"
# --- end caf-perf sync marker block ---


def _caf_perf_sync_description() -> str | None:
    """Human-readable description including revision marker for ``caf-perf`` sync checks."""
    marker = (_CAF_PERF_SYNC_MARKER_VALUE or "").strip() or os.environ.get(
        "CAF_PERF_SYNC_MARKER", ""
    ).strip()
    if not marker:
        return None
    parts = [f"caf_perf rev={marker}"]
    push = (_CAF_PERF_PUSH_TIME_ISO or "").strip()
    if push:
        parts.append(f"pushed_at={push}")
    return " | ".join(parts)


def create_dag(dag_id):
    with DAG(
        dag_id,
        schedule=None,
        default_args=default_args,
        max_active_runs=1,
        is_paused_upon_creation=False,
        catchup=False,
        description=_caf_perf_sync_description(),
        params={
            "sleep_sec": Param(60, type="integer", minimum=0),
            "sleep_task_num": Param(1, type="integer", minimum=0),
            "cpu_task_num": Param(1, type="integer", minimum=0),
        },
        render_template_as_native_obj=True,
    ) as dag:

        @task
        def create_commands(params: dict):
            commands = [
                {"bash_command": f"sleep {params['sleep_sec']}"}
                for i in range(params["sleep_task_num"])
            ]
            return commands

        @task
        def get_task_num_list(params: dict):
            return [{"x": i} for i in range(params["cpu_task_num"])]

        @task
        def trigger_trial_div(x, **context):
            # times are measured on 2,6 GHz 6-Core Intel Core i7 CPU
            # number = 4993487543923847  # ~10s - 1CPU
            # number = 499348754392384774  # ~92s - 1CPU
            # number = 4993487543923847743  # ~378s - 1CPU

            # times are measured on AWS
            # number = 309348754392384774  # ~80s my measurements 6min?????
            # number = 499348754392384774  # ~5-25minutes
            number = 4993487543923847  # ~30-44s

            print(f"Checking if {number} is prime.")
            tic = time.perf_counter()
            print(t_div(number))
            toc = time.perf_counter()
            print(f"Done in {toc - tic:0.4f} seconds")

        start = EmptyOperator(task_id="start", dag=dag)
        sleep_commands = create_commands()
        cpu_task_list = get_task_num_list()

        sleep_tasks = BashOperator.partial(task_id="sleep_task").expand_kwargs(
            sleep_commands
        )
        python_tasks = trigger_trial_div.expand(x=cpu_task_list)

        chain_linear(
            start, [cpu_task_list, sleep_commands], [sleep_tasks, python_tasks]
        )

    return dag


def create_start_dag(dag_id):
    with DAG(
        dag_id,
        schedule=None,
        default_args=default_args,
        is_paused_upon_creation=True,
        catchup=False,
        description=_caf_perf_sync_description(),
        params={
            "sleep_sec": Param(60, type="integer", minimum=0),
            "sleep_task_num": Param(1, type="integer", minimum=0),
            "cpu_task_num": Param(1, type="integer", minimum=0),
            "dag_num": Param(
                1,
                type="integer",
                minimum=1,
                maximum=100,
            ),
        },
        render_template_as_native_obj=True,
    ):

        def collect_trigger_dag_ids():
            context = get_current_context()
            return [f"test_dag_{i}" for i in range(context["params"]["dag_num"])]

        collect_dag_ids = PythonOperator(
            task_id="collect_triggers_dag",
            python_callable=collect_trigger_dag_ids,
            do_xcom_push=True,
        )

        trigger_tasks = TriggerDagRunOperator.partial(
            trigger_rule=TriggerRule.ALL_SUCCESS,
            task_id="trigger",
            reset_dag_run=True,
            map_index_template="{{ task.trigger_run_id }}",
            conf={
                "sleep_sec": "{{ params['sleep_sec'] }}",
                "cpu_task_num": "{{ params['cpu_task_num'] }}",
                "sleep_task_num": "{{ params['sleep_task_num'] }}",
            },
        ).expand(trigger_dag_id=collect_dag_ids.output)

        collect_dag_ids >> trigger_tasks


for id in range(0, 100):
    dag_id = f"test_dag_{id}"
    dyn_dag = create_dag(dag_id)
    globals()[dag_id] = dyn_dag


create_start_dag("trigger_test_dags")
