# Example DAGs for Demos and manual tests

Example DAGs for demonstration and manual testing purposes.

## Preparation

Make sure that the airflow.dags.gitSync is set for CDH/airflow repository.

Run cloudera-airflow locally in a Kind cluster:
```shell
make kind-cluster
make kind-install
k9s --context kind-caf
# port-forward airflow-api-server:8080
```

## DAGs

 - [BashOperator](dags/bashoperator.py)
 - [PythonOperator](dags/pythonoperator.py)
 - [TaskFlow ETL](dags/taskflow_etl.py)
 - [Long Running](dags/long_running.py)
 - [Branching, TaskGroup, XCom, Join](dags/branching_taskgroup_with_xcom_and_join.py)

## Dynamic DAGs

 - [Dynamic DAG creation](dags/dynamic_dags_for_connections.py): creates a DAG for each matching connection name (%core-vc%) and runs `example-scala-pi` with `CdeRunJobOperator`.
 - [Dynamic task mapping](dags/dynamic_tasks.py): dynamic task mappings example.

## Airflow 3.2.x performance driver (`caf-perf`)

Python CLI under [`caf_perf/`](caf_perf/) drives the **stable REST API** (`/api/v2`, Airflow **3.2.1**) to:

- Wait until expected DAG ids from [`dags/test_dags.py`](dags/test_dags.py) appear (DAG sync / visibility). Typical flow: set `sync.git_publish.repo_path` (absolute, relative to the scenario file, or `__cwd__` for the git root that contains your shell cwd) and `branch` for your CAF git-sync checkout; `caf-perf` patches the DAG marker block, pushes, waits until the API sees the new marker in DAG `description`, then runs triggers and writes the report (see [`caf_perf/README.md`](caf_perf/README.md)).
- Optionally treat import errors as failures (with substring ignores for known-broken demo DAGs).
- Unpause selected DAGs, trigger runs with `conf`, wait for terminal states, and record **peak concurrent** DAG runs / task instances among the runs you triggered.
- Optionally include child DAG runs spawned by `trigger_test_dags` in the same wait/metrics window (`execution.wait.include_triggered_children=true`).

Configuration: [`perf_configs/example.cloudera-oauth.json`](perf_configs/example.cloudera-oauth.json). Align `dag_expectations.count` with the number of `test_dag_*` DAGs defined in `test_dags.py` (100 by default). The default orchestrator DAG id is `trigger_test_dags` (override with `dag_expectations.trigger_dag_id` once if needed).

```bash
cd workloads
uv sync
uv run caf-perf --config perf_configs/example.cloudera-oauth.json --report /tmp/caf_perf_report.json
```

See `make caf-perf-help`.

## Task failure DAGs

 - [Memory Exhaustion - OOMKill](dags/memory_eater.py): allocates memory until the pod is OOMKilled.
 - [Task with import error](dags/task_with_import_error.py)
 - [Task Failure](dags/task_failures.py)

## Invalid DAGs

- [import error](dags/import_error.py): contains an invalid import.
- [cycle in the tasks](dags/invalid_dag.py): contains a cycle in the tasks.
