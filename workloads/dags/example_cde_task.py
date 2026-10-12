"""
Example DAG to trigger a spark job in CDE once.
Assumes that there's a CDE connection available with 'cde_runtime_api' ID.
"""

from pendulum import datetime
from airflow import DAG
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator


def create_dag(dag_id, schedule, conn_id, default_args):
    with DAG(
        dag_id,
        schedule=schedule,
        default_args=default_args,
        description=f"Dynamic DAG created for connection='{conn_id}' "
        "(Variable, Connection, CDEJobRunOperator, PythonOperator)",
    ) as dag:
        CdeRunJobOperator(
            task_id=f"spark_pi_{conn_id}",
            dag=dag,
            job_name="example-scala-pi",
            connection_id=conn_id,
            overrides={"spark": {"numExecutors": 2}},
        )
        return dag


conn_id = "cde_runtime_api"
dag_id = "dynamic_cderunjob_{}".format(conn_id)
default_args = {"owner": "airflow", "start_date": datetime(2024, 1, 1)}
globals()[dag_id] = create_dag(dag_id, None, conn_id, default_args)
