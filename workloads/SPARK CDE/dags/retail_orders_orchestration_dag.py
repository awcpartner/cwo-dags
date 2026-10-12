"""
CDE Airflow DAG — orchestrates the retail orders Spark demo.

Prerequisites (create in CDE UI before enabling this DAG):
  1. A Spark job whose **name** matches SPARK_CDE_JOB_NAME below.
  2. That Spark job runs src/basic_cde_spark_job.py from a mounted Files resource.

This DAG does not process data itself; it triggers the Spark job and applies
orchestration patterns (retries, timeouts, clear task boundaries).
"""

from datetime import datetime, timedelta

from airflow import DAG

try:
    from airflow.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator

try:
    from cloudera.airflow.providers.operators.cde import CdeRunJobOperator
except ImportError:
    from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator as CdeRunJobOperator


# Must match the Spark job name exactly as defined in the CDE UI.
SPARK_CDE_JOB_NAME = "retail-orders-spark-demo"

# Example CDE_CONNECTION_ID = "cde_vc_prod"
CDE_CONNECTION_ID = cde_vc_prod

DEFAULT_ARGS = {
    "owner": "cde-demo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="retail_orders_orchestration",
    description="Trigger retail orders Spark analytics demo on CDE",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["cde", "spark", "retail-demo"],
) as dag:
    start = EmptyOperator(task_id="start")

    spark_task_kwargs = {
        "task_id": "run_retail_orders_spark",
        "job_name": SPARK_CDE_JOB_NAME,
        "execution_timeout": timedelta(minutes=30),
    }
    if CDE_CONNECTION_ID:
        spark_task_kwargs["connection_id"] = CDE_CONNECTION_ID

    run_retail_orders_spark = CdeRunJobOperator(**spark_task_kwargs)

    end = EmptyOperator(task_id="end")

    start >> run_retail_orders_spark >> end
