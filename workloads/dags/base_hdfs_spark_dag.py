from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DEFAULT_HDFS_INPUT_PATH = "/user/hkhan/spark-airflow-cde/customers/input"
DEFAULT_HDFS_OUTPUT_PATH = "/user/hkhan/spark-airflow-cde/customers/output"
HDFS_INPUT_PATH = os.getenv("HDFS_INPUT_PATH", DEFAULT_HDFS_INPUT_PATH)
HDFS_OUTPUT_PATH = os.getenv("HDFS_OUTPUT_PATH", DEFAULT_HDFS_OUTPUT_PATH)
DAG_DIR = Path(__file__).resolve().parent
DEFAULT_SPARK_APPLICATION = str(DAG_DIR / "customer_state_summary.py")
SPARK_APPLICATION = os.getenv("SPARK_APPLICATION", DEFAULT_SPARK_APPLICATION)
AIRFLOW_SPARK_CONN_ID = os.getenv("AIRFLOW_SPARK_CONN_ID", "spark_default")
SPARK_DEPLOY_MODE = os.getenv("SPARK_DEPLOY_MODE", "cluster")

with DAG(
    dag_id="base_hdfs_spark_dag",
    description="Submit the customer summary Spark job directly against HDFS data.",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "hdfs", "base"],
) as dag:
    run_customer_summary = SparkSubmitOperator(
        task_id="run_customer_summary",
        conn_id=AIRFLOW_SPARK_CONN_ID,
        application=SPARK_APPLICATION,
        deploy_mode=SPARK_DEPLOY_MODE,
        application_args=[
            "--input",
            HDFS_INPUT_PATH,
            "--output",
            f"{HDFS_OUTPUT_PATH.rstrip('/')}/base",
            "--format",
            "csv",
        ],
    )
