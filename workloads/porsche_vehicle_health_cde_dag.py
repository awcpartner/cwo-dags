"""Run the Porsche vehicle health CDE Spark job only (no Impala / Iceberg)."""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.models.param import Param
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator

from pipeline_settings import (
    AWS_REGION,
    CDE_CONNECTION_ID,
    CDE_SPARK_JOB_NAME,
    CURATED_S3_ROOT,
    RAW_S3_ROOT,
    TEMP_S3_URI,
)

BUSINESS_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('business_date', macros.datetime.utcnow().strftime('%Y-%m-%d')) }}"
)
SPARK_JOB_ARGS_TEMPLATE = (
    "--input-s3-uri {{ params.raw_s3_root }}/vehicle_health_"
    "{{ dag_run.conf.get('business_date', macros.datetime.utcnow().strftime('%Y-%m-%d')) | replace('-', '_') }}.zip "
    "--output-s3-uri {{ params.curated_s3_root }} "
    "--business-date "
    + BUSINESS_DATE_TEMPLATE
    + " "
    "--temp-dir {{ params.temp_s3_uri }} "
    "--max-reject-rate 0.10 --write-format parquet "
    "--aws-region {{ params.aws_region }}"
)


with DAG(
    dag_id="porsche_vehicle_health_cde_orchestration",
    description="Trigger CDE Spark ETL for Porsche vehicle health; writes curated Parquet to S3.",
    start_date=pendulum.datetime(2026, 6, 25, tz="UTC"),
    schedule=None,
    catchup=False,
    params={
        "curated_s3_root": Param(default=CURATED_S3_ROOT, type="string"),
        "raw_s3_root": Param(default=RAW_S3_ROOT, type="string"),
        "temp_s3_uri": Param(default=TEMP_S3_URI, type="string"),
        "aws_region": Param(default=AWS_REGION, type="string"),
    },
    tags=["porsche", "cde", "spark", "vehicle-health"],
) as dag:
    run_vehicle_health_pipeline = CdeRunJobOperator(
        task_id="run_vehicle_health_pipeline",
        job_name=CDE_SPARK_JOB_NAME,
        connection_id=CDE_CONNECTION_ID,
        wait=True,
        overrides={"args": [SPARK_JOB_ARGS_TEMPLATE]},
    )
