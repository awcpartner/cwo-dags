"""Orders typed load.

Second job on the same daily raw orders export in S3. It loads the file two ways and writes a typed copy back to S3.
Trigger it manually from the Airflow UI.

    raw   : s3://ccf-s3-backup-uswest2/airflow-demo/raw/raw_orders_2026-11-25.csv
    typed : s3://ccf-s3-backup-uswest2/airflow-demo/typed/orders_typed_2026-11-25.jsonl

    fetch_raw_file -> load_legacy_export
    fetch_raw_file -> load_typed_snapshot
"""
import csv
import io
import json
from datetime import date, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

AWS_CONN_ID = "aws_s3_cwo_conn"
BUCKET = "ccf-s3-backup-uswest2"
RAW_KEY = "airflow-demo/raw/raw_orders_2026-11-25.csv"
TYPED_KEY = "airflow-demo/typed/orders_typed_2026-11-25.jsonl"

# Declared type of every column in the export. "amount" is kept as raw text here, money parsing happens downstream. "order_date" is an ISO date (YYYY-MM-DD) parsed with date.fromisoformat.
COLUMN_TYPES = {
    "order_id": int,
    "customer_email": str,
    "region": str,
    "order_date": date.fromisoformat,
    "amount": str,
    "status": str,
}


def fetch_raw_file():
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    body = hook.read_key(key=RAW_KEY, bucket_name=BUCKET)
    print(f"fetched {len(body)} characters from s3://{BUCKET}/{RAW_KEY}")
    return body


def load_legacy_export(ti):
    """Loads the raw CSV export and returns the number of records parsed."""
    body = ti.xcom_pull(task_ids="fetch_raw_file")
    records = list(csv.DictReader(io.StringIO(body)))
    print(f"loaded {len(records)} records from the export")
    return len(records)


def load_typed_snapshot(ti):
    """Applies COLUMN_TYPES to every row and writes the typed rows back to S3 as JSON lines."""
    body = ti.xcom_pull(task_ids="fetch_raw_file")
    typed_rows = []
    for row in csv.DictReader(io.StringIO(body)):
        typed_rows.append({column: cast(row[column]) for column, cast in COLUMN_TYPES.items()})

    text = "\n".join(json.dumps(r, default=str) for r in typed_rows) + "\n"
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    hook.load_string(text, key=TYPED_KEY, bucket_name=BUCKET, replace=True)
    print(f"wrote {len(typed_rows)} typed rows to s3://{BUCKET}/{TYPED_KEY}")


with DAG(
    dag_id="orders_typed_load",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "data-eng", "retries": 0, "retry_delay": timedelta(seconds=30)},
    description="Loads the raw daily orders export from S3 and writes a typed copy back to S3",
    tags=["demo", "s3"],
):
    fetch = PythonOperator(task_id="fetch_raw_file", python_callable=fetch_raw_file)
    legacy = PythonOperator(task_id="load_legacy_export", python_callable=load_legacy_export)
    typed = PythonOperator(task_id="load_typed_snapshot", python_callable=load_typed_snapshot)

    fetch >> legacy
    fetch >> typed
