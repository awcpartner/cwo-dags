"""Orders cleaning pipeline.

Reads the daily raw orders export from S3 and produces two files back in S3. Trigger it manually from the Airflow UI.

    raw     : s3://ccf-s3-backup-uswest2/airflow-demo/raw/raw_orders_2026-11-25.csv
    clean   : s3://ccf-s3-backup-uswest2/airflow-demo/clean/orders_2026-11-25.csv
    summary : s3://ccf-s3-backup-uswest2/airflow-demo/clean/region_summary_2026-11-25.csv

    read_raw_orders -> clean_orders -> write_clean_orders
    read_raw_orders -> region_summary
"""
import csv
import io
from datetime import timedelta

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

AWS_CONN_ID = "aws_s3_cwo_conn"
BUCKET = "ccf-s3-backup-uswest2"
RAW_KEY = "airflow-demo/raw/raw_orders_2026-11-25.csv"
CLEAN_KEY = "airflow-demo/clean/orders_2026-11-25.csv"
SUMMARY_KEY = "airflow-demo/clean/region_summary_2026-11-25.csv"
CLEAN_COLUMNS = ["order_id", "customer_email", "region", "order_date", "amount", "status"]


def read_raw_orders():
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    body = hook.read_key(key=RAW_KEY, bucket_name=BUCKET)
    rows = list(csv.DictReader(io.StringIO(body)))
    print(f"read {len(rows)} raw rows from s3://{BUCKET}/{RAW_KEY}")
    return rows


def clean_orders(ti):
    raw_rows = ti.xcom_pull(task_ids="read_raw_orders")
    seen_ids = set()
    cleaned = []
    for row in raw_rows:
        order_id = int(row["order_id"])
        if order_id in seen_ids:
            continue
        seen_ids.add(order_id)

        status = row["status"].strip().lower()
        if status == "cancelled":
            continue

        cleaned.append(
            {
                "order_id": order_id,
                "customer_email": row["customer_email"].strip().lower(),
                "region": row["region"].strip().lower(),
                "order_date": row["order_date"].strip(),
                "amount": round(float(row["amount"].replace("$", "").replace(",", "").strip()), 2),
                "status": status,
            }
        )
    print(f"cleaned {len(cleaned)} of {len(raw_rows)} rows")
    return cleaned


def write_clean_orders(ti):
    cleaned = ti.xcom_pull(task_ids="clean_orders")
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=CLEAN_COLUMNS)
    writer.writeheader()
    writer.writerows(cleaned)
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    hook.load_string(out.getvalue(), key=CLEAN_KEY, bucket_name=BUCKET, replace=True)
    print(f"wrote {len(cleaned)} rows to s3://{BUCKET}/{CLEAN_KEY}")


def region_summary(ti):
    """Counts raw orders per region and writes a small summary file."""
    raw_rows = ti.xcom_pull(task_ids="read_raw_orders")
    counts = {}
    for row in raw_rows:
        region = row["region"].strip().lower()
        counts[region] = counts.get(region, 0) + 1

    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["region", "raw_order_count"])
    for region in sorted(counts):
        writer.writerow([region, counts[region]])
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    hook.load_string(out.getvalue(), key=SUMMARY_KEY, bucket_name=BUCKET, replace=True)
    print(f"wrote summary for {len(counts)} regions to s3://{BUCKET}/{SUMMARY_KEY}")


with DAG(
    dag_id="orders_cleaning_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "data-eng", "retries": 0, "retry_delay": timedelta(seconds=30)},
    description="Cleans the raw daily orders export in S3 and writes the result back to S3",
    tags=["demo", "s3"],
):
    read_raw = PythonOperator(task_id="read_raw_orders", python_callable=read_raw_orders)
    clean = PythonOperator(task_id="clean_orders", python_callable=clean_orders)
    write_clean = PythonOperator(task_id="write_clean_orders", python_callable=write_clean_orders)
    summary = PythonOperator(task_id="region_summary", python_callable=region_summary)

    read_raw >> clean >> write_clean
    read_raw >> summary
