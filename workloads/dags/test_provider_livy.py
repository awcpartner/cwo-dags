from datetime import datetime
from airflow import DAG
from airflow.providers.apache.livy.operators.livy import LivyOperator

with DAG(
    dag_id="test_provider_livy",
    default_args={"args": [10]},
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    livy_python_task = LivyOperator(
        task_id="pi_python_task", file="hdfs:///spark/jobs/pi.py", polling_interval=60
    )
