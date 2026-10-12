import logging
from pendulum import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

try:
    from airflow.sdk import Variable
except ImportError:
    # Retry import with Airflow 2.x libraries
    from airflow.models import Variable

with DAG(
    dag_id="test_vault_access_dag",
    start_date=datetime(2025, 11, 14),
    schedule=None,
    catchup=False,
):

    def test_vault_access():
        """
        Try to read a secret from Hashicorp Vault through Airflow's secrets backend.
        Compatible with Airflow 2.10.0 and above.
        """
        try:
            # Read Airflow Variable 'test_vault_secret_variable' which is assumed to be stored in Vault
            secret_value = Variable.get("test_vault_secret_variable")
            logging.info(f"✅ Successfully retrieved secret from Vault: {secret_value}")
        except Exception as e:
            logging.error(f"❌ Failed to retrieve secret from Vault: {e}")
            raise

    run_this = PythonOperator(
        task_id="test_vault_access", python_callable=test_vault_access
    )
