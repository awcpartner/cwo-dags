from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id="test_provider_spark",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    # Prerequisites:
    # - Existing "spark_yarn" connection (ex. airflow connections add --conn-uri "spark://yarn?deploy-mode=cluster" spark_yarn)
    # - All Hadoop and YARN configuration files (ex. core-site.xml) available in executor filesystem at /etc/hadoop/conf
    spark_pi = SparkSubmitOperator(
        application="https://raw.githubusercontent.com/apache/spark/refs/tags/v4.1.1/examples/src/main/python/pi.py",
        task_id="spark_pi_on_yarn",
        deploy_mode="cluster",
        conn_id="spark_yarn",
        # Uncomment to use different Spark version for run jobs in older environments
        # spark_binary="/opt/spark-3.5.4/bin/spark-submit",
        env_vars={
            "HADOOP_USER_NAME": "hdfs",
            "YARN_CONF_DIR": "/etc/hadoop/conf",
            "HADOOP_CONF_DIR": "/etc/hadoop/conf",
        },
    )
