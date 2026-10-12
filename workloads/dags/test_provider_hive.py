from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    dag_id="test_provider_hive",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    create_table_hive_task = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="hive_conn",
        sql="""
                CREATE TABLE IF NOT EXISTS example_hive (
                    a STRING,
                    b INT
                )
                PARTITIONED BY (c INT)
            """,
    )

    write_table_hive_task = SQLExecuteQueryOperator(
        task_id="write_table",
        conn_id="hive_conn",
        sql="INSERT INTO example_hive PARTITION (c)  VALUES ('test1', 1, 2)",
    )

    read_table_hive_task = SQLExecuteQueryOperator(
        task_id="read_table",
        conn_id="hive_conn",
        sql="SELECT * FROM example_hive",
    )

    drop_table_hive_task = SQLExecuteQueryOperator(
        task_id="drop_table",
        conn_id="hive_conn",
        sql="DROP TABLE example_hive",
    )

    # fmt: off
    # pylint: disable=pointless-statement
    create_table_hive_task >> write_table_hive_task >> read_table_hive_task >> drop_table_hive_task
    # fmt: on
