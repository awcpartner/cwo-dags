from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    dag_id="test_provider_impala",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    create_table_impala_task = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="impala_default",
        sql="""
                CREATE TABLE IF NOT EXISTS cde_test_impala_base (
                    a STRING,
                    b INT
                )
                PARTITIONED BY (c INT)
            """,
    )

    write_table_impala_task = SQLExecuteQueryOperator(
        task_id="write_table",
        conn_id="impala_default",
        sql="INSERT INTO cde_test_impala_base PARTITION (c)  VALUES ('test1', 1, 2)",
    )

    read_table_impala_task = SQLExecuteQueryOperator(
        task_id="read_table",
        conn_id="impala_default",
        sql="SELECT * FROM cde_test_impala_base",
    )

    drop_table_impala_task = SQLExecuteQueryOperator(
        task_id="drop_table",
        conn_id="impala_default",
        sql="DROP TABLE cde_test_impala_base",
    )

    # fmt: off
    # pylint: disable=pointless-statement
    create_table_impala_task >> write_table_impala_task >> read_table_impala_task >> drop_table_impala_task
    # fmt: on
