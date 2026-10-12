import pendulum
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.sensors.time_delta import TimeDeltaSensorAsync


@task
def continue_processing():
    """This task runs after the waiting period is over."""
    print(f"The wait is over! Resuming pipeline at {pendulum.now('UTC')}.")
    return {"status": "resumed_successfully"}


@dag(
    dag_id="deferrable_timedelta_sensor_async_example",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    doc_md="""
    ### Deferrable TimeDeltaSensor DAG
    
    This DAG demonstrates a deferrable operator.
    1.  **wait_for_30_seconds**: This task defers its execution for a fixed duration. 
        It releases its worker slot and tells the Triggerer to 'wake it up' after 30 seconds.
    2.  **continue_processing**: This task runs only after the time has elapsed.
    """,
    tags=["deferrable"],
)
def deferrable_timedelta_dag():
    # This is the deferrable operator.
    # It will wait for 30 seconds without holding a worker slot.
    # The Triggerer simply manages a timer in the background.
    wait_for_30_seconds = TimeDeltaSensorAsync(
        task_id="wait_for_30_seconds",
        delta=timedelta(seconds=30),
    )

    wait_for_30_seconds >> continue_processing()


# Instantiate the DAG
deferrable_timedelta_dag()
