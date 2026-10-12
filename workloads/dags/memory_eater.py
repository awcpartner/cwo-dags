from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def memory_eater():
    @task
    def forever_eat():
        import time

        big_memory = bytearray()
        while True:
            print(f"Allocating big chunk of memory; allocated={len(big_memory)}")
            chunk = bytearray(512000000)
            print(f"  chunk={len(chunk)}")
            big_memory += chunk
            print(f"  new size={len(big_memory)}")
            time.sleep(5)

    forever_eat()


memory_eater()
