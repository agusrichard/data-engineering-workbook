from datetime import datetime
from airflow.utils.helpers import chain
from airflow.decorators import dag, task


@dag(
    dag_id="first_dag",
    description="My First Dag",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def first_dag():
    @task
    def task_1():
        print("Task 1")

    @task
    def task_2():
        print("Task 2")

    @task
    def task_3():
        print("Task 3")

    task_1() >> task_2() >> task_3()


first_dag()
