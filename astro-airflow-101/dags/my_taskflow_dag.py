from airflow.decorators import dag, task
from datetime import datetime


default_args = {
    'retries': 3,
}


@dag(
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    description="This is a DAG for my taskflow DAG",
    schedule="@daily",
    catchup=False,
    default_args=default_args,
)
def my_taskflow_dag():

    @task
    def print_a():
        print("Hello from a")

    @task
    def print_b():
        print("Hello from b")

    @task
    def print_c():
        print("Hello from c")

    print_a() >> print_b() >> print_c()


my_taskflow_dag()