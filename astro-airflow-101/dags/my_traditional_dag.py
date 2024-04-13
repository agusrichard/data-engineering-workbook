from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator


default_args = {
    'retries': 3,
}

def print_a():
    print("Hello from a")


def print_b():
    print("Hello from b")


def print_c():
    print("Hello from c")


with DAG(
    'my_traditional_dag',
    start_date=datetime(2021, 1, 1),
    description='My traditional DAG',
    tags=['my_traditional'],
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
) as dag:
    task_a = PythonOperator(task_id="task_a", python_callable=print_a)
    task_b = PythonOperator(task_id="task_b", python_callable=print_b)
    task_c = PythonOperator(task_id="task_c", python_callable=print_c)

    task_a >> task_b >> task_c
