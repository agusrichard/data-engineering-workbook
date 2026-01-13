from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='taskflow_hello_world_v2',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['example', 'taskflow'],
)
def hello_world_etl():
    @task()
    def get_name():
        return "Airflow User"

    @task()
    def greet(name):
        print(f"Hello, {name}! Welcome to the TaskFlow API.")

    print_date = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    name_data = get_name()
    print_date >> name_data
    greet(name_data)


hello_world_etl()