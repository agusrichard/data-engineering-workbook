from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from datetime import datetime


@dag(
    schedule=None,
    start_date=datetime(2021, 11, 1),
    tags=['file_sensor'],
    catchup=False,
)
def first_dag():
    wait_for_files = FileSensor.partial(
        task_id='wait_for_files',
        fs_conn_id='fs_default',
    ).expand(
        filepath=['data_1.csv', 'data_2.csv', 'data_3.csv'],
    )

    @task
    def process_file():
        print('Process the file')

    wait_for_files >> process_file()

first_dag()
