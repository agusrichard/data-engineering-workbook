import datetime
from airflow.decorators import dag, task



default_args = {
    'retries': 3
}


@dag(
    dag_id='check_file',
    description='Check file contents',
    schedule='@daily',
    default_args=default_args,
    start_date=datetime.datetime.now() - datetime.timedelta(days=1),
    catchup=False,
)
def check_file():
    @task.bash(env={'MY_VAR': 'Hello World!'})
    def create_file():
        return 'echo "Hi there!" >/tmp/dummy'

    @task.bash
    def check_file_exists():
        return 'test -f /tmp/dummy'

    @task
    def read_file():
        with open("/tmp/dummy", "r") as f:
            print(f.readlines())

    create_file() >> check_file_exists() >> read_file()


check_file()
