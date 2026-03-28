from datetime import datetime

from airflow.sdk import DAG, task


with DAG(
    dag_id="branch_python_dag",
    default_args={"owner": "airflow"},
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    @task
    def hello_world():
        print("Hello World!")

    hello_world()
