from datetime import datetime

from airflow.sdk import DAG, task
from airflow.operators.python import BranchPythonOperator


def choose_branch(**context):
    """Routes to a different task based on the day of the week."""
    logical_date = context['logical_date']
    if logical_date.weekday() < 5:
        return "process_weekday_data"
    else:
        return "process_weekend_data"


with DAG(
    dag_id="branch_python_dag",
    default_args={"owner": "airflow"},
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    branch = BranchPythonOperator(
        task_id="choose_branch",
        python_callable=choose_branch,
    )

    @task
    def process_weekday_data():
        print("Processing weekday data — heavy workload!")

    @task
    def process_weekend_data():
        print("Processing weekend data — light workload!")

    @task(trigger_rule="none_failed_min_one_success")
    def send_report():
        print("Sending report — runs regardless of which branch was taken!")

    weekday = process_weekday_data()
    weekend = process_weekend_data()

    branch >> [weekday, weekend] >> send_report()
