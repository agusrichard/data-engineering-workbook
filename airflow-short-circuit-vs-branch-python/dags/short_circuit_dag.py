from datetime import datetime, timezone

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import ShortCircuitOperator


def is_weekday(**context):
    """Returns True on weekdays, False on weekends — skipping downstream tasks on weekends."""
    logical_date = context['logical_date']
    return logical_date.weekday() < 5


with DAG(
    dag_id="short_circuit_dag",
    default_args={"owner": "airflow"},
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    check_weekday = ShortCircuitOperator(
        task_id="check_weekday",
        python_callable=is_weekday,
    )

    @task
    def process_data():
        print("Processing data — only runs on weekdays!")

    @task
    def send_report():
        print("Sending report — only runs on weekdays!")

    check_weekday >> process_data() >> send_report()
