from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.sensors.time_delta import TimeDeltaSensorAsync, TimeDeltaSensor
from airflow.sensors.time_sensor import TimeSensorAsync
from airflow.operators.python import PythonOperator

# Define the DAG
dag = DAG(
    'example_time_sensor_async',
    default_args={'owner': 'airflow'},
    start_date=datetime(2024, 11, 4),
    schedule_interval='@daily',
)

# Add a TimeDeltaSensorAsync task to wait 2 hours from the start of the DAG run
wait_2_hours = TimeSensorAsync(
    dag=dag,
    task_id="timeout_after_second_date_in_the_future_async",
    timeout=1,
    soft_fail=True,
    target_time=(datetime.now(tz=timezone.utc) + timedelta(minutes=2)).time(),
)

# Define other tasks that run after the delay
def sample_task():
    print("Task executed 2 hours after the DAG run start")

sample_task = PythonOperator(
    task_id='sample_task',
    python_callable=sample_task,
    dag=dag,
)

# Set the task dependencies
wait_2_hours >> sample_task
