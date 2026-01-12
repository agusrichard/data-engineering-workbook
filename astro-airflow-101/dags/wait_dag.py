import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.time_delta import TimeDeltaSensorAsync

with DAG(
    dag_id="example_time_delta_sensor_async",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    wait = TimeDeltaSensorAsync(task_id="wait", delta=datetime.timedelta(minutes=2))

    finish = EmptyOperator(task_id="finish")
    wait >> finish
