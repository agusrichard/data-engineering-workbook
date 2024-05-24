from airflow.decorators import task_group

from pokeapi_etl import tasks


@task_group
def ensure_prerequisites():
    tasks.ensure_pokeapi()
    tasks.ensure_mongo()


@task_group
def request_list():
    pass
