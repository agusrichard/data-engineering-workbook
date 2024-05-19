import asyncio

from airflow.decorators import task_group, task
from airflow.exceptions import AirflowException

from pokeapi_etl.utils import request_pokeapi, ping_mongo


@task
def ensure_pokeapi():
    async def inner():
        response = await request_pokeapi("/pokemon/ditto")
        if not response:
            raise AirflowException("Failed to connect to PokeAPI")

    asyncio.run(inner())


@task
def ensure_mongo():
    ping_mongo()


@task_group
def ensure_prerequisites():
    ensure_pokeapi()
    ensure_mongo()
