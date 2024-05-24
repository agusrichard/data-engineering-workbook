import asyncio

from airflow.decorators import task
from airflow.exceptions import AirflowException

from pokeapi_etl.utils import ping_mongo, request_pokeapi_data


@task
def ensure_pokeapi():
    async def inner():
        response = await request_pokeapi_data("pokemon", 1)
        if not response:
            raise AirflowException("Failed to connect to PokeAPI")

    asyncio.run(inner())


@task
def ensure_mongo():
    ping_mongo()
