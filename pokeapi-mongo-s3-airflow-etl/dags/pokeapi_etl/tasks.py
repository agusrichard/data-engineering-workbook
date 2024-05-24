import asyncio
from typing import List

from airflow.decorators import task_group, task
from airflow.exceptions import AirflowException

from pokeapi_etl.operators import PokeAPIAsyncOperator
from pokeapi_etl.context import context_provider
from pokeapi_etl.utils import ping_mongo, request_pokeapi


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
