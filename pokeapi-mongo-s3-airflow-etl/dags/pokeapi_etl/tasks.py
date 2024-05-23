import asyncio
from typing import List

from airflow.decorators import task_group, task
from airflow.exceptions import AirflowException

from pokeapi_etl.operators import HTTPAsyncOperator
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


@task_group()
def request_pokemon():
    operator = HTTPAsyncOperator(task_id="request_pokemon", url="/pokemon/1", n_concurrency=10)
    print("request_pokemon_result", operator.output)

@task
def something(result: List[dict]):
    print("result", result)


@task_group
def request_list():
    print("SOMETHING IS HERE")
    request_pokemon_task = HTTPAsyncOperator(task_id="request_pokemon", url="/pokemon/1", n_concurrency=10)
    something(request_pokemon_task.output)
    # print("request_pokemon_result", operator.output)
