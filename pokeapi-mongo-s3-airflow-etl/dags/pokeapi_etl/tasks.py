import asyncio

from airflow.decorators import task
from airflow.exceptions import AirflowException

from pokeapi_etl.constants import ENTITY_LIST
from pokeapi_etl.utils import request_pokeapi_data, get_mongo_client, ping_mongo


@task
def ensure_pokeapi():
    async def inner():
        response = await request_pokeapi_data("pokemon", 1)
        if not response:
            raise AirflowException("Failed to connect to PokeAPI")

    asyncio.run(inner())


@task
def ensure_mongo_connection():
    ping_mongo()


@task
def drop_all_collections():
    for db_name in ["pokemon_list", "pokemon_data"]:
        for collection_name in ENTITY_LIST:
            client = get_mongo_client()
            db = client[db_name]
            collection = db[collection_name]
            collection.drop()
            print(f"{db_name} -- {collection_name} is dropped")
