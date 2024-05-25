import aiohttp
from typing import List

from airflow.models import Connection
from airflow.exceptions import AirflowException
from airflow.providers.mongo.hooks.mongo import MongoHook


def get_pokeapi_url(entity_name: str) -> str:
    """
    Get the PokeAPI URL based on the entity_name (e.g pokemon, ability, etc)

    Parameters
    ----------
    entity_name : str
        The resource name (and path name) of PokeAPI

    Returns
    -------
    str
        The full url for PokeAPI resource

    """
    connection = Connection.get_connection_from_secrets("pokeapi")
    base_url = f"{connection.schema}://{connection.host}"
    return f"{base_url}/{entity_name}"


async def request_pokeapi(full_url: str) -> dict:
    """
    Send an asynchronous GET request to the specified URL and return the JSON response.

    Parameters
    ----------
    full_url : str
        The full URL to send the GET request to.

    Returns
    -------
    dict
        The JSON response from the PokeAPI.

    """
    async with aiohttp.ClientSession() as session:
        async with session.get(full_url) as response:
            return await response.json()


async def request_pokeapi_data(entity_name: str, entity_id: int) -> dict:
    """
    Request specific data from the PokeAPI for a given entity.

    Parameters
    ----------
    entity_name : str
        The name of the entity to request (e.g., 'pokemon', 'ability').
    entity_id : int
        The ID of the entity to request.

    Returns
    -------
    dict
        The data returned from the PokeAPI for the specified entity.

    Raises
    ------
    Exception
        If the request fails, an exception with the error message is printed.
    """
    try:
        request_url = get_pokeapi_url(f"{entity_name}/{entity_id}")
        print(f"Request PokeAPI data {entity_name}: {request_url}")
        data = await request_pokeapi(request_url)
        return data
    except Exception as e:
        print(f"Failed requesting PokeAPI data: {str(e)}")


async def request_pokeapi_list(
    entity_name: str, offset: int, limit: int = 0
) -> List[dict]:
    """
    Request a list of entities from the PokeAPI with pagination support.

    Parameters
    ----------
    entity_name : str
        The name of the entity list to request (e.g., 'pokemon', 'ability').
    offset : int
        The number of items to skip before starting to collect the result set.
    limit : int, optional
        The maximum number of items to return (default is 0).

    Returns
    -------
    List[dict]
        The list of entities returned from the PokeAPI.

    Raises
    ------
    Exception
        If the request fails, an exception with the error message is printed.
    """
    try:
        request_url = get_pokeapi_url(f"{entity_name}/?limit={limit}&offset={offset}")
        print(f"Request PokeAPI list {entity_name}: {request_url}")
        data = await request_pokeapi(request_url)
        return data["results"]
    except Exception as e:
        print(f"Failed requesting PokeAPI list: {str(e)}")


def get_mongo_client():
    hook = MongoHook(mongo_conn_id="mongo_default")
    return hook.get_conn()


def ping_mongo():
    """
    Ping the MongoDB deployment to ensure a successful connection.

    Raises
    ------
    AirflowException
        If the connection to MongoDB fails.
    """
    client = get_mongo_client()
    try:
        client.admin.command("ping")
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception:
        raise AirflowException("Failed to connect to MongoDB")


def upload_pokeapi_list(entity_name: str, batch: List[dict]):
    try:
        client = get_mongo_client()
        db = client.pokeapi_list
        collection = db[entity_name]
        if batch:
            result = collection.insert_many(batch)
            print(f"Inserted to '{entity_name}' collection: {result.inserted_ids}")
    except Exception as e:
        print(f"Failed to insert: {e}")
