import aiohttp

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.mongo.hooks.mongo import MongoHook


def get_pokeapi_url(url: str) -> str:
    connection = Connection.get_connection_from_secrets("pokeapi")
    base_url = f"{connection.schema}://{connection.host}"
    return f"{base_url}{url}"

async def request_pokeapi(url: str):
    request_url = get_pokeapi_url(url)
    async with aiohttp.ClientSession() as session:
        async with session.get(request_url) as response:
            return await response.json()


def ping_mongo():
    hook = MongoHook(mongo_conn_id="mongo_default")
    client = hook.get_conn()
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception:
        raise AirflowException("Failed to connect to MongoDB")
