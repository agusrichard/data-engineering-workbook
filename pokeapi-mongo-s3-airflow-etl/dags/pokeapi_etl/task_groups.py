from airflow.decorators import task_group

from pokeapi_etl import tasks
from pokeapi_etl.operators import PokeAPIAsyncOperator


@task_group
def ensure_prerequisites():
    tasks.ensure_pokeapi()
    tasks.ensure_mongo()


@task_group
def ingest_list():
    PokeAPIAsyncOperator(
        task_id="ingest_pokemon_list", entity_name="pokemon", operation_type="list"
    )
