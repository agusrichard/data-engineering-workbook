from airflow.decorators import task_group

from pokeapi_etl import tasks
from pokeapi_etl.operators import PokeAPIAsyncOperator


@task_group
def ensure_mongo():
    tasks.ensure_mongo_connection() >> tasks.drop_all_collections()


@task_group
def ensure_prerequisites():
    tasks.ensure_pokeapi()
    ensure_mongo()


@task_group
def ingest_list():
    entity_list = ["pokemon", "type", "pokemon-habitat", "pokemon-species", "ability"]
    for entity in entity_list:
        PokeAPIAsyncOperator(
            task_id=f"ingest_{entity}_list", entity_name=entity, operation_type="list"
        )
