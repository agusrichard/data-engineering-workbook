"""
This ETL DAG is consuming PokeAPI data, store them in MongoDB as raw data,
them store them in their own folders based on Pokemon type

Resource: https://pokeapi.co/docs/v2#pokemon
"""

from datetime import datetime
from airflow.decorators import dag

from pokeapi_etl.task_groups import ensure_prerequisites, ingest_list


@dag(
    doc_md=__doc__,
    schedule=None,
    start_date=datetime(2024, 5, 18),
    catchup=False,
    default_args={
        "retries": 0,
    },
)
def pokeapi_etl():
    ensure_prerequisites_group = ensure_prerequisites()
    ingest_list_group = ingest_list()

    ensure_prerequisites_group >> ingest_list_group


pokeapi_etl()
