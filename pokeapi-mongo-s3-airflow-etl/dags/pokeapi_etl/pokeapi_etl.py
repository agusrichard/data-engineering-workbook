"""
This ETL DAG is consuming PokeAPI data, store them in MongoDB as raw data,
them store them in their own folders based on Pokemon type

Resource: https://pokeapi.co/docs/v2#pokemon
"""

from datetime import datetime
from airflow.decorators import dag

from pokeapi_etl.tasks import ensure_prerequisites


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
    ensure_prerequisites()

pokeapi_etl()
