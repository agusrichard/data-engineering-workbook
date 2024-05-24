import asyncio
from typing import List

from airflow.utils.context import Context
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator

from pokeapi_etl.utils import request_pokeapi
from pokeapi_etl.context import PokeAPIContext


class PokeAPIAsyncOperator(BaseOperator):
    def __init__(self, url: str, operation_type: str = "list", **kwargs):
        super().__init__(**kwargs)
        self.url = url
        self._context = None
        if operation_type not in ["list", "data"]:
            raise AirflowException("Operation types allowed only list or data")
        self.operation_type = operation_type

    def execute(self, context: Context):
        pokeapi_context = PokeAPIContext(context)
        self._context = pokeapi_context
        if self.operation_type == "list":
            asyncio.run(self._ingest_list())
        else:
            asyncio.run(self._ingest_data())

    def _request_list(self):
        pass

    def _write_list(self):
        pass

    async def _ingest_list(self):
        pass

    async def _ingest_data(self):
        pass

    async def _run(self, n_concurrency: int) -> List[dict]:
        tasks = [request_pokeapi(self.url) for _ in range(n_concurrency)]
        values = await asyncio.gather(*tasks)
        values = list(values)
        return values
