import asyncio
from typing import List

from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator

from pokeapi_etl.utils import request_pokeapi
# from pokeapi_etl.context import PokeAPIContext


class HTTPAsyncOperator(BaseOperator):
    def __init__(self, url: str, n_concurrency: int = 10, **kwargs):
        super().__init__(**kwargs)
        self.url = url
        self.n_concurrency = n_concurrency

    def execute(self, context: Context) -> List[dict]:
        result = asyncio.run(self._run())
        return result

    async def _run(self) -> List[dict]:
        tasks = [request_pokeapi(self.url) for _ in range(self.n_concurrency)]
        values = await asyncio.gather(*tasks)
        values = list(values)
        return values



