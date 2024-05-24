import asyncio
from asyncio import Queue
from random import random

from airflow.utils.context import Context
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator

from pokeapi_etl.context import PokeAPIContext
from pokeapi_etl.exceptions import StopException
from pokeapi_etl.utils import request_pokeapi_list


class PokeAPIAsyncOperator(BaseOperator):
    def __init__(self, entity_name: str, operation_type: str = "list", **kwargs):
        super().__init__(**kwargs)
        self.entity_name = entity_name
        self._context: PokeAPIContext | None = None
        if operation_type not in ["list", "data"]:
            raise AirflowException("Operation types allowed only list or data")
        self.operation_type = operation_type

    def execute(self, context: Context):
        pokeapi_context = PokeAPIContext(context)
        self._context = pokeapi_context
        asyncio.run(self._ingest_list())

    async def producer(self, queue: Queue):
        print("Producer: Running")
        i = 0
        while True:
            await queue.put(i)
            i += self._context.list_offset

    async def consumer(self, queue: Queue, num: int):
        print(f"Consumer {num}: Running")
        while True:
            item = await queue.get()
            data = await request_pokeapi_list(
                self.entity_name, item, self._context.list_offset
            )
            await asyncio.sleep(random())
            if item > 500:
                raise StopException
            print(f">got {data}")

    async def _ingest_list(self):
        try:
            queue = Queue(self._context.max_size_queue)
            await asyncio.gather(
                self.producer(queue),
                *[
                    self.consumer(queue, i)
                    for i in range(self._context.concurrency_num)
                ],
            )
        except StopException:
            print("Producer: Done")
            print("Consumer: Done")
