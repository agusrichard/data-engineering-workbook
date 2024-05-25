import asyncio
from asyncio import Queue

from airflow.utils.context import Context
from airflow.models.baseoperator import BaseOperator

from pokeapi_etl.context import PokeAPIContext
from pokeapi_etl.exceptions import StopException, OutOfQuotaException
from pokeapi_etl.utils import (
    insert_pokeapi_data,
    insert_pokeapi_list,
    request_pokeapi_data,
    request_pokeapi_list,
    get_pokeapi_list_collection,
)


class IngestPokeAPIBaseOperator(BaseOperator):
    def __init__(self, entity_name: str, **kwargs):
        super().__init__(**kwargs)
        self.entity_name = entity_name
        self.context: PokeAPIContext | None = None

    def execute(self, context: Context):
        pokeapi_context = PokeAPIContext(context)
        self.context = pokeapi_context
        asyncio.run(self._ingest())

    async def _producer(self, queue: Queue):
        raise NotImplementedError("Should be implemented by subclasses")

    async def _consumer(self, queue: Queue, num: int):
        raise NotImplementedError("Should be implemented by subclasses")

    async def _ingest(self):
        try:
            queue = Queue(self.context.max_size_queue)
            await asyncio.gather(
                self._producer(queue),
                *[
                    self._consumer(queue, i)
                    for i in range(self.context.concurrency_num)
                ],
            )
            await queue.join()
        except (StopException, OutOfQuotaException):
            print("Producer: Done")
            print("Consumer: Done")


class IngestPokeAPIListOperator(IngestPokeAPIBaseOperator):
    async def _producer(self, queue: Queue):
        print("Producer: Running")
        i = 0
        while True:
            await queue.put(i)
            i += self.context.list_offset

    async def _consumer(self, queue: Queue, num: int):
        print(f"Consumer {num}: Running")
        while True:
            item = await queue.get()
            data = await request_pokeapi_list(
                self.entity_name, item, self.context.list_offset
            )
            insert_pokeapi_list(self.entity_name, data)
            if not data:
                raise StopException


class IngestPokeAPIDataOperator(IngestPokeAPIBaseOperator):
    async def _producer(self, queue: Queue):
        print("Producer: Running")
        collection = get_pokeapi_list_collection(self.entity_name)
        batch = []
        for item in collection.find():
            batch.append(item["url"])
            if len(batch) > self.context.batch_size_data:
                await queue.put(batch)
                batch = []

    async def _consumer(self, queue: Queue, num: int):
        print(f"Consumer {num}: Running")
        while True:
            batch = await queue.get()
            data = await request_pokeapi_data(batch)
            insert_pokeapi_data(self.entity_name, data)
            queue.task_done()
