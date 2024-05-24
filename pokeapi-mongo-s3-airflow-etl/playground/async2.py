import asyncio
from asyncio import Queue
from random import random


LIMIT = 10000
OFFSET = 50


class StopException(Exception):
    pass


async def producer(queue: Queue):
    print("Producer: Running")
    i = 0
    while True:
        await queue.put(i)
        i += OFFSET


async def consumer(queue: Queue, num: int):
    print(f"Consumer {num}: Running")
    while True:
        item = await queue.get()
        await asyncio.sleep(random())
        if item > LIMIT:
            raise StopException
        print(f">got {item}")


async def run():
    try:
        queue = Queue(100)
        await asyncio.gather(producer(queue), *[consumer(queue, i) for i in range(10)])
    except StopException:
        print("Producer: Done")
        print("All Consumers: Done")


if __name__ == "__main__":
    asyncio.run(run())
