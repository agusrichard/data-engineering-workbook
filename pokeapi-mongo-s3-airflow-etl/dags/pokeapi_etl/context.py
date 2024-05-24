from typing import Callable
from functools import wraps

from airflow.decorators import task
from airflow.models import Variable


class PokeAPIContext:
    __slots__ = ("context", "concurrency_num", "list_offset", "max_size_queue")

    def __init__(self, airflow_context: dict):
        self.context = airflow_context
        self.concurrency_num = Variable.get("CONCURRENCY_NUM", default_var=10)
        self.list_offset = Variable.get("LIST_OFFSET", default_var=50)
        self.max_size_queue = Variable.get("MAX_SIZE_QUEUE", default_var=10)


def context_provider(func: Callable):
    @task
    def inner(**kwargs):
        context = PokeAPIContext(kwargs)
        return func(context)

    return inner
