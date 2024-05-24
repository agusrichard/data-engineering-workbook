from typing import Callable
from functools import wraps

from airflow.decorators import task
from airflow.models import Variable


class PokeAPIContext:
    __slots__ = (
        "context",
        "request_concurrency_num",
        "request_list_offset",
    )

    def __init__(self, airflow_context: dict):
        self.context = airflow_context
        self.request_concurrency_num = Variable.get(
            "REQUEST_CONCURRENCY_NUM", default_var=10
        )
        self.request_list_offset = Variable.get("REQUEST_LIST_OFFSET", default_var=50)


def context_provider(func: Callable):
    @task
    def inner(**kwargs):
        context = PokeAPIContext(kwargs)
        return func(context)

    return inner
