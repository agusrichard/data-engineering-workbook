from typing import Callable
from functools import wraps

from airflow.decorators import task


class PokeAPIContext:
    __slots__ = ("context",)

    def __init__(self, airflow_context: dict):
        self.context = airflow_context


def context_provider(func: Callable):
    @wraps(func)
    @task
    def inner(**kwargs):
        context = PokeAPIContext(kwargs)
        return func(context)

    return inner
