from airflow.utils.context import Context
from airflow.providers.http.hooks.http import HttpHook
from airflow.models.baseoperator import BaseOperator


class HTTPAsyncOperator(BaseOperator):
    def __init__(self, conn_id: str, **kwargs):
        super().__init__(**kwargs)
        hook = HttpHook(http_conn_id=conn_id)
        self.base_url = hook.base_url

    def execute(self, context: Context) -> dict:
        print("base_url", self.base_url)
