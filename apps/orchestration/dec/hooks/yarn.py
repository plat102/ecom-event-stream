"""Airflow glue for the shared YARN client."""
from airflow.hooks.base import BaseHook

from shared.connectors.yarn import YarnClient


class YarnHook(BaseHook):
    conn_name_attr = "yarn_conn_id"
    default_conn_name = "yarn_rm"
    conn_type = "http"
    hook_name = "YARN ResourceManager"

    def __init__(self, yarn_conn_id: str = default_conn_name, timeout: float = 10.0) -> None:
        super().__init__()
        self.yarn_conn_id = yarn_conn_id
        self.timeout = timeout

    def base_url(self) -> str:
        conn = self.get_connection(self.yarn_conn_id)
        if not conn.host:
            raise ValueError(f"connection {self.yarn_conn_id!r} has no host")
        scheme = conn.schema or "http"
        port = f":{conn.port}" if conn.port else ""
        return f"{scheme}://{conn.host}{port}"

    def get_conn(self) -> YarnClient:
        return YarnClient(self.base_url(), timeout=self.timeout)
