"""Airflow glue for the shared Postgres client, which only reads attributes off what it gets."""
from types import SimpleNamespace

from airflow.hooks.base import BaseHook

from shared.connectors.postgres import PostgresClient


class WarehouseHook(BaseHook):
    conn_name_attr = "postgres_conn_id"
    default_conn_name = "postgres_warehouse"
    conn_type = "postgres"
    hook_name = "Analytics warehouse"

    def __init__(self, postgres_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.postgres_conn_id = postgres_conn_id

    def connection_settings(self) -> SimpleNamespace:
        conn = self.get_connection(self.postgres_conn_id)
        return SimpleNamespace(
            POSTGRES_HOST=conn.host,
            POSTGRES_PORT=conn.port or 5432,
            POSTGRES_DB=conn.schema,
            POSTGRES_USER=conn.login,
            POSTGRES_PASSWORD=conn.password,
        )

    def get_conn(self) -> PostgresClient:
        return PostgresClient(self.connection_settings())
