"""The warehouse flavour of a health check: the probe is a query returning exactly one row.

Its columns are what the predicate judges and what the message is formatted with, so the
numbers travel with the failure.
"""
from airflow.exceptions import AirflowException

from hooks.warehouse import WarehouseHook
from operators.health_check import HealthCheckOperator, jsonable

__all__ = ["SqlCheckOperator", "jsonable"]


class SqlCheckOperator(HealthCheckOperator):
    template_fields = ("sql", "message")

    def __init__(self, *, sql: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.sql = sql

    def client(self):
        return WarehouseHook(self.conn_id or WarehouseHook.default_conn_name).get_conn()

    def measure(self, client) -> dict:
        """Overridden rather than passed as `probe`: `sql` is a template field, and a bound
        method would read it off the pre-render copy of this operator."""
        rows, columns = client.fetch_all(self.sql)
        if len(rows) != 1:
            raise AirflowException(f"check query returned {len(rows)} rows, expected exactly 1")
        return {name: jsonable(value) for name, value in zip(columns, rows[0])}
