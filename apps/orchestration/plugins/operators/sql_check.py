"""One query, one predicate, one message that carries the measured numbers."""
from collections.abc import Callable
from datetime import timedelta
from decimal import Decimal

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from hooks.warehouse import WarehouseHook


def jsonable(value):
    """XCom is JSON, and Postgres hands back Decimal and timedelta."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, timedelta):
        return value.total_seconds()
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


class SqlCheckOperator(BaseOperator):
    """`sql` must return exactly one row; its columns are what `predicate` judges and
    `message` is formatted with, so the numbers travel with the failure."""

    template_fields = ("sql", "message")

    def __init__(
        self,
        *,
        sql: str,
        predicate: Callable[[dict], bool],
        message: str,
        conn_id: str = WarehouseHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.predicate = predicate
        self.message = message
        self.conn_id = conn_id

    def execute(self, context) -> dict:
        with WarehouseHook(self.conn_id).get_conn() as client:
            rows, columns = client.fetch_all(self.sql)
        if len(rows) != 1:
            raise AirflowException(f"check query returned {len(rows)} rows, expected exactly 1")
        measured = {name: jsonable(value) for name, value in zip(columns, rows[0])}
        if not self.predicate(measured):
            raise AirflowException(self.message.format(**measured))
        self.log.info("check passed: %s", measured)
        return measured
