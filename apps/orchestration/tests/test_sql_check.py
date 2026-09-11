"""Unit tests for the SQL check operator: the predicate decides, the message carries the
measured value. Needs the airflow package, so it runs in the image."""
from datetime import timedelta
from decimal import Decimal

import pytest

pytest.importorskip("airflow", reason="airflow is only installed in the Airflow image")

from airflow.exceptions import AirflowException

from operators import sql_check
from operators.sql_check import SqlCheckOperator, jsonable


class FakeClient:
    def __init__(self, rows, columns) -> None:
        self._rows = rows
        self._columns = columns
        self.closed = False

    def fetch_all(self, sql, params=None):
        return self._rows, self._columns

    def close(self) -> None:
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class FakeTaskInstance:
    def __init__(self) -> None:
        self.pushed: dict = {}

    def xcom_push(self, key, value):
        self.pushed[key] = value


def _context() -> dict:
    return {"ti": FakeTaskInstance()}


class FakeHook:
    """Replaces WarehouseHook so no Connection and no Postgres are needed."""

    last: "FakeHook | None" = None
    default_conn_name = "postgres_warehouse"

    def __init__(self, conn_id) -> None:
        self.conn_id = conn_id
        self.client = None
        FakeHook.last = self

    def get_conn(self):
        self.client = FakeClient(FakeHook.rows, FakeHook.columns)
        return self.client


@pytest.fixture()
def warehouse(monkeypatch):
    monkeypatch.setattr(sql_check, "WarehouseHook", FakeHook)
    return FakeHook


def _operator(**kwargs) -> SqlCheckOperator:
    defaults = {
        "task_id": "check",
        "sql": "SELECT 1",
        "predicate": lambda row: True,
        "message": "failed",
    }
    return SqlCheckOperator(**{**defaults, **kwargs})


# ── jsonable ──────────────────────────────────────────────────────────


def test_postgres_types_are_coerced_for_xcom():
    # Decimal and timedelta come straight out of psycopg2 and neither is JSON
    assert jsonable(Decimal("52.4")) == 52.4
    assert jsonable(timedelta(minutes=3)) == 180.0
    assert jsonable(None) is None


# ── SqlCheckOperator ──────────────────────────────────────────────────


def test_passing_check_returns_the_measured_row(warehouse):
    warehouse.rows, warehouse.columns = [(120.0,)], ["staleness_seconds"]
    assert _operator(predicate=lambda row: row["staleness_seconds"] < 600).execute(_context()) == {
        "staleness_seconds": 120.0
    }


def test_failure_message_is_formatted_with_the_measured_row(warehouse):
    warehouse.rows, warehouse.columns = [(Decimal("1830.5"),)], ["staleness_seconds"]
    operator = _operator(
        predicate=lambda row: row["staleness_seconds"] < 600,
        message="fact_event is {staleness_seconds:.0f}s stale",
    )
    with pytest.raises(AirflowException, match="fact_event is 1830s stale"):
        operator.execute(_context())


def test_query_returning_more_than_one_row_is_a_check_error(warehouse):
    warehouse.rows, warehouse.columns = [(1,), (2,)], ["n"]
    with pytest.raises(AirflowException, match="returned 2 rows"):
        _operator().execute(_context())


def test_the_connection_is_closed_after_the_check(warehouse):
    warehouse.rows, warehouse.columns = [(1,)], ["n"]
    _operator().execute(_context())
    assert FakeHook.last.client.closed
