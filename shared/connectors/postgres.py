"""JDBC connection helpers for Spark, plus a raw connection for server-side SQL Spark's
JDBC writer can't run (e.g. `INSERT ... ON CONFLICT`).
"""
from typing import Self

import psycopg2
from psycopg2.extras import execute_values as pg_execute_values


class PostgresClient:
    def __init__(self, settings) -> None:
        self._conn = psycopg2.connect(
            host=settings.POSTGRES_HOST,
            port=settings.POSTGRES_PORT,
            dbname=settings.POSTGRES_DB,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
        )

    def execute(self, sql: str, params: tuple | None = None) -> int:
        """Returns cursor.rowcount — accurate here since a single execute() isn't paginated
        (unlike execute_values below)."""
        with self._conn.cursor() as cursor:
            cursor.execute(sql, params)
            rowcount = cursor.rowcount
        self._conn.commit()
        return rowcount

    def fetch_one(self, sql: str, params: tuple | None = None):
        with self._conn.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone()

    def fetch_all(
        self, sql: str, params: tuple | None = None
    ) -> tuple[list[tuple], list[str]]:
        """Returns (rows, column_names) — enough to build a DataFrame without SQLAlchemy,
        which pandas would otherwise want for read_sql."""
        with self._conn.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall(), [c.name for c in cursor.description]

    def execute_values(self, sql: str, rows: list[tuple], fetch: bool = False) -> list | None:
        """`sql` must contain one `%s` for the VALUES list, e.g. `... FROM (VALUES %s) AS v(...)`.

        `fetch=True` is needed to get an accurate row count back — execute_values pages
        internally (default page_size=100), so cursor.rowcount after the call would only
        reflect the last page, not the true total.
        """
        with self._conn.cursor() as cursor:
            result = pg_execute_values(cursor, sql, rows, fetch=fetch)
        self._conn.commit()
        return result

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_) -> None:
        self.close()


def jdbc_url(settings) -> str:
    return (
        f"jdbc:postgresql://{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
        "?stringtype=unspecified"  # required for JSONB write, avoids payload VARCHAR vs jsonb error
    )


def jdbc_properties(settings) -> dict:
    return {
        "user": settings.POSTGRES_USER,
        "password": settings.POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }
