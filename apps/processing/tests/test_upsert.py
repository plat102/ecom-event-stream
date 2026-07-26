"""
Unit tests for dim_product upsert — needs the live Postgres instance (JDBC write +
raw SQL, not something a seed DataFrame can stand in for).
"""
import datetime

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from shared.config.settings import settings
from shared.connectors.postgres import PostgresClient
from upsert import relookup_dynamic_dims, upsert_dim_product

BATCH_SCHEMA = StructType([
    StructField("product_id", StringType()),
    StructField("event_timestamp", TimestampType()),
])

TEST_PRODUCT_ID = "test-upsert-product"


def _utc(*args):
    return datetime.datetime(*args, tzinfo=datetime.timezone.utc)


def _batch(spark, rows):
    return spark.createDataFrame(rows, BATCH_SCHEMA)


def _cleanup():
    with PostgresClient(settings) as pg:
        pg.execute("DELETE FROM dim_product WHERE product_id = %s", (TEST_PRODUCT_ID,))


def _fetch():
    with PostgresClient(settings) as pg:
        return pg.fetch_one(
            "SELECT first_seen_at, last_seen_at FROM dim_product WHERE product_id = %s",
            (TEST_PRODUCT_ID,),
        )


def test_new_product_creates_row_with_correct_seen_at(spark):
    _cleanup()
    try:
        ts = _utc(2026, 7, 14, 10, 0, 0)
        upsert_dim_product(_batch(spark, [(TEST_PRODUCT_ID, ts)]))

        first_seen_at, last_seen_at = _fetch()
        assert first_seen_at == ts
        assert last_seen_at == ts
    finally:
        _cleanup()


def test_later_batch_with_earlier_timestamp_moves_first_seen_at_down(spark):
    _cleanup()
    try:
        upsert_dim_product(_batch(spark, [(TEST_PRODUCT_ID, _utc(2026, 7, 14, 10, 0, 0))]))
        upsert_dim_product(_batch(spark, [(TEST_PRODUCT_ID, _utc(2026, 7, 10, 8, 0, 0))]))

        first_seen_at, last_seen_at = _fetch()
        assert first_seen_at == _utc(2026, 7, 10, 8, 0, 0)  # LEAST
        assert last_seen_at == _utc(2026, 7, 14, 10, 0, 0)  # GREATEST unchanged
    finally:
        _cleanup()


def test_catalog_seeded_null_seen_at_gets_set_by_first_real_event(spark):
    """Simulates a catalog-only row (no event ever seen, first_seen_at/last_seen_at NULL).
    Relies on Postgres's LEAST/GREATEST ignoring NULL arguments rather than propagating
    them — see upsert.py's docstring."""
    _cleanup()
    try:
        with PostgresClient(settings) as pg:
            pg.execute(
                "INSERT INTO dim_product (product_id, product_name) VALUES (%s, %s)",
                (TEST_PRODUCT_ID, "Catalog Seeded Product"),
            )

        ts = _utc(2026, 7, 14, 10, 0, 0)
        upsert_dim_product(_batch(spark, [(TEST_PRODUCT_ID, ts)]))

        first_seen_at, last_seen_at = _fetch()
        assert first_seen_at == ts
        assert last_seen_at == ts
    finally:
        _cleanup()


def test_relookup_attaches_product_key_in_same_batch(spark):
    _cleanup()
    try:
        batch = _batch(spark, [(TEST_PRODUCT_ID, _utc(2026, 7, 14, 10, 0, 0))])
        upsert_dim_product(batch)

        result = relookup_dynamic_dims(batch, spark).collect()[0]
        assert result.product_key is not None
    finally:
        _cleanup()
