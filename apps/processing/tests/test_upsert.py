"""
Unit tests for the dim_product and dim_device upserts — need the live Postgres instance
(JDBC write + raw SQL, not something a seed DataFrame can stand in for).
"""
import datetime

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from shared.config.settings import settings
from shared.connectors.postgres import PostgresClient
from upsert import relookup_dynamic_dims, upsert_dim_device, upsert_dim_product

# The device triple is part of the schema because relookup_dynamic_dims joins dim_device too.
BATCH_SCHEMA = StructType([
    StructField("product_id", StringType()),
    StructField("event_timestamp", TimestampType()),
    StructField("browser", StringType()),
    StructField("os", StringType()),
    StructField("device_category", StringType()),
])

TEST_PRODUCT_ID = "test-upsert-product"
TEST_BROWSER = "test-upsert-browser"  # distinctive so cleanup cannot touch real rows
TEST_DEVICE = (TEST_BROWSER, "test-os", "Desktop")


def _utc(*args):
    return datetime.datetime(*args, tzinfo=datetime.timezone.utc)


def _row(product_id=TEST_PRODUCT_ID, event_timestamp=None, device=TEST_DEVICE):
    return (product_id, event_timestamp, *device)


def _batch(spark, rows):
    return spark.createDataFrame(rows, BATCH_SCHEMA)


def _cleanup():
    with PostgresClient(settings) as pg:
        pg.execute("DELETE FROM dim_product WHERE product_id = %s", (TEST_PRODUCT_ID,))
        pg.execute("DELETE FROM dim_device WHERE browser = %s", (TEST_BROWSER,))


def _fetch():
    with PostgresClient(settings) as pg:
        return pg.fetch_one(
            "SELECT first_seen_at, last_seen_at FROM dim_product WHERE product_id = %s",
            (TEST_PRODUCT_ID,),
        )


def _fetch_device(device=TEST_DEVICE):
    with PostgresClient(settings) as pg:
        return pg.fetch_one(
            "SELECT device_key, is_mobile FROM dim_device"
            " WHERE browser = %s AND os = %s AND device_category = %s",
            device,
        )


def test_new_product_creates_row_with_correct_seen_at(spark):
    _cleanup()
    try:
        ts = _utc(2026, 7, 14, 10, 0, 0)
        upsert_dim_product(_batch(spark, [_row(event_timestamp=ts)]))

        first_seen_at, last_seen_at = _fetch()
        assert first_seen_at == ts
        assert last_seen_at == ts
    finally:
        _cleanup()


def test_later_batch_with_earlier_timestamp_moves_first_seen_at_down(spark):
    _cleanup()
    try:
        upsert_dim_product(_batch(spark, [_row(event_timestamp=_utc(2026, 7, 14, 10, 0, 0))]))
        upsert_dim_product(_batch(spark, [_row(event_timestamp=_utc(2026, 7, 10, 8, 0, 0))]))

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
        upsert_dim_product(_batch(spark, [_row(event_timestamp=ts)]))

        first_seen_at, last_seen_at = _fetch()
        assert first_seen_at == ts
        assert last_seen_at == ts
    finally:
        _cleanup()


def test_relookup_attaches_product_key_in_same_batch(spark):
    _cleanup()
    try:
        batch = _batch(spark, [_row(event_timestamp=_utc(2026, 7, 14, 10, 0, 0))])
        upsert_dim_product(batch)

        result = relookup_dynamic_dims(batch, spark).collect()[0]
        assert result.product_key is not None
    finally:
        _cleanup()


def test_new_device_combo_creates_row(spark):
    _cleanup()
    try:
        upsert_dim_device(_batch(spark, [_row()]))

        device_key, is_mobile = _fetch_device()
        assert device_key is not None
        assert is_mobile is False  # "Desktop"
    finally:
        _cleanup()


def test_repeated_upsert_does_not_duplicate_the_combo(spark):
    """Twice in one batch and twice across batches — dedup plus ON CONFLICT DO NOTHING."""
    _cleanup()
    try:
        batch = _batch(spark, [_row(), _row()])
        upsert_dim_device(batch)
        first_key = _fetch_device()[0]
        upsert_dim_device(batch)

        with PostgresClient(settings) as pg:
            count = pg.fetch_one(
                "SELECT COUNT(*) FROM dim_device WHERE browser = %s", (TEST_BROWSER,)
            )[0]
        assert count == 1
        assert _fetch_device()[0] == first_key  # key is stable, not re-issued
    finally:
        _cleanup()


def test_is_mobile_derived_from_device_category(spark):
    _cleanup()
    try:
        upsert_dim_device(
            _batch(
                spark,
                [
                    _row(device=(TEST_BROWSER, "test-os", "Mobile")),
                    _row(device=(TEST_BROWSER, "test-os", "Desktop")),
                    _row(device=(TEST_BROWSER, "test-os", "Tablet")),
                ],
            )
        )

        assert _fetch_device((TEST_BROWSER, "test-os", "Mobile"))[1] is True
        assert _fetch_device((TEST_BROWSER, "test-os", "Desktop"))[1] is False
        assert _fetch_device((TEST_BROWSER, "test-os", "Tablet"))[1] is False
    finally:
        _cleanup()


def test_relookup_attaches_device_key_in_same_batch(spark):
    _cleanup()
    try:
        batch = _batch(spark, [_row(event_timestamp=_utc(2026, 7, 14, 10, 0, 0))])
        upsert_dim_device(batch)

        result = relookup_dynamic_dims(batch, spark).collect()[0]
        assert result.device_key == _fetch_device()[0]
    finally:
        _cleanup()


def test_null_device_triple_is_skipped_instead_of_failing(spark):
    """A null triple means the UA parser threw; all three columns are NOT NULL in the DDL."""
    _cleanup()
    try:
        upsert_dim_device(_batch(spark, [_row(device=(None, None, None))]))

        with PostgresClient(settings) as pg:
            assert pg.fetch_one("SELECT COUNT(*) FROM dim_device WHERE browser IS NULL")[0] == 0
    finally:
        _cleanup()
