"""
Unit tests for the fact_event writer — needs the live Postgres instance (JDBC write plus
the ON CONFLICT insert and the generated columns, none of which a seed DataFrame can fake).

The replay tests are the point: Spark's checkpoint is at-least-once, so writing the same
batch twice is the normal consequence of a crash between the JDBC write and `commits/N`,
not an exotic case.
"""
import datetime

from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from shared.config.settings import settings
from shared.connectors.postgres import PostgresClient
from write import write_fact_event

BATCH_SCHEMA = StructType([
    StructField("event_type", StringType()),
    StructField("date_key", IntegerType()),
    StructField("site_key", IntegerType()),
    StructField("location_key", IntegerType()),
    StructField("product_key", IntegerType()),
    StructField("device_key", IntegerType()),
    StructField("id", StringType()),
    StructField("report_date", DateType()),
    StructField("hour", IntegerType()),
    StructField("event_timestamp", TimestampType()),
    StructField("time_stamp", LongType()),
    StructField("store_id", StringType()),
    StructField("device_id", StringType()),
    StructField("user_id_db", StringType()),
    StructField("email_hash", StringType()),
    StructField("ip", StringType()),
    StructField("current_url", StringType()),
    StructField("referrer_url", StringType()),
    StructField("user_agent", StringType()),
    StructField("resolution", StringType()),
    StructField("utm_source", StringType()),
    StructField("utm_medium", StringType()),
    StructField("payload", StringType()),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
])

EVENT_ID_PREFIX = "test-write-"
TIMESTAMP = datetime.datetime(2026, 7, 14, 10, 30, 0, tzinfo=datetime.timezone.utc)
PAYLOAD = '{"key_search": "solitaire ring", "order_id": "test-order-1"}'


def _row(event_id: str, offset: int) -> tuple:
    return (
        "view_product_detail",
        20260714,
        None,
        None,
        None,
        None,
        EVENT_ID_PREFIX + event_id,
        datetime.date(2026, 7, 14),
        10,
        TIMESTAMP,
        1784027400,
        "62",
        "device-abc",
        None,
        None,
        "1.2.3.4",
        "https://example.com/p/1",
        None,
        "Mozilla/5.0",
        "1920x1080",
        None,
        None,
        PAYLOAD,
        0,
        offset,
    )


def _batch(spark, rows):
    return spark.createDataFrame(rows, BATCH_SCHEMA)


def _cleanup():
    with PostgresClient(settings) as pg:
        pg.execute("DELETE FROM fact_event WHERE event_id LIKE %s", (EVENT_ID_PREFIX + "%",))


def _count() -> int:
    with PostgresClient(settings) as pg:
        return pg.fetch_one(
            "SELECT COUNT(*) FROM fact_event WHERE event_id LIKE %s",
            (EVENT_ID_PREFIX + "%",),
        )[0]


def test_replayed_batch_inserts_nothing_and_does_not_raise(spark):
    """The exact restart scenario: the batch wrote its rows, died before `commits/N`, and
    Spark replays it. Under a plain append this raised a UNIQUE violation that no retry
    could clear."""
    _cleanup()
    try:
        batch = _batch(spark, [_row("a", 100), _row("b", 101)])

        assert write_fact_event(batch) == 2
        assert write_fact_event(batch) == 0  # replay: every row conflicts
        assert _count() == 2
    finally:
        _cleanup()


def test_partial_replay_inserts_only_the_new_rows(spark):
    """A replayed batch is not always identical — the next batch overlaps it when the
    source itself redelivers. Only the genuinely new events should land."""
    _cleanup()
    try:
        write_fact_event(_batch(spark, [_row("a", 100), _row("b", 101)]))

        overlapping = _batch(spark, [_row("b", 101), _row("c", 102)])
        assert write_fact_event(overlapping) == 1

        assert _count() == 3
    finally:
        _cleanup()


def test_timestamp_and_payload_survive_the_round_trip(spark):
    """Guards the two values that psycopg2 has to adapt rather than pass through: the
    timestamp (wrong handling shifts it by the server's UTC offset) and the payload, which
    reaches a JSONB column as a plain string. If Postgres stored it as a JSON string
    instead of an object, the generated columns would come back NULL — silently."""
    _cleanup()
    try:
        write_fact_event(_batch(spark, [_row("a", 100)]))

        with PostgresClient(settings) as pg:
            event_timestamp, key_search, order_id = pg.fetch_one(
                "SELECT event_timestamp, key_search, order_id FROM fact_event"
                " WHERE event_id = %s",
                (EVENT_ID_PREFIX + "a",),
            )

        assert event_timestamp == TIMESTAMP
        assert key_search == "solitaire ring"   # generated column, so payload really is jsonb
        assert order_id == "test-order-1"
    finally:
        _cleanup()
