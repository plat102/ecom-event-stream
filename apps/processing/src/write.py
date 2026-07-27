"""Insert the enriched batch into fact_event, skipping events already written."""
from types import SimpleNamespace

from pyspark.sql.functions import col, lit

from shared.config.settings import settings
from shared.connectors.postgres import PostgresClient


def write_fact_event(enriched_batch) -> int:
    """Insert with ON CONFLICT (event_id) DO NOTHING, one connection per Spark partition.
    """
    fact_df = enriched_batch.select(
        "event_type",
        "date_key",
        "site_key",
        "location_key",
        "product_key",
        "device_key",
        col("id").alias("event_id"),
        "report_date",
        "hour",
        "event_timestamp",
        col("time_stamp").alias("event_timestamp_unix"),
        "store_id",
        lit(None).cast("string").alias("session_id"),  # NULL for now, no session grain yet
        "device_id",
        "user_id_db",
        "email_hash",
        "ip",
        "current_url",
        "referrer_url",
        "user_agent",
        "resolution",
        "utm_source",
        "utm_medium",
        col("payload").cast("string"),  # an untyped literal, so Postgres reads it as jsonb
        "kafka_partition",
        "kafka_offset",
    )

    # Built from fact_df.columns
    insert_sql = f"""
        INSERT INTO fact_event ({", ".join(fact_df.columns)})
        VALUES %s
        ON CONFLICT (event_id) DO NOTHING
        RETURNING 1
    """

    # Plain values, not the pydantic settings object — this crosses into the executors
    conn = SimpleNamespace(
        POSTGRES_HOST=settings.POSTGRES_HOST,
        POSTGRES_PORT=settings.POSTGRES_PORT,
        POSTGRES_DB=settings.POSTGRES_DB,
        POSTGRES_USER=settings.POSTGRES_USER,
        POSTGRES_PASSWORD=settings.POSTGRES_PASSWORD,
    )

    def insert_partition(rows):
        values = [tuple(row) for row in rows]
        if not values:
            return iter([0])  # no connection for an empty partition
        with PostgresClient(conn) as pg:
            # fetch=True is required for an accurate count: execute_values pages internally,
            # so cursor.rowcount would only reflect the last page. RETURNING 1 emits a row
            # per insert and nothing for a skipped conflict.
            inserted = pg.execute_values(insert_sql, values, fetch=True)
        return iter([len(inserted)])

    return sum(fact_df.rdd.mapPartitions(insert_partition).collect())
