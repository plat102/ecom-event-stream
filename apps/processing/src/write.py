"""Append the enriched batch to fact_event via JDBC."""
from pyspark.sql.functions import col, lit

from shared.config.settings import settings
from shared.connectors.postgres import jdbc_properties, jdbc_url


def write_fact_event(enriched_batch):
    """Write mode is append, not overwrite — idempotency comes from Spark's checkpoint plus
    fact_event's UNIQUE(event_id) constraint, not from truncating and reloading.
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
        col("payload").cast("string"),  # PostgreSQL casts this to jsonb on insert
        "kafka_partition",
        "kafka_offset",
    )

    fact_df.write.jdbc(
        url=jdbc_url(settings),
        table="fact_event",
        mode="append",
        properties={
            **jdbc_properties(settings),
            "batchsize": "5000",
            "reWriteBatchedInserts": "true",  # PostgreSQL JDBC driver batch-insert optimization
        },
    )
