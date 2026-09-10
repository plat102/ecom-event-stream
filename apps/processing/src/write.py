"""Insert the enriched batch into fact_event, skipping events already written."""
from pyspark.sql.functions import col, lit

from shared.config.settings import settings
from shared.connectors.postgres import PostgresClient, jdbc_properties, jdbc_url

_STAGING_TABLE = "stg_fact_event"


def write_fact_event(enriched_batch) -> int:
    """Stage the batch over JDBC, then merge it in with ON CONFLICT (event_id) DO NOTHING.
    
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
        col("payload").cast("string"),  # staged as text, cast back to jsonb on the way in
        "kafka_partition",
        "kafka_offset",
    )

    # Built from fact_df.columns
    columns = fact_df.columns
    select_list = ", ".join(
        "payload::jsonb" if c == "payload" else c for c in columns
    )
    insert_sql = f"""
        INSERT INTO fact_event ({", ".join(columns)})
        SELECT {select_list} FROM {_STAGING_TABLE}
        ON CONFLICT (event_id) DO NOTHING
    """

    # Staged over JDBC so the write stays on the executors — they run the cluster's bare
    # python, which has no psycopg2, and a batch replaying a backlog is too big to collect
    fact_df.write.jdbc(
        url=jdbc_url(settings),
        table=_STAGING_TABLE,
        mode="overwrite",
        properties=jdbc_properties(settings),
    )

    with PostgresClient(settings) as pg:
        return pg.execute(insert_sql)
