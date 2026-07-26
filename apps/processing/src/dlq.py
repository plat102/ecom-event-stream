"""Route events whose event_type isn't in the static allow-list to the dead-letter queue.

Must run inside foreachBatch's process_batch, not the streaming layer directly 
"""
from pyspark.sql.functions import col, lit, struct, to_json

from shared.config.settings import settings
from shared.connectors.spark_kafka import kafka_sink_options
from shared.schemas.event_type import KNOWN_EVENT_TYPES


def route_dlq(batch_df):
    """`.head(1)` instead of `.count()` — cheaper, stops as soon as one row is found."""
    dlq_df = batch_df.filter(~col("event_type").isin(KNOWN_EVENT_TYPES))
    if not dlq_df.head(1):
        return

    payload = dlq_df.select(
        to_json(struct("*")).alias("value"),
        lit("unknown_event_type").alias("_dlq_reason"),
    )
    kafka_sink_options(payload.write, settings, settings.SINK_KAFKA_DLQ_TOPIC).save()
