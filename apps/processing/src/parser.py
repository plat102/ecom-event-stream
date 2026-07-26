"""RawParser: parse Kafka message JSON into a typed DataFrame."""
from pyspark.sql.functions import col, from_json

from shared.schemas.event_spark import EVENT_SCHEMA


def parse_raw(df):
    """Parse the Kafka `value` column (JSON); keeps kafka_partition/kafka_offset as an audit trail."""
    parsed = df.select(
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
    )
    return parsed.select("data.*", "kafka_partition", "kafka_offset")
