"""
Entry point: wire the full streaming pipeline (Kafka -> parse -> filter -> enrich ->
    split payload -> lookup static dims -> foreachBatch) and run it.

Usage:
    TZ=UTC poetry run spark-submit \\
      --master local[*] \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.postgresql:postgresql:42.7.3 \\
      --conf spark.sql.session.timeZone=UTC \\
      apps/processing/src/main.py
"""

from dlq import route_dlq
from enrich import enrich
from filter import validate_events
from lookup import load_static_dims, lookup_static_dims
from parse import parse_raw
from payload_split import split_payload
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from upsert import relookup_dynamic_dims, upsert_dim_device, upsert_dim_product
from write import write_fact_event

from shared.config.settings import settings
from shared.connectors.spark_kafka import kafka_source_options
from shared.schemas.event_type import KNOWN_EVENT_TYPES
from shared.utils.logger import get_logger

log = get_logger("main")

CHECKPOINT_LOCATION = (
    "checkpoints/ecom-stream-processor"  # relative to local[*] spark-submit's cwd
)


def build_process_batch(spark):
    def process_batch(batch_df, batch_id):
        # Dedup by id within the batch
        batch_df = batch_df.dropDuplicates(["id"]).cache()
        row_count = batch_df.count()

        route_dlq(batch_df)
        valid_df = batch_df.filter(col("event_type").isin(KNOWN_EVENT_TYPES))

        upsert_dim_product(valid_df)
        upsert_dim_device(valid_df)
        enriched_batch = relookup_dynamic_dims(valid_df, spark)
        write_fact_event(enriched_batch)

        batch_df.unpersist()
        log.info(f"batch {batch_id}: {row_count} rows processed")

    return process_batch


def main():
    spark = SparkSession.builder.appName("ecom-stream-processor").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    dims = load_static_dims(spark)

    raw = (
        kafka_source_options(spark.readStream, settings, settings.SINK_KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = parse_raw(raw)
    valid = validate_events(parsed)
    enriched = enrich(valid)
    split = split_payload(enriched)
    lookup_df = lookup_static_dims(split, dims)

    query = (
        lookup_df.writeStream.foreachBatch(build_process_batch(spark))
        .trigger(processingTime="60 seconds")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
