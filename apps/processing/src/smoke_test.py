"""
P2.1 smoke test: confirm Spark can reach Kafka and PostgreSQL before writing the real job.

Usage:
    poetry run spark-submit \\
      --master local[*] \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.postgresql:postgresql:42.7.3 \\
      apps/processing/src/smoke_test.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from shared.config.settings import settings
from shared.connectors.postgres import jdbc_properties, jdbc_url
from shared.connectors.spark_kafka import kafka_source_options
from shared.utils.logger import get_logger

log = get_logger("smoke_test")


def main():
    spark = SparkSession.builder.appName("smoke-test").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    log.info("JDBC read: dim_date (P2.0 seed)")
    dim_date = spark.read.jdbc(url=jdbc_url(settings), table="dim_date", properties=jdbc_properties(settings))
    dim_date.show(5)
    log.info(f"dim_date row count: {dim_date.count()}")

    log.info(f"Kafka readStream: {settings.SINK_KAFKA_TOPIC} (5 messages, trigger once)")
    df = kafka_source_options(spark.readStream, settings, settings.SINK_KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    query = (
        df.select(
            col("key").cast("string"),
            col("value").cast("string"),
            "topic", "partition", "offset", "timestamp",
        )
        .writeStream.format("console")
        .option("truncate", "false")
        .option("numRows", 5)
        .trigger(once=True)
        .start()
    )
    query.awaitTermination()

    spark.stop()


if __name__ == "__main__":
    main()
