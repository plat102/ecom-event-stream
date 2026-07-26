"""Spark Structured Streaming Kafka connection options (sink cluster, SASL_PLAINTEXT)."""


def kafka_source_options(spark_reader, settings, topic: str):
    """Apply Kafka connection options to a Spark readStream/read builder."""
    jaas_config = (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{settings.SINK_KAFKA_SASL_USERNAME}" password="{settings.SINK_KAFKA_SASL_PASSWORD}";'
    )
    return (
        spark_reader.format("kafka")
        .option("kafka.bootstrap.servers", settings.SINK_KAFKA_BROKERS)
        .option("kafka.security.protocol", settings.SINK_KAFKA_SECURITY_PROTOCOL)
        .option("kafka.sasl.mechanism", settings.SINK_KAFKA_SASL_MECHANISM)
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("subscribe", topic)
    )
