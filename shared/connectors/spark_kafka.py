"""Spark Structured Streaming Kafka connection options (sink cluster, SASL_PLAINTEXT)."""


def _connection_options(builder, settings):
    jaas_config = (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{settings.SINK_KAFKA_SASL_USERNAME}" password="{settings.SINK_KAFKA_SASL_PASSWORD}";'
    )
    return (
        builder.format("kafka")
        .option("kafka.bootstrap.servers", settings.SINK_KAFKA_BROKERS)
        .option("kafka.security.protocol", settings.SINK_KAFKA_SECURITY_PROTOCOL)
        .option("kafka.sasl.mechanism", settings.SINK_KAFKA_SASL_MECHANISM)
        .option("kafka.sasl.jaas.config", jaas_config)
    )


def kafka_source_options(spark_reader, settings, topic: str):
    """Apply Kafka connection options to a Spark readStream/read builder."""
    return _connection_options(spark_reader, settings).option("subscribe", topic)


def kafka_sink_options(spark_writer, settings, topic: str):
    """Apply Kafka connection options to a Spark writeStream/write builder."""
    return _connection_options(spark_writer, settings).option("topic", topic)
