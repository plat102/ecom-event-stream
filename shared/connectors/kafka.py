from typing import Self

from confluent_kafka import Consumer, Producer


class KafkaConsumerClient:
    def __init__(self, config: dict) -> None:
        self._consumer = Consumer(config)

    def subscribe(self, topics: list[str]) -> None:
        self._consumer.subscribe(topics)

    def poll(self, timeout: float = 1.0):
        return self._consumer.poll(timeout)

    def commit(self, message=None, asynchronous: bool = False) -> None:
        self._consumer.commit(message=message, asynchronous=asynchronous)

    def close(self) -> None:
        self._consumer.close()
  
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_) -> None:
        self.close()


class KafkaProducerClient:
    def __init__(self, config: dict) -> None:
        self._producer = Producer(config)

    def produce(self, topic: str, value: bytes, on_delivery=None) -> None:
        self._producer.produce(topic, value=value, on_delivery=on_delivery)

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_) -> None:
        self.flush()


def make_source_consumer(
    settings,
    group_id: str | None = None,
    offset_reset: str = "latest",
) -> KafkaConsumerClient:
    """Consumer for source Kafka cluster (SASL_PLAINTEXT).

    offset_reset: 'latest' for bridge (new events only), 'earliest' for exploration.
    """
    config = {
        "bootstrap.servers": settings.SOURCE_KAFKA_BROKERS,
        "group.id": group_id or settings.SOURCE_KAFKA_GROUP_ID,
        "security.protocol": settings.SOURCE_KAFKA_SECURITY_PROTOCOL,
        "sasl.mechanism": settings.SOURCE_KAFKA_SASL_MECHANISM,
        "sasl.username": settings.SOURCE_KAFKA_SASL_USERNAME,
        "sasl.password": settings.SOURCE_KAFKA_SASL_PASSWORD,
        "auto.offset.reset": offset_reset,
        "enable.auto.commit": "false",
    }
    return KafkaConsumerClient(config)


def make_sink_producer(settings) -> KafkaProducerClient:
    """Producer for sink Kafka cluster (acks=all, idempotent)."""
    config = {
        "bootstrap.servers": settings.SINK_KAFKA_BROKERS,
        "security.protocol": settings.SINK_KAFKA_SECURITY_PROTOCOL,
        "sasl.mechanism": settings.SINK_KAFKA_SASL_MECHANISM,
        "sasl.username": settings.SINK_KAFKA_SASL_USERNAME,
        "sasl.password": settings.SINK_KAFKA_SASL_PASSWORD,
        "acks": "all",
        "enable.idempotence": "true",
        "retries": "5",
        "retry.backoff.ms": "300",
        "linger.ms": "10",
        "batch.size": "32768",
    }
    return KafkaProducerClient(config)


def make_sink_consumer(settings, group_id: str) -> KafkaConsumerClient:
    """Consumer for sink Kafka cluster, manual commit."""
    config = {
        "bootstrap.servers": settings.SINK_KAFKA_BROKERS,
        "security.protocol": settings.SINK_KAFKA_SECURITY_PROTOCOL,
        "sasl.mechanism": settings.SINK_KAFKA_SASL_MECHANISM,
        "sasl.username": settings.SINK_KAFKA_SASL_USERNAME,
        "sasl.password": settings.SINK_KAFKA_SASL_PASSWORD,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
        "max.poll.interval.ms": "300000",
    }
    return KafkaConsumerClient(config)
