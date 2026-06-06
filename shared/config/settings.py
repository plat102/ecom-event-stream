from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

_PROJECT_ROOT = Path(__file__).parent.parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Source Kafka (external)
    SOURCE_KAFKA_BROKERS: str = ""
    SOURCE_KAFKA_TOPIC: str = ""
    SOURCE_KAFKA_GROUP_ID: str = ""
    SOURCE_KAFKA_SECURITY_PROTOCOL: str = "PLAINTEXT"
    SOURCE_KAFKA_SASL_MECHANISM: str = ""
    SOURCE_KAFKA_SASL_USERNAME: str = ""
    SOURCE_KAFKA_SASL_PASSWORD: str = ""

    # Sink Kafka (self-hosted)
    SINK_KAFKA_BROKERS: str = ""
    SINK_KAFKA_TOPIC: str = ""
    SINK_KAFKA_DLQ_TOPIC: str = ""
    SINK_KAFKA_SECURITY_PROTOCOL: str = "SASL_PLAINTEXT"
    SINK_KAFKA_SASL_MECHANISM: str = "PLAIN"
    SINK_KAFKA_SASL_USERNAME: str = "kafka"
    SINK_KAFKA_SASL_PASSWORD: str = ""

    # MongoDB
    MONGO_URI: str = ""
    MONGO_DB: str = ""

    # Tuning
    BATCH_SIZE: int = 500
    FLUSH_INTERVAL_SECONDS: float = 2.0

    # Logging
    LOG_FORMAT: str = "pretty"  # "pretty" | "json"

settings = Settings()
