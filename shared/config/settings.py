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

    # Local Kafka (self-hosted)
    LOCAL_KAFKA_BROKERS: str = ""
    LOCAL_KAFKA_TOPIC: str = ""
    LOCAL_KAFKA_DLQ_TOPIC: str = ""

    # MongoDB
    MONGO_URI: str = ""
    MONGO_DB: str = ""

    # Logging
    LOG_FORMAT: str = "pretty"  # "pretty" | "json"

settings = Settings()
