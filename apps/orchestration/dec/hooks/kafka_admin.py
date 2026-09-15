"""Airflow glue for the shared Kafka admin client."""
from airflow.hooks.base import BaseHook

from shared.connectors.kafka_admin import KafkaAdminClient


class KafkaAdminHook(BaseHook):
    """The Connection's `extra` *is* the librdkafka config, so there is nothing to translate."""

    conn_name_attr = "kafka_conn_id"
    default_conn_name = "kafka_sink"
    conn_type = "kafka"
    hook_name = "Kafka admin (monitoring)"

    def __init__(self, kafka_conn_id: str = default_conn_name, timeout: float = 10.0) -> None:
        super().__init__()
        self.kafka_conn_id = kafka_conn_id
        self.timeout = timeout

    @classmethod
    def from_cluster(cls, cluster: dict, *, conn_id: str | None = None, timeout: float = 10.0):
        """The one reading of a cluster entry, so the operator and the tasks cannot diverge."""
        resolved = cluster.get("conn_id") or conn_id
        if not resolved:
            raise ValueError(f"cluster entry carries no conn_id: {sorted(cluster)}")
        return cls(resolved, timeout=float(cluster.get("timeout", timeout)))

    def client_config(self) -> dict:
        conn = self.get_connection(self.kafka_conn_id)
        config = dict(conn.extra_dejson)
        if not config.get("bootstrap.servers"):
            raise ValueError(
                f"connection {self.kafka_conn_id!r} carries no bootstrap.servers in its extra"
            )
        return config

    def get_conn(self) -> KafkaAdminClient:
        return KafkaAdminClient(self.client_config(), timeout=self.timeout)
