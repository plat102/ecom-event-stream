"""One probe, one predicate, one message that carries the measured numbers."""
from collections.abc import Callable
from contextlib import nullcontext

from airflow.models import BaseOperator

from dec.callbacks.report import fail, record
from dec.hooks.kafka_admin import KafkaAdminHook
from dec.hooks.yarn import YarnHook
from dec.links import YarnResourceManagerLink


class HealthCheckOperator(BaseOperator):
    """`probe` measures, `predicate` judges, `message` is formatted with the result."""

    # Config is templated so a run's real values show in Rendered Template;
    # probe/predicate/message stay callables because they are logic, not values.
    template_fields = ("conn_id", "timeout")
    ui_color = "#e8f4f8"

    def __init__(
        self,
        *,
        probe: Callable[[object], dict] | None = None,
        predicate: Callable[[dict], bool] | None = None,
        message: str = "",
        conn_id: str | None = None,
        timeout: float = 10.0,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.probe = probe
        self.predicate = predicate
        self.message = message
        self.conn_id = conn_id
        self.timeout = timeout

    def client(self):
        """A context manager yielding the client `probe` is handed."""
        raise NotImplementedError

    def measure(self, client) -> dict:
        """Extension point for a check that is more than one probe call."""
        return self.probe(client)

    def execute(self, context) -> dict:
        with self.client() as client:
            measured = self.measure(client)

        # `fail` records the numbers before raising, so the digest still gets them.
        if self.predicate is not None and not self.predicate(measured):
            fail(context, measured, self.message.format(**measured))

        self.log.info("check passed: %s", measured)
        return record(context, measured)


class KafkaHealthCheckOperator(HealthCheckOperator):
    """`cluster` is templated, so the map can be repointed without a deploy."""

    template_fields = (*HealthCheckOperator.template_fields, "cluster")
    ui_color = "#fff3cd"

    def __init__(self, *, cluster: dict | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.cluster = cluster

    def client(self):
        return KafkaAdminHook.from_cluster(
            self.cluster or {},
            conn_id=self.conn_id or KafkaAdminHook.default_conn_name,
            timeout=self.timeout,
        ).get_conn()


class SparkHealthCheckOperator(HealthCheckOperator):
    """`nullcontext` because the YARN client is stateless and has nothing to close."""

    ui_color = "#d4edda"
    operator_extra_links = (YarnResourceManagerLink(),)

    def client(self):
        return nullcontext(
            YarnHook(
                self.conn_id or YarnHook.default_conn_name, timeout=float(self.timeout)
            ).get_conn()
        )
