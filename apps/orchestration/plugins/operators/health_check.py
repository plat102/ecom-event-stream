"""One probe, one predicate, one message that carries the measured numbers.

Every health check has this shape: ask one system for a few numbers, judge them, fail with
those numbers in the text. Only the client answering the probe differs, so that is all a
subclass supplies.
"""
from collections.abc import Callable
from contextlib import nullcontext
from datetime import timedelta
from decimal import Decimal

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from hooks.kafka_admin import KafkaAdminHook
from hooks.yarn import YarnHook


def jsonable(value):
    """XCom is JSON, and Postgres hands back Decimal and timedelta."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, timedelta):
        return value.total_seconds()
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


class HealthCheckOperator(BaseOperator):
    """`probe` returns measured values, `predicate` judges them, `message` is formatted with
    them. No predicate means a pure reading — the call succeeding is the check."""

    template_fields = ("message",)

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
        """Subclasses reading a template field override this instead of passing `probe`: a
        bound method points at the pre-render copy of the operator, so it would see raw Jinja."""
        return self.probe(client)

    def execute(self, context) -> dict:
        with self.client() as client:
            measured = self.measure(client)

        failure = None
        if self.predicate is not None and not self.predicate(measured):
            failure = self.message.format(**measured)
            measured = {**measured, "error": failure}
        # Pushed before the raise — xcom_push commits immediately, so a failed check still
        # contributes its numbers to the run report.
        context["ti"].xcom_push(key="measured", value=measured)
        if failure:
            raise AirflowException(failure)

        self.log.info("check passed: %s", measured)
        return measured


class KafkaHealthCheckOperator(HealthCheckOperator):
    """Probes a Kafka cluster. `conn_id` and `timeout` are templated so the cluster map in a
    Variable supplies them at run time rather than on every scheduler parse."""

    template_fields = ("message", "conn_id", "timeout")

    def client(self):
        return KafkaAdminHook(
            self.conn_id or KafkaAdminHook.default_conn_name, timeout=float(self.timeout)
        ).get_conn()


class SparkHealthCheckOperator(HealthCheckOperator):
    """Probes the YARN ResourceManager. `nullcontext` because that client is stateless over
    stdlib urllib and has nothing to close, unlike the Kafka and Postgres ones."""

    def client(self):
        return nullcontext(
            YarnHook(self.conn_id or YarnHook.default_conn_name, timeout=self.timeout).get_conn()
        )
