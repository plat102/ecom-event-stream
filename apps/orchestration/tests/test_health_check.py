"""Unit tests for the health-check operator family: the probe/predicate/message contract, and
the guarantee the run report depends on — measurements reach XCom even when the check fails.
Needs the airflow package, so it runs in the image.
"""
import pytest

pytest.importorskip("airflow", reason="airflow is only installed in the Airflow image")

from airflow.exceptions import AirflowException

from dec.operators import health_check
from dec.operators.health_check import (
    HealthCheckOperator,
    KafkaHealthCheckOperator,
    SparkHealthCheckOperator,
)


class FakeTaskInstance:
    def __init__(self) -> None:
        self.pushed: dict = {}

    def xcom_push(self, key, value):
        self.pushed[key] = value


def _context() -> dict:
    return {"ti": FakeTaskInstance()}


class FakeClient:
    def __init__(self) -> None:
        self.closed = False

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class StubCheck(HealthCheckOperator):
    """Binds a client the test owns, so no hook and no Connection are involved."""

    def __init__(self, client=None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.stub_client = client or FakeClient()

    def client(self):
        return self.stub_client


def _operator(**kwargs) -> StubCheck:
    defaults = {"task_id": "check", "probe": lambda client: {"value": 1}}
    return StubCheck(**{**defaults, **kwargs})


# ── the contract ──────────────────────────────────────────────────────


def test_a_probe_without_a_predicate_is_a_pure_reading():
    # check_spark_job_status takes one: the branch downstream does the judging
    assert _operator(probe=lambda c: {"apps": []}).execute(_context()) == {"apps": []}


def test_a_passing_predicate_returns_the_measured_values():
    operator = _operator(probe=lambda c: {"lag": 13}, predicate=lambda m: m["lag"] < 100)
    assert operator.execute(_context()) == {"lag": 13}


def test_the_message_is_formatted_with_the_measured_values():
    operator = _operator(
        probe=lambda c: {"lag": 900, "limit": 100},
        predicate=lambda m: m["lag"] < m["limit"],
        message="lag is {lag}, over {limit}",
    )
    with pytest.raises(AirflowException, match="lag is 900, over 100"):
        operator.execute(_context())


def test_the_client_is_closed_after_the_probe():
    client = FakeClient()
    _operator(client=client).execute(_context())
    assert client.closed


# ── what the run report depends on ────────────────────────────────────


def test_a_passing_check_records_its_measurements():
    context = _context()
    _operator(probe=lambda c: {"nodes": 1}).execute(context)
    assert context["ti"].pushed["measured"] == {"nodes": 1}


def test_a_failing_check_records_its_measurements_and_the_reason():
    # without this the digest can only say "task X failed", not what the numbers were
    context = _context()
    operator = _operator(
        probe=lambda c: {"nodes": 0},
        predicate=lambda m: m["nodes"] > 0,
        message="{nodes} nodes",
    )
    with pytest.raises(AirflowException):
        operator.execute(context)
    assert context["ti"].pushed["measured"] == {"nodes": 0, "error": "0 nodes"}


# ── the bound hooks ───────────────────────────────────────────────────


@pytest.fixture()
def built(monkeypatch):
    """Record what the operator asks the hook for, without needing a Connection."""
    seen = []

    def get_conn(hook):
        seen.append((hook.kafka_conn_id, hook.timeout))
        return FakeClient()

    monkeypatch.setattr(health_check.KafkaAdminHook, "get_conn", get_conn)
    return seen


def test_the_cluster_entry_is_templated(built):
    # the cluster map lives in a Variable, and Airflow renders these per task instance, so a
    # cluster repointed there takes effect without a deploy
    assert "cluster" in KafkaHealthCheckOperator.template_fields
    # what execute() is handed is the rendered dict, never a reader
    operator = KafkaHealthCheckOperator(
        task_id="check", cluster={"conn_id": "kafka_source", "timeout": "20"}, probe=lambda c: {}
    )
    operator.execute(_context())
    assert built == [("kafka_source", 20.0)]


def test_a_cluster_without_a_timeout_falls_back_to_the_operator_default(built):
    # how the cluster entry itself is read is the hook's contract, tested next to the hook
    operator = KafkaHealthCheckOperator(
        task_id="check", cluster={"conn_id": "kafka_sink"}, probe=lambda c: {}
    )
    operator.execute(_context())
    assert built == [("kafka_sink", 10.0)]


def test_the_yarn_flavour_does_not_try_to_close_a_stateless_client(monkeypatch):
    # YarnClient talks over stdlib urllib and has no close(); a plain `with` would raise
    class BareClient:
        pass

    class FakeHook:
        default_conn_name = "yarn_rm"

        def __init__(self, conn_id, timeout):
            pass

        def get_conn(self):
            return BareClient()

    monkeypatch.setattr(health_check, "YarnHook", FakeHook)
    operator = SparkHealthCheckOperator(
        task_id="check", probe=lambda client: {"ok": isinstance(client, BareClient)}
    )
    assert operator.execute(_context()) == {"ok": True}


def test_a_subclass_can_replace_the_probe_call_entirely():
    # a check that is more than one probe call overrides `measure` rather than passing a probe
    class TwoStepCheck(HealthCheckOperator):
        def client(self):
            return FakeClient()

        def measure(self, client) -> dict:
            return {"steps": 2}

    operator = TwoStepCheck(task_id="check")
    assert operator.probe is None
    assert operator.execute(_context()) == {"steps": 2}
