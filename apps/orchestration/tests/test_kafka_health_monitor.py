"""Unit tests for the Kafka monitoring DAG: which readings count as healthy, and whether a
failure names the group, topic or number it is about. Needs airflow, so it runs in the image.
"""
from contextlib import contextmanager

import pytest

pytest.importorskip("airflow", reason="airflow is only installed in the Airflow image")

from airflow.exceptions import AirflowException, AirflowSkipException

from dec import dag_config
import kafka_health_monitor as dag_module
from dec import marks
from shared.connectors.kafka_admin import ConsumerGroupLag, ConsumerGroupStatus

CONFIG = {
    "lag_threshold": 50000,
    "min_throughput": 100,
    "max_dlq_rate": 30,
    "clusters": {
        "sink": {
            "conn_id": "kafka_sink",
            "group": "mongo-sink",
            "topic": "user-events",
            "dlq_topic": "user-events-dlq",
        },
        "source": {
            "conn_id": "kafka_source",
            "group": "ingestion-bridge",
            "topic": "product_view",
            "timeout": 20,
        },
    },
}


class FakeKafka:
    """Stands in for the admin client with whatever cluster shape a test needs."""

    def __init__(self, *, topics=(), status=None, lag=None) -> None:
        self._topics = list(topics)
        self._status = status
        self._lag = lag

    def list_topics(self):
        return sorted(self._topics)

    def consumer_group_state(self, group):
        return self._status

    def consumer_group_lag(self, group, topic):
        return self._lag


class FakeTaskInstance:
    """Collects what a task records for the run digest, and what it stages as a mark."""

    def __init__(self, task_id="check") -> None:
        self.task_id = task_id
        self.pushed: dict = {}

    def xcom_push(self, key, value):
        self.pushed[key] = value

    def xcom_pull(self, task_ids=None, key=None):
        return self.pushed.get(key)


class FakeVariableStore:
    def __init__(self, values=None) -> None:
        self.values = dict(values or {})

    def get(self, key, default_var=None, deserialize_json=False):
        return self.values.get(key, default_var)

    def set(self, key, value):
        self.values[key] = value


@pytest.fixture(autouse=True)
def settings(monkeypatch):
    """Thresholds and the cluster map come from the DAG's config Variable."""
    monkeypatch.setattr(dag_config.DagSettings, "config", lambda self: CONFIG)


@pytest.fixture()
def store(monkeypatch):
    fake = FakeVariableStore()
    monkeypatch.setattr(marks, "Variable", fake)
    return fake


def _context(task_id="check") -> dict:
    return {"ti": FakeTaskInstance(task_id)}


def _mark(store, key, value) -> None:
    store.values[dag_module.SETTINGS.state_variable] = {key: value}


@contextmanager
def _client(fake):
    yield fake


def with_client(monkeypatch, fake) -> None:
    """Every task reaches Kafka through `admin_client`, so one patch covers them all."""
    monkeypatch.setattr(dag_module, "admin_client", lambda cluster: _client(fake))


def task(task_id):
    """The `@task` body as the DAG wires it."""
    return dag_module.kafka_health_monitor().get_task(task_id).python_callable


def check(task_id):
    return dag_module.kafka_health_monitor().get_task(task_id)


def status(state, members=1) -> ConsumerGroupStatus:
    return ConsumerGroupStatus(group="mongo-sink", state=state, members=members)


def lag(total, per_partition=None, uncommitted=()) -> ConsumerGroupLag:
    return ConsumerGroupLag(
        group="mongo-sink",
        topic="user-events",
        total=total,
        per_partition=per_partition or {0: total},
        uncommitted_partitions=list(uncommitted),
    )


# ── topics ────────────────────────────────────────────────────────────


def test_both_sink_topics_present_is_healthy():
    measured = dag_module.probe_sink_topics(FakeKafka(topics=["user-events", "user-events-dlq"]))
    assert measured["missing"] == []


def test_a_missing_dlq_is_reported_by_name():
    # silent otherwise: nothing notices until the first event needs rejecting
    measured = dag_module.probe_sink_topics(FakeKafka(topics=["user-events"]))
    assert measured["missing"] == ["user-events-dlq"]
    operator = check("sink.check_topics_exist")
    assert not operator.predicate(measured)
    assert "user-events-dlq" in operator.message.format(**measured)


def test_listing_topics_doubles_as_the_connectivity_check():
    # no predicate: the call either returned or raised
    assert check("sink.check_brokers").predicate is None
    assert check("sink.check_brokers").retries == 3


# ── consumer group status ─────────────────────────────────────────────


def test_a_stable_group_with_members_is_consuming(monkeypatch, store):
    with_client(monkeypatch, FakeKafka(status=status("STABLE")))
    measured = task("sink.check_group_status")("sink", **_context())
    assert measured["state"] == "STABLE" and measured["members"] == 1


def test_an_empty_group_fails_and_names_the_topic(monkeypatch, store):
    with_client(monkeypatch, FakeKafka(status=status("EMPTY", members=0)))
    with pytest.raises(AirflowException) as failure:
        task("sink.check_group_status")("sink", **_context())
    assert "mongo-sink" in str(failure.value) and "user-events" in str(failure.value)


def test_a_first_rebalance_is_transient_not_an_incident(monkeypatch, store):
    with_client(monkeypatch, FakeKafka(status=status("PREPARING_REBALANCING")))
    measured = task("sink.check_group_status")("sink", **_context())
    assert measured["state"] == "PREPARING_REBALANCING"


def test_a_rebalance_lasting_across_two_runs_fails(monkeypatch, store):
    # a rebalance that outlives one schedule interval is not a rebalance any more
    _mark(store, "group_state.mongo-sink", "PREPARING_REBALANCING")
    with_client(monkeypatch, FakeKafka(status=status("COMPLETING_REBALANCING")))
    with pytest.raises(AirflowException, match="across two runs"):
        task("sink.check_group_status")("sink", **_context())


def test_the_group_state_is_staged_for_the_next_run(monkeypatch, store):
    with_client(monkeypatch, FakeKafka(status=status("STABLE")))
    context = _context()
    task("sink.check_group_status")("sink", **context)
    assert context["ti"].pushed["staged_marks"]["group_state.mongo-sink"] == "STABLE"


# ── consumer lag ──────────────────────────────────────────────────────


def test_lag_under_the_threshold_passes(monkeypatch, store):
    with_client(monkeypatch, FakeKafka(lag=lag(25)))
    assert task("sink.check_consumer_lag")("sink", **_context())["lag"] == 25


def test_lag_over_the_threshold_but_draining_does_not_alert(monkeypatch, store):
    # one breach is a sink restart; the judgement is the direction, not the number
    _mark(store, "lag.mongo-sink", {"at": 0, "total": 90000})
    with_client(monkeypatch, FakeKafka(lag=lag(60000)))
    measured = task("sink.check_consumer_lag")("sink", **_context())
    assert measured["draining_from"] == 90000


def test_lag_over_the_threshold_and_not_draining_fails(monkeypatch, store):
    _mark(store, "lag.mongo-sink", {"at": 0, "total": 60000})
    with_client(monkeypatch, FakeKafka(lag=lag(60001)))
    with pytest.raises(AirflowException) as failure:
        task("sink.check_consumer_lag")("sink", **_context())
    assert "not draining" in str(failure.value) and "60001" in str(failure.value)


def test_lag_stuck_at_exactly_the_previous_value_fails(monkeypatch, store):
    # unchanged is not draining, and a stuck consumer is what this check exists to catch
    _mark(store, "lag.mongo-sink", {"at": 0, "total": 60000})
    with_client(monkeypatch, FakeKafka(lag=lag(60000)))
    with pytest.raises(AirflowException, match="not draining"):
        task("sink.check_consumer_lag")("sink", **_context())


def test_a_first_breach_with_no_earlier_mark_fails(monkeypatch, store):
    # nothing to compare against, so the breach has to be taken at face value
    with_client(monkeypatch, FakeKafka(lag=lag(60000)))
    with pytest.raises(AirflowException):
        task("sink.check_consumer_lag")("sink", **_context())


def test_an_uncommitted_partition_warns_without_failing(monkeypatch, store):
    # it would never clear on its own, and "nothing is consuming" is another check's call
    with_client(monkeypatch, FakeKafka(lag=lag(10, uncommitted=[0, 1])))
    measured = task("sink.check_consumer_lag")("sink", **_context())
    assert measured["uncommitted_partitions"] == [0, 1]


def test_a_cluster_may_carry_its_own_lag_threshold(monkeypatch, store):
    # the two clusters are different queues, so one number cannot be right for both
    monkeypatch.setitem(CONFIG["clusters"]["sink"], "lag_threshold", 10)
    with_client(monkeypatch, FakeKafka(lag=lag(25)))
    with pytest.raises(AirflowException, match="threshold 10"):
        task("sink.check_consumer_lag")("sink", **_context())


# ── the rate comparison ───────────────────────────────────────────────


def _compare(throughput, processing, context=None):
    return task("sink.compare_processing_rate_to_throughput")(
        throughput, processing, **(context or _context())
    )


def test_production_with_no_consumption_fails():
    with pytest.raises(AirflowException, match="committed"):
        _compare({"rate_per_minute": 590.0}, {"rate_per_minute": 0.0})


def test_an_unmeasurable_processing_rate_says_so():
    # the rate check skipped, which is the run this comparison matters most on
    with pytest.raises(AirflowException, match="reset"):
        _compare({"rate_per_minute": 590.0}, None)


def test_both_flowing_is_healthy():
    measured = _compare({"rate_per_minute": 590.0}, {"rate_per_minute": 587.0})
    assert measured == {"produced_per_minute": 590.0, "consumed_per_minute": 587.0}


def test_a_quiet_source_is_not_an_incident():
    # lambda = 0 makes mu = 0 correct, not a failure
    assert _compare({"rate_per_minute": 0.0}, {"rate_per_minute": 0.0})


def test_nothing_to_compare_against_skips():
    with pytest.raises(AirflowSkipException):
        _compare(None, {"rate_per_minute": 587.0})


def test_the_comparison_still_runs_when_the_rate_check_skipped():
    assert check("sink.compare_processing_rate_to_throughput").trigger_rule == "none_failed"


# ── structure ─────────────────────────────────────────────────────────


def test_every_rate_check_bounds_its_window():
    """A mark left behind by an outage would average one interval's work over a gap, which
    reads as a slowdown on the first run back rather than as the gap it is."""
    rates = [
        "sink.check_throughput",
        "source.check_throughput",
        "sink.check_processing_rate",
        "sink.check_dlq_growth",
    ]
    assert all(check(t).max_interval_seconds == dag_module.MAX_RATE_WINDOW_SECONDS for t in rates)


def test_measurement_tasks_do_not_retry():
    # a retry re-measures and shrinks the window every rate is derived from
    assert check("sink.check_throughput").retries == 0
    assert check("sink.check_consumer_lag").retries == 0


def test_the_sink_checks_wait_on_the_topics_gate():
    # one incident at the bottom of the chain should not send five alerts
    for task_id in ("sink.check_group_status", "sink.check_consumer_lag", "sink.check_dlq_growth"):
        assert check(task_id).upstream_task_ids == {"sink.check_topics_exist"}


def test_a_dead_sink_does_not_blind_the_source_branch():
    # the source queue is the one with a retention deadline, so it gates itself
    for task_id in ("source.check_group_status", "source.check_consumer_lag"):
        assert check(task_id).upstream_task_ids == {"source.check_brokers"}


def test_the_mark_committer_waits_on_every_leaf():
    """ALL_DONE fires as soon as the listed upstreams settle, so a leaf left off the list
    could still be running when the marks are committed and the digest built."""
    assert check("run_tail.commit_run_marks").upstream_task_ids == {
        "sink.check_group_status",
        "sink.check_consumer_lag",
        "sink.check_dlq_growth",
        "sink.compare_processing_rate_to_throughput",
        "source.check_group_status",
        "source.check_consumer_lag",
        "source.check_throughput",
    }
    assert check("run_tail.report_run").upstream_task_ids == {"run_tail.commit_run_marks"}
    assert check("run_tail.mark_run_state").upstream_task_ids == {"run_tail.report_run"}


def test_only_the_report_alerts():
    # one incident trips several checks, so the checks stay quiet and the digest speaks once
    assert check("run_tail.report_run").on_failure_callback is not None
    assert check("sink.check_group_status").on_failure_callback in (None, [])
