"""Unit tests for the rate operator: the arithmetic, and the two cases that must skip rather
than fail. Needs the airflow package, so it runs in the image."""
import json

import pytest

pytest.importorskip("airflow", reason="airflow is only installed in the Airflow image")

from airflow.exceptions import AirflowException, AirflowSkipException  # noqa: E402

import marks  # noqa: E402
from operators import rate_check  # noqa: E402
from operators.rate_check import RateCheckOperator, rate_per_minute  # noqa: E402

STATE_KEY = "TEST_RATE_MARK"


class FakeTaskInstance:
    """Collects the measurements the operator records for the run report."""

    def __init__(self) -> None:
        self.pushed: dict = {}

    def xcom_push(self, key, value):
        self.pushed[key] = value


class FakeDagRun:
    def __init__(self, run_type: str = "scheduled") -> None:
        self.run_type = run_type


def _context(run_type: str = "scheduled") -> dict:
    return {"ti": FakeTaskInstance(), "dag_run": FakeDagRun(run_type)}


class FakeVariableStore:
    """Stands in for the Variable table: the operator only get/sets one key."""

    def __init__(self, initial: dict | None = None) -> None:
        self.values = dict(initial or {})

    def get(self, key, default_var=None):
        return self.values.get(key, default_var)

    def set(self, key, value):
        self.values[key] = value


def _operator(measure, min_rate=None) -> RateCheckOperator:
    return RateCheckOperator(
        task_id="rate", state_key=STATE_KEY, measure=measure, min_rate=min_rate
    )


@pytest.fixture()
def store(monkeypatch):
    fake = FakeVariableStore()
    monkeypatch.setattr(rate_check, "Variable", fake)
    monkeypatch.setattr(marks, "Variable", fake)
    return fake


def _mark(store, at: float, value: float) -> None:
    store.values[STATE_KEY] = json.dumps({"at": at, "value": value})


# ── rate_per_minute ───────────────────────────────────────────────────


def test_rate_over_two_marks():
    previous = {"at": 1000.0, "value": 9_490_545}
    current = {"at": 1300.0, "value": 9_493_395}
    assert rate_per_minute(previous, current) == pytest.approx(570.0)


def test_non_positive_interval_is_rejected():
    with pytest.raises(ValueError, match="non-positive interval"):
        rate_per_minute({"at": 10.0, "value": 1}, {"at": 10.0, "value": 2})


# ── RateCheckOperator ─────────────────────────────────────────────────


def test_first_run_skips_and_stores_the_mark(store):
    # failing here would alert once after every deploy, for a healthy pipeline
    context = _context()
    operator = _operator(measure=lambda: 100)
    with pytest.raises(AirflowSkipException):
        operator.execute(context)
    assert json.loads(store.values[STATE_KEY])["value"] == 100
    assert context["ti"].pushed["measured"]["value"] == 100


def test_second_run_returns_the_rate(store):
    import time

    _mark(store, at=time.time() - 60, value=1000)
    result = _operator(measure=lambda: 1600).execute(_context())
    assert result["rate_per_minute"] == pytest.approx(600, rel=0.05)
    assert result["previous_value"] == 1000


def test_counter_going_backwards_skips_instead_of_reporting_a_negative_rate(store):
    # a recreated topic or reset offsets make the rate meaningless, not low
    _mark(store, at=1000.0, value=5000)
    with pytest.raises(AirflowSkipException, match="backwards"):
        _operator(measure=lambda: 10).execute(_context())


def test_rate_below_the_floor_fails_with_the_measured_number(store):
    import time

    _mark(store, at=time.time() - 60, value=1000)
    context = _context()
    with pytest.raises(AirflowException) as failure:
        _operator(measure=lambda: 1010, min_rate=100).execute(context)
    assert "10/min" in str(failure.value)
    # the report needs the number even though the check failed
    assert context["ti"].pushed["measured"]["rate_per_minute"] == pytest.approx(10, rel=0.1)


def test_rate_above_the_ceiling_fails_with_the_measured_number(store):
    # the DLQ check is the other direction: new rejects per minute, not a floor
    import time

    _mark(store, at=time.time() - 60, value=500)
    operator = RateCheckOperator(
        task_id="dlq", state_key=STATE_KEY, measure=lambda: 600, max_rate=30
    )
    with pytest.raises(AirflowException) as failure:
        operator.execute(_context())
    assert "100/min" in str(failure.value) and "100 new" in str(failure.value)


def test_a_threshold_arriving_as_a_rendered_string_is_still_a_number(store):
    # thresholds come from a Variable through a Jinja template, so they arrive as text
    import time

    _mark(store, at=time.time() - 60, value=1000)
    operator = RateCheckOperator(
        task_id="rate", state_key=STATE_KEY, measure=lambda: 1010, min_rate="100"
    )
    with pytest.raises(AirflowException, match="below the 100/min floor"):
        operator.execute(_context())


def test_an_unset_threshold_renders_empty_and_means_no_limit(store):
    import time

    _mark(store, at=time.time() - 60, value=1000)
    operator = RateCheckOperator(
        task_id="rate", state_key=STATE_KEY, measure=lambda: 1001, min_rate="", max_rate=""
    )
    assert operator.execute(_context())["rate_per_minute"] == pytest.approx(1, rel=0.1)


def test_a_manual_run_measures_without_moving_the_mark(store):
    # otherwise the next scheduled window collapses to the seconds since the manual trigger
    import time

    _mark(store, at=time.time() - 60, value=1000)
    _operator(measure=lambda: 1600).execute(_context(run_type="manual"))
    assert json.loads(store.values[STATE_KEY])["value"] == 1000


def test_the_new_mark_is_stored_even_when_the_check_fails(store):
    import time

    _mark(store, at=time.time() - 60, value=1000)
    with pytest.raises(AirflowException):
        _operator(measure=lambda: 1010, min_rate=100).execute(_context())
    assert json.loads(store.values[STATE_KEY])["value"] == 1010
