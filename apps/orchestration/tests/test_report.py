"""Unit tests for the run digest: what it always contains, and what it drops when the message
would not fit."""
from datetime import timedelta
from decimal import Decimal

import pytest

from dec.callbacks import report
from dec.callbacks.report import MESSAGE_LIMIT, build_report, describe, jsonable


# ── jsonable ──────────────────────────────────────────────────────────


def test_postgres_types_are_coerced_for_xcom():
    # Decimal and timedelta come straight out of psycopg2 and neither is JSON
    assert jsonable(Decimal("52.4")) == 52.4
    assert jsonable(timedelta(minutes=3)) == 180.0
    assert jsonable(None) is None
    assert jsonable(True) is True


def test_an_unknown_type_degrades_to_text_rather_than_breaking_the_push():
    # a measurement reaching XCom matters more than its exact type: the digest only prints it
    assert jsonable(object()).startswith("<object object")

LOG_URL = "http://localhost:18080/log?task_id=check_sink_group_status"


def _row(task_id, state, measured=None, log_url=LOG_URL) -> dict:
    return {"task_id": task_id, "state": state, "measured": measured, "log_url": log_url}


def _report(rows, **kwargs) -> str:
    return build_report(
        dag_id="kafka_health_monitor", logical_date="2026-09-10 16:20 UTC", rows=rows, **kwargs
    )


# ── describe ──────────────────────────────────────────────────────────


def test_describe_leaves_the_error_out_of_the_measurement_line():
    # the error already has its own line under "failed"; repeating it wastes the budget
    line = describe({"lag": 13, "threshold": 1, "error": "lag too high"})
    assert line == "lag=13 · threshold=1"


def test_describe_drops_the_absolute_counter_behind_a_rate():
    # the rate is the story; the absolute offset behind it is not
    line = describe({"rate_per_minute": 589.05, "value": 9683251.0, "previous_value": 9676631.0})
    assert line == "rate_per_minute=589.05"


def test_describe_says_so_when_nothing_was_recorded():
    assert describe(None) == "no measurement recorded"


# ── build_report ──────────────────────────────────────────────────────


def test_header_counts_states_worst_first():
    rows = [
        _row("a", "success", {"x": 1}),
        _row("b", "failed", {"error": "boom"}),
        _row("c", "skipped"),
    ]
    header = _report(rows).splitlines()[1]
    assert header == "1 failed · 1 skipped · 1 success"


def test_failed_checks_carry_their_reason_and_log_link():
    rows = [_row("check_sink_group_status", "failed", {"state": "DEAD", "error": "group is DEAD"})]
    report = _report(rows)
    assert "**failed**" in report
    assert "`check_sink_group_status` — group is DEAD" in report
    assert LOG_URL in report


def test_a_failure_without_a_recorded_measurement_still_appears():
    report = _report([_row("check_topics_exist", "failed", None)])
    assert "no reason recorded" in report


def test_a_task_blocked_by_an_upstream_failure_says_so_without_a_log_link():
    # its log is empty, and the link would cost a chunk of the budget
    report = _report([_row("compare_rates", "upstream_failed", None)])
    assert "upstream failed, did not run" in report
    assert LOG_URL not in report


def test_healthy_run_has_no_failed_section():
    report = _report([_row("check_source_lag", "success", {"lag": 13})])
    assert "**failed**" not in report
    assert "`check_source_lag` — lag=13" in report


def test_measurements_are_trimmed_to_fit_and_say_so():
    rows = [_row(f"check_{i}", "success", {"per_partition": "x" * 200}) for i in range(40)]
    report = _report(rows)
    assert len(report) <= MESSAGE_LIMIT
    assert "line(s) trimmed to fit" in report


def test_failures_alone_over_the_limit_are_still_cut_to_fit():
    # a message the webhook refuses is no report at all
    rows = [_row(f"check_{i}", "failed", {"error": "z" * 300}) for i in range(20)]
    report = _report(rows)
    assert len(report) <= MESSAGE_LIMIT
    assert report.endswith("…")


def test_failures_come_before_measurements_when_the_budget_runs_out():
    # failures are what has to survive, so measurements go first and the cut is last
    failures = [_row(f"check_{i}", "failed", {"error": "y" * 150}) for i in range(12)]
    report = _report(failures + [_row("ok", "success", {"value": 1})])
    assert len(report) <= MESSAGE_LIMIT
    assert "`check_0`" in report
    assert "**measured**" not in report


# ── run state ─────────────────────────────────────────────────────────


class FakeInstance:
    def __init__(self, task_id, state) -> None:
        self.task_id = task_id
        self.state = state
        self.log_url = LOG_URL


class FakeDagRun:
    dag_id = "spark_health_monitor"

    def __init__(self, instances) -> None:
        self._instances = instances

    def get_task_instances(self):
        return self._instances


class FakeTaskInstance:
    task_id = "mark_run_state"

    def xcom_pull(self, task_ids, key):
        return None


def _run_context(*states) -> dict:
    return {
        "ti": FakeTaskInstance(),
        "dag_run": FakeDagRun([FakeInstance(f"check_{i}", s) for i, s in enumerate(states)]),
    }


def test_a_clean_run_stays_green():
    report.fail_run_if_checks_failed(_run_context("success", "skipped"))


@pytest.fixture()
def airflow_installed():
    """Only the raising path needs it — a clean run never touches Airflow."""
    pytest.importorskip("airflow", reason="airflow is only installed in the Airflow image")


def test_a_run_with_a_failed_check_goes_red(airflow_installed):
    # the report task succeeds by design on ALL_DONE, so without this the grid lies
    with pytest.raises(Exception, match=r"1 check\(s\) failed"):
        report.fail_run_if_checks_failed(_run_context("success", "failed"))


def test_upstream_failures_count_too(airflow_installed):
    with pytest.raises(Exception, match=r"2 check\(s\) failed"):
        report.fail_run_if_checks_failed(_run_context("failed", "upstream_failed", "success"))
