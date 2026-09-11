"""Unit tests for the run digest: what it always contains, and what it drops when the message
would not fit."""
from callbacks.report import MESSAGE_LIMIT, build_report, describe

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
    # 589/min is the story; offset 9683251 is not
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
    report = _report([_row("check_topics_exist", "upstream_failed", None)])
    assert "no reason recorded" in report


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
