"""Unit tests for run-to-run marks. Two rules carry the weight: only a scheduled run moves a
mark, and the state Variable has exactly one writer per run. Needs airflow, so it runs in the
image.
"""
import json

import pytest

pytest.importorskip("airflow", reason="airflow is only installed in the Airflow image")

import marks
from marks import commit_marks, read_mark, stage_mark

STATE_VARIABLE = "KAFKA_MONITOR_STATE"


class FakeTaskInstance:
    def __init__(self, task_id="check", pushed=None) -> None:
        self.task_id = task_id
        self.pushed = dict(pushed or {})

    def xcom_push(self, key, value):
        self.pushed[key] = value

    def xcom_pull(self, task_ids, key):
        return self.staged.get(task_ids, {}).get(key)


class FakeDagRun:
    def __init__(self, instances, run_type="scheduled") -> None:
        self.run_type = run_type
        self._instances = instances

    def get_task_instances(self):
        return self._instances


class FakeVariableStore:
    def __init__(self, values=None) -> None:
        self.values = dict(values or {})
        self.writes = 0

    def get(self, key, default_var=None, deserialize_json=False):
        return self.values.get(key, default_var)

    def set(self, key, value):
        self.writes += 1
        self.values[key] = json.loads(value)


@pytest.fixture()
def store(monkeypatch):
    fake = FakeVariableStore()
    monkeypatch.setattr(marks, "Variable", fake)
    return fake


def _committer(staged: dict, run_type="scheduled") -> dict:
    """A committer task whose siblings staged `staged` — {task_id: {mark_key: value}}."""
    committer = FakeTaskInstance("commit_run_marks")
    committer.staged = {task_id: {"staged_marks": v} for task_id, v in staged.items()}
    instances = [FakeTaskInstance(task_id) for task_id in staged] + [committer]
    return {"ti": committer, "dag_run": FakeDagRun(instances, run_type)}


# ── staging ───────────────────────────────────────────────────────────


def test_one_task_can_stage_several_marks():
    task_instance = FakeTaskInstance()
    task_instance.staged = {"check": {}}
    context = {"ti": task_instance}
    stage_mark(context, "lag.mongo-sink", {"total": 13})
    task_instance.staged["check"] = task_instance.pushed
    stage_mark(context, "group_state.mongo-sink", "STABLE")
    assert set(task_instance.pushed["staged_marks"]) == {
        "lag.mongo-sink",
        "group_state.mongo-sink",
    }


# ── committing ────────────────────────────────────────────────────────


def test_marks_from_every_task_land_in_one_write(store):
    context = _committer(
        {"check_sink_lag": {"lag.mongo-sink": 13}, "check_dlq": {"dlq_watermark": 562}}
    )
    assert commit_marks(context, STATE_VARIABLE) == {
        "lag.mongo-sink": 13,
        "dlq_watermark": 562,
    }
    # the point of staging: eight parallel checks, one write, nothing overwritten
    assert store.writes == 1
    assert store.values[STATE_VARIABLE] == {"lag.mongo-sink": 13, "dlq_watermark": 562}


def test_a_manual_run_reads_the_marks_without_moving_them(store):
    store.values[STATE_VARIABLE] = {"lag.mongo-sink": 1}
    context = _committer({"check_sink_lag": {"lag.mongo-sink": 999}}, run_type="manual")
    assert commit_marks(context, STATE_VARIABLE) == {}
    assert store.writes == 0
    assert store.values[STATE_VARIABLE] == {"lag.mongo-sink": 1}


def test_marks_not_staged_this_run_survive(store):
    # a check that failed early stages nothing; its previous mark must not be wiped
    store.values[STATE_VARIABLE] = {"throughput.source": {"at": 1, "value": 5}}
    context = _committer({"check_dlq": {"dlq_watermark": 562}})
    commit_marks(context, STATE_VARIABLE)
    assert store.values[STATE_VARIABLE]["throughput.source"] == {"at": 1, "value": 5}


def test_a_run_that_staged_nothing_writes_nothing(store):
    store.values[STATE_VARIABLE] = {"dlq_watermark": 1}
    commit_marks(_committer({}), STATE_VARIABLE)
    assert store.writes == 0


# ── reading ───────────────────────────────────────────────────────────


def test_reading_a_mark_that_was_never_written(store):
    assert read_mark(STATE_VARIABLE, "nothing.here") is None


def test_reading_a_stored_mark(store):
    store.values[STATE_VARIABLE] = {"lag.mongo-sink": {"at": 1.0, "total": 13}}
    assert read_mark(STATE_VARIABLE, "lag.mongo-sink") == {"at": 1.0, "total": 13}
