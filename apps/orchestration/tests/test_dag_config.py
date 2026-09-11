"""Unit tests for the DAG registry: one Variable names every other, and a DAG must still load
when the registry is missing or wrong. Needs the airflow package, so it runs in the image.
"""
import pytest

pytest.importorskip("airflow", reason="airflow is only installed in the Airflow image")

import dag_config
from dag_config import DagSettings, default_names, for_dag

DAG_ID = "spark_health_monitor"
DEFAULT = "*/10 * * * *"
ENTRY = {
    DAG_ID: {
        "schedule": "*/30 * * * *",
        "config": "SPARK_MONITOR_CONFIG",
        "state": "SPARK_MONITOR_STATE",
    }
}


class FakeVariableStore:
    def __init__(self, values=None) -> None:
        self.values = dict(values or {})
        self.asked: list[str] = []

    def get(self, key, default_var=None, deserialize_json=False):
        self.asked.append(key)
        return self.values.get(key, default_var)


@pytest.fixture()
def store(monkeypatch):
    fake = FakeVariableStore()
    monkeypatch.setattr(dag_config, "Variable", fake)
    return fake


# ── resolving a DAG's settings ────────────────────────────────────────


def test_the_registry_is_the_only_variable_name_in_code(store):
    store.values["DAG_REGISTRY"] = ENTRY
    settings = for_dag(DAG_ID, default_schedule=DEFAULT)
    assert store.asked == ["DAG_REGISTRY"]
    assert settings.config_variable == "SPARK_MONITOR_CONFIG"
    assert settings.schedule == "*/30 * * * *"


def test_an_unseeded_database_still_produces_a_loadable_dag(store):
    settings = for_dag(DAG_ID, default_schedule=DEFAULT)
    assert settings.schedule == DEFAULT
    assert (settings.config_variable, settings.state_variable) == default_names(DAG_ID)


def test_a_typo_in_the_cron_falls_back_instead_of_breaking_the_import(store):
    # an invalid cron fails DAG import — monitoring that does not load is the worse fault
    store.values["DAG_REGISTRY"] = {DAG_ID: {"schedule": "*/30 * * *"}}
    assert for_dag(DAG_ID, default_schedule=DEFAULT).schedule == DEFAULT


def test_an_entry_for_another_dag_is_ignored(store):
    store.values["DAG_REGISTRY"] = {"someone_else": {"schedule": "@daily"}}
    assert for_dag(DAG_ID, default_schedule=DEFAULT).schedule == DEFAULT


# ── reaching the config ───────────────────────────────────────────────


def _settings() -> DagSettings:
    return DagSettings(DEFAULT, "SPARK_MONITOR_CONFIG", "SPARK_MONITOR_STATE")


def test_a_template_defers_the_read_to_run_time():
    assert _settings().template("min_row_growth") == (
        "{{ var.json.SPARK_MONITOR_CONFIG.min_row_growth }}"
    )


def test_a_template_can_reach_a_nested_key():
    # the cluster map is one key of the config, not a Variable of its own any more
    assert _settings().template("clusters.sink.conn_id") == (
        "{{ var.json.SPARK_MONITOR_CONFIG.clusters.sink.conn_id }}"
    )


def test_a_value_read_inside_a_task_comes_from_the_config_variable(store):
    store.values["SPARK_MONITOR_CONFIG"] = {"min_row_growth": 100}
    assert _settings().value("min_row_growth") == 100


def test_a_missing_setting_returns_the_given_default(store):
    store.values["SPARK_MONITOR_CONFIG"] = {}
    assert _settings().value("nothing_here", "fallback") == "fallback"
