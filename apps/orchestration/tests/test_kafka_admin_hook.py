"""Unit tests for the Kafka hook's reading of a cluster entry — the one place the operator
and the @task measure functions must agree. Needs airflow, so it runs in the image.
"""
import pytest

pytest.importorskip("airflow", reason="airflow is only installed in the Airflow image")

import kafka_health_monitor as dag_module
from dec.hooks.kafka_admin import KafkaAdminHook
from dec.operators.health_check import KafkaHealthCheckOperator


def test_a_cluster_supplies_the_connection_and_the_timeout():
    hook = KafkaAdminHook.from_cluster({"conn_id": "kafka_source", "timeout": 20})
    assert (hook.kafka_conn_id, hook.timeout) == ("kafka_source", 20.0)


def test_a_timeout_written_as_text_becomes_a_number():
    # the cluster map is hand-edited JSON; "20" used to reach the client as a string and
    # only blow up later, inside the division that shares the timeout across partitions
    hook = KafkaAdminHook.from_cluster({"conn_id": "kafka_source", "timeout": "20"})
    assert hook.timeout == 20.0


def test_a_cluster_without_a_timeout_takes_the_given_default():
    hook = KafkaAdminHook.from_cluster({"conn_id": "kafka_sink"}, timeout=30)
    assert hook.timeout == 30.0


def test_a_cluster_without_a_conn_id_is_loud():
    # silently falling back would report a healthy sink while the source is the one asked about
    with pytest.raises(ValueError, match="no conn_id"):
        KafkaAdminHook.from_cluster({"group": "mongo-sink"})


def test_the_operator_may_supply_a_fallback_connection():
    hook = KafkaAdminHook.from_cluster({}, conn_id="kafka_sink")
    assert hook.kafka_conn_id == "kafka_sink"


def test_the_task_path_and_the_operator_path_build_the_same_client(monkeypatch):
    """The regression this guards: two readings of the same dict that disagree."""
    built = []
    monkeypatch.setattr(
        KafkaAdminHook, "get_conn", lambda self: built.append((self.kafka_conn_id, self.timeout))
    )
    cluster = {"conn_id": "kafka_source", "timeout": "20"}

    dag_module.admin_client(cluster)
    KafkaHealthCheckOperator(task_id="check", cluster=cluster, probe=lambda c: {}).client()

    assert built[0] == built[1] == ("kafka_source", 20.0)
