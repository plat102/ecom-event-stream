"""
Unit tests for the failure callback: carries the numbers and the log link, stays under the
webhook's size limit, never raises. Needs the airflow package, so it runs in the image.
"""
import pytest

pytest.importorskip("airflow", reason="airflow is only installed in the Airflow image")

import sys

from dec.callbacks import alert
from dec.callbacks.alert import MESSAGE_LIMIT, failure_message, notify_failure

LOG_URL = "http://localhost:18080/log?dag_id=kafka_health_monitor&task_id=check_sink_brokers"
HOOK_MODULE = "airflow.providers.discord.hooks.discord_webhook"


class FakeTaskInstance:
    dag_id = "kafka_health_monitor"
    task_id = "check_sink_brokers"
    log_url = LOG_URL


def _context(**overrides) -> dict:
    context = {
        "task_instance": FakeTaskInstance(),
        "logical_date": "2026-09-10T11:00:00+00:00",
        "exception": RuntimeError("consumer group 'mongo-sink' is DEAD with 0 members"),
    }
    context.update(overrides)
    return context


def _install_hook(monkeypatch, hook_class) -> None:
    monkeypatch.setitem(
        sys.modules, HOOK_MODULE, type("module", (), {"DiscordWebhookHook": hook_class})
    )


# ── failure_message ───────────────────────────────────────────────────


def test_message_carries_dag_task_date_reason_and_log_url():
    message = failure_message(_context())
    for fragment in (
        "kafka_health_monitor",
        "check_sink_brokers",
        "2026-09-10T11:00:00+00:00",
        "mongo-sink' is DEAD with 0 members",
        LOG_URL,
    ):
        assert fragment in message


def test_the_log_url_stays_outside_the_code_fence():
    # inside a fence Discord would render it as text, and the link is the point
    message = failure_message(_context())
    assert message.endswith(f"\nlog: {LOG_URL}")


def test_a_long_reason_is_truncated_to_the_webhook_limit():
    # too long and the hook raises instead of sending, losing the alert entirely
    message = failure_message(_context(exception=RuntimeError("x" * 5000)))
    assert len(message) <= MESSAGE_LIMIT
    assert "truncated" in message
    assert message.endswith(f"\nlog: {LOG_URL}")


def test_message_survives_a_context_without_a_task_instance():
    # a callback fired outside a task run must still say something, not crash
    message = failure_message({"reason": "dag import failed"})
    assert "unknown_dag" in message and "dag import failed" in message


# ── notify_failure ────────────────────────────────────────────────────


def test_a_broken_channel_does_not_propagate(monkeypatch):
    # a raising callback would replace the real failure with its own and hide the cause
    class ExplodingHook:
        def __init__(self, http_conn_id=None, message=""):
            raise ConnectionError("no route to discord.com")

    _install_hook(monkeypatch, ExplodingHook)
    notify_failure(_context())


def test_a_failing_send_does_not_propagate(monkeypatch):
    class FailingHook:
        def __init__(self, http_conn_id=None, message=""):
            pass

        def execute(self):
            raise RuntimeError("404 unknown webhook")

    _install_hook(monkeypatch, FailingHook)
    notify_failure(_context())


def test_the_message_reaches_the_hook(monkeypatch):
    sent = []

    class RecordingHook:
        def __init__(self, http_conn_id=None, message=""):
            self.http_conn_id = http_conn_id
            self.message = message

        def execute(self):
            sent.append((self.http_conn_id, self.message))

    _install_hook(monkeypatch, RecordingHook)
    notify_failure(_context())
    conn_id, message = sent[0]
    assert conn_id == alert.DISCORD_CONN_ID
    assert "check_sink_brokers" in message
