"""Broker liveness, topic availability, consumer group health and data-flow rates.
Two limits: **Spark has no consumer lag** (checkpoint offsets, no group), and **a mongo sink
restart spikes lag without being an incident** — hence lag judged on draining, not on one
breach. Rates compare against a mark that only scheduled runs move, so triggering this DAG by
hand measures without disturbing the cadence.
"""

from __future__ import annotations

import logging
import time
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule

import dag_config
from callbacks.alert import notify_failure
from callbacks.defaults import DEFAULT_ARGS
from callbacks.report import (
    fail,
    fail_run_if_checks_failed,
    record,
    send_run_report,
)
from hooks.kafka_admin import KafkaAdminHook
from marks import commit_marks, read_mark, stage_mark
from operators.health_check import KafkaHealthCheckOperator
from operators.rate_check import RateCheckOperator

log = logging.getLogger(__name__)

DAG_ID = "kafka_health_monitor"
# Cadence, thresholds, the cluster map and the marks all come from DAG_REGISTRY — see
# plugins/dag_config.py. This is only the fallback for a database that has never been seeded.
SETTINGS = dag_config.for_dag(DAG_ID, default_schedule="*/5 * * * *")

# Normal for a moment, and a problem when it outlasts one schedule interval.
REBALANCING_STATES = {"PREPARING_REBALANCING", "COMPLETING_REBALANCING"}


def cluster_config(name: str) -> dict:
    """Read inside a task, not at parse time: a topic can be repointed without a deploy."""
    return SETTINGS.value("clusters")[name]


def admin_client(cluster: dict):
    return KafkaAdminHook(cluster["conn_id"], timeout=cluster.get("timeout", 10)).get_conn()


def watermark_total(cluster_name: str, topic_key: str = "topic") -> int:
    """End of the log, summed over partitions — the counter throughput is derived from."""
    cluster = cluster_config(cluster_name)
    with admin_client(cluster) as client:
        return sum(client.topic_high_watermark(cluster[topic_key]).values())


def committed_total(cluster_name: str) -> int:
    """Committed offsets, summed — the counter the processing rate is derived from."""
    cluster = cluster_config(cluster_name)
    with admin_client(cluster) as client:
        offsets = client.committed_offsets(cluster["group"], cluster["topic"])
    return sum(offset for offset in offsets.values() if offset is not None)


def cluster_template(cluster_name: str, key: str, default: str | None = None) -> str:
    """A Jinja reference into the cluster map, rendered at run time. `default` matters for
    optional keys — an absent one renders empty, not missing."""
    fallback = f" | default({default})" if default is not None else ""
    return SETTINGS.template(f"clusters.{cluster_name}.{key}{fallback}")


def probe_topics(client, cluster_name: str) -> dict:
    topics = client.list_topics()
    return {"cluster": cluster_name, "topics": len(topics), "names": sorted(topics)}


def probe_sink_topics(client) -> dict:
    """Both topics: a missing DLQ is silent until the first event needs rejecting."""
    cluster = cluster_config("sink")
    expected = {cluster["topic"], cluster["dlq_topic"]}
    present = set(client.list_topics())
    return {
        "expected": sorted(expected),
        "present": sorted(present),
        "missing": sorted(expected - present),
    }


def broker_check(cluster_name: str) -> KafkaHealthCheckOperator:
    return KafkaHealthCheckOperator(
        task_id=f"check_{cluster_name}_brokers",
        conn_id=cluster_template(cluster_name, "conn_id"),
        timeout=cluster_template(cluster_name, "timeout", default="10"),
        probe=lambda client: probe_topics(client, cluster_name),
        retries=3,
        retry_delay=timedelta(seconds=30),
    )


@dag(
    dag_id=DAG_ID,
    description="Broker, topic, consumer group and data-flow checks on both Kafka clusters",
    schedule=SETTINGS.schedule,
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    # One incident trips several checks, so the checks stay quiet and the digest speaks once;
    # report_run keeps the callback. retries=0: a retry re-measures and shrinks every window.
    default_args={
        **DEFAULT_ARGS,
        "execution_timeout": timedelta(minutes=2),
        "on_failure_callback": None,
        "retries": 0,
    },
    tags=["monitoring", "kafka"],
    doc_md=__doc__,
)
def kafka_health_monitor():
    @task
    def check_consumer_group_status(cluster_name: str, **context) -> dict:
        """Earliest signal that a consumer died: brokers stay healthy and lag takes minutes to
        build, but the group empties at once."""
        cluster = cluster_config(cluster_name)
        group = cluster["group"]
        with admin_client(cluster) as client:
            status = client.consumer_group_state(group)

        state_key = f"group_state.{group}"
        previous = read_mark(SETTINGS.state_variable, state_key)
        stage_mark(context, state_key, status.state)
        measured = {"group": group, "state": status.state, "members": status.members}

        if status.state in REBALANCING_STATES:
            if previous in REBALANCING_STATES:
                fail(
                    context,
                    measured,
                    f"consumer group {group!r} was {previous} and is now {status.state} — "
                    "a rebalance lasting across two runs is not a rebalance any more",
                )
            log.info("group %r is %s — transient, will re-check next run", group, status.state)
            return record(context, measured)
        if not status.is_consuming:
            fail(
                context,
                measured,
                f"consumer group {group!r} is {status.state} with {status.members} member(s) — "
                f"the brokers are up but nothing is consuming {cluster['topic']!r}",
            )
        log.info("group %r is %s with %d member(s)", group, status.state, status.members)
        return record(context, measured)

    @task
    def check_consumer_lag(cluster_name: str, **context) -> dict:
        """Fails on a breach that is not draining — one breach is a sink restart, a breach that
        stays is a stuck consumer."""
        cluster = cluster_config(cluster_name)
        threshold = int(SETTINGS.value("lag_threshold"))
        with admin_client(cluster) as client:
            lag = client.consumer_group_lag(cluster["group"], cluster["topic"])

        state_key = f"lag.{cluster['group']}"
        previous_mark = read_mark(SETTINGS.state_variable, state_key)
        stage_mark(context, state_key, {"at": time.time(), "total": lag.total})
        measured = {
            "group": lag.group,
            "topic": lag.topic,
            "lag": lag.total,
            "per_partition": lag.per_partition,
            "threshold": threshold,
        }

        if lag.uncommitted_partitions:
            # Warn, never fail: a partition with no committed offset would alert every run
            # with no way to clear. "Nothing is consuming" is the group-status check's call.
            log.warning(
                "group %r has no committed offset on partition(s) %s of %r — lag there is "
                "unknown, not zero",
                lag.group, lag.uncommitted_partitions, lag.topic,
            )
            measured["uncommitted_partitions"] = lag.uncommitted_partitions
        if lag.total <= threshold:
            log.info("lag %d of %d allowed: %s", lag.total, threshold, lag.per_partition)
            return record(context, measured)

        previous_total = previous_mark["total"] if previous_mark else None
        if previous_total is not None and lag.total < previous_total:
            log.warning(
                "lag %d is over the %d threshold but draining (was %d) — not alerting",
                lag.total,
                threshold,
                previous_total,
            )
            return record(context, {**measured, "draining_from": previous_total})
        fail(
            context,
            {**measured, "previous_lag": previous_total},
            f"consumer group {lag.group!r} lag on {lag.topic!r} is {lag.total} "
            f"(threshold {threshold}, previous run {previous_total}) and not draining — "
            f"per partition {lag.per_partition}",
        )

    # NONE_FAILED, not the default: the processing rate skips when the group's committed
    # offsets reset, and that is the run where this comparison matters most.
    @task(trigger_rule=TriggerRule.NONE_FAILED)
    def compare_processing_rate_to_throughput(
        throughput: dict | None, processing: dict | None, **context
    ) -> dict:
        """Together these separate "nothing to do" from "nothing is being done" — lag alone
        reads healthy when the source has simply stopped."""
        if throughput is None:
            raise AirflowSkipException("no throughput measurement to compare against")
        produced = throughput["rate_per_minute"]
        consumed = processing["rate_per_minute"] if processing else 0.0
        measured = {"produced_per_minute": produced, "consumed_per_minute": consumed}
        if produced > 0 and consumed <= 0:
            unmeasured = "" if processing else " (committed offsets unmeasurable — reset?)"
            fail(
                context,
                measured,
                f"{produced:.0f} msg/min are being produced but the consumer group committed "
                f"nothing{unmeasured}",
            )
        log.info("produced %.0f/min, consumed %.0f/min", produced, consumed)
        return record(context, measured)

    @task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def commit_run_marks(**context) -> dict:
        """The single writer for this DAG's state Variable. Eight checks stage marks in
        parallel; each writing for itself would drop all but the last."""
        return commit_marks(context, SETTINGS.state_variable)

    @task(
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
        on_failure_callback=notify_failure,
    )
    def report_run(**context) -> str:
        """ALL_DONE: a report that only appeared on healthy runs would be one nobody needs.
        Keeps the failure callback, so an undeliverable digest is not a silent run."""
        return send_run_report(context)

    @task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def mark_run_state(**context) -> None:
        """Inherits the DAG's `on_failure_callback: None` — the digest already notified."""
        fail_run_if_checks_failed(context)

    # Listing topics round-trips to a broker, so it doubles as the connectivity check — no
    # predicate needed, the call either returned or raised.
    sink_brokers = broker_check("sink")
    source_brokers = broker_check("source")
    topics_exist = KafkaHealthCheckOperator(
        task_id="check_topics_exist",
        conn_id=cluster_template("sink", "conn_id"),
        probe=probe_sink_topics,
        predicate=lambda measured: not measured["missing"],
        message=(
            "topic(s) missing from the sink cluster: {missing} — present: {present}"
        ),
    )
    sink_brokers >> topics_exist

    sink_group_status = check_consumer_group_status.override(task_id="check_sink_group_status")(
        "sink"
    )
    source_group_status = check_consumer_group_status.override(
        task_id="check_source_group_status"
    )("source")

    sink_lag = check_consumer_lag.override(task_id="check_sink_consumer_lag")("sink")
    source_lag = check_consumer_lag.override(task_id="check_source_consumer_lag")("source")

    sink_throughput = RateCheckOperator(
        task_id="check_message_throughput",
        state_variable=SETTINGS.state_variable,
        state_key="throughput.sink",
        measure=lambda: watermark_total("sink"),
        min_rate=SETTINGS.template("min_throughput"),
    )
    source_throughput = RateCheckOperator(
        task_id="check_source_throughput",
        state_variable=SETTINGS.state_variable,
        state_key="throughput.source",
        measure=lambda: watermark_total("source"),
        min_rate=SETTINGS.template("min_throughput"),
    )
    processing_rate = RateCheckOperator(
        task_id="check_processing_rate",
        state_variable=SETTINGS.state_variable,
        state_key="committed.sink",
        # No floor of its own: a source that stopped producing would fail this too, and one
        # incident should not produce two alerts. The comparison below is the judgement.
        measure=lambda: committed_total("sink"),
    )
    dlq_growth = RateCheckOperator(
        task_id="check_dlq_growth",
        state_variable=SETTINGS.state_variable,
        state_key="dlq_watermark",
        # Growth, not depth: a topic's depth only ever rises, so the total says nothing
        # about now — new rejects per minute do.
        measure=lambda: watermark_total("sink", topic_key="dlq_topic"),
        max_rate=SETTINGS.template("max_dlq_rate"),
    )

    topics_exist >> [sink_group_status, sink_lag, sink_throughput, processing_rate, dlq_growth]
    source_brokers >> [source_group_status, source_lag, source_throughput]
    comparison = compare_processing_rate_to_throughput(
        sink_throughput.output, processing_rate.output
    )

    commit = commit_run_marks()
    report = report_run()
    [
        sink_group_status,
        sink_lag,
        dlq_growth,
        comparison,
        source_group_status,
        source_lag,
        source_throughput,
    ] >> commit
    # Marks are committed before the digest is built, and both run on ALL_DONE: a run that
    # failed still has to leave the next one a baseline to measure against.
    commit >> report >> mark_run_state()


kafka_health_monitor()
