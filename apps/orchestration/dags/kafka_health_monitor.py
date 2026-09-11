"""Broker liveness, topic availability, consumer group health and data-flow rates.
Two limits: **Spark has no consumer lag** (checkpoint offsets, no group), and **a mongo sink
restart spikes lag without being an incident** — hence lag judged on draining, not on one
breach. Rates compare against a mark that only scheduled runs move, so triggering this DAG by
hand measures without disturbing the cadence.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

from callbacks.alert import notify_failure, send_to_discord
from callbacks.defaults import DEFAULT_ARGS
from callbacks.report import build_report
from hooks.kafka_admin import KafkaAdminHook
from marks import advance_mark
from operators.rate_check import RateCheckOperator

log = logging.getLogger(__name__)

CLUSTERS_VARIABLE = "KAFKA_CONSUMER_GROUPS"
# Normal for a moment, and a problem when it outlasts one schedule interval.
REBALANCING_STATES = {"PREPARING_REBALANCING", "COMPLETING_REBALANCING"}


def cluster_config(name: str) -> dict:
    """A Variable so a topic can be repointed without a deploy; read inside tasks, because at
    parse time it would query the metadata database on every scheduler pass."""
    return json.loads(Variable.get(CLUSTERS_VARIABLE))[name]


def admin_client(cluster: dict):
    return KafkaAdminHook(cluster["conn_id"], timeout=cluster.get("timeout", 10)).get_conn()


def watermark_total(cluster_name: str, topic_key: str = "topic") -> int:
    """End of the log, summed over partitions — the counter throughput is derived from."""
    cluster = cluster_config(cluster_name)
    with admin_client(cluster) as client:
        return sum(client.topic_high_watermark(cluster[topic_key]).values())


def record(context, measured: dict) -> dict:
    """The run report reads this key, so a failed check still contributes its numbers."""
    context["ti"].xcom_push(key="measured", value=measured)
    return measured


def fail(context, measured: dict, message: str) -> None:
    record(context, {**measured, "error": message})
    raise AirflowException(message)


def committed_total(cluster_name: str) -> int:
    """Committed offsets, summed — the counter the processing rate is derived from."""
    cluster = cluster_config(cluster_name)
    with admin_client(cluster) as client:
        offsets = client.committed_offsets(cluster["group"], cluster["topic"])
    return sum(offset for offset in offsets.values() if offset is not None)


@dag(
    dag_id="kafka_health_monitor",
    description="Broker, topic, consumer group and data-flow checks on both Kafka clusters",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    # One incident trips several checks, so this DAG reports once per run instead of alerting
    # per task; the report task keeps the callback as its backstop.
    # retries=0: retrying a measurement measures something else — a retry 30s later would
    # store its own mark and shrink the window every rate and trend is judged over. The next
    # scheduled run is the retry.
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
    @task(retries=3, retry_delay=timedelta(seconds=30))
    def check_brokers(cluster_name: str, **context) -> list[str]:
        """Listing topics round-trips to a broker, so it doubles as the connectivity check.
        Three retries: a rolling restart of one broker is back within tens of seconds."""
        cluster = cluster_config(cluster_name)
        with admin_client(cluster) as client:
            topics = client.list_topics()
        log.info("%s cluster reachable, %d topic(s): %s", cluster_name, len(topics), topics)
        record(context, {"cluster": cluster_name, "topics": len(topics)})
        return topics

    @task
    def check_topics_exist(topics: list[str], **context) -> None:
        """Both topics: a missing DLQ is silent until the first event needs rejecting."""
        cluster = cluster_config("sink")
        expected = {cluster["topic"], cluster["dlq_topic"]}
        missing = sorted(expected - set(topics))
        if missing:
            fail(
                context,
                {"expected": sorted(expected), "present": sorted(topics)},
                f"topic(s) missing from the sink cluster: {missing} — present: {sorted(topics)}",
            )
        log.info("topics present: %s", sorted(expected))
        record(context, {"topics": sorted(expected)})

    @task
    def check_consumer_group_status(cluster_name: str, **context) -> dict:
        """Earliest signal that a consumer died: brokers stay healthy and lag takes minutes to
        build, but the group empties at once."""
        cluster = cluster_config(cluster_name)
        group = cluster["group"]
        with admin_client(cluster) as client:
            status = client.consumer_group_state(group)

        state_key = f"KAFKA_GROUP_STATE_{group}"
        previous = Variable.get(state_key, default_var=None)
        advance_mark(context, state_key, status.state)
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
        threshold = int(Variable.get("KAFKA_LAG_THRESHOLD"))
        with admin_client(cluster) as client:
            lag = client.consumer_group_lag(cluster["group"], cluster["topic"])

        state_key = f"KAFKA_LAG_MARK_{cluster['group']}"
        raw_previous = Variable.get(state_key, default_var=None)
        advance_mark(context, state_key, json.dumps({"at": time.time(), "total": lag.total}))
        measured = {
            "group": lag.group,
            "topic": lag.topic,
            "lag": lag.total,
            "per_partition": lag.per_partition,
            "threshold": threshold,
        }

        if lag.uncommitted_partitions:
            # Warn, never fail: a partition that has delivered nothing yet has no committed
            # offset, and would otherwise alert on every run with no way to clear. "Nothing
            # is consuming" is the group-status check's call, not this one's.
            log.warning(
                "group %r has no committed offset on partition(s) %s of %r — lag there is "
                "unknown, not zero",
                lag.group, lag.uncommitted_partitions, lag.topic,
            )
            measured["uncommitted_partitions"] = lag.uncommitted_partitions
        if lag.total <= threshold:
            log.info("lag %d of %d allowed: %s", lag.total, threshold, lag.per_partition)
            return record(context, measured)

        previous_total = json.loads(raw_previous)["total"] if raw_previous else None
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

    @task(
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
        on_failure_callback=notify_failure,
    )
    def report_run(**context) -> str:
        """ALL_DONE: a report that only appeared on healthy runs would be one nobody needs.
        Keeps the failure callback, so an undeliverable digest is not a silent run."""
        task_instance, dag_run = context["ti"], context["dag_run"]
        rows = [
            {
                "task_id": other.task_id,
                "state": other.state,
                "measured": task_instance.xcom_pull(task_ids=other.task_id, key="measured"),
                "log_url": other.log_url,
            }
            for other in dag_run.get_task_instances()
            if other.task_id != task_instance.task_id
        ]
        report = build_report(
            dag_id=dag_run.dag_id,
            logical_date=context["logical_date"].strftime("%Y-%m-%d %H:%M UTC"),
            rows=rows,
        )
        log.info("run report:\n%s", report)
        send_to_discord(report)
        return report

    sink_topics = check_brokers.override(task_id="check_sink_brokers")("sink")
    topics_exist = check_topics_exist(sink_topics)
    source_topics = check_brokers.override(task_id="check_source_brokers")("source")

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
        state_key="KAFKA_THROUGHPUT_MARK_SINK",
        measure=lambda: watermark_total("sink"),
        min_rate="{{ var.value.KAFKA_MIN_THROUGHPUT }}",
    )
    source_throughput = RateCheckOperator(
        task_id="check_source_throughput",
        state_key="KAFKA_THROUGHPUT_MARK_SOURCE",
        measure=lambda: watermark_total("source"),
        min_rate="{{ var.value.KAFKA_MIN_THROUGHPUT }}",
    )
    processing_rate = RateCheckOperator(
        task_id="check_processing_rate",
        state_key="KAFKA_COMMITTED_MARK_SINK",
        # No floor of its own: a source that stopped producing would fail this too, and one
        # incident should not produce two alerts. The comparison below is the judgement.
        measure=lambda: committed_total("sink"),
    )
    dlq_growth = RateCheckOperator(
        task_id="check_dlq_growth",
        state_key="KAFKA_DLQ_MARK",
        # Growth, not depth: a topic's depth only ever rises, so the total says nothing
        # about now — new rejects per minute do.
        measure=lambda: watermark_total("sink", topic_key="dlq_topic"),
        max_rate="{{ var.value.KAFKA_MAX_DLQ_RATE }}",
    )

    topics_exist >> [sink_group_status, sink_lag, sink_throughput, processing_rate, dlq_growth]
    source_topics >> [source_group_status, source_lag, source_throughput]
    comparison = compare_processing_rate_to_throughput(
        sink_throughput.output, processing_rate.output
    )

    report = report_run()
    [
        sink_group_status,
        sink_lag,
        dlq_growth,
        comparison,
        source_group_status,
        source_lag,
        source_throughput,
    ] >> report


kafka_health_monitor()
