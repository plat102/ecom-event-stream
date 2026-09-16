"""Broker liveness, topic availability, consumer group health and data-flow rates."""

from __future__ import annotations

import logging
import time
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from dec import dag_config
from dec.callbacks.defaults import DEFAULT_ARGS
from dec.callbacks.report import fail, record
from dec.hooks.kafka_admin import KafkaAdminHook
from dec.marks import read_mark, stage_mark
from dec.monitoring.probes import kafka as probes
from dec.monitoring.run_tail import add_run_tail
from dec.operators.health_check import KafkaHealthCheckOperator
from dec.operators.rate_check import RateCheckOperator

log = logging.getLogger(__name__)

DAG_ID = "kafka_health_monitor"
# Only the fallback: cadence and thresholds come from the registry Variable.
SETTINGS = dag_config.for_dag(DAG_ID, default_schedule="*/5 * * * *")

# Normal for a moment, a problem when it outlasts one schedule interval.
REBALANCING_STATES = {"PREPARING_REBALANCING", "COMPLETING_REBALANCING"}

# A mark left behind by an outage would average one interval's work over a gap.
MAX_RATE_WINDOW_SECONDS = 900


def cluster_config(name: str) -> dict:
    """Read per task, so a topic can be repointed without a deploy."""
    clusters = SETTINGS.require("clusters")
    if name not in clusters:
        raise AirflowException(f"{SETTINGS.config_variable}.clusters has no {name!r}")
    return clusters[name]


def cluster_setting(cluster_name: str, key: str):
    """Per-cluster when set, DAG-wide otherwise: the two clusters are different queues."""
    return cluster_config(cluster_name).get(key, SETTINGS.require(key))


def cluster_threshold(cluster_name: str, key: str) -> str:
    """The same fallback as a Jinja template, for the operator parameters."""
    config = f"var.json.{SETTINGS.config_variable}"
    return f"{{{{ {config}.clusters.{cluster_name}.{key} | default({config}.{key}, true) }}}}"


def cluster_template(name: str) -> dict:
    """The cluster entry as templated fields, so `from_cluster` still does the one reading."""
    entry = f"var.json.{SETTINGS.config_variable}.clusters.{name}"
    return {
        "conn_id": f"{{{{ {entry}.conn_id }}}}",
        "timeout": f"{{{{ {entry}.timeout | default(10, true) }}}}",
    }


def admin_client(cluster: dict):
    return KafkaAdminHook.from_cluster(cluster).get_conn()


def watermark_total(cluster_name: str, topic_key: str = "topic") -> int:
    cluster = cluster_config(cluster_name)
    with admin_client(cluster) as client:
        return probes.high_watermark_total(client, cluster[topic_key])


def committed_total(cluster_name: str) -> int:
    cluster = cluster_config(cluster_name)
    with admin_client(cluster) as client:
        return probes.committed_total(client, cluster["group"], cluster["topic"])


def probe_sink_topics(client) -> dict:
    cluster = cluster_config("sink")
    return probes.required_topics(client, expected={cluster["topic"], cluster["dlq_topic"]})


def broker_check(cluster_name: str) -> KafkaHealthCheckOperator:
    return KafkaHealthCheckOperator(
        task_id="check_brokers",
        cluster=cluster_template(cluster_name),
        probe=lambda client: probes.topics(client, cluster_name=cluster_name),
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
    # Checks stay quiet so one incident sends one alert; a retry would re-measure.
    default_args={
        **DEFAULT_ARGS,
        "execution_timeout": timedelta(minutes=2),
        "retries": 0,
    },
    tags=["monitoring", "kafka"],
    doc_md=__doc__,
)
def kafka_health_monitor():
    @task
    def check_consumer_group_status(cluster_name: str, **context) -> dict:
        """Earliest signal that a consumer died: the group empties before lag builds."""
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
        """Fails on a breach that is not draining: one breach is only a sink restart."""
        cluster = cluster_config(cluster_name)
        threshold = int(cluster_setting(cluster_name, "lag_threshold"))
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
            # Warn, never fail: it would alert every run with no way to clear.
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

    # NONE_FAILED: the run where the processing rate skips is the one that matters most.
    @task(trigger_rule=TriggerRule.NONE_FAILED)
    def compare_processing_rate_to_throughput(
        throughput: dict | None, processing: dict | None, **context
    ) -> dict:
        """Separates "nothing to do" from "nothing is being done"; lag alone cannot."""
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

    # Two groups because the clusters fail independently: each gates its own chain,
    # and a dead sink must not stop the source checks that guard against losing data.
    with TaskGroup("sink") as sink:
        # Listing topics round-trips to a broker, so it doubles as the connectivity check.
        sink_brokers = broker_check("sink")
        topics_exist = KafkaHealthCheckOperator(
            task_id="check_topics_exist",
            cluster=cluster_template("sink"),
            probe=probe_sink_topics,
            predicate=lambda measured: not measured["missing"],
            message="topic(s) missing from the sink cluster: {missing} — present: {present}",
        )
        sink_group_status = check_consumer_group_status.override(
            task_id="check_group_status"
        )("sink")
        sink_lag = check_consumer_lag.override(task_id="check_consumer_lag")("sink")
        sink_throughput = RateCheckOperator(
            task_id="check_throughput",
            state_variable=SETTINGS.state_variable,
            # Keyed on the cluster, not the task, so renaming a task keeps the mark.
            state_key="throughput.sink",
            measure=lambda: watermark_total("sink"),
            min_rate=cluster_threshold("sink", "min_throughput"),
            max_interval_seconds=MAX_RATE_WINDOW_SECONDS,
        )
        processing_rate = RateCheckOperator(
            task_id="check_processing_rate",
            state_variable=SETTINGS.state_variable,
            state_key="committed.sink",
            # No floor of its own: the comparison below is the judgement.
            measure=lambda: committed_total("sink"),
            max_interval_seconds=MAX_RATE_WINDOW_SECONDS,
        )
        dlq_growth = RateCheckOperator(
            task_id="check_dlq_growth",
            state_variable=SETTINGS.state_variable,
            state_key="dlq_watermark",
            # Growth, not depth: a topic's depth only ever rises, so it says nothing about now.
            measure=lambda: watermark_total("sink", topic_key="dlq_topic"),
            max_rate=SETTINGS.template("max_dlq_rate"),
            max_interval_seconds=MAX_RATE_WINDOW_SECONDS,
        )
        comparison = compare_processing_rate_to_throughput(
            sink_throughput.output, processing_rate.output
        )
        sink_brokers >> topics_exist
        topics_exist >> [
            sink_group_status,
            sink_lag,
            sink_throughput,
            processing_rate,
            dlq_growth,
        ]

    with TaskGroup("source") as source:
        source_brokers = broker_check("source")
        source_group_status = check_consumer_group_status.override(
            task_id="check_group_status"
        )("source")
        source_lag = check_consumer_lag.override(task_id="check_consumer_lag")("source")
        source_throughput = RateCheckOperator(
            task_id="check_throughput",
            state_variable=SETTINGS.state_variable,
            state_key="throughput.source",
            measure=lambda: watermark_total("source"),
            min_rate=cluster_threshold("source", "min_throughput"),
            max_interval_seconds=MAX_RATE_WINDOW_SECONDS,
        )
        source_brokers >> [source_group_status, source_lag, source_throughput]

    # A group stands for its own leaves, so the tail cannot miss one.
    add_run_tail([sink, source], state_variable=SETTINGS.state_variable)


kafka_health_monitor()
