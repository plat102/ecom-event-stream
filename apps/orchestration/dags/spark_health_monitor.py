"""Spark monitoring: cluster health, the job's state, and the rows it lands; never submits."""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

from dec import dag_config
from dec.callbacks.defaults import DEFAULT_ARGS
from dec.callbacks.report import fail, record
from dec.hooks.warehouse import WarehouseHook
from dec.monitoring.probes import spark as probes
from dec.monitoring.run_tail import add_run_tail
from dec.operators.health_check import SparkHealthCheckOperator
from dec.operators.rate_check import RateCheckOperator
from dec.operators.threshold_check import ReportingThresholdCheck

log = logging.getLogger(__name__)

DAG_ID = "spark_health_monitor"
# Only the fallback: cadence and thresholds come from the registry Variable.
SETTINGS = dag_config.for_dag(DAG_ID, default_schedule="*/10 * * * *")

# The ResourceManager serves requests in exactly one combination of the two.
RM_SERVING = ("STARTED", "ACTIVE")
JOB_RUNNING_TASK = "job_running"
JOB_NOT_RUNNING_TASK = "job_not_running"

# A query-shaping constant, not a tuning knob: it only has to exceed the rows written in
# `max_staleness_minutes` by a wide margin, and it is a literal so nothing is interpolated.
FRESHNESS_SCAN_KEYS = 200_000
# An empty window has no MAX(), so it reports as maximally stale rather than as NULL.
NO_WRITES_SENTINEL = 999_999

# Bounded by the primary key, and read on ingested_at: the event clock runs behind.
FRESHNESS_SQL = f"""
WITH recent AS (
    SELECT ingested_at
    FROM fact_event
    WHERE event_key > (SELECT MAX(event_key) FROM fact_event) - {FRESHNESS_SCAN_KEYS}
)
SELECT COALESCE(
           ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(ingested_at))) / 60, 1),
           {NO_WRITES_SENTINEL}
       )
FROM recent
"""


def probe_workers(client) -> dict:
    return probes.workers(client, minimum=int(SETTINGS.require("min_healthy_nodes")))


def probe_headroom(client) -> dict:
    return probes.headroom(
        client,
        min_mb=int(SETTINGS.require("min_available_mb")),
        min_vcores=int(SETTINGS.require("min_available_vcores")),
    )


def probe_spark_apps(client) -> dict:
    return probes.running_apps(client, app_name=SETTINGS.require("app_name"))


def fact_row_count() -> int:
    with WarehouseHook().get_conn() as client:
        return client.fetch_one("SELECT COUNT(*) FROM fact_event")[0]


@dag(
    dag_id=DAG_ID,
    description="YARN cluster health, Spark application state, and the rows reaching fact_event",
    schedule=SETTINGS.schedule,
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    # Retries kept: these probes re-read an instant, unlike a rate that re-measures.
    default_args={
        **DEFAULT_ARGS,
        "execution_timeout": timedelta(minutes=2),
    },
    tags=["monitoring", "spark", "yarn"],
    doc_md=__doc__,
)
def spark_health_monitor():
    # Runs alone and first: with the ResourceManager down every later check fails alike.
    master = SparkHealthCheckOperator(
        task_id="check_cluster_master",
        probe=probes.cluster_master,
        predicate=lambda m: (m["state"], m["ha_state"]) == RM_SERVING,
        message=(
            "ResourceManager is state={state} haState={ha_state}, expected "
            f"{RM_SERVING[0]}/{RM_SERVING[1]} — no Spark job can be scheduled"
        ),
    )
    # Separate from the job check: a degraded node decays throughput without killing it.
    workers = SparkHealthCheckOperator(
        task_id="check_workers_available",
        probe=probe_workers,
        predicate=lambda m: not m["degraded"] and m["running_nodes"] >= m["minimum"],
        message=(
            "{running_nodes} NodeManager(s) RUNNING of {minimum} required; "
            "not RUNNING: {degraded} — healthy nodes seen: {node_ids}"
        ),
    )
    # A forecast, not an incident: at zero the next application parks in ACCEPTED.
    headroom = SparkHealthCheckOperator(
        task_id="check_worker_resources",
        probe=probe_headroom,
        predicate=lambda m: (
            m["largest_node_available_mb"] >= m["min_mb"]
            and m["largest_node_available_vcores"] >= m["min_vcores"]
        ),
        message=(
            "no NodeManager can host a container of {min_mb}MB/{min_vcores} vcore: best offer "
            "is {largest_node_available_mb}MB/{largest_node_available_vcores} vcore (cluster "
            "total {cluster_available_mb}MB free, {cluster_allocated_mb}MB allocated) — "
            "per node {per_node}"
        ),
    )
    status = SparkHealthCheckOperator(task_id="check_spark_job_status", probe=probe_spark_apps)

    @task.branch
    def branch_on_job_state(measured: dict, **context) -> str:
        """ACCEPTED counts as not running: a job that never got its containers."""
        branch = JOB_RUNNING_TASK if measured["running"] else JOB_NOT_RUNNING_TASK
        record(context, {"branch": branch})
        return branch

    @task(task_id=JOB_RUNNING_TASK)
    def job_running(measured: dict, **context) -> None:
        uptimes = {}
        for app in measured["apps"]:
            minutes = app["elapsed_ms"] / 60000
            uptimes[app["id"]] = round(minutes, 1)
            log.info("%s is %s, up %.1f minutes", app["id"], app["state"], minutes)
        record(context, {"uptime_minutes": uptimes})

    @task(task_id=JOB_NOT_RUNNING_TASK, retries=0)
    def job_not_running(measured: dict, **context) -> None:
        parked = [app for app in measured["apps"] if app["state"] != "RUNNING"]
        detail = (
            f"application(s) parked in {[app['state'] for app in parked]}: "
            f"{[app['id'] for app in parked]}"
            if parked
            else "no application under that name on the cluster at all"
        )
        fail(
            context,
            {"app_name": measured["app_name"], "parked": [app["state"] for app in parked]},
            f"no RUNNING Spark application named {measured['app_name']!r} — {detail}. "
            "Start it with `make run-yarn`. Note `make run-local` runs outside YARN, so a job "
            "started that way is healthy but invisible here and will keep failing this check.",
        )

    @task
    def check_no_concurrent_jobs(measured: dict, **context) -> dict:
        """Two streaming jobs cannot coexist; the collision reads as a data problem."""
        running = measured["running"]
        if len(running) > 1:
            fail(
                context,
                {"running_count": len(running)},
                f"{len(running)} applications named {measured['app_name']!r} are RUNNING at "
                f"once: {running} — kill all but one before they corrupt the checkpoint",
            )
        return record(context, {"running_count": len(running)})

    # The only place Spark's latency shows: checkpoint offsets leave no lag to read.
    freshness = ReportingThresholdCheck(
        task_id="check_fact_freshness",
        conn_id="postgres_warehouse",
        sql=FRESHNESS_SQL,
        min_threshold=0,
        max_threshold=SETTINGS.template("max_staleness_minutes"),
        message=(
            "fact_event: last write {result} minutes ago (limit {max_threshold}) — the job "
            "holds the cluster but is not writing"
        ),
    )
    # COUNT(*), not MAX(event_key): a replay that lands nothing still burns sequence values.
    growth = RateCheckOperator(
        task_id="check_row_growth",
        state_variable=SETTINGS.state_variable,
        state_key="fact_row_count",
        measure=fact_row_count,
        min_rate=SETTINGS.template("min_row_growth"),
        # A mark left behind by an outage would average one interval's work over a gap.
        max_interval_seconds=3600,
        retries=0,  # a retry overwrites the mark and shrinks the measured window
    )

    master >> [workers, headroom, status]
    concurrency = check_no_concurrent_jobs(status.output)
    # Hung off `job_running`, not the branch, which would skip them on healthy runs.
    running, not_running = job_running(status.output), job_not_running(status.output)
    branch_on_job_state(status.output) >> [running, not_running]
    running >> freshness >> growth

    add_run_tail(
        [workers, headroom, concurrency, not_running, growth],
        state_variable=SETTINGS.state_variable,
    )


spark_health_monitor()
