"""Spark monitoring: cluster health, the streaming job's state, and the rows it lands.

One DAG, two halves — the same shape as `kafka_health_monitor`. Monitor only, never submits
(ADR-16). The output checks hang off `job_running` so a dead job skips them instead of adding
two more failures to the one alert.

Two traps: freshness is measured on `ingested_at`, the Postgres clock, because the clock
inside the events runs ~30 hours behind by design; and nothing filters on today's date, since
the generator replays the past and never catches up.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
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
from hooks.warehouse import WarehouseHook
from marks import commit_marks
from operators.health_check import SparkHealthCheckOperator
from operators.rate_check import RateCheckOperator
from operators.sql_check import SqlCheckOperator

log = logging.getLogger(__name__)

DAG_ID = "spark_health_monitor"
# Cadence, thresholds and marks all come from DAG_REGISTRY — see plugins/dag_config.py. This
# is only the fallback for a metadata database that has never been seeded.
SETTINGS = dag_config.for_dag(DAG_ID, default_schedule="*/10 * * * *")

# The ResourceManager serves requests in exactly one combination of the two.
RM_SERVING = ("STARTED", "ACTIVE")
JOB_RUNNING_TASK = "job_running"
JOB_NOT_RUNNING_TASK = "job_not_running"

# Bounded by the primary key: unindexed, MAX(ingested_at) is a seq scan (7.5s at 10M rows).
# The newest row always sits on the highest event_key, so a PK window answers it in ~300ms.
FRESHNESS_SQL = f"""
WITH recent AS (
    SELECT ingested_at
    FROM fact_event
    WHERE event_key > (SELECT MAX(event_key) FROM fact_event)
                      - {SETTINGS.template("freshness_scan_keys")}
)
SELECT COUNT(*)                                                       AS scanned_rows,
       ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(ingested_at))) / 60, 1)  AS staleness_minutes,
       {SETTINGS.template("max_staleness_minutes")}::numeric          AS limit_minutes
FROM recent
"""


def describe_node(node) -> str:
    report = f" — {node.health_report}" if node.health_report else ""
    return f"{node.id} ({node.state}{report})"


def probe_cluster_master(client) -> dict:
    info = client.cluster_info()
    return {
        "state": info.get("state"),
        "ha_state": info.get("haState"),
        "rm_version": info.get("resourceManagerVersion"),
    }


def probe_workers(client) -> dict:
    """Counts healthy NodeManagers and names the ones that are not, in one reading — a single
    message covering both says more than two checks that can only ever fail one at a time."""
    nodes = client.nodes()
    healthy = [node for node in nodes if node.is_healthy]
    degraded = [node for node in nodes if not node.is_healthy]
    return {
        "running_nodes": len(healthy),
        "minimum": int(SETTINGS.value("min_healthy_nodes")),
        "node_ids": [node.id for node in healthy],
        "degraded": [describe_node(node) for node in degraded],
    }


def probe_headroom(client) -> dict:
    """Judged per node as well as cluster-wide: a container has to fit on one node, so a
    cluster whose headroom is spread thin across nodes can still schedule nothing."""
    metrics = client.cluster_metrics()
    nodes = [node for node in client.nodes() if node.is_healthy]
    return {
        "cluster_available_mb": metrics.get("availableMB", 0),
        "cluster_allocated_mb": metrics.get("allocatedMB", 0),
        "cluster_available_vcores": metrics.get("availableVirtualCores", 0),
        "largest_node_available_mb": max((node.available_mb for node in nodes), default=0),
        "largest_node_available_vcores": max(
            (node.available_vcores for node in nodes), default=0
        ),
        "min_mb": int(SETTINGS.value("min_available_mb")),
        "min_vcores": int(SETTINGS.value("min_available_vcores")),
        "per_node": {node.id: [node.available_mb, node.available_vcores] for node in nodes},
    }


def probe_spark_apps(client) -> dict:
    """No predicate: this is a pure reading. The branch and the concurrency check both read
    it, so YARN is asked once — two readings could disagree inside one run."""
    app_name = SETTINGS.value("app_name")
    apps = client.apps(name=app_name)
    return {
        "app_name": app_name,
        "apps": [
            {"id": app.id, "state": app.state, "elapsed_ms": app.elapsed_ms} for app in apps
        ],
        "running": [app.id for app in apps if app.is_running],
    }


def fact_row_count() -> int:
    with WarehouseHook().get_conn() as client:
        return client.fetch_one("SELECT COUNT(*) FROM fact_event")[0]


def is_fresh(row: dict) -> bool:
    return row["scanned_rows"] > 0 and row["staleness_minutes"] <= row["limit_minutes"]


@dag(
    dag_id=DAG_ID,
    description="YARN cluster health, Spark application state, and the rows reaching fact_event",
    schedule=SETTINGS.schedule,
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    # One incident trips several checks (an UNHEALTHY node costs a worker and headroom), so
    # the checks stay quiet and the digest speaks once; report_run keeps the callback.
    default_args={
        **DEFAULT_ARGS,
        "execution_timeout": timedelta(minutes=2),
        "on_failure_callback": None,
    },
    tags=["monitoring", "spark", "yarn"],
    doc_md=__doc__,
)
def spark_health_monitor():
    # Runs alone and first: with the ResourceManager down every later check would fail for
    # the same reason, and one incident should not send seven alerts.
    master = SparkHealthCheckOperator(
        task_id="check_cluster_master",
        probe=probe_cluster_master,
        predicate=lambda m: (m["state"], m["ha_state"]) == RM_SERVING,
        message=(
            "ResourceManager is state={state} haState={ha_state}, expected "
            f"{RM_SERVING[0]}/{RM_SERVING[1]} — no Spark job can be scheduled"
        ),
    )
    # Separate from the job check on purpose: an UNHEALTHY NodeManager does not kill a
    # running job, it just stops receiving containers, so throughput decays silently.
    workers = SparkHealthCheckOperator(
        task_id="check_workers_available",
        probe=probe_workers,
        predicate=lambda m: not m["degraded"] and m["running_nodes"] >= m["minimum"],
        message=(
            "{running_nodes} NodeManager(s) RUNNING of {minimum} required; "
            "not RUNNING: {degraded} — healthy nodes seen: {node_ids}"
        ),
    )
    # Headroom is a forecast, not an incident: at zero the next application is ACCEPTED and
    # parks indefinitely without raising. The numbers are logged pass or fail.
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
        """ACCEPTED counts as not running. Treating it as healthy would blind this DAG to the
        exact failure it exists to catch — a job that never got its containers."""
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

    @task(task_id=JOB_NOT_RUNNING_TASK)
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
        """The reverse failure: two streaming jobs cannot coexist. Same checkpoint gives
        CONCURRENT_STREAM_LOG_UPDATE; different ones collide on UNIQUE(event_id), which reads
        as a data problem and costs far more to diagnose.

        A task rather than an operator: it judges the reading `check_spark_job_status` already
        took, so re-probing YARN here could see a different cluster in the same run."""
        running = measured["running"]
        if len(running) > 1:
            fail(
                context,
                {"running_count": len(running)},
                f"{len(running)} applications named {measured['app_name']!r} are RUNNING at "
                f"once: {running} — kill all but one before they corrupt the checkpoint",
            )
        return record(context, {"running_count": len(running)})

    @task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def commit_run_marks(**context) -> dict:
        """The single writer for this DAG's state Variable. Several checks stage marks in
        parallel; each writing for itself would drop all but the last."""
        return commit_marks(context, SETTINGS.state_variable)

    @task(
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
        on_failure_callback=notify_failure,
    )
    def report_run(**context) -> str:
        """ALL_DONE: headroom is a forecast, so the run everyone needs to see is the healthy
        one — an alert-only DAG hides the trend until it is already an incident."""
        return send_run_report(context)

    @task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def mark_run_state(**context) -> None:
        """Inherits the DAG's `on_failure_callback: None` — the digest already notified."""
        fail_run_if_checks_failed(context)

    # This is the only place Spark's latency is measurable: Structured Streaming keeps its
    # offsets in the checkpoint rather than in a consumer group, so it has no lag to read.
    freshness = SqlCheckOperator(
        task_id="check_fact_freshness",
        sql=FRESHNESS_SQL,
        predicate=is_fresh,
        message=(
            "fact_event: {scanned_rows} row(s) in the scanned window, last write "
            "{staleness_minutes} minutes ago (limit {limit_minutes}) — the job holds the "
            "cluster but is not writing"
        ),
    )
    # COUNT(*) (~3s), not MAX(event_key) (12ms): ON CONFLICT DO NOTHING burns sequence
    # values, so the key climbs through a replay that lands nothing.
    growth = RateCheckOperator(
        task_id="check_row_growth",
        state_variable=SETTINGS.state_variable,
        state_key="fact_row_count",
        measure=fact_row_count,
        min_rate=SETTINGS.template("min_row_growth"),
        # A mark left behind by an outage would average one interval's work over hours.
        max_interval_seconds=3600,
        retries=0,  # a retry overwrites the mark and shrinks the measured window
    )

    master >> [workers, headroom, status]
    concurrency = check_no_concurrent_jobs(status.output)
    # Output checks hang off `job_running`, not the branch: the branch skips every direct
    # downstream it does not return, which would skip them on healthy runs too.
    running, not_running = job_running(status.output), job_not_running(status.output)
    branch_on_job_state(status.output) >> [running, not_running]
    running >> freshness >> growth

    # Every leaf: ALL_DONE fires once the listed upstreams settle, so one left off could
    # still be running. Marks commit before the digest — a failed run still owes a baseline.
    commit = commit_run_marks()
    [workers, headroom, concurrency, not_running, growth] >> commit
    commit >> report_run() >> mark_run_state()


spark_health_monitor()
