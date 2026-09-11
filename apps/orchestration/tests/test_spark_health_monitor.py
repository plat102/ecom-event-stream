"""Unit tests for the Spark monitoring DAG: what each probe reports, which readings are
allowed to count as healthy, and whether a failure names the node, application or number it
is about. Needs the airflow package, so it runs in the image.
"""
import pathlib

import pytest

pytest.importorskip("airflow", reason="airflow is only installed in the Airflow image")

from airflow.exceptions import AirflowException

import dag_config
import spark_health_monitor as dag_module
from shared.connectors.yarn import YarnApp, YarnNode

APP_NAME = "ecom-stream-processor"
CONFIG = {
    "app_name": APP_NAME,
    "min_healthy_nodes": 1,
    "min_available_mb": 1024,
    "min_available_vcores": 1,
}
DAG_SOURCE = pathlib.Path(dag_module.__file__).read_text()


class FakeYarn:
    """Stands in for the YARN client with whatever cluster shape a test needs."""

    def __init__(self, *, info=None, nodes=(), metrics=None, apps=()) -> None:
        self._info = info or {"state": "STARTED", "haState": "ACTIVE"}
        self._nodes = list(nodes)
        self._metrics = metrics or {}
        self._apps = list(apps)

    def cluster_info(self):
        return self._info

    def nodes(self):
        return list(self._nodes)

    def cluster_metrics(self):
        return self._metrics

    def apps(self, name=None):
        return [app for app in self._apps if name is None or app.name == name]





def node(node_id, state="RUNNING", mb=4096, vcores=4, report="") -> YarnNode:
    return YarnNode(
        id=node_id,
        state=state,
        available_mb=mb,
        used_mb=0,
        available_vcores=vcores,
        health_report=report,
    )


def app(app_id, state="RUNNING", name=APP_NAME) -> YarnApp:
    return YarnApp(
        id=app_id, name=name, state=state, elapsed_ms=60000, allocated_mb=3072, allocated_vcores=2
    )


@pytest.fixture(autouse=True)
def settings(monkeypatch):
    """Thresholds come from the DAG's config Variable; the probes read them through this."""
    monkeypatch.setattr(dag_config.DagSettings, "config", lambda self: CONFIG)


def check(task_id):
    """The operator as the DAG wires it — predicate and message included."""
    return dag_module.spark_health_monitor().get_task(task_id)


def judge(task_id, measured) -> None:
    """Apply one check's predicate and message to a measurement, as the operator would."""
    operator = check(task_id)
    if not operator.predicate(measured):
        raise AirflowException(operator.message.format(**measured))


class FakeTaskInstance:
    """Collects what a task records for the run digest."""

    def __init__(self) -> None:
        self.pushed: dict = {}

    def xcom_push(self, key, value):
        self.pushed[key] = value


def _context() -> dict:
    return {"ti": FakeTaskInstance()}


def run_task(task_id, measured, context=None):
    """Call a `@task` body the way Airflow would, context included."""
    return check(task_id).python_callable(measured, **(context or _context()))


# ── the ResourceManager gate ──────────────────────────────────────────


def test_started_and_active_master_passes():
    measured = dag_module.probe_cluster_master(
        FakeYarn(info={"state": "STARTED", "haState": "ACTIVE", "resourceManagerVersion": "3.3.6"})
    )
    assert measured["rm_version"] == "3.3.6"
    judge("check_cluster_master", measured)


def test_standby_master_fails_and_says_so():
    # a STANDBY ResourceManager answers the REST call but schedules nothing
    measured = dag_module.probe_cluster_master(
        FakeYarn(info={"state": "STARTED", "haState": "STANDBY"})
    )
    with pytest.raises(AirflowException, match="haState=STANDBY"):
        judge("check_cluster_master", measured)


# ── node health ───────────────────────────────────────────────────────


def test_unhealthy_node_fails_with_its_id_and_health_report():
    measured = dag_module.probe_workers(
        FakeYarn(nodes=[node("nodemanager1:35139", "UNHEALTHY", report="1/1 local-dirs are bad")])
    )
    with pytest.raises(AirflowException) as failure:
        judge("check_workers_available", measured)
    assert "nodemanager1:35139" in str(failure.value)
    assert "1/1 local-dirs are bad" in str(failure.value)


def test_too_few_running_nodes_fails_even_when_none_are_degraded():
    measured = dag_module.probe_workers(FakeYarn(nodes=[]))
    with pytest.raises(AirflowException, match="0 NodeManager"):
        judge("check_workers_available", measured)


def test_healthy_nodes_are_counted_and_named():
    measured = dag_module.probe_workers(FakeYarn(nodes=[node("nodemanager1:35139")]))
    assert measured["running_nodes"] == 1
    assert measured["node_ids"] == ["nodemanager1:35139"]
    judge("check_workers_available", measured)


# ── headroom ──────────────────────────────────────────────────────────


def test_headroom_is_judged_per_node_not_on_the_cluster_total():
    # 2 x 512MB free is 1024MB cluster-wide and still cannot host a 1024MB container
    measured = dag_module.probe_headroom(
        FakeYarn(
            nodes=[node("nm1", mb=512, vcores=1), node("nm2", mb=512, vcores=1)],
            metrics={"availableMB": 1024, "allocatedMB": 7168, "availableVirtualCores": 2},
        )
    )
    with pytest.raises(AirflowException) as failure:
        judge("check_worker_resources", measured)
    assert "best offer is 512MB" in str(failure.value)


def test_sufficient_headroom_reports_both_the_total_and_the_best_node():
    measured = dag_module.probe_headroom(
        FakeYarn(
            nodes=[node("nm1", mb=5120, vcores=6)],
            metrics={"availableMB": 5120, "allocatedMB": 3072, "availableVirtualCores": 6},
        )
    )
    assert measured["cluster_available_mb"] == 5120
    assert measured["largest_node_available_mb"] == 5120
    judge("check_worker_resources", measured)


# ── application state ─────────────────────────────────────────────────


def test_accepted_application_does_not_count_as_running():
    measured = dag_module.probe_spark_apps(FakeYarn(apps=[app("application_1_0001", "ACCEPTED")]))
    assert measured["running"] == []
    assert run_task("branch_on_job_state", measured) == dag_module.JOB_NOT_RUNNING_TASK


def test_a_running_application_takes_the_running_branch():
    measured = dag_module.probe_spark_apps(FakeYarn(apps=[app("application_1_0001")]))
    assert run_task("branch_on_job_state", measured) == dag_module.JOB_RUNNING_TASK


def test_the_job_status_check_has_no_predicate():
    # it is a pure reading; the branch downstream does the judging
    assert check("check_spark_job_status").predicate is None


def test_missing_job_points_at_the_command_that_starts_it():
    measured = dag_module.probe_spark_apps(FakeYarn(apps=[]))
    with pytest.raises(AirflowException) as failure:
        run_task(dag_module.JOB_NOT_RUNNING_TASK, measured)
    assert "make run-yarn" in str(failure.value)
    assert "make run-local" in str(failure.value)


def test_a_parked_application_is_named_in_the_failure():
    measured = dag_module.probe_spark_apps(FakeYarn(apps=[app("application_1_0007", "ACCEPTED")]))
    with pytest.raises(AirflowException, match="application_1_0007"):
        run_task(dag_module.JOB_NOT_RUNNING_TASK, measured)


def test_applications_of_another_name_are_ignored():
    measured = dag_module.probe_spark_apps(
        FakeYarn(apps=[app("application_1_0002", name="someone-elses-job")])
    )
    assert measured["apps"] == []


# ── concurrency ───────────────────────────────────────────────────────


def test_two_running_jobs_under_one_name_fail():
    measured = dag_module.probe_spark_apps(
        FakeYarn(apps=[app("application_1_0001"), app("application_1_0002")])
    )
    with pytest.raises(AirflowException, match="2 applications"):
        run_task("check_no_concurrent_jobs", measured)


def test_one_running_job_is_the_expected_state():
    measured = dag_module.probe_spark_apps(FakeYarn(apps=[app("application_1_0001")]))
    assert run_task("check_no_concurrent_jobs", measured) == {"running_count": 1}


# ── the output half ───────────────────────────────────────────────────


def _row(scanned_rows=1000, staleness=2.0, limit=10.0) -> dict:
    return {
        "scanned_rows": scanned_rows,
        "staleness_minutes": staleness,
        "limit_minutes": limit,
    }


def test_recent_write_is_fresh():
    assert dag_module.is_fresh(_row(staleness=2.0))


def test_a_write_exactly_at_the_limit_is_still_fresh():
    assert dag_module.is_fresh(_row(staleness=10.0, limit=10.0))


def test_a_stale_write_is_not_fresh():
    assert not dag_module.is_fresh(_row(staleness=42.0))


def test_an_empty_window_is_not_fresh():
    # MAX() over no rows is NULL, so staleness cannot be compared — absence is the answer
    assert not dag_module.is_fresh(_row(scanned_rows=0, staleness=None))


def test_the_freshness_failure_carries_every_measured_number():
    message = check("check_fact_freshness").message
    assert message.format(**_row(scanned_rows=0, staleness=None, limit=10.0)).startswith(
        "fact_event: 0 row(s) in the scanned window, last write None minutes ago (limit 10.0)"
    )


def test_the_output_checks_run_behind_the_job_branch():
    # a dead job already fails job_not_running; these must skip, not add two more failures
    assert check("check_fact_freshness").upstream_task_ids == {dag_module.JOB_RUNNING_TASK}
    assert check("check_row_growth").upstream_task_ids == {"check_fact_freshness"}


def test_row_growth_does_not_retry():
    # a retry would overwrite the mark and shrink the window the rate is measured over
    assert check("check_row_growth").retries == 0


def test_row_growth_ignores_a_window_left_behind_by_an_outage():
    assert check("check_row_growth").max_interval_seconds == 3600


# ── the two traps ─────────────────────────────────────────────────────


def test_freshness_is_measured_on_the_warehouse_clock():
    assert "MAX(ingested_at)" in dag_module.FRESHNESS_SQL


def test_nothing_compares_against_todays_date():
    # the generator replays ~30 hours behind and never catches up with the wall clock
    assert "CURRENT_DATE" not in DAG_SOURCE


def test_the_staleness_limit_is_read_from_the_config_variable_at_run_time():
    assert dag_module.SETTINGS.template("max_staleness_minutes") in dag_module.FRESHNESS_SQL


def test_the_freshness_scan_is_bounded_by_the_primary_key():
    # unbounded, MAX(ingested_at) is a parallel seq scan: 7.5s at 10M rows and growing
    assert "event_key > (SELECT MAX(event_key) FROM fact_event)" in dag_module.FRESHNESS_SQL


# ── the run digest ────────────────────────────────────────────────────


def test_the_mark_committer_waits_on_every_leaf():
    """ALL_DONE fires as soon as the listed upstreams settle, so a leaf left off the list
    could still be running when the marks are committed and the digest built."""
    report = check("report_run")
    assert report.upstream_task_ids == {"commit_run_marks"}
    assert check("commit_run_marks").upstream_task_ids == {
        "check_workers_available",
        "check_worker_resources",
        "check_no_concurrent_jobs",
        dag_module.JOB_NOT_RUNNING_TASK,
        "check_row_growth",
    }
    assert report.trigger_rule == "all_done"
    assert check("commit_run_marks").trigger_rule == "all_done"


def test_only_the_report_alerts():
    # one incident trips more than one check here, so the checks stay quiet and the digest
    # speaks once; the report keeps the callback so an undeliverable digest is not silent
    dag = dag_module.spark_health_monitor()
    assert dag.get_task("report_run").on_failure_callback is not None
    assert dag.get_task("check_workers_available").on_failure_callback in (None, [])


def test_a_missing_job_records_its_reason_for_the_digest():
    measured = dag_module.probe_spark_apps(FakeYarn(apps=[]))
    context = _context()
    with pytest.raises(AirflowException):
        run_task(dag_module.JOB_NOT_RUNNING_TASK, measured, context)
    assert "make run-yarn" in context["ti"].pushed["measured"]["error"]


def test_the_branch_records_which_way_it_went():
    measured = dag_module.probe_spark_apps(FakeYarn(apps=[app("application_1_0001")]))
    context = _context()
    run_task("branch_on_job_state", measured, context)
    assert context["ti"].pushed["measured"] == {"branch": dag_module.JOB_RUNNING_TASK}
