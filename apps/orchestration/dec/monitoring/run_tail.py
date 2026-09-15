"""The three tasks every monitoring DAG ends with."""
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule

from dec.callbacks.alert import notify_failure
from dec.callbacks.report import fail_run_if_checks_failed, send_run_report
from dec.marks import commit_marks


def add_run_tail(leaves: list, *, state_variable: str) -> None:
    """`leaves` must name every leaf: ALL_DONE fires once the listed upstreams settle."""

    @task(task_id="commit_run_marks", trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def commit_run_marks(**context) -> dict:
        """The single writer for this DAG's state Variable."""
        return commit_marks(context, state_variable)

    @task(
        task_id="report_run",
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
        on_failure_callback=notify_failure,
    )
    def report_run(**context) -> str:
        """Keeps the failure callback, so an undeliverable digest is not a silent run."""
        return send_run_report(context)

    @task(task_id="mark_run_state", trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def mark_run_state(**context) -> None:
        """Colours the run red; the digest has already notified."""
        fail_run_if_checks_failed(context)

    commit = commit_run_marks()
    # Marks commit before the digest: a failed run still owes the next one a baseline.
    leaves >> commit
    commit >> report_run() >> mark_run_state()
