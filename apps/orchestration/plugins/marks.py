"""Run-to-run marks: the earlier observation a rate or a trend is compared against.

Only a scheduled run moves a mark. A manual trigger or a `dags test` reads the history but
must not disturb it, or the next scheduled window collapses to the seconds between them.
"""
from airflow.models import Variable

SCHEDULED_RUN = "scheduled"


def advance_mark(context, key: str, value: str) -> None:
    if getattr(context.get("dag_run"), "run_type", None) == SCHEDULED_RUN:
        Variable.set(key, value)
