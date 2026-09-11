"""Run-to-run marks: the earlier observation a rate or a trend is compared against.

Two rules. **Only a scheduled run moves a mark** — a manual trigger reads the history but must
not disturb it. **Marks are staged on XCom and committed once per run**, because several tasks
produce marks in parallel and each writing the shared Variable would drop all but the last.
"""
import json
import logging

from airflow.models import Variable

log = logging.getLogger(__name__)

SCHEDULED_RUN = "scheduled"
STAGED_KEY = "staged_marks"


def is_scheduled(context) -> bool:
    return getattr(context.get("dag_run"), "run_type", None) == SCHEDULED_RUN


def read_state(state_variable: str) -> dict:
    return Variable.get(state_variable, default_var={}, deserialize_json=True)


def read_mark(state_variable: str, key: str):
    return read_state(state_variable).get(key)


def stage_mark(context, key: str, value) -> None:
    """Hand a mark to the end-of-run committer, merging with any this task already staged."""
    task_instance = context["ti"]
    staged = dict(task_instance.xcom_pull(task_ids=task_instance.task_id, key=STAGED_KEY) or {})
    staged[key] = value
    task_instance.xcom_push(key=STAGED_KEY, value=staged)


def collect_staged(context) -> dict:
    """Every mark staged by every other task in this run."""
    task_instance, dag_run = context["ti"], context["dag_run"]
    marks: dict = {}
    for other in dag_run.get_task_instances():
        if other.task_id == task_instance.task_id:
            continue
        staged = task_instance.xcom_pull(task_ids=other.task_id, key=STAGED_KEY)
        if staged:
            marks.update(staged)
    return marks


def commit_marks(context, state_variable: str) -> dict:
    """The single writer. Merges, so a check that staged nothing keeps its previous mark."""
    staged = collect_staged(context)
    if not is_scheduled(context):
        log.info("run is not scheduled — %d staged mark(s) read but not committed", len(staged))
        return {}
    if not staged:
        log.info("no marks staged this run, leaving %s untouched", state_variable)
        return {}
    Variable.set(state_variable, json.dumps({**read_state(state_variable), **staged}))
    log.info("committed %d mark(s) to %s: %s", len(staged), state_variable, sorted(staged))
    return staged
