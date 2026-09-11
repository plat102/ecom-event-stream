"""One digest per DAG run instead of one alert per failed check — a single incident trips
several at once. The write side lives here too: a check records what it measured under the
`measured` XCom key *before* raising, so the digest can print numbers for a failed task.
"""
import logging
from collections import Counter

from callbacks.alert import send_to_discord

log = logging.getLogger(__name__)

MEASURED_KEY = "measured"

# Discord rejects longer messages, and the hook raises before it ever sends one.
MESSAGE_LIMIT = 2000
UPSTREAM_FAILED = "upstream_failed"
FAILED_STATES = ("failed", UPSTREAM_FAILED)
# Kept out of the digest: keys a sibling already implies, on a 2000-character budget.
NOISY_KEYS = (
    "value",
    "previous_value",
    "error",
    "apps",
    "per_node",
    "names",
    "expected",
    "present",
)
# Worst first: a reader scanning the top of the message should see the problems.
STATE_ORDER = {"failed": 0, "upstream_failed": 1, "skipped": 2, "success": 3}


def describe(measured: dict | None) -> str:
    if not measured:
        return "no measurement recorded"
    return " · ".join(
        f"{key}={value}" for key, value in measured.items() if key not in NOISY_KEYS
    ) or "no measurement recorded"


def build_report(
    *, dag_id: str, logical_date: str, rows: list[dict], limit: int = MESSAGE_LIMIT
) -> str:
    """One row per task: `task_id`, `state`, optional `measured` and `log_url`. Failures are
    never trimmed, measurements are — a message Discord refuses is worse than a short one."""
    counts = Counter(row.get("state") or "no_state" for row in rows)
    head = [
        f"**{dag_id}** · {logical_date}",
        " · ".join(
            f"{count} {state}"
            for state, count in sorted(counts.items(), key=lambda kv: STATE_ORDER.get(kv[0], 9))
        ),
    ]

    failures = [row for row in rows if row.get("state") in FAILED_STATES]
    if failures:
        head.append("")
        head.append("**failed**")
        for row in sorted(failures, key=lambda r: r["task_id"]):
            blocked = row.get("state") == UPSTREAM_FAILED
            reason = (row.get("measured") or {}).get("error") or (
                "upstream failed, did not run" if blocked else "no reason recorded"
            )
            head.append(f"• `{row['task_id']}` — {reason}")
            # No log link for a task that never ran: the log is empty and the line is ~130
            # characters of the budget.
            if row.get("log_url") and not blocked:
                head.append(f"  {row['log_url']}")

    measured = [
        f"• `{row['task_id']}` — {describe(row.get('measured'))}"
        for row in sorted(rows, key=lambda r: r["task_id"])
        if row.get("state") == "success" and row.get("measured")
    ]

    dropped = 0
    while True:
        tail = ["", "**measured**", *measured] if measured else []
        if dropped:
            tail.append(f"… {dropped} more line(s) trimmed to fit")
        report = "\n".join(head + tail)
        if len(report) <= limit or not measured:
            # A wide outage can fill the budget with failures alone, and a message the
            # webhook refuses is no report at all — so the hard cut is last, not optional.
            return report if len(report) <= limit else report[: limit - 1] + "…"
        measured.pop()
        dropped += 1


def record(context, measured: dict) -> dict:
    """`xcom_push` commits immediately, so this survives a raise on the very next line."""
    context["ti"].xcom_push(key=MEASURED_KEY, value=measured)
    return measured


def fail(context, measured: dict, message: str) -> None:
    """Record the numbers *and* the reason, then fail. Airflow is imported inside so
    `build_report` stays testable without it, as `alert.py` does for the Discord hook."""
    from airflow.exceptions import AirflowException

    record(context, {**measured, "error": message})
    raise AirflowException(message)


def collect_rows(context) -> list[dict]:
    """Every other task in this run, with whatever it measured."""
    task_instance, dag_run = context["ti"], context["dag_run"]
    return [
        {
            "task_id": other.task_id,
            "state": other.state,
            "measured": task_instance.xcom_pull(task_ids=other.task_id, key=MEASURED_KEY),
            "log_url": other.log_url,
        }
        for other in dag_run.get_task_instances()
        # Skip this task and anything still stateless — the run-state marker sits downstream
        # of the report, so it has not started yet and would count as its own category.
        if other.task_id != task_instance.task_id and other.state
    ]


def send_run_report(context) -> str:
    """Sends unguarded: this task carries the failure callback, so an undeliverable digest
    must not pass silently."""
    report = build_report(
        dag_id=context["dag_run"].dag_id,
        logical_date=context["logical_date"].strftime("%Y-%m-%d %H:%M UTC"),
        rows=collect_rows(context),
    )
    log.info("run report:\n%s", report)
    send_to_discord(report)
    return report


def failed_task_ids(context) -> list[str]:
    return sorted(
        row["task_id"] for row in collect_rows(context) if row.get("state") in FAILED_STATES
    )


def fail_run_if_checks_failed(context) -> None:
    """Colour the run red — the digest already said what broke. Without it the report task,
    succeeding on ALL_DONE as the only leaf, makes every run green however many checks failed.
    """
    failed = failed_task_ids(context)
    if failed:
        from airflow.exceptions import AirflowException

        raise AirflowException(
            f"{len(failed)} check(s) failed this run: {failed} — reasons are in the digest"
        )
