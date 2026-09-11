"""One digest per DAG run, instead of one alert per failed check — a single incident trips
several checks at once. Failed checks keep their log links here, so nothing is lost.
"""
from collections import Counter

# Discord rejects longer messages, and the hook raises before it ever sends one.
MESSAGE_LIMIT = 2000
FAILED_STATES = ("failed", "upstream_failed")
# Kept out of the digest: the counter behind a rate says nothing that the rate does not.
NOISY_KEYS = ("value", "previous_value", "error")
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
            reason = (row.get("measured") or {}).get("error") or "no reason recorded"
            head.append(f"• `{row['task_id']}` — {reason}")
            if row.get("log_url"):
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
