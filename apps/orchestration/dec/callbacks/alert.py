"""Failure notification shared by every monitoring DAG."""
import logging

log = logging.getLogger(__name__)

DISCORD_CONN_ID = "discord_alert"
# Discord rejects anything longer, and the hook raises before it ever sends.
MESSAGE_LIMIT = 2000
TRUNCATED = " […truncated]"


def failure_message(context) -> str:
    """The log link matters more than the traceback, so it stays outside the fence."""
    task_instance = context.get("task_instance")
    dag_id = getattr(task_instance, "dag_id", None) or "unknown_dag"
    task_id = getattr(task_instance, "task_id", None) or "unknown_task"
    reason = str(context.get("exception") or context.get("reason") or "no reason reported")
    log_url = getattr(task_instance, "log_url", None)

    head = f"**[ALERT] {dag_id} — {task_id}** failed @ {context.get('logical_date')}"
    tail = f"\nlog: {log_url}" if log_url else ""
    # Fenced: Discord reads _ and * as formatting, and exception text is full of both.
    message = f"{head}\n```\n{reason}\n```{tail}"

    overflow = len(message) - MESSAGE_LIMIT
    if overflow > 0:
        keep = max(len(reason) - overflow - len(TRUNCATED), 0)
        message = f"{head}\n```\n{reason[:keep]}{TRUNCATED}\n```{tail}"
    return message


def send_to_discord(text: str) -> None:
    """Raises on failure — callers decide whether that is visible or swallowed."""
    from airflow.providers.discord.hooks.discord_webhook import DiscordWebhookHook

    DiscordWebhookHook(http_conn_id=DISCORD_CONN_ID, message=text).execute()


def notify_failure(context) -> None:
    try:
        send_to_discord(failure_message(context))
    except Exception:
        # A callback that raises turns one failed task into two and buries the reason.
        log.warning("could not deliver the Discord alert", exc_info=True)
