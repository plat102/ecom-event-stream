"""Default arguments every monitoring DAG shares."""
from datetime import timedelta

from callbacks.alert import notify_failure

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    # Runs only after the last retry is spent, which is what makes it different from an
    # alert task inside the graph.
    "on_failure_callback": notify_failure,
    "depends_on_past": False,
}
