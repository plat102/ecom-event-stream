"""Default arguments every monitoring DAG shares."""
from datetime import timedelta

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    # No alert callback: one incident trips several checks, so only the digest speaks.
    "depends_on_past": False,
}
