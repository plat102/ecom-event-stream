"""Operator extra links: the page a person opens next after reading an alert."""
from airflow.models import BaseOperatorLink

from dec.hooks.yarn import YarnHook


class YarnResourceManagerLink(BaseOperatorLink):
    """Sends the reader to the cluster the check just questioned."""

    name = "YARN ResourceManager"

    def get_link(self, operator, *, ti_key=None) -> str:
        conn_id = getattr(operator, "conn_id", None) or YarnHook.default_conn_name
        try:
            return YarnHook(conn_id).base_url()
        except Exception:  # noqa: BLE001 — a missing Connection must not break the task page
            return ""
