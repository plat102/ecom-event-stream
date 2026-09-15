"""The one thing Airflow must discover here; everything else lives in `dec` and is imported."""
from airflow.plugins_manager import AirflowPlugin

from dec.links import YarnResourceManagerLink


class MonitoringPlugin(AirflowPlugin):
    name = "monitoring"
    # Without this the webserver cannot deserialise the link off a task and drops it.
    operator_extra_links = [YarnResourceManagerLink()]
