"""Per-DAG config and state, named by one registry Variable."""
import logging
from dataclasses import dataclass

from airflow.exceptions import AirflowException
from airflow.models import Variable
from croniter import croniter

log = logging.getLogger(__name__)

REGISTRY_VARIABLE = "DAG_REGISTRY"


@dataclass(frozen=True)
class DagSettings:
    """Where one DAG finds its own configuration."""

    schedule: str
    config_variable: str
    state_variable: str

    def value(self, key: str, default=None):
        return self.config().get(key, default)

    def require(self, key: str):
        """One readable error beats the same KeyError raised by every task at once."""
        value = self.value(key)
        if value is None:
            raise AirflowException(
                f"{self.config_variable}.{key} is not set — run `make airflow-seed`"
            )
        return value

    def template(self, key: str) -> str:
        """A Jinja reference to one config key, rendered per task instance."""
        return f"{{{{ var.json.{self.config_variable}.{key} }}}}"

    def config(self) -> dict:
        return Variable.get(self.config_variable, default_var={}, deserialize_json=True)


def default_names(dag_id: str) -> tuple[str, str]:
    """Derived from the dag_id, so a DAG still loads on an unseeded database."""
    stem = dag_id.upper()
    return f"{stem}_CONFIG", f"{stem}_STATE"


def for_dag(dag_id: str, *, default_schedule: str) -> DagSettings:
    """Resolve a DAG's settings once, at parse time."""
    entry = Variable.get(REGISTRY_VARIABLE, default_var={}, deserialize_json=True).get(dag_id)
    entry = entry or {}
    config_default, state_default = default_names(dag_id)

    schedule = entry.get("schedule") or default_schedule
    if not isinstance(schedule, str) or not croniter.is_valid(schedule):
        # Monitoring on the wrong cadence beats monitoring that fails to import.
        log.warning(
            "%s[%s].schedule is not a valid cron (%r) — falling back to %r",
            REGISTRY_VARIABLE,
            dag_id,
            schedule,
            default_schedule,
        )
        schedule = default_schedule

    return DagSettings(
        schedule=schedule,
        config_variable=entry.get("config") or config_default,
        state_variable=entry.get("state") or state_default,
    )
