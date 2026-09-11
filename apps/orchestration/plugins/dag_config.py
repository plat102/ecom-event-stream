"""Per-DAG config and state, named by one registry Variable.

`DAG_REGISTRY` maps a dag_id to its cadence and to the Variables holding everything else:
`{"<dag_id>": {"schedule": "*/5 * * * *", "config": "X_CONFIG", "state": "X_STATE"}}`.
Registry is read at parse time (the scheduler needs `schedule` before any task exists);
config and state are read at run time.
"""
import logging
from dataclasses import dataclass

from airflow.models import Variable
from croniter import croniter

log = logging.getLogger(__name__)

REGISTRY_VARIABLE = "DAG_REGISTRY"


@dataclass(frozen=True)
class DagSettings:
    """Where one DAG finds its own configuration. Built once, at parse time."""

    schedule: str
    config_variable: str
    state_variable: str

    def template(self, key: str) -> str:
        """A Jinja reference, so the operator reads the setting at run time."""
        return f"{{{{ var.json.{self.config_variable}.{key} }}}}"

    def value(self, key: str, default=None):
        """The same setting, read inside a task."""
        return self.config().get(key, default)

    def config(self) -> dict:
        return Variable.get(self.config_variable, default_var={}, deserialize_json=True)


def default_names(dag_id: str) -> tuple[str, str]:
    """Names derived from the dag_id, so a DAG still loads on an unseeded database."""
    stem = dag_id.upper()
    return f"{stem}_CONFIG", f"{stem}_STATE"


def for_dag(dag_id: str, *, default_schedule: str) -> DagSettings:
    """Resolve a DAG's settings. Call once at parse time and pass the result around."""
    entry = Variable.get(REGISTRY_VARIABLE, default_var={}, deserialize_json=True).get(dag_id)
    entry = entry or {}
    config_default, state_default = default_names(dag_id)

    schedule = entry.get("schedule") or default_schedule
    if not isinstance(schedule, str) or not croniter.is_valid(schedule):
        # Falling back beats raising: an invalid cron fails the DAG import, and monitoring
        # that does not load is worse than monitoring on the wrong cadence.
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
