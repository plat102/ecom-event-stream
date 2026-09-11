"""Turn two observations into a per-minute rate. Throughput and processing rate are
derivatives, so the earlier mark must outlive the process that took it — hence the Variable.
"""
import time
from collections.abc import Callable

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import BaseOperator

from marks import read_mark, stage_mark


def rate_per_minute(previous: dict, current: dict) -> float:
    elapsed = current["at"] - previous["at"]
    if elapsed <= 0:
        raise ValueError(f"non-positive interval between marks: {elapsed}s")
    return (current["value"] - previous["value"]) * 60.0 / elapsed


class RateCheckOperator(BaseOperator):
    """Measures a monotonic counter, stores the mark, compares it with the previous one.
    The first run skips, and so does a counter that moved backwards — that rate would be
    meaningless, not low. Thresholds are templated so a Variable supplies them at run time.

    A window outside [`min_interval_seconds`, `max_interval_seconds`] skips too: a run
    triggered by hand seconds after a scheduled one divides a handful of messages by a handful
    of seconds, and a mark left behind by an outage divides one interval's work by hours.
    """

    template_fields = ("min_rate", "max_rate")

    def __init__(
        self,
        *,
        state_variable: str,
        state_key: str,
        measure: Callable[[], float],
        min_rate: float | str | None = None,
        max_rate: float | str | None = None,
        min_interval_seconds: float = 60.0,
        max_interval_seconds: float | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.state_variable = state_variable
        self.state_key = state_key
        self.measure = measure
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.min_interval_seconds = min_interval_seconds
        self.max_interval_seconds = max_interval_seconds

    @staticmethod
    def _threshold(value) -> float | None:
        """A rendered template arrives as a string, an unset Variable as an empty one."""
        return None if value is None or value == "" else float(value)

    def _record(self, context, measured: dict) -> None:
        """The run report reads this key, so a failed check still contributes its numbers."""
        context["ti"].xcom_push(key="measured", value=measured)

    def execute(self, context) -> dict:
        current = {"at": time.time(), "value": float(self.measure())}
        previous = read_mark(self.state_variable, self.state_key)
        # Staged before any raise, so one failure does not blind the next run.
        stage_mark(context, self.state_key, current)

        if previous is None:
            self._record(context, {"value": current["value"], "error": "first mark stored"})
            raise AirflowSkipException(
                f"no earlier mark under {self.state_key!r} yet — measured {current['value']:.0f}"
            )

        if current["value"] < previous["value"]:
            self._record(
                context,
                {"value": current["value"], "error": "counter reset, no rate this run"},
            )
            raise AirflowSkipException(
                f"counter moved backwards ({previous['value']:.0f} to {current['value']:.0f}) "
                "— treating as a reset, not a rate"
            )

        elapsed = current["at"] - previous["at"]
        if elapsed < self.min_interval_seconds:
            self._record(context, {"value": current["value"], "error": "window too short"})
            raise AirflowSkipException(
                f"only {elapsed:.0f}s since the last mark, under the "
                f"{self.min_interval_seconds:.0f}s a rate needs to mean anything"
            )
        if self.max_interval_seconds is not None and elapsed > self.max_interval_seconds:
            # The task was skipped or the DAG was down in between, so the mark predates the
            # gap. Averaging one interval's work over that window reads as a slowdown.
            self._record(context, {"value": current["value"], "error": "window too long"})
            raise AirflowSkipException(
                f"{elapsed:.0f}s since the last mark, over the "
                f"{self.max_interval_seconds:.0f}s this rate is comparable across — the mark "
                "predates a gap, not a slow run"
            )

        rate = rate_per_minute(previous, current)
        result = {
            "rate_per_minute": round(rate, 2),
            "value": current["value"],
            "previous_value": previous["value"],
            "interval_seconds": round(current["at"] - previous["at"], 1),
        }
        window = f"{rate:.0f}/min over the last {result['interval_seconds']:.0f}s"
        minimum, maximum = self._threshold(self.min_rate), self._threshold(self.max_rate)
        if minimum is not None and rate < minimum:
            message = f"{self.state_key}: {window}, below the {minimum:.0f}/min floor"
            self._record(context, {**result, "error": message})
            raise AirflowException(message)
        if maximum is not None and rate > maximum:
            message = (
                f"{self.state_key}: {window} "
                f"({result['value'] - result['previous_value']:.0f} new), "
                f"above the {maximum:.0f}/min ceiling"
            )
            self._record(context, {**result, "error": message})
            raise AirflowException(message)
        self._record(context, result)
        self.log.info("rate: %s", result)
        return result
