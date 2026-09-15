"""Turn two observations into a per-minute rate; the earlier mark lives in a Variable."""
import time
from collections.abc import Callable

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import BaseOperator

from dec.callbacks.report import fail, record
from dec.marks import read_mark, stage_mark

# Templated, so a threshold reaches execute() as text.
Threshold = float | str | None


def rate_per_minute(previous: dict, current: dict) -> float:
    elapsed = current["at"] - previous["at"]
    if elapsed <= 0:
        raise ValueError(f"non-positive interval between marks: {elapsed}s")
    return (current["value"] - previous["value"]) * 60.0 / elapsed


class RateCheckOperator(BaseOperator):
    """Measures a monotonic counter against the previous mark, skipping a meaningless rate."""

    # Config, so it is templated and the run's real threshold shows in Rendered Template.
    # `measure` stays a callable: it is logic, not a value.
    template_fields = ("state_variable", "state_key", "min_rate", "max_rate")
    ui_color = "#e8f4f8"

    def __init__(
        self,
        *,
        state_variable: str,
        state_key: str,
        measure: Callable[[], float],
        min_rate: Threshold = None,
        max_rate: Threshold = None,
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

    def _threshold(self, value: Threshold, name: str) -> float | None:
        """A rendered field arrives as text; an unset one as None or an empty string."""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            # Jinja renders whatever the Variable holds, so say which key to go and fix.
            raise AirflowException(
                f"{name} rendered to {value!r}, which is not a number — fix the key it "
                f"reads in the DAG's config Variable"
            ) from None

    def execute(self, context) -> dict:
        current = {"at": time.time(), "value": float(self.measure())}
        previous = read_mark(self.state_variable, self.state_key)
        # Staged before any raise, so one failure does not blind the next run.
        stage_mark(context, self.state_key, current)

        if previous is None:
            record(context, {"value": current["value"], "error": "first mark stored"})
            raise AirflowSkipException(
                f"no earlier mark under {self.state_key!r} yet — measured {current['value']:.0f}"
            )

        if current["value"] < previous["value"]:
            record(context, {"value": current["value"], "error": "counter reset, no rate this run"})
            raise AirflowSkipException(
                f"counter moved backwards ({previous['value']:.0f} to {current['value']:.0f}) "
                "— treating as a reset, not a rate"
            )

        elapsed = current["at"] - previous["at"]
        if elapsed < self.min_interval_seconds:
            # Too little has happened for the quotient to be a rate rather than noise.
            record(context, {"value": current["value"], "error": "window too short"})
            raise AirflowSkipException(
                f"only {elapsed:.0f}s since the last mark, under the "
                f"{self.min_interval_seconds:.0f}s a rate needs to mean anything"
            )
        if self.max_interval_seconds is not None and elapsed > self.max_interval_seconds:
            # The mark predates a gap, so the average would read as a slowdown.
            record(context, {"value": current["value"], "error": "window too long"})
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
            "interval_seconds": round(elapsed, 1),
        }
        window = f"{rate:.0f}/min over the last {result['interval_seconds']:.0f}s"
        minimum = self._threshold(self.min_rate, "min_rate")
        maximum = self._threshold(self.max_rate, "max_rate")
        if minimum is not None and rate < minimum:
            fail(context, result, f"{self.state_key}: {window}, below the {minimum:.0f}/min floor")
        if maximum is not None and rate > maximum:
            fail(
                context,
                result,
                f"{self.state_key}: {window} "
                f"({result['value'] - result['previous_value']:.0f} new), "
                f"above the {maximum:.0f}/min ceiling",
            )
        self.log.info("rate: %s", result)
        return record(context, result)
