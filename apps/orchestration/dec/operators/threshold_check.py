"""The warehouse check, built on the provider's threshold operator instead of our own."""
from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import SQLThresholdCheckOperator

from dec.callbacks.report import fail, jsonable, record


class ReportingThresholdCheck(SQLThresholdCheckOperator):
    """The provider owns SQL, thresholds and templating; this adds the run digest.

    `message` is formatted with what `push` recorded, so an alert still carries the number.
    """

    ui_color = "#d4edda"

    def __init__(self, *, message: str = "", **kwargs) -> None:
        super().__init__(**kwargs)
        self.message = message
        self._measured: dict = {}
        self._context: dict | None = None

    def push(self, meta_data) -> None:
        """The provider calls this before it raises, which is exactly what the digest needs."""
        self._measured = {
            key: jsonable(value) for key, value in meta_data.items() if key != "task_id"
        }
        record(self._context, self._measured)

    def execute(self, context):
        self._context, self._measured = context, {}
        try:
            return super().execute(context)
        except AirflowException as error:
            fail(context, self._measured, self._describe(error))

    def _describe(self, error: AirflowException) -> str:
        if self._measured and self.message:
            return self.message.format(**self._measured)
        # Zero rows: `push` never ran, and the provider's own message is long and multi-line.
        return f"{self.task_id}: {error}".replace("\n", " ")[:300]
