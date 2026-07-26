"""Defensive NOT-NULL check — the bridge already routes invalid records to DLQ."""

from pyspark.sql.functions import col

from shared.schemas.event import REQUIRED_FIELDS


def validate_events(df):
    """Keep rows where every REQUIRED_FIELDS column is NOT NULL. Does not filter by `collection` value."""
    condition = None
    for field in REQUIRED_FIELDS:
        is_not_null = col(field).isNotNull()
        condition = is_not_null if condition is None else condition & is_not_null
    return df.filter(condition)
