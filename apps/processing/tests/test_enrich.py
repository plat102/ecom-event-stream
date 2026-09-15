"""
Unit tests for Enricher.
"""

import hashlib

from pyspark.sql.types import LongType, StringType, StructField, StructType

from enrich import enrich

# Explicit schema — some tests pass None for every row of a column (e.g. current_url),
# which Spark's type inference can't handle on its own.
ROW_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("collection", StringType()),
        StructField("local_time", StringType()),
        StructField("time_stamp", LongType()),
        StructField("current_url", StringType()),
        StructField("user_agent", StringType()),
        StructField("email_address", StringType()),
    ]
)

# The two time fields describe the same instant: 1784036380 is 2026-07-14 13:39:40 UTC, and
# local_time is that rendered on the source generator's UTC+7 clock.
BASE_ROW = {
    "id": "abc-123",
    "collection": "view_product_detail",
    "local_time": "2026-07-14 20:39:40",
    "time_stamp": 1784036380,
    "current_url": "https://www.glamira.cl/bratara/585-aur-galben/",
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
    "email_address": None,
}

MOBILE_UA = (
    "Mozilla/5.0 (Linux; Android 9; CLT-L29) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/81.0.4044.138 Mobile Safari/537.36"
)


def _to_df(spark, rows):
    return spark.createDataFrame(rows, ROW_SCHEMA)


def test_event_timestamp_comes_from_the_epoch_not_local_time(spark):
    """local_time reads 20:39:40 but the instant is 13:39:40 UTC. Taking local_time at face
    value put every timestamp 7 hours late."""
    df = _to_df(spark, [BASE_ROW])
    row = enrich(df).collect()[0]

    assert str(row.event_timestamp) == "2026-07-14 13:39:40"
    assert str(row.report_date) == "2026-07-14"
    assert row.hour == 13


def test_local_time_fallback_when_epoch_missing(spark):
    """~5 in 640k events arrive without time_stamp; local_time still resolves them, converted
    out of the generator's UTC+7 clock rather than read as UTC."""
    df = _to_df(spark, [{**BASE_ROW, "time_stamp": None}])
    row = enrich(df).collect()[0]

    assert str(row.event_timestamp) == "2026-07-14 13:39:40"
    assert row.hour == 13


def test_epoch_wins_when_the_two_time_fields_disagree(spark):
    """A source that changes its clock must not silently move event_timestamp."""
    df = _to_df(spark, [{**BASE_ROW, "local_time": "2020-01-01 00:00:00"}])

    assert str(enrich(df).collect()[0].event_timestamp) == "2026-07-14 13:39:40"


def test_row_dropped_only_when_both_time_fields_are_unusable(spark):
    usable = {**BASE_ROW, "local_time": "not-a-timestamp"}  # epoch still present
    unusable = {**BASE_ROW, "local_time": "not-a-timestamp", "time_stamp": None}

    assert enrich(_to_df(spark, [usable])).count() == 1
    assert enrich(_to_df(spark, [unusable])).count() == 0


def test_country_domain_simple_tld(spark):
    df = _to_df(spark, [BASE_ROW])
    result = enrich(df).collect()[0]

    assert result.country_domain == "cl"


def test_country_domain_compound_tld_takes_last_segment(spark):
    row = {**BASE_ROW, "current_url": "https://glamira.com.br/foo"}
    df = _to_df(spark, [row])
    result = enrich(df).collect()[0]

    assert result.country_domain == "br"


def test_country_domain_null_url_is_unknown(spark):
    row = {**BASE_ROW, "current_url": None}
    df = _to_df(spark, [row])
    result = enrich(df).collect()[0]

    assert result.country_domain == "unknown"


def test_country_domain_malformed_url_is_unknown(spark):
    row = {**BASE_ROW, "current_url": "not-a-url"}
    df = _to_df(spark, [row])
    result = enrich(df).collect()[0]

    assert result.country_domain == "unknown"


def test_user_agent_parsed_for_mobile(spark):
    row = {**BASE_ROW, "user_agent": MOBILE_UA}
    df = _to_df(spark, [row])
    result = enrich(df).collect()[0]

    assert result.browser == "Chrome Mobile"
    assert result.os == "Android"
    assert result.device_category == "Mobile"


def test_user_agent_parsed_for_desktop(spark):
    df = _to_df(spark, [BASE_ROW])
    result = enrich(df).collect()[0]

    assert result.browser == "Chrome"
    assert result.os == "Windows"
    assert result.device_category == "Desktop"


def test_null_user_agent_is_unknown(spark):
    row = {**BASE_ROW, "user_agent": None}
    df = _to_df(spark, [row])
    result = enrich(df).collect()[0]

    assert result.browser == "unknown"
    assert result.os == "unknown"
    assert result.device_category == "unknown"


def test_email_hash_computed_when_present(spark):
    row = {**BASE_ROW, "email_address": "user@example.com"}
    df = _to_df(spark, [row])
    result = enrich(df).collect()[0]

    assert result.email_hash == hashlib.sha256(b"user@example.com").hexdigest()


def test_email_hash_null_when_absent(spark):
    df = _to_df(spark, [BASE_ROW])  # email_address is None
    result = enrich(df).collect()[0]

    assert result.email_hash is None


def test_email_hash_null_when_empty_string(spark):
    row = {**BASE_ROW, "email_address": ""}
    df = _to_df(spark, [row])
    result = enrich(df).collect()[0]

    assert result.email_hash is None


def test_event_type_renamed_from_collection(spark):
    df = _to_df(spark, [BASE_ROW])
    result = enrich(df).collect()[0]

    assert result.event_type == "view_product_detail"
    assert "collection" not in result.asDict()
