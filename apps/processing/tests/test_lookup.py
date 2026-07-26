"""
Unit tests for DimLookup — uses seed DataFrames, no live Postgres needed.
"""
import datetime

from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

from lookup import lookup_static_dims

EVENT_SCHEMA = StructType([
    StructField("report_date", DateType()),
    StructField("country_domain", StringType()),
    StructField("browser", StringType()),
    StructField("os", StringType()),
    StructField("device_category", StringType()),
])


def _event(spark, row):
    return spark.createDataFrame([row], EVENT_SCHEMA)


def _dim_date(spark):
    schema = StructType([
        StructField("date_key", IntegerType()),
        StructField("full_date", DateType()),
    ])
    return spark.createDataFrame([(20260714, datetime.date(2026, 7, 14))], schema)


def _dim_site(spark):
    # "unknown" mirrors the seed row required in the DDL migration script
    schema = StructType([
        StructField("country_domain", StringType()),
        StructField("site_key", IntegerType()),
    ])
    return spark.createDataFrame([("cl", 1), ("unknown", 99)], schema)


def _dim_device(spark):
    # "unknown"/"unknown"/"unknown" mirrors the seed row required in the DDL migration script
    schema = StructType([
        StructField("browser", StringType()),
        StructField("os", StringType()),
        StructField("device_category", StringType()),
        StructField("device_key", IntegerType()),
    ])
    return spark.createDataFrame(
        [("Chrome", "Windows", "Desktop", 1), ("unknown", "unknown", "unknown", 99)],
        schema,
    )


def _lookup(spark, row):
    df = _event(spark, row)
    return lookup_static_dims(df, _dim_date(spark), _dim_site(spark), _dim_device(spark)).collect()[0]


def test_date_key_resolved(spark):
    result = _lookup(spark, (datetime.date(2026, 7, 14), "cl", "Chrome", "Windows", "Desktop"))

    assert result.date_key == 20260714


def test_site_key_resolved(spark):
    result = _lookup(spark, (datetime.date(2026, 7, 14), "cl", "Chrome", "Windows", "Desktop"))

    assert result.site_key == 1


def test_unknown_country_domain_resolves_seeded_unknown_row(spark):
    result = _lookup(spark, (datetime.date(2026, 7, 14), "unknown", "Chrome", "Windows", "Desktop"))

    assert result.site_key == 99


def test_device_key_resolved(spark):
    result = _lookup(spark, (datetime.date(2026, 7, 14), "cl", "Chrome", "Windows", "Desktop"))

    assert result.device_key == 1


def test_unknown_device_combo_resolves_seeded_unknown_row(spark):
    result = _lookup(spark, (datetime.date(2026, 7, 14), "cl", "unknown", "unknown", "unknown"))

    assert result.device_key == 99


def test_no_match_leaves_key_null_instead_of_dropping_row(spark):
    result = _lookup(spark, (datetime.date(2026, 7, 14), "de", "Chrome", "Windows", "Desktop"))  # "de" not seeded

    assert result.site_key is None
