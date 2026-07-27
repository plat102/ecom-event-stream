"""
Unit tests for DimLookup — uses seed DataFrames, no live Postgres needed.
"""
import datetime

from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from lookup import lookup_static_dims

EVENT_SCHEMA = StructType([
    StructField("report_date", DateType()),
    StructField("country_domain", StringType()),
    StructField("browser", StringType()),
    StructField("os", StringType()),
    StructField("device_category", StringType()),
    StructField("ip", StringType()),
])

DEFAULT_EVENT = (datetime.date(2026, 7, 14), "cl", "Chrome", "Windows", "Desktop", "1.1.1.1")


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


def _ip_locations(spark):
    schema = StructType([
        StructField("ip", StringType()),
        StructField("country", StringType()),
        StructField("region", StringType()),
        StructField("city", StringType()),
    ])
    return spark.createDataFrame(
        [
            ("1.1.1.1", "Chile", "Region Metropolitana", "Santiago"),  # full geo
            ("2.2.2.2", "Germany", None, None),                        # country only
            ("3.3.3.3", None, None, None),                             # looked up, no geo
        ],
        schema,
    )


def _dim_location(spark):
    schema = StructType([
        StructField("country_name", StringType()),
        StructField("region_name", StringType()),
        StructField("city_name", StringType()),
        StructField("location_key", IntegerType()),
        StructField("has_geo_data", BooleanType()),
    ])
    return spark.createDataFrame(
        [
            ("Chile", "Region Metropolitana", "Santiago", 10, True),
            ("Germany", None, None, 20, True),
            (None, None, None, 30, False),  # the "resolved to nothing" combination
        ],
        schema,
    )


def _lookup(spark, row):
    dims = {
        "dim_date": _dim_date(spark),
        "dim_site": _dim_site(spark),
        "ip_locations": _ip_locations(spark),
        "dim_location": _dim_location(spark),
    }
    return lookup_static_dims(_event(spark, row), dims).collect()[0]


def _with(**overrides):
    """DEFAULT_EVENT with named fields replaced."""
    fields = dict(zip(EVENT_SCHEMA.fieldNames(), DEFAULT_EVENT))
    fields.update(overrides)
    return tuple(fields[name] for name in EVENT_SCHEMA.fieldNames())


def test_date_key_resolved(spark):
    assert _lookup(spark, DEFAULT_EVENT).date_key == 20260714


def test_site_key_resolved(spark):
    assert _lookup(spark, DEFAULT_EVENT).site_key == 1


def test_unknown_country_domain_resolves_seeded_unknown_row(spark):
    assert _lookup(spark, _with(country_domain="unknown")).site_key == 99


def test_no_match_leaves_key_null_instead_of_dropping_row(spark):
    assert _lookup(spark, _with(country_domain="de")).site_key is None  # "de" not seeded


def test_location_key_resolved_for_full_geo(spark):
    assert _lookup(spark, DEFAULT_EVENT).location_key == 10


def test_location_key_resolved_when_only_country_is_known(spark):
    """Partial geo: NULLs on both sides, so a plain `=` would miss. Needs eqNullSafe."""
    assert _lookup(spark, _with(ip="2.2.2.2")).location_key == 20


def test_ip_resolved_to_no_geo_gets_the_no_geo_dimension_row(spark):
    assert _lookup(spark, _with(ip="3.3.3.3")).location_key == 30


def test_ip_absent_from_lookup_leaves_location_key_null(spark):
    """Never looked up is not the same as looked up and unknown."""
    assert _lookup(spark, _with(ip="9.9.9.9")).location_key is None


def test_null_ip_leaves_location_key_null(spark):
    assert _lookup(spark, _with(ip=None)).location_key is None


def test_intermediate_geo_columns_are_dropped(spark):
    columns = set(_lookup(spark, DEFAULT_EVENT).asDict())

    assert not columns & {
        "_geo_ip", "_geo_country", "_geo_region", "_geo_city",
        "country_name", "region_name", "city_name",
    }
