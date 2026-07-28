"""Derive event_timestamp/report_date/hour, country_domain, browser/os/device_category, email_hash."""

from pyspark.sql.functions import (
    coalesce,
    col,
    hour,
    lit,
    regexp_extract,
    sha2,
    timestamp_seconds,
    to_date,
    to_timestamp,
    to_utc_timestamp,
    udf,
    when,
)
from pyspark.sql.types import StringType, StructField, StructType
from user_agents import parse as parse_user_agent

# local_time is the source generator's clock, not the visitor's and not UTC
_SOURCE_CLOCK_TZ = "Asia/Ho_Chi_Minh"

_UA_RESULT_SCHEMA = StructType(
    [
        StructField("browser", StringType()),
        StructField("os", StringType()),
        StructField("device_category", StringType()),
    ]
)


@udf(returnType=_UA_RESULT_SCHEMA)
def _parse_ua(user_agent):
    if not user_agent:
        return ("unknown", "unknown", "unknown")
    ua = parse_user_agent(user_agent)
    if ua.is_mobile:
        device_category = "Mobile"
    elif ua.is_tablet:
        device_category = "Tablet"
    elif ua.is_pc:
        device_category = "Desktop"
    else:
        device_category = "unknown"
    return (ua.browser.family or "unknown", ua.os.family or "unknown", device_category)


def enrich(df):
    """Needs BOTH `TZ=UTC` in the environment (before the JVM starts) AND
    `spark.sql.session.timeZone=UTC` on the session, or timestamps shift by the host's offset on
    collect()/JDBC read-write, not just in SQL functions.

    Drops rows where neither time source is usable — report_date is NOT NULL in fact_event.
    """
    df = (
        # time_stamp is an absolute epoch, so it carries no timezone assumption at all.
        # local_time only covers the ~5-in-640k events that arrive without one.
        df.withColumn(
            "event_timestamp",
            coalesce(
                timestamp_seconds(col("time_stamp")),
                to_utc_timestamp(
                    to_timestamp(col("local_time"), "yyyy-MM-dd HH:mm:ss"), _SOURCE_CLOCK_TZ
                ),
            ),
        )
        .filter(col("event_timestamp").isNotNull())
        .withColumn("report_date", to_date(col("event_timestamp")))
        .withColumn("hour", hour(col("event_timestamp")))
    )

    # glamira.cl -> "cl", glamira.com.br -> "br", glamira.com -> "com".
    df = df.withColumn(
        "country_domain",
        regexp_extract(col("current_url"), r"https?://(?:www\.)?[^/]+\.([a-zA-Z]{2,})", 1),
    )
    df = df.withColumn(
        "country_domain",
        when(
            col("country_domain").isNull() | (col("country_domain") == ""), lit("unknown")
        ).otherwise(col("country_domain")),
    )

    ua = _parse_ua(col("user_agent"))
    df = (
        df.withColumn("browser", ua["browser"])
        .withColumn("os", ua["os"])
        .withColumn("device_category", ua["device_category"])
    )

    df = df.withColumn(
        "email_hash",
        when(
            col("email_address").isNotNull() & (col("email_address") != ""),
            sha2(col("email_address"), 256),
        ).otherwise(lit(None)),
    )

    return df.withColumnRenamed("collection", "event_type")
