"""Derive event_timestamp/report_date/hour, country_domain, browser/os/device_category, email_hash."""

from pyspark.sql.functions import (
    col,
    hour,
    lit,
    regexp_extract,
    sha2,
    to_date,
    to_timestamp,
    udf,
    when,
)
from pyspark.sql.types import StringType, StructField, StructType
from user_agents import parse as parse_user_agent

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
    """local_time is a naive UTC string — the process needs BOTH `TZ=UTC` in its environment
    (before the JVM starts) AND `spark.sql.session.timeZone=UTC` on the session, or timestamps
    get shifted by the host's local offset on collect()/JDBC read-write, not just SQL functions.

    Drops rows whose local_time fails to parse (event_timestamp would be null, which
    fact_event's NOT NULL report_date can't accept).
    """
    df = (
        df.withColumn("event_timestamp", to_timestamp(col("local_time"), "yyyy-MM-dd HH:mm:ss"))
        .filter(col("event_timestamp").isNotNull())
        .withColumn("report_date", to_date(col("event_timestamp")))
        .withColumn("hour", hour(col("event_timestamp")))
    )

    # Last TLD segment: glamira.cl -> "cl", glamira.com.br -> "br", glamira.com -> "com".
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
