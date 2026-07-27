"""Broadcast-join surrogate keys from the static dimensions onto the event stream."""

from pyspark.sql.functions import broadcast, col

from shared.config.settings import settings
from shared.connectors.postgres import jdbc_properties, jdbc_url

STATIC_TABLES = ("dim_date", "dim_site", "dim_device", "ip_locations", "dim_location")


def load_static_dims(spark):
    """Load once when the job starts — none of these change for the job's lifetime."""
    url, props = jdbc_url(settings), jdbc_properties(settings)
    return {
        table: spark.read.jdbc(url=url, table=table, properties=props)
        for table in STATIC_TABLES
    }


def _lookup_location(df, ip_locations, dim_location):
    """Resolve location_key in two hops: ip -> (country, region, city) -> location_key.

    Two hops because dim_location's grain is the deduped geo combination, not the ip.
    """

    geo = ip_locations.select(
        col("ip").alias("_geo_ip"),
        col("country").alias("_geo_country"),
        col("region").alias("_geo_region"),
        col("city").alias("_geo_city"),
    )
    df = df.join(broadcast(geo), df.ip == col("_geo_ip"), "left")

    keys = dim_location.select("country_name", "region_name", "city_name", "location_key")
    matches = (
        col("_geo_ip").isNotNull()
        & col("_geo_country").eqNullSafe(keys.country_name)
        & col("_geo_region").eqNullSafe(keys.region_name)
        & col("_geo_city").eqNullSafe(keys.city_name)
    )

    return df.join(broadcast(keys), matches, "left").drop(
        "_geo_ip", "_geo_country", "_geo_region", "_geo_city",
        "country_name", "region_name", "city_name",
    )


def lookup_static_dims(df, dims):
    """Left joins — a miss leaves the surrogate key NULL rather than dropping the event.

    `dim_site`/`dim_device` must have an "unknown" seed row so events with
    country_domain/browser/os/device_category = "unknown" still resolve a key.
    """
    dim_date = dims["dim_date"]
    joined = (
        df.join(
            broadcast(dim_date.select("full_date", "date_key")),
            df.report_date == dim_date.full_date,
            "left",
        )
        .join(
            broadcast(dims["dim_site"].select("country_domain", "site_key")),
            on="country_domain",
            how="left",
        )
        .join(
            broadcast(dims["dim_device"].select("browser", "os", "device_category", "device_key")),
            on=["browser", "os", "device_category"],
            how="left",
        )
    )
    return _lookup_location(joined, dims["ip_locations"], dims["dim_location"])
