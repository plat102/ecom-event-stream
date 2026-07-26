"""Broadcast-join surrogate keys from dim_date/dim_site/dim_device onto the event stream."""

from pyspark.sql.functions import broadcast

from shared.config.settings import settings
from shared.connectors.postgres import jdbc_properties, jdbc_url


def load_static_dims(spark):
    """Load once when the job starts — these 3 dims don't change for the job's lifetime."""
    url, props = jdbc_url(settings), jdbc_properties(settings)
    dim_date = spark.read.jdbc(url=url, table="dim_date", properties=props)
    dim_site = spark.read.jdbc(url=url, table="dim_site", properties=props)
    dim_device = spark.read.jdbc(url=url, table="dim_device", properties=props)
    return dim_date, dim_site, dim_device


def lookup_static_dims(df, dim_date, dim_site, dim_device):
    """Left joins — a miss leaves the surrogate key NULL rather than dropping the event.

    `dim_site`/`dim_device` must have an "unknown" seed row so events with
    country_domain/browser/os/device_category = "unknown" still resolve a key.
    """
    return (
        df.join(
            broadcast(dim_date.select("full_date", "date_key")),
            df.report_date == dim_date.full_date,
            "left",
        )
        .join(
            broadcast(dim_site.select("country_domain", "site_key")),
            on="country_domain",
            how="left",
        )
        .join(
            broadcast(dim_device.select("browser", "os", "device_category", "device_key")),
            on=["browser", "os", "device_category"],
            how="left",
        )
    )
