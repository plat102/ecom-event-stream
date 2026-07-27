"""Upsert dim_product and dim_device on first sight, then re-attach their surrogate keys."""
from pyspark.sql.functions import broadcast, col
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min

from shared.config.settings import settings
from shared.connectors.postgres import PostgresClient, jdbc_properties, jdbc_url

_UPSERT_SQL = """
    INSERT INTO dim_product (product_id, first_seen_at, last_seen_at)
    SELECT product_id, first_seen_at, last_seen_at FROM stg_product
    ON CONFLICT (product_id) DO UPDATE
        SET first_seen_at = LEAST(dim_product.first_seen_at, EXCLUDED.first_seen_at),
            last_seen_at  = GREATEST(dim_product.last_seen_at, EXCLUDED.last_seen_at);
"""

_UPSERT_DEVICE_SQL = """
    INSERT INTO dim_device (browser, os, device_category, is_mobile)
    SELECT v.browser, v.os, v.device_category, v.device_category = 'Mobile'
    FROM (VALUES %s) AS v (browser, os, device_category)
    ON CONFLICT (browser, os, device_category) DO NOTHING;
"""


def upsert_dim_product(batch_df):
    """LEAST/GREATEST keep first_seen_at/last_seen_at correct across late-arriving events.
    Both ignore NULL arguments in Postgres (unlike most functions, which would propagate
    NULL) — so this also does the right thing when the row already exists but with a NULL
    timestamp (catalog-seeded, never actually observed before): the real event's value wins
    without needing an explicit NULL guard.

    `stg_product` is scratch space, not a dimension — Spark drops and recreates it
    every call (mode="overwrite"), it never gets a `dim_` prefix.
    """
    new_products = (
        batch_df.filter(col("product_id").isNotNull())
        .select("product_id", "event_timestamp")
        .groupBy("product_id")
        .agg(
            spark_min("event_timestamp").alias("first_seen_at"),
            spark_max("event_timestamp").alias("last_seen_at"),
        )
    )
    new_products.write.jdbc(
        url=jdbc_url(settings),
        table="stg_product",
        mode="overwrite",
        properties=jdbc_properties(settings),
    )

    with PostgresClient(settings) as pg:
        pg.execute(_UPSERT_SQL)


def upsert_dim_device(batch_df):
    """is_mobile derives from device_category — exactly how enrich.py's UA parser sets it."""
    combos = (
        batch_df.select("browser", "os", "device_category")
        # all three are NOT NULL; a null triple means the UA parser threw
        .filter(
            col("browser").isNotNull()
            & col("os").isNotNull()
            & col("device_category").isNotNull()
        )
        .distinct()
        .collect()
    )
    if not combos:
        return
    with PostgresClient(settings) as pg:
        pg.execute_values(_UPSERT_DEVICE_SQL, [tuple(row) for row in combos])


def relookup_dynamic_dims(batch_df, spark):
    """Re-read right after upsert so a value first seen in this batch still gets its key in the
    same batch. A miss under concurrent batches is acceptable — the next batch catches up.
    """
    url, props = jdbc_url(settings), jdbc_properties(settings)
    dim_product = spark.read.jdbc(url=url, table="dim_product", properties=props).select(
        "product_id", "product_key"
    )

    dim_device = spark.read.jdbc(url=url, table="dim_device", properties=props).select(
        "browser", "os", "device_category", "device_key"
    )
    return batch_df.join(broadcast(dim_product), on="product_id", how="left").join(
        broadcast(dim_device), on=["browser", "os", "device_category"], how="left"
    )
