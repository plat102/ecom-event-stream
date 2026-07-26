"""Upsert dim_product on first sight of a product_id, then re-attach product_key."""
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


def relookup_dynamic_dims(batch_df, spark):
    """Re-read dim_product right after upsert so a product seen for the first time in this
    batch still gets a product_key in the same batch. A miss under concurrent batches is
    acceptable — the next batch's re-lookup catches up.
    """
    dim_product = spark.read.jdbc(
        url=jdbc_url(settings), table="dim_product", properties=jdbc_properties(settings)
    ).select("product_id", "product_key")
    return batch_df.join(broadcast(dim_product), on="product_id", how="left")
