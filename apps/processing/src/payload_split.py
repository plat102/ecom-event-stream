"""Pack event-specific fields (not already typed columns) into a JSONB payload."""

from pyspark.sql.functions import col, from_json, struct, to_json

from shared.schemas.event_spark import CART_SCHEMA, OPTION_SCHEMA


def split_payload(df):
    """ignoreNullFields=true keeps payload small and avoids crashing Postgres's generated
    columns on a field that's absent for a given event category (e.g. cart_item_count on
    a browse event, which never has cart_products).
    """
    df = df.withColumn("option_parsed", from_json(col("option"), OPTION_SCHEMA)).withColumn(
        "cart_products_parsed", from_json(col("cart_products"), CART_SCHEMA)
    )

    df = df.withColumn(
        "payload",
        to_json(
            struct(
                col("option_parsed").alias("option"),
                col("cart_products_parsed").alias("cart_products"),
                col("order_id"),
                col("is_paypal"),
                col("key_search"),
                col("recommendation_product_id"),
                col("recommendation_clicked_position"),
                col("price"),
                col("currency"),
                col("cat_id"),
            ),
            {"ignoreNullFields": "true"},
        ),
    )
    return df.drop("option_parsed", "cart_products_parsed")
