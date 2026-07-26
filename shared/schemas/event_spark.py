"""
PySpark StructType for the raw Kafka event JSON — mirrors event.py's RawEvent field list.

Fields marked "Any" in RawEvent (mixed bool/str/int in production data) are typed
StringType here: from_json nulls non-string scalars but keeps nested JSON (option,
cart_products) intact as text, which PayloadSplitter (P2.4) parses later.
"""
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

EVENT_SCHEMA = StructType([
    # Always-present fields
    StructField("id", StringType(), nullable=False),
    StructField("collection", StringType(), nullable=False),
    StructField("local_time", StringType(), nullable=False),

    # System
    StructField("_id", StringType(), True),
    StructField("api_version", StringType(), True),
    StructField("collect_id", StringType(), True),
    StructField("time_stamp", LongType(), True),

    # Navigation
    StructField("current_url", StringType(), True),
    StructField("referrer_url", StringType(), True),

    # Identity
    StructField("device_id", StringType(), True),
    StructField("email_address", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("user_id_db", StringType(), True),

    # Context
    StructField("resolution", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("utm_medium", StringType(), True),  # Any: bool or str in raw data
    StructField("utm_source", StringType(), True),  # Any: bool or str in raw data

    # Product (absent in browsing/recommendation events)
    StructField("cat_id", StringType(), True),  # Any
    StructField("currency", StringType(), True),
    StructField("key_search", StringType(), True),  # Any
    StructField("option", StringType(), True),  # raw JSON (object or array), parsed in P2.4
    StructField("price", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("viewing_product_id", StringType(), True),

    # Cart / checkout
    StructField("cart_products", StringType(), True),  # raw JSON array, parsed in P2.4
    StructField("is_paypal", StringType(), True),  # Any
    StructField("order_id", StringType(), True),  # Any: str, int, or float

    # Recommendation
    StructField("recommendation", BooleanType(), True),
    StructField("recommendation_clicked_position", StringType(), True),  # Any
    StructField("recommendation_product_id", StringType(), True),
    StructField("recommendation_product_position", StringType(), True),  # Any: int or str
    StructField("show_recommendation", StringType(), True),
])
