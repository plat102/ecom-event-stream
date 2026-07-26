"""
Unit tests for PayloadSplitter.
"""
import json

from pyspark.sql.types import StringType, StructField, StructType

from payload_split import split_payload

# Explicit schema — most tests leave several of these columns as None across all rows.
ROW_SCHEMA = StructType([
    StructField("option", StringType()),
    StructField("cart_products", StringType()),
    StructField("order_id", StringType()),
    StructField("is_paypal", StringType()),
    StructField("key_search", StringType()),
    StructField("recommendation_product_id", StringType()),
    StructField("recommendation_clicked_position", StringType()),
    StructField("price", StringType()),
    StructField("currency", StringType()),
    StructField("cat_id", StringType()),
])

EMPTY_ROW = {
    "option": None,
    "cart_products": None,
    "order_id": None,
    "is_paypal": None,
    "key_search": None,
    "recommendation_product_id": None,
    "recommendation_clicked_position": None,
    "price": None,
    "currency": None,
    "cat_id": None,
}


def _to_df(spark, rows):
    return spark.createDataFrame(rows, ROW_SCHEMA)


def test_cart_products_parsed_into_payload(spark):
    row = {**EMPTY_ROW, "cart_products": json.dumps([{"product_id": "96878", "qty": "1"}])}
    df = _to_df(spark, [row])
    payload = json.loads(split_payload(df).collect()[0].payload)

    assert payload["cart_products"] == [{"product_id": "96878", "qty": "1"}]


def test_browse_event_has_no_cart_products_key(spark):
    df = _to_df(spark, [EMPTY_ROW])
    payload = json.loads(split_payload(df).collect()[0].payload)

    assert "cart_products" not in payload


def test_search_event_only_has_key_search(spark):
    row = {**EMPTY_ROW, "key_search": "diamond ring"}
    df = _to_df(spark, [row])
    payload = json.loads(split_payload(df).collect()[0].payload)

    assert payload == {"key_search": "diamond ring"}


def test_option_parsed_into_payload(spark):
    option = [{"option_label": "alloy", "option_id": "167265", "value_label": "yellow_white-750"}]
    row = {**EMPTY_ROW, "option": json.dumps(option)}
    df = _to_df(spark, [row])
    payload = json.loads(split_payload(df).collect()[0].payload)

    assert payload["option"] == option


def test_intermediate_parsed_columns_are_dropped(spark):
    df = _to_df(spark, [EMPTY_ROW])
    result = split_payload(df).collect()[0].asDict()

    assert "option_parsed" not in result
    assert "cart_products_parsed" not in result
