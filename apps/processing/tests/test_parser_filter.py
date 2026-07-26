"""
Unit tests for RawParser + ValidationFilter.
"""
import json

from filter import validate_events
from parser import parse_raw


VALID_EVENT = {
    "id": "9913c12e-1556-434c-abcc-c555d036b389",
    "collection": "view_listing_page",
    "local_time": "2026-07-14 20:39:40",
    "current_url": "https://www.glamira.ro/bratara/585-aur-galben/",
}


def _to_df(spark, raw_jsons):
    rows = [(j, "user-events", 0, i) for i, j in enumerate(raw_jsons)]
    return spark.createDataFrame(rows, ["value", "topic", "partition", "offset"])


def test_multiple_collection_types_pass_filter(spark):
    events = [
        {**VALID_EVENT, "collection": "view_product_detail"},
        {**VALID_EVENT, "collection": "add_to_cart_action"},
        {**VALID_EVENT, "collection": "search_box_action"},
    ]
    df = _to_df(spark, [json.dumps(e) for e in events])
    result = validate_events(parse_raw(df))

    assert result.count() == 3
    assert set(r.collection for r in result.collect()) == {
        "view_product_detail", "add_to_cart_action", "search_box_action",
    }


def test_kafka_partition_and_offset_kept(spark):
    df = _to_df(spark, [json.dumps(VALID_EVENT)])
    result = parse_raw(df).collect()[0]

    assert result.kafka_partition == 0
    assert result.kafka_offset == 0


def test_missing_id_dropped(spark):
    event = {k: v for k, v in VALID_EVENT.items() if k != "id"}
    df = _to_df(spark, [json.dumps(event)])

    assert validate_events(parse_raw(df)).count() == 0


def test_missing_collection_dropped(spark):
    event = {k: v for k, v in VALID_EVENT.items() if k != "collection"}
    df = _to_df(spark, [json.dumps(event)])

    assert validate_events(parse_raw(df)).count() == 0


def test_missing_local_time_dropped(spark):
    event = {k: v for k, v in VALID_EVENT.items() if k != "local_time"}
    df = _to_df(spark, [json.dumps(event)])

    assert validate_events(parse_raw(df)).count() == 0


def test_malformed_json_dropped(spark):
    df = _to_df(spark, ["{this is not valid json"])

    assert validate_events(parse_raw(df)).count() == 0


def test_option_and_cart_products_kept_as_raw_string(spark):
    event = {
        **VALID_EVENT,
        "collection": "select_product_option",
        "option": [{"option_label": "alloy", "value_label": "yellow_white-750"}],
        "cart_products": [{"product_id": "96878", "qty": 1}],
    }
    df = _to_df(spark, [json.dumps(event)])
    row = parse_raw(df).collect()[0]

    # Not parsed into a struct/array yet — PayloadSplitter (P2.4) does that.
    assert json.loads(row.option) == event["option"]
    assert json.loads(row.cart_products) == event["cart_products"]
