"""
Unit tests for SchemaValidator.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from validator import SchemaValidator, ValidationResult

VALID_EVENT = {
    "id": "abc-123",
    "product_id": "96672",
    "local_time": "2024-05-28 08:31:22",
    "collection": "view_product_detail",
    "store_id": "85",
    "email": None,
}


def _encode(d: dict) -> bytes:
    return json.dumps(d).encode()


def test_valid_event():
    result = SchemaValidator().validate(_encode(VALID_EVENT))
    assert result.is_valid is True
    assert result.payload == VALID_EVENT


def test_missing_product_id_is_valid():
    # product_id is optional — excluded from REQUIRED_FIELDS
    event = {k: v for k, v in VALID_EVENT.items() if k != "product_id"}
    result = SchemaValidator().validate(_encode(event))
    assert result.is_valid is True


def test_missing_id():
    event = {k: v for k, v in VALID_EVENT.items() if k != "id"}
    result = SchemaValidator().validate(_encode(event))
    assert result.is_valid is False
    assert result.error_reason == "missing_required_field"
    assert result.error_field == "id"


def test_missing_local_time():
    event = {k: v for k, v in VALID_EVENT.items() if k != "local_time"}
    result = SchemaValidator().validate(_encode(event))
    assert result.is_valid is False
    assert result.error_reason == "missing_required_field"
    assert result.error_field == "local_time"


def test_missing_collection():
    event = {k: v for k, v in VALID_EVENT.items() if k != "collection"}
    result = SchemaValidator().validate(_encode(event))
    assert result.is_valid is False
    assert result.error_reason == "missing_required_field"
    assert result.error_field == "collection"


def test_invalid_json():
    result = SchemaValidator().validate(b"not json")
    assert result.is_valid is False
    assert result.error_reason == "json_parse_error"


def test_empty_bytes():
    result = SchemaValidator().validate(b"")
    assert result.is_valid is False
    assert result.error_reason == "json_parse_error"


def test_optional_field_null_is_valid():
    event = {**VALID_EVENT, "email": None, "product_id": None}
    result = SchemaValidator().validate(_encode(event))
    assert result.is_valid is True
