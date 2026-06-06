"""
Validate raw Kafka message bytes.
Designed to be unit-tested in isolation.
Only checks field existence (not type/format)
"""
import json
from dataclasses import dataclass
from typing import Optional

from shared.schemas.event import REQUIRED_FIELDS

@dataclass
class ValidationResult:
    is_valid: bool
    payload: Optional[dict] = None
    error_reason: Optional[str] = None
    error_field: Optional[str] = None


class SchemaValidator:
    def validate(self, raw_bytes: bytes) -> ValidationResult:
        try:
            payload = json.loads(raw_bytes)
        except json.JSONDecodeError:
            return ValidationResult(is_valid=False, error_reason="json_parse_error")

        for field in REQUIRED_FIELDS:
            if field not in payload:
                return ValidationResult(
                    is_valid=False,
                    error_reason="missing_required_field",
                    error_field=field,
                )

        return ValidationResult(is_valid=True, payload=payload)
