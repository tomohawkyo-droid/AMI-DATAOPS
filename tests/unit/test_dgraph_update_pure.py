"""Pure-function tests for dgraph_update helpers.

Covers validation, formatting, encoding, and response-extraction
utilities without touching the network or transaction layer.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from enum import Enum
from typing import Any, cast

import pytest

from ami.core.exceptions import StorageError, StorageValidationError
from ami.implementations.graph.dgraph_update import (
    _encode_query,
    _extract_mutation_count,
    _format_value,
    _validate_collection_name,
    _validate_field_key,
    _validate_item_id,
    _validate_raw_query,
)

# -- test helpers --

_INT_42 = 42
_EXPECTED_TWO = 2


class _Status(Enum):
    ACTIVE = "active"


class _FakeResponse:
    """Minimal stand-in for a Dgraph mutation response."""

    def __init__(self, uids: dict[str, str] | None = None):
        self.uids = uids


# ---- _validate_collection_name ----


class TestValidateCollectionName:
    """Boundary tests for _validate_collection_name."""

    def test_simple_alpha(self) -> None:
        _validate_collection_name("users")

    def test_with_underscore(self) -> None:
        _validate_collection_name("my_collection")

    def test_alphanumeric_mixed_case(self) -> None:
        _validate_collection_name("Test123")

    def test_single_char(self) -> None:
        _validate_collection_name("A")

    def test_max_length_accepted(self) -> None:
        _validate_collection_name("a" * 64)

    def test_empty_raises(self) -> None:
        with pytest.raises(StorageValidationError, match="cannot be empty"):
            _validate_collection_name("")

    def test_non_string_raises(self) -> None:
        with pytest.raises(StorageValidationError, match="must be a string"):
            _validate_collection_name(cast(Any, 42))

    def test_hyphen_raises(self) -> None:
        with pytest.raises(StorageValidationError, match="alphanumeric"):
            _validate_collection_name("my-collection")

    def test_space_raises(self) -> None:
        with pytest.raises(StorageValidationError, match="alphanumeric"):
            _validate_collection_name("my collection")

    def test_leading_underscore_raises(self) -> None:
        with pytest.raises(
            StorageValidationError,
            match="cannot start or end with underscore",
        ):
            _validate_collection_name("_users")

    def test_trailing_underscore_raises(self) -> None:
        with pytest.raises(
            StorageValidationError,
            match="cannot start or end with underscore",
        ):
            _validate_collection_name("users_")

    def test_consecutive_underscores_raises(self) -> None:
        with pytest.raises(StorageValidationError, match="consecutive"):
            _validate_collection_name("my__col")

    def test_too_long_raises(self) -> None:
        with pytest.raises(StorageValidationError, match="too long"):
            _validate_collection_name("a" * 65)


# ---- _validate_item_id ----


class TestValidateItemId:
    """Boundary tests for _validate_item_id."""

    def test_plain_id(self) -> None:
        _validate_item_id("abc123")

    def test_hex_prefixed_id(self) -> None:
        _validate_item_id("0xabc")

    def test_empty_raises(self) -> None:
        with pytest.raises(StorageValidationError, match="cannot be empty"):
            _validate_item_id("")

    def test_non_string_raises(self) -> None:
        with pytest.raises(StorageValidationError, match="must be a string"):
            _validate_item_id(cast(Any, 99))


# ---- _validate_field_key ----


class TestValidateFieldKey:
    """Boundary tests for _validate_field_key."""

    def test_simple_alpha(self) -> None:
        _validate_field_key("name")

    def test_with_underscore(self) -> None:
        _validate_field_key("user_name")

    def test_dotted_path(self) -> None:
        _validate_field_key("collection.field")

    def test_semicolon_raises(self) -> None:
        with pytest.raises(StorageValidationError, match="Invalid field name"):
            _validate_field_key("name;drop")

    def test_braces_raises(self) -> None:
        with pytest.raises(StorageValidationError, match="Invalid field name"):
            _validate_field_key("name{}")

    def test_space_raises(self) -> None:
        with pytest.raises(StorageValidationError, match="Invalid field name"):
            _validate_field_key("a b")

    def test_non_string_raises(self) -> None:
        with pytest.raises(StorageValidationError, match="must be a string"):
            _validate_field_key(cast(Any, 7))


# ---- _format_value ----


class TestFormatValue:
    """Tests for _format_value type coercion."""

    def test_list_to_json(self) -> None:
        result = _format_value("tags", ["a", "b"])
        assert result == json.dumps(["a", "b"])

    def test_dict_to_json(self) -> None:
        payload = {"nested": True}
        result = _format_value("meta", payload)
        assert result == json.dumps(payload)

    def test_datetime_to_iso(self) -> None:
        dt = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)
        result = _format_value("created_at", dt)
        assert result == dt.isoformat()

    def test_enum_passes_through(self) -> None:
        result = _format_value("status", _Status.ACTIVE)
        assert result is _Status.ACTIVE

    def test_timestamp_key_normalises_iso_string(self) -> None:
        iso = "2025-06-01T10:30:00"
        result = _format_value("created_at", iso)
        expected = datetime.fromisoformat(iso).isoformat()
        assert result == expected

    def test_date_key_normalises_iso_string(self) -> None:
        iso = "2025-06-01T10:30:00"
        result = _format_value("birth_date", iso)
        expected = datetime.fromisoformat(iso).isoformat()
        assert result == expected

    def test_timestamp_key_literal(self) -> None:
        iso = "2025-06-01T10:30:00"
        result = _format_value("timestamp", iso)
        expected = datetime.fromisoformat(iso).isoformat()
        assert result == expected

    def test_plain_string_passthrough(self) -> None:
        result = _format_value("title", "hello world")
        assert result == "hello world"

    def test_integer_passthrough(self) -> None:
        result = _format_value("count", _INT_42)
        assert result == _INT_42

    def test_non_timestamp_string_unchanged(self) -> None:
        result = _format_value("name", "2025-06-01T10:30:00")
        assert result == "2025-06-01T10:30:00"


# ---- _validate_raw_query ----


class TestValidateRawQuery:
    """Tests for _validate_raw_query."""

    def test_valid_query(self) -> None:
        _validate_raw_query("{ query { uid } }")

    def test_empty_raises(self) -> None:
        with pytest.raises(StorageValidationError, match="cannot be empty"):
            _validate_raw_query("")

    def test_whitespace_only_raises(self) -> None:
        with pytest.raises(StorageValidationError, match="cannot be empty"):
            _validate_raw_query("   ")

    def test_non_string_raises(self) -> None:
        with pytest.raises(StorageValidationError, match="must be a string"):
            _validate_raw_query(cast(Any, 123))


# ---- _encode_query ----


class TestEncodeQuery:
    """Tests for _encode_query."""

    def test_ascii(self) -> None:
        assert _encode_query("hello") == b"hello"

    def test_unicode(self) -> None:
        result = _encode_query("h\u00e9llo")
        assert result == "h\u00e9llo".encode()

    def test_empty_string(self) -> None:
        assert _encode_query("") == b""


# ---- _extract_mutation_count ----


class TestExtractMutationCount:
    """Tests for _extract_mutation_count."""

    def test_returns_uid_count(self) -> None:
        resp = _FakeResponse(uids={"blank-0": "0x1"})
        assert _extract_mutation_count(resp) == 1

    def test_multiple_uids(self) -> None:
        uids = {"blank-0": "0x1", "blank-1": "0x2"}
        resp = _FakeResponse(uids=uids)
        assert _extract_mutation_count(resp) == _EXPECTED_TWO

    def test_empty_dict_returns_zero(self) -> None:
        resp = _FakeResponse(uids={})
        assert _extract_mutation_count(resp) == 0

    def test_none_uids_returns_zero(self) -> None:
        resp = _FakeResponse(uids=None)
        assert _extract_mutation_count(resp) == 0

    def test_missing_uids_attr_raises(self) -> None:
        obj = object()
        with pytest.raises(StorageError, match="missing 'uids' attribute"):
            _extract_mutation_count(obj)
