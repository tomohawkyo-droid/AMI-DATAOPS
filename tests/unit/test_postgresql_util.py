"""Tests for PostgreSQL utility functions."""

from __future__ import annotations

import enum
import json
from datetime import UTC, datetime

import pytest
from pydantic import SecretStr

from ami.core.exceptions import StorageError
from ami.implementations.sql.postgresql_util import (
    build_where_clause,
    deserialize_row,
    get_safe_table_name,
    infer_column_type,
    is_valid_identifier,
    parse_affected_count,
    serialize_value,
)

AFFECTED_3 = 3
AFFECTED_5 = 5
AFFECTED_10 = 10
AFFECTED_99999 = 99999

INT_42 = 42
INT_30 = 30
FLOAT_PI = 3.14
FLOAT_PRICE = 9.99
ENUM_BLUE_VAL = 3
WHERE_PARTS_COUNT = 3


class Color(enum.Enum):
    """Simple enum for serialization tests."""

    RED = "red"
    GREEN = "green"
    BLUE = ENUM_BLUE_VAL


# ------------------------------------------------------------------
# parse_affected_count
# ------------------------------------------------------------------


class TestParseAffectedCount:
    """Verify count extraction from asyncpg status strings."""

    def test_delete_status(self) -> None:
        assert parse_affected_count("DELETE 3") == AFFECTED_3

    def test_update_status(self) -> None:
        assert parse_affected_count("UPDATE 1") == 1

    def test_insert_status(self) -> None:
        assert parse_affected_count("INSERT 0 1") == 1

    def test_insert_multiple(self) -> None:
        assert parse_affected_count("INSERT 0 5") == AFFECTED_5

    def test_select_status(self) -> None:
        assert parse_affected_count("SELECT 10") == AFFECTED_10

    def test_none_input(self) -> None:
        assert parse_affected_count(None) == 0

    def test_empty_string(self) -> None:
        assert parse_affected_count("") == 0

    def test_malformed_no_number(self) -> None:
        assert parse_affected_count("INVALID") == 0

    def test_malformed_garbage(self) -> None:
        assert parse_affected_count("abc xyz") == 0

    def test_zero_affected(self) -> None:
        assert parse_affected_count("DELETE 0") == 0

    def test_large_count(self) -> None:
        expected = AFFECTED_99999
        assert parse_affected_count("UPDATE 99999") == expected


# ------------------------------------------------------------------
# is_valid_identifier
# ------------------------------------------------------------------


class TestIsValidIdentifier:
    """Validate SQL identifier safety checks."""

    def test_simple_name(self) -> None:
        assert is_valid_identifier("users") is True

    def test_underscore_prefix(self) -> None:
        assert is_valid_identifier("_col") is True

    def test_mixed_case_digits(self) -> None:
        assert is_valid_identifier("A1") is True

    def test_snake_case(self) -> None:
        assert is_valid_identifier("my_table") is True

    def test_all_underscores(self) -> None:
        assert is_valid_identifier("__") is True

    def test_space_in_name(self) -> None:
        assert is_valid_identifier("my table") is False

    def test_hyphen_in_name(self) -> None:
        assert is_valid_identifier("my-table") is False

    def test_starts_with_digit(self) -> None:
        assert is_valid_identifier("1col") is False

    def test_sql_injection_attempt(self) -> None:
        assert is_valid_identifier("drop;table") is False

    def test_empty_string(self) -> None:
        assert is_valid_identifier("") is False

    def test_space_only(self) -> None:
        assert is_valid_identifier("a b") is False

    def test_dot_separated(self) -> None:
        assert is_valid_identifier("schema.table") is False

    def test_single_char(self) -> None:
        assert is_valid_identifier("x") is True

    def test_single_underscore(self) -> None:
        assert is_valid_identifier("_") is True


# ------------------------------------------------------------------
# get_safe_table_name
# ------------------------------------------------------------------


class TestGetSafeTableName:
    """Verify safe table name validation."""

    def test_valid_name_returned(self) -> None:
        assert get_safe_table_name("users") == "users"

    def test_valid_snake_case(self) -> None:
        result = get_safe_table_name("user_data")
        assert result == "user_data"

    def test_invalid_name_raises(self) -> None:
        with pytest.raises(StorageError, match="Invalid table name"):
            get_safe_table_name("my table")

    def test_hyphen_name_raises(self) -> None:
        with pytest.raises(StorageError, match="Invalid table name"):
            get_safe_table_name("my-table")

    def test_empty_name_raises(self) -> None:
        with pytest.raises(StorageError, match="Invalid table name"):
            get_safe_table_name("")

    def test_digit_prefix_raises(self) -> None:
        with pytest.raises(StorageError, match="Invalid table name"):
            get_safe_table_name("1table")

    def test_injection_raises(self) -> None:
        with pytest.raises(StorageError, match="Invalid table name"):
            get_safe_table_name("users; DROP TABLE")

    def test_error_message_contains_name(self) -> None:
        bad_name = "no good"
        with pytest.raises(StorageError, match=bad_name):
            get_safe_table_name(bad_name)


# ------------------------------------------------------------------
# infer_column_type
# ------------------------------------------------------------------


class TestInferColumnType:
    """Verify Python-to-SQL type inference logic."""

    def test_none_value(self) -> None:
        assert infer_column_type(None) == "TEXT"

    def test_bool_true(self) -> None:
        assert infer_column_type(True) == "BOOLEAN"

    def test_bool_false(self) -> None:
        assert infer_column_type(False) == "BOOLEAN"

    def test_integer(self) -> None:
        assert infer_column_type(INT_42) == "BIGINT"

    def test_zero(self) -> None:
        assert infer_column_type(0) == "BIGINT"

    def test_negative_int(self) -> None:
        assert infer_column_type(-10) == "BIGINT"

    def test_float(self) -> None:
        assert infer_column_type(FLOAT_PI) == "DOUBLE PRECISION"

    def test_datetime_value(self) -> None:
        dt = datetime(2024, 1, 1, tzinfo=UTC)
        result = infer_column_type(dt)
        assert result == "TIMESTAMP WITH TIME ZONE"

    def test_dict_value(self) -> None:
        assert infer_column_type({"key": "val"}) == "JSONB"

    def test_empty_dict(self) -> None:
        assert infer_column_type({}) == "JSONB"

    def test_list_value(self) -> None:
        assert infer_column_type([1, 2]) == "JSONB"

    def test_empty_list(self) -> None:
        assert infer_column_type([]) == "JSONB"

    def test_plain_string(self) -> None:
        assert infer_column_type("hello") == "TEXT"

    def test_created_at_with_iso_string(self) -> None:
        result = infer_column_type(
            "2024-01-01T00:00:00Z",
            column_name="created_at",
        )
        assert result == "TIMESTAMP WITH TIME ZONE"

    def test_some_date_with_iso_string(self) -> None:
        result = infer_column_type(
            "2024-06-15T12:30:00+00:00",
            column_name="some_date",
        )
        assert result == "TIMESTAMP WITH TIME ZONE"

    def test_updated_at_with_iso_string(self) -> None:
        result = infer_column_type(
            "2024-01-01 10:00:00",
            column_name="updated_at",
        )
        assert result == "TIMESTAMP WITH TIME ZONE"

    def test_random_col_with_non_iso_string(self) -> None:
        result = infer_column_type(
            "not a date",
            column_name="random_col",
        )
        assert result == "TEXT"

    def test_timestamp_column_non_iso_value(self) -> None:
        result = infer_column_type(
            "just text",
            column_name="created_at",
        )
        assert result == "TEXT"

    def test_no_column_hint_iso_string_stays_text(self) -> None:
        result = infer_column_type("2024-01-01T00:00:00Z")
        assert result == "TEXT"

    def test_bool_not_confused_with_int(self) -> None:
        """bool is subclass of int; BOOLEAN must come first."""
        assert infer_column_type(True) == "BOOLEAN"
        assert infer_column_type(False) == "BOOLEAN"

    def test_expires_at_column_hint(self) -> None:
        result = infer_column_type(
            "2025-12-31T23:59:59Z",
            column_name="expires_at",
        )
        assert result == "TIMESTAMP WITH TIME ZONE"

    def test_date_suffix_column_hint(self) -> None:
        result = infer_column_type(
            "2025-03-01T08:00:00",
            column_name="birth_date",
        )
        assert result == "TIMESTAMP WITH TIME ZONE"


# ------------------------------------------------------------------
# serialize_value
# ------------------------------------------------------------------


class TestSerializeValue:
    """Verify value serialization for PostgreSQL storage."""

    def test_dict_to_json(self) -> None:
        result = serialize_value({"a": 1})
        assert result == json.dumps({"a": 1})

    def test_list_to_json(self) -> None:
        result = serialize_value([1, 2])
        assert result == json.dumps([1, 2])

    def test_empty_dict_to_json(self) -> None:
        result = serialize_value({})
        assert result == "{}"

    def test_nested_dict_to_json(self) -> None:
        data = {"outer": {"inner": [1, 2]}}
        result = serialize_value(data)
        assert json.loads(result) == data

    def test_enum_returns_value_string(self) -> None:
        assert serialize_value(Color.RED) == "red"

    def test_enum_returns_value_int(self) -> None:
        assert serialize_value(Color.BLUE) == ENUM_BLUE_VAL

    def test_datetime_passthrough(self) -> None:
        dt = datetime(2024, 1, 1, tzinfo=UTC)
        assert serialize_value(dt) is dt

    def test_plain_string_passthrough(self) -> None:
        assert serialize_value("hello") == "hello"

    def test_int_passthrough(self) -> None:
        assert serialize_value(INT_42) == INT_42

    def test_float_passthrough(self) -> None:
        assert serialize_value(FLOAT_PI) == FLOAT_PI

    def test_none_passthrough(self) -> None:
        assert serialize_value(None) is None

    def test_bool_passthrough(self) -> None:
        assert serialize_value(True) is True

    def test_secret_str_raises_type_error(self) -> None:
        secret = SecretStr("my_secret")
        with pytest.raises(TypeError, match="SecretStr"):
            serialize_value(secret)


# ------------------------------------------------------------------
# deserialize_row
# ------------------------------------------------------------------


class TestDeserializeRow:
    """Verify row deserialization from PostgreSQL."""

    def test_json_object_parsed(self) -> None:
        row = {"data": '{"a": 1}'}
        result = deserialize_row(row)
        assert result["data"] == {"a": 1}

    def test_json_array_parsed(self) -> None:
        row = {"items": "[1, 2, 3]"}
        result = deserialize_row(row)
        assert result["items"] == [1, 2, 3]

    def test_non_json_string_kept(self) -> None:
        row = {"name": "hello"}
        result = deserialize_row(row)
        assert result["name"] == "hello"

    def test_integer_passthrough(self) -> None:
        row = {"count": INT_42}
        result = deserialize_row(row)
        assert result["count"] == INT_42

    def test_none_passthrough(self) -> None:
        row = {"field": None}
        result = deserialize_row(row)
        assert result["field"] is None

    def test_bool_passthrough(self) -> None:
        row = {"active": True}
        result = deserialize_row(row)
        assert result["active"] is True

    def test_empty_row(self) -> None:
        assert deserialize_row({}) == {}

    def test_mixed_columns(self) -> None:
        row = {
            "name": "alice",
            "meta": '{"role": "admin"}',
            "age": INT_30,
            "tags": '["a", "b"]',
        }
        result = deserialize_row(row)
        assert result["name"] == "alice"
        assert result["meta"] == {"role": "admin"}
        assert result["age"] == INT_30
        assert result["tags"] == ["a", "b"]

    def test_json_number_string_parsed(self) -> None:
        """A bare number in a string is valid JSON."""
        row = {"val": "42"}
        result = deserialize_row(row)
        assert result["val"] == INT_42

    def test_nested_json_parsed(self) -> None:
        nested = {"x": {"y": [1, 2]}}
        row = {"payload": json.dumps(nested)}
        result = deserialize_row(row)
        assert result["payload"] == nested

    def test_float_passthrough(self) -> None:
        row = {"price": FLOAT_PRICE}
        result = deserialize_row(row)
        assert result["price"] == FLOAT_PRICE


# ------------------------------------------------------------------
# build_where_clause
# ------------------------------------------------------------------


class TestBuildWhereClause:
    """Verify WHERE clause construction from filter dicts."""

    def test_single_filter(self) -> None:
        clause, params = build_where_clause({"name": "x"})
        assert clause == "name = $1"
        assert params == ["x"]

    def test_multiple_filters_and_joined(self) -> None:
        clause, params = build_where_clause({"name": "x", "age": INT_30})
        assert "name = $1" in clause
        assert "age = $2" in clause
        assert " AND " in clause
        assert params == ["x", INT_30]

    def test_none_value_uses_is_null(self) -> None:
        clause, params = build_where_clause({"deleted": None})
        assert clause == "deleted IS NULL"
        assert params == []

    def test_none_with_other_filters(self) -> None:
        clause, params = build_where_clause({"status": None, "name": "bob"})
        assert "status IS NULL" in clause
        assert "name = $1" in clause
        assert params == ["bob"]

    def test_invalid_identifier_skipped(self) -> None:
        clause, params = build_where_clause({"valid_col": 1, "bad col": 2})
        assert "valid_col = $1" in clause
        assert "bad col" not in clause
        assert params == [1]

    def test_all_invalid_identifiers(self) -> None:
        clause, params = build_where_clause({"bad col": 1, "also-bad": 2})
        assert clause == ""
        assert params == []

    def test_empty_filters(self) -> None:
        clause, params = build_where_clause({})
        assert clause == ""
        assert params == []

    def test_dict_value_serialized(self) -> None:
        clause, params = build_where_clause({"meta": {"key": "val"}})
        assert clause == "meta = $1"
        assert params == [json.dumps({"key": "val"})]

    def test_list_value_serialized(self) -> None:
        clause, params = build_where_clause({"tags": [1, 2]})
        assert clause == "tags = $1"
        assert params == [json.dumps([1, 2])]

    def test_enum_value_serialized(self) -> None:
        clause, params = build_where_clause({"color": Color.RED})
        assert clause == "color = $1"
        assert params == ["red"]

    def test_param_numbering_skips_null(self) -> None:
        """NULL filters do not consume a $N parameter."""
        clause, params = build_where_clause({"a": None, "b": "yes", "c": "no"})
        assert "a IS NULL" in clause
        assert "b = $1" in clause
        assert "c = $2" in clause
        assert params == ["yes", "no"]

    def test_three_filters(self) -> None:
        clause, params = build_where_clause({"x": 1, "y": 2, "z": WHERE_PARTS_COUNT})
        parts = clause.split(" AND ")
        assert len(parts) == WHERE_PARTS_COUNT
        assert params == [1, 2, WHERE_PARTS_COUNT]
