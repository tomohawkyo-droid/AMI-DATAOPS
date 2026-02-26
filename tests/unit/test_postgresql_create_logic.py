"""Tests for pure functions in postgresql_create module."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import get_type_hints
from uuid import UUID

from pydantic import BaseModel as PydanticBaseModel

from ami.implementations.sql.postgresql_create import (
    _annotation_to_sql_type,
    _get_model_defined_columns,
    _strip_optional,
    convert_datetime_strings_for_timestamps,
)

# ------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------

_INT_SENTINEL = 12345
_AGE_VALUE = 30
_EXPECTED_MICROSECONDS = 123456
_SAMPLE_MODEL_FIELD_COUNT = 4


class _OptionalHints:
    """Carrier whose resolved hints yield typing.Union (not UnionType)."""

    opt_str: str | None = None
    opt_int: int | None = None


_resolved = get_type_hints(_OptionalHints)
_OPTIONAL_STR: type = _resolved["opt_str"]
_OPTIONAL_INT: type = _resolved["opt_int"]


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


class _Color(Enum):
    RED = "red"


class _SampleModel(PydanticBaseModel):
    uid: str | None = None
    name: str = ""
    count: int = 0
    active: bool = True


class _FakeDAO:
    def __init__(
        self,
        model_cls: type | None = None,
        omit: set[str] | None = None,
    ) -> None:
        self.model_cls = model_cls
        self._omit_columns = omit or set()


# ------------------------------------------------------------------
# convert_datetime_strings_for_timestamps
# ------------------------------------------------------------------


class TestConvertDatetimeStringsForTimestamps:
    """Verify ISO string to datetime conversion logic."""

    def test_known_column_created_at(self) -> None:
        data = {"created_at": "2024-01-01T00:00:00+00:00"}
        result = convert_datetime_strings_for_timestamps(data)
        assert isinstance(result["created_at"], datetime)

    def test_column_ending_with_at(self) -> None:
        data = {
            "finished_at": "2024-06-15T12:30:00+00:00",
        }
        result = convert_datetime_strings_for_timestamps(data)
        assert isinstance(result["finished_at"], datetime)

    def test_column_ending_with_date(self) -> None:
        data = {
            "birth_date": "2024-03-20T08:00:00+00:00",
        }
        result = convert_datetime_strings_for_timestamps(data)
        assert isinstance(result["birth_date"], datetime)

    def test_column_ending_with_time(self) -> None:
        data = {
            "start_time": "2024-07-04T16:45:00+00:00",
        }
        result = convert_datetime_strings_for_timestamps(data)
        assert isinstance(result["start_time"], datetime)

    def test_non_datetime_string_unchanged(self) -> None:
        data = {"created_at": "not-a-date"}
        result = convert_datetime_strings_for_timestamps(data)
        assert result["created_at"] == "not-a-date"

    def test_non_string_value_unchanged(self) -> None:
        data = {"created_at": _INT_SENTINEL}
        result = convert_datetime_strings_for_timestamps(data)
        assert result["created_at"] == _INT_SENTINEL

    def test_z_suffix_handled(self) -> None:
        data = {"updated_at": "2024-01-01T00:00:00Z"}
        result = convert_datetime_strings_for_timestamps(data)
        assert isinstance(result["updated_at"], datetime)

    def test_non_timestamp_column_stays_string(self) -> None:
        iso = "2024-01-01T00:00:00+00:00"
        data = {"description": iso}
        result = convert_datetime_strings_for_timestamps(data)
        assert result["description"] == iso

    def test_multiple_fields(self) -> None:
        data = {
            "created_at": "2024-01-01T00:00:00+00:00",
            "name": "alice",
            "ended_at": "2024-12-31T23:59:59Z",
            "age": _AGE_VALUE,
        }
        result = convert_datetime_strings_for_timestamps(data)
        assert isinstance(result["created_at"], datetime)
        assert result["name"] == "alice"
        assert isinstance(result["ended_at"], datetime)
        assert result["age"] == _AGE_VALUE

    def test_empty_dict_returns_empty(self) -> None:
        result = convert_datetime_strings_for_timestamps({})
        assert result == {}

    def test_microseconds_preserved(self) -> None:
        iso = "2024-01-15T10:30:45.123456+00:00"
        data = {"expires_at": iso}
        result = convert_datetime_strings_for_timestamps(data)
        dt = result["expires_at"]
        assert isinstance(dt, datetime)
        assert dt.microsecond == _EXPECTED_MICROSECONDS


# ------------------------------------------------------------------
# _strip_optional
# ------------------------------------------------------------------


class TestStripOptional:
    """Verify Optional/Union wrapper removal."""

    def test_optional_str_returns_str(self) -> None:
        assert _strip_optional(_OPTIONAL_STR) is str

    def test_union_str_none_returns_str(self) -> None:
        assert _strip_optional(str | None) is str

    def test_plain_str_unchanged(self) -> None:
        assert _strip_optional(str) is str

    def test_list_str_returns_first_arg(self) -> None:
        result = _strip_optional(list[str])
        assert result is str

    def test_optional_int_returns_int(self) -> None:
        assert _strip_optional(_OPTIONAL_INT) is int

    def test_union_int_none_returns_int(self) -> None:
        assert _strip_optional(int | None) is int

    def test_plain_int_unchanged(self) -> None:
        assert _strip_optional(int) is int

    def test_dict_str_str_returns_first_arg(self) -> None:
        result = _strip_optional(dict[str, str])
        assert result is str


# ------------------------------------------------------------------
# _annotation_to_sql_type
# ------------------------------------------------------------------


class TestAnnotationToSqlType:
    """Verify Python annotation to SQL type mapping."""

    def test_bool_to_boolean(self) -> None:
        assert _annotation_to_sql_type(bool) == "BOOLEAN"

    def test_int_to_bigint(self) -> None:
        assert _annotation_to_sql_type(int) == "BIGINT"

    def test_float_to_double_precision(self) -> None:
        result = _annotation_to_sql_type(float)
        assert result == "DOUBLE PRECISION"

    def test_decimal_to_numeric(self) -> None:
        assert _annotation_to_sql_type(Decimal) == "NUMERIC"

    def test_datetime_to_timestamptz(self) -> None:
        result = _annotation_to_sql_type(datetime)
        assert result == "TIMESTAMPTZ"

    def test_date_to_date(self) -> None:
        assert _annotation_to_sql_type(date) == "DATE"

    def test_uuid_to_uuid(self) -> None:
        assert _annotation_to_sql_type(UUID) == "UUID"

    def test_bytes_to_bytea(self) -> None:
        assert _annotation_to_sql_type(bytes) == "BYTEA"

    def test_dict_to_jsonb(self) -> None:
        assert _annotation_to_sql_type(dict) == "JSONB"

    def test_list_to_jsonb(self) -> None:
        assert _annotation_to_sql_type(list) == "JSONB"

    def test_enum_to_text(self) -> None:
        assert _annotation_to_sql_type(_Color) == "TEXT"

    def test_generic_list_str_to_jsonb(self) -> None:
        assert _annotation_to_sql_type(list[str]) == "JSONB"

    def test_unknown_type_to_text(self) -> None:
        class _Custom:
            pass

        assert _annotation_to_sql_type(_Custom) == "TEXT"

    def test_none_to_text(self) -> None:
        assert _annotation_to_sql_type(None) == "TEXT"

    def test_str_to_text(self) -> None:
        assert _annotation_to_sql_type(str) == "TEXT"

    def test_generic_dict_to_jsonb(self) -> None:
        result = _annotation_to_sql_type(dict[str, int])
        assert result == "JSONB"


# ------------------------------------------------------------------
# _get_model_defined_columns
# ------------------------------------------------------------------


class TestGetModelDefinedColumns:
    """Verify column derivation from DAO model class."""

    def test_model_produces_correct_columns(self) -> None:
        dao = _FakeDAO(model_cls=_SampleModel)
        cols = _get_model_defined_columns(dao)
        assert cols["uid"] == "TEXT"
        assert cols["name"] == "TEXT"
        assert cols["count"] == "BIGINT"
        assert cols["active"] == "BOOLEAN"

    def test_no_model_returns_empty(self) -> None:
        dao = _FakeDAO(model_cls=None)
        assert _get_model_defined_columns(dao) == {}

    def test_omit_columns_excluded(self) -> None:
        dao = _FakeDAO(
            model_cls=_SampleModel,
            omit={"count", "active"},
        )
        cols = _get_model_defined_columns(dao)
        assert "count" not in cols
        assert "active" not in cols
        assert "name" in cols

    def test_all_fields_omitted(self) -> None:
        dao = _FakeDAO(
            model_cls=_SampleModel,
            omit={"uid", "name", "count", "active"},
        )
        assert _get_model_defined_columns(dao) == {}

    def test_column_count_matches_model(self) -> None:
        dao = _FakeDAO(model_cls=_SampleModel)
        cols = _get_model_defined_columns(dao)
        assert len(cols) == _SAMPLE_MODEL_FIELD_COUNT
