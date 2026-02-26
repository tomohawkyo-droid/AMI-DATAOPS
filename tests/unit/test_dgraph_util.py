"""Tests for Dgraph utility pure functions."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from uuid import UUID

import pytest
from pydantic import BaseModel as PydanticBaseModel

from ami.core.exceptions import StorageError, StorageValidationError
from ami.implementations.graph.dgraph_util import (
    _convert_field_value,
    _escape_dql_value,
    _validate_identifier,
    build_count_query,
    build_dql_query,
    build_filter,
    from_dgraph_format,
    get_dgraph_type,
    process_dgraph_value,
    to_dgraph_format,
)

# -- Test helpers --


_INT_42 = 42
_COUNT_FIVE = 5
_COUNT_THREE = 3


class _SampleModel(PydanticBaseModel):
    uid: str | None = None
    name: str = ""
    count: int = 0


class _Color(Enum):
    RED = "red"
    BLUE = "blue"


# -- _validate_identifier --


class TestValidateIdentifier:
    """Verify DQL identifier validation."""

    def test_simple_alpha(self) -> None:
        assert _validate_identifier("foo") == "foo"

    def test_underscore_separated(self) -> None:
        assert _validate_identifier("foo_bar") == "foo_bar"

    def test_dotted(self) -> None:
        assert _validate_identifier("foo.bar") == "foo.bar"

    def test_leading_underscore(self) -> None:
        assert _validate_identifier("_private") == "_private"

    def test_alphanumeric(self) -> None:
        assert _validate_identifier("field2") == "field2"

    def test_space_rejected(self) -> None:
        with pytest.raises(StorageValidationError):
            _validate_identifier("foo bar")

    def test_parens_rejected(self) -> None:
        with pytest.raises(StorageValidationError):
            _validate_identifier("foo()")

    def test_semicolon_rejected(self) -> None:
        with pytest.raises(StorageValidationError):
            _validate_identifier("foo;bar")

    def test_empty_rejected(self) -> None:
        with pytest.raises(StorageValidationError):
            _validate_identifier("")

    def test_leading_digit_rejected(self) -> None:
        with pytest.raises(StorageValidationError):
            _validate_identifier("3field")


# -- _escape_dql_value --


class TestEscapeDqlValue:
    """Verify DQL string-literal escaping."""

    def test_backslash_escaped(self) -> None:
        assert _escape_dql_value("a\\b") == "a\\\\b"

    def test_quote_escaped(self) -> None:
        assert _escape_dql_value('say "hi"') == 'say \\"hi\\"'

    def test_newline_escaped(self) -> None:
        assert _escape_dql_value("line1\nline2") == "line1\\nline2"

    def test_clean_string_unchanged(self) -> None:
        assert _escape_dql_value("hello world") == "hello world"

    def test_combined_escaping(self) -> None:
        raw = 'a\\b"c\nd'
        assert _escape_dql_value(raw) == 'a\\\\b\\"c\\nd'


# -- get_dgraph_type --


class TestGetDgraphType:
    """Verify Python-to-Dgraph type mapping."""

    @pytest.mark.parametrize(
        ("py_type", "expected"),
        [
            (str, "string"),
            (int, "int"),
            (float, "float"),
            (bool, "bool"),
            (datetime, "datetime"),
            (date, "datetime"),
            (Decimal, "float"),
            (UUID, "string"),
            (bytes, "string"),
            (list, "[string]"),
            (dict, "string"),
        ],
    )
    def test_direct_mapping(self, py_type: type, expected: str) -> None:
        assert get_dgraph_type(py_type) == expected

    def test_enum_maps_to_string(self) -> None:
        assert get_dgraph_type(_Color) == "string"

    def test_optional_str(self) -> None:
        assert get_dgraph_type(str | None) == "string"

    def test_optional_int(self) -> None:
        assert get_dgraph_type(int | None) == "int"

    def test_generic_list(self) -> None:
        assert get_dgraph_type(list[str]) == "[string]"

    def test_generic_dict(self) -> None:
        assert get_dgraph_type(dict[str, int]) == "string"

    def test_unsupported_raises(self) -> None:
        with pytest.raises(StorageError):
            get_dgraph_type(complex)


# -- _convert_field_value --


class TestConvertFieldValue:
    """Verify field value conversion for Dgraph storage."""

    def test_enum_returns_value(self) -> None:
        assert _convert_field_value(_Color.RED) == "red"

    def test_list_returns_json(self) -> None:
        result = _convert_field_value([1, 2, 3])
        assert json.loads(result) == [1, 2, 3]

    def test_dict_returns_json(self) -> None:
        result = _convert_field_value({"a": 1})
        assert json.loads(result) == {"a": 1}

    def test_datetime_returns_iso(self) -> None:
        dt = datetime(2025, 1, 15, 12, 30, 0)
        assert _convert_field_value(dt) == dt.isoformat()

    def test_plain_string_passthrough(self) -> None:
        assert _convert_field_value("hello") == "hello"

    def test_plain_int_passthrough(self) -> None:
        assert _convert_field_value(_INT_42) == _INT_42


# -- to_dgraph_format --


class TestToDgraphFormat:
    """Verify conversion of model/dict to Dgraph format."""

    def test_model_fields_are_prefixed(self) -> None:
        inst = _SampleModel(name="alice", count=_COUNT_FIVE)
        result = to_dgraph_format(inst, "items")
        assert result["items.name"] == "alice"
        assert result["items.count"] == _COUNT_FIVE

    def test_uid_mapped_to_app_uid(self) -> None:
        inst = _SampleModel(uid="u1", name="bob")
        result = to_dgraph_format(inst, "items")
        assert result["items.app_uid"] == "u1"
        assert "items.uid" not in result

    def test_model_class_stored(self) -> None:
        inst = _SampleModel(name="x")
        result = to_dgraph_format(inst, "col")
        key = "col._model_class"
        assert key in result
        assert "_SampleModel" in result[key]

    def test_dict_input(self) -> None:
        data = {"name": "carol", "count": _COUNT_THREE}
        result = to_dgraph_format(data, "things")
        assert result["things.name"] == "carol"
        assert result["things.count"] == _COUNT_THREE

    def test_dict_has_no_model_class(self) -> None:
        result = to_dgraph_format({"k": "v"}, "ns")
        assert "ns._model_class" not in result

    def test_none_uid_still_maps_to_app_uid(self) -> None:
        inst = _SampleModel(uid=None, name="z")
        result = to_dgraph_format(inst, "col")
        # uid is always mapped to app_uid (even None)
        assert "col.app_uid" in result

    def test_invalid_type_raises(self) -> None:
        with pytest.raises(ValueError, match="Cannot convert"):
            to_dgraph_format(12345, "col")


# -- process_dgraph_value --


class TestProcessDgraphValue:
    """Verify Dgraph value post-processing."""

    def test_single_element_json_list(self) -> None:
        val = ["[1, 2, 3]"]
        assert process_dgraph_value(val) == [1, 2, 3]

    def test_single_element_json_obj_list(self) -> None:
        val = ['{"a": 1}']
        assert process_dgraph_value(val) == {"a": 1}

    def test_single_element_plain_string_list(self) -> None:
        val = ["hello"]
        assert process_dgraph_value(val) == "hello"

    def test_json_string_parsed(self) -> None:
        val = '{"key": "val"}'
        assert process_dgraph_value(val) == {"key": "val"}

    def test_double_encoded_json(self) -> None:
        # Double-encoded: string starts with { after first parse
        inner = '{"x": 1}'
        outer = json.dumps(inner)  # '"{\\"x\\": 1}"'
        # outer starts with '"' not '{', so no JSON parsing occurs
        assert process_dgraph_value(outer) == outer

    def test_plain_value_passthrough(self) -> None:
        assert process_dgraph_value(_INT_42) == _INT_42

    def test_plain_string_passthrough(self) -> None:
        assert process_dgraph_value("hello") == "hello"


# -- from_dgraph_format --


class TestFromDgraphFormat:
    """Verify conversion from Dgraph format to model."""

    def test_empty_data_returns_none(self) -> None:
        assert from_dgraph_format({}, _SampleModel, "col") is None

    def test_prefixed_fields_unprefixed(self) -> None:
        data = {
            "col.name": "alice",
            "col.count": _COUNT_FIVE,
        }
        result = from_dgraph_format(data, _SampleModel, "col")
        assert result is not None
        assert result.name == "alice"
        assert result.count == _COUNT_FIVE

    def test_app_uid_mapped_to_uid(self) -> None:
        data = {"col.app_uid": "u1", "col.name": "bob"}
        result = from_dgraph_format(data, _SampleModel, "col")
        assert result is not None
        assert result.uid == "u1"

    def test_dgraph_uid_ignored(self) -> None:
        data = {
            "uid": "0x1",
            "col.name": "x",
        }
        result = from_dgraph_format(data, _SampleModel, "col")
        assert result is not None
        assert result.uid is None

    def test_non_prefixed_fields_kept(self) -> None:
        data = {"col.name": "a", "extra": "val"}
        result = from_dgraph_format(data, _SampleModel, "col")
        assert result is not None


# -- build_dql_query --


class TestBuildDqlQuery:
    """Verify DQL query construction."""

    def test_simple_equality(self) -> None:
        q = build_dql_query({"name": "alice"}, "items")
        assert 'eq(items.name, "alice")' in q
        assert "type(items)" in q

    def test_or_operator(self) -> None:
        q = build_dql_query(
            {"$or": [{"name": "a"}, {"name": "b"}]},
            "col",
        )
        assert "OR" in q

    def test_and_operator(self) -> None:
        q = build_dql_query(
            {"$and": [{"name": "a"}, {"count": 1}]},
            "col",
        )
        assert "AND" in q

    def test_pagination_limit(self) -> None:
        q = build_dql_query({}, "col", limit=10)
        assert "first: 10" in q

    def test_pagination_offset(self) -> None:
        q = build_dql_query({}, "col", offset=5)
        assert "offset: 5" in q

    def test_empty_query(self) -> None:
        q = build_dql_query({}, "col")
        assert "type(col)" in q
        assert "@filter" not in q

    def test_query_has_expand_all(self) -> None:
        q = build_dql_query({}, "col")
        assert "expand(_all_)" in q


# -- build_filter --


class TestBuildFilter:
    """Verify filter expression construction."""

    def test_equality(self) -> None:
        f = build_filter({"name": "alice"}, "col")
        assert f == 'eq(col.name, "alice")'

    def test_in_operator(self) -> None:
        f = build_filter({"status": {"$in": ["a", "b"]}}, "col")
        assert "eq(col.status," in f
        assert '"a"' in f
        assert '"b"' in f

    def test_gt_operator(self) -> None:
        f = build_filter({"age": {"$gt": 18}}, "col")
        assert "gt(col.age, 18)" in f

    def test_lt_operator(self) -> None:
        f = build_filter({"age": {"$lt": 65}}, "col")
        assert "lt(col.age, 65)" in f

    def test_regex_operator(self) -> None:
        f = build_filter({"name": {"$regex": "^al"}}, "col")
        assert "regexp(col.name," in f
        assert "/^al/" in f

    def test_multiple_fields_joined_with_and(self) -> None:
        f = build_filter({"name": "a", "count": 1}, "col")
        assert " AND " in f


# -- build_count_query --


class TestBuildCountQuery:
    """Verify count query construction."""

    def test_with_filter(self) -> None:
        q = build_count_query({"name": "alice"}, "col")
        assert "count(func: type(col))" in q
        assert "@filter" in q
        assert "total: count(uid)" in q

    def test_without_filter(self) -> None:
        q = build_count_query({}, "col")
        assert "count(func: type(col))" in q
        assert "@filter" not in q
        assert "total: count(uid)" in q
