"""Tests for pure helper functions in redis_create and redis_update."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest
from pydantic import BaseModel as PydanticBaseModel

from ami.core.exceptions import StorageError
from ami.implementations.mem.redis_create import (
    _ensure_id_and_timestamps,
    _normalize_data,
)
from ami.implementations.mem.redis_update import (
    _get_index_fields,
    _prepare_data_with_ttl,
)

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


class _SimpleModel(PydanticBaseModel):
    uid: str | None = None
    name: str = "test"


class _NoUidModel(PydanticBaseModel):
    name: str = "no-uid"
    value: int = 42


# ------------------------------------------------------------------
# _normalize_data
# ------------------------------------------------------------------


class TestNormalizeData:
    """Verify dict/model normalization for redis create."""

    def test_dict_passthrough(self) -> None:
        data: dict[str, Any] = {"uid": "abc", "name": "hi"}
        result = _normalize_data(data)
        assert result is data

    def test_dict_identity_preserves_keys(self) -> None:
        data: dict[str, Any] = {"a": 1, "b": 2}
        result = _normalize_data(data)
        assert result == {"a": 1, "b": 2}

    def test_pydantic_model_returns_dict(self) -> None:
        model = _SimpleModel(uid="x1", name="alice")
        result = _normalize_data(model)
        assert isinstance(result, dict)
        assert result["uid"] == "x1"
        assert result["name"] == "alice"

    def test_pydantic_model_excludes_none(self) -> None:
        model = _SimpleModel(name="bob")
        result = _normalize_data(model)
        assert isinstance(result, dict)
        assert "uid" not in result
        assert result["name"] == "bob"

    def test_pydantic_model_without_uid_field(self) -> None:
        model = _NoUidModel(name="test", value=99)
        result = _normalize_data(model)
        assert result == {"name": "test", "value": 99}

    def test_plain_string_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Cannot create from type"):
            _normalize_data("just a string")

    def test_integer_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Cannot create from type"):
            _normalize_data(12345)

    def test_list_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Cannot create from type"):
            _normalize_data([1, 2, 3])

    def test_none_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Cannot create from type"):
            _normalize_data(None)

    def test_empty_dict_passthrough(self) -> None:
        data: dict[str, Any] = {}
        result = _normalize_data(data)
        assert result == {}
        assert result is data


# ------------------------------------------------------------------
# _ensure_id_and_timestamps
# ------------------------------------------------------------------


class TestEnsureIdAndTimestamps:
    """Verify ID generation and timestamp injection."""

    def test_generates_uid_when_missing(self) -> None:
        data: dict[str, Any] = {"name": "test"}
        item_id, key_id = _ensure_id_and_timestamps(data)
        assert "uid" in data
        assert item_id == str(data["uid"])
        assert key_id == item_id

    def test_preserves_existing_uid(self) -> None:
        data: dict[str, Any] = {"uid": "keep-me", "name": "test"}
        item_id, _ = _ensure_id_and_timestamps(data)
        assert item_id == "keep-me"
        assert data["uid"] == "keep-me"

    def test_uses_id_when_no_uid(self) -> None:
        data: dict[str, Any] = {"id": "from-id", "name": "test"}
        item_id, _ = _ensure_id_and_timestamps(data)
        assert item_id == "from-id"

    def test_prefers_uid_over_id(self) -> None:
        data: dict[str, Any] = {
            "uid": "uid-wins",
            "id": "id-loses",
        }
        item_id, _ = _ensure_id_and_timestamps(data)
        assert item_id == "uid-wins"

    def test_adds_created_at_timestamp(self) -> None:
        data: dict[str, Any] = {"uid": "ts-test"}
        _ensure_id_and_timestamps(data)
        assert "created_at" in data
        parsed = datetime.fromisoformat(data["created_at"])
        assert isinstance(parsed, datetime)

    def test_adds_updated_at_timestamp(self) -> None:
        data: dict[str, Any] = {"uid": "ts-test"}
        _ensure_id_and_timestamps(data)
        assert "updated_at" in data
        parsed = datetime.fromisoformat(data["updated_at"])
        assert isinstance(parsed, datetime)

    def test_timestamps_are_equal(self) -> None:
        data: dict[str, Any] = {"uid": "ts-eq"}
        _ensure_id_and_timestamps(data)
        assert data["created_at"] == data["updated_at"]

    def test_returns_tuple_of_two_equal_strings(self) -> None:
        expected_len = 2
        data: dict[str, Any] = {"uid": "tuple-test"}
        result = _ensure_id_and_timestamps(data)
        assert isinstance(result, tuple)
        assert len(result) == expected_len
        assert result[0] == result[1]

    def test_generated_uid_is_string_convertible(self) -> None:
        data: dict[str, Any] = {"name": "auto"}
        item_id, _ = _ensure_id_and_timestamps(data)
        assert isinstance(item_id, str)
        assert len(item_id) > 0


# ------------------------------------------------------------------
# _prepare_data_with_ttl
# ------------------------------------------------------------------


class TestPrepareDataWithTtl:
    """Verify data preparation for redis update."""

    def test_adds_updated_at(self) -> None:
        data: dict[str, Any] = {"name": "val"}
        _prepare_data_with_ttl(data, "item-1", 300)
        assert "updated_at" in data
        parsed = datetime.fromisoformat(data["updated_at"])
        assert isinstance(parsed, datetime)

    def test_adds_created_at_when_missing(self) -> None:
        data: dict[str, Any] = {"name": "val"}
        _prepare_data_with_ttl(data, "item-1", 300)
        assert "created_at" in data
        assert data["created_at"] == data["updated_at"]

    def test_preserves_existing_created_at(self) -> None:
        original = "2024-01-01T00:00:00+00:00"
        data: dict[str, Any] = {
            "name": "val",
            "created_at": original,
        }
        _prepare_data_with_ttl(data, "item-1", 300)
        assert data["created_at"] == original

    def test_sets_uid_to_item_id_when_missing(self) -> None:
        data: dict[str, Any] = {"name": "val"}
        _prepare_data_with_ttl(data, "my-id", 300)
        assert data["uid"] == "my-id"

    def test_preserves_existing_uid(self) -> None:
        data: dict[str, Any] = {"uid": "original", "name": "val"}
        _prepare_data_with_ttl(data, "new-id", 300)
        assert data["uid"] == "original"

    def test_uses_data_ttl_over_default(self) -> None:
        data_ttl = 600
        data: dict[str, Any] = {
            "name": "val",
            "_ttl": data_ttl,
        }
        result = _prepare_data_with_ttl(data, "item-1", 300)
        assert result["_ttl"] == data_ttl

    def test_uses_default_ttl_when_no_data_ttl(self) -> None:
        default_ttl = 900
        data: dict[str, Any] = {"name": "val"}
        result = _prepare_data_with_ttl(data, "item-1", default_ttl)
        assert result["_ttl"] == default_ttl

    def test_zero_ttl_raises_storage_error(self) -> None:
        data: dict[str, Any] = {"name": "val", "_ttl": 0}
        with pytest.raises(StorageError):
            _prepare_data_with_ttl(data, "item-1", 300)

    def test_negative_ttl_raises_storage_error(self) -> None:
        data: dict[str, Any] = {"name": "val", "_ttl": -10}
        with pytest.raises(StorageError):
            _prepare_data_with_ttl(data, "item-1", 300)

    def test_negative_default_ttl_raises_storage_error(self) -> None:
        data: dict[str, Any] = {"name": "val"}
        with pytest.raises(StorageError):
            _prepare_data_with_ttl(data, "item-1", -5)

    def test_returns_same_dict_reference(self) -> None:
        data: dict[str, Any] = {"name": "val"}
        result = _prepare_data_with_ttl(data, "item-1", 300)
        assert result is data

    def test_ttl_converted_to_int(self) -> None:
        expected_ttl = 45
        data: dict[str, Any] = {"name": "val", "_ttl": 45.9}
        result = _prepare_data_with_ttl(data, "item-1", 300)
        assert result["_ttl"] == expected_ttl
        assert isinstance(result["_ttl"], int)


# ------------------------------------------------------------------
# _get_index_fields
# ------------------------------------------------------------------


class TestGetIndexFields:
    """Verify index field selection logic."""

    def test_uses_explicit_index_fields(self) -> None:
        data: dict[str, Any] = {
            "name": "val",
            "status": "active",
            "_index_fields": ["name"],
        }
        result = _get_index_fields(data)
        assert result == ["name"]

    def test_explicit_empty_list(self) -> None:
        data: dict[str, Any] = {
            "name": "val",
            "_index_fields": [],
        }
        result = _get_index_fields(data)
        assert result == []

    def test_non_list_index_fields_returns_empty(self) -> None:
        data: dict[str, Any] = {
            "name": "val",
            "_index_fields": "not-a-list",
        }
        result = _get_index_fields(data)
        assert result == []

    def test_int_index_fields_returns_empty(self) -> None:
        data: dict[str, Any] = {
            "name": "val",
            "_index_fields": 42,
        }
        result = _get_index_fields(data)
        assert result == []

    def test_auto_indexes_non_special_fields(self) -> None:
        data: dict[str, Any] = {
            "name": "alice",
            "status": "active",
            "score": 100,
        }
        result = _get_index_fields(data)
        assert sorted(result) == ["name", "score", "status"]

    def test_excludes_underscore_prefixed(self) -> None:
        data: dict[str, Any] = {
            "name": "val",
            "_private": "hidden",
            "_ttl": 300,
        }
        result = _get_index_fields(data)
        assert result == ["name"]

    def test_excludes_created_at(self) -> None:
        data: dict[str, Any] = {
            "name": "val",
            "created_at": "2024-01-01",
        }
        result = _get_index_fields(data)
        assert "created_at" not in result
        assert result == ["name"]

    def test_excludes_updated_at(self) -> None:
        data: dict[str, Any] = {
            "name": "val",
            "updated_at": "2024-01-01",
        }
        result = _get_index_fields(data)
        assert "updated_at" not in result

    def test_excludes_uid(self) -> None:
        data: dict[str, Any] = {
            "uid": "abc",
            "name": "val",
        }
        result = _get_index_fields(data)
        assert "uid" not in result
        assert result == ["name"]

    def test_excludes_id(self) -> None:
        data: dict[str, Any] = {
            "id": "abc",
            "name": "val",
        }
        result = _get_index_fields(data)
        assert "id" not in result
        assert result == ["name"]

    def test_empty_dict_returns_empty(self) -> None:
        result = _get_index_fields({})
        assert result == []

    def test_only_special_fields_returns_empty(self) -> None:
        data: dict[str, Any] = {
            "uid": "x",
            "id": "y",
            "created_at": "t",
            "updated_at": "t",
            "_meta": "z",
        }
        result = _get_index_fields(data)
        assert result == []
