"""Tests for Redis sub-module operations (read, delete, inmem, util)."""

from __future__ import annotations

import json
from datetime import datetime
from enum import Enum
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from ami.core.exceptions import StorageError
from ami.implementations.mem.redis_delete import clear_collection, delete
from ami.implementations.mem.redis_inmem import expire, touch
from ami.implementations.mem.redis_read import (
    count,
    get_metadata,
    list_all,
    query,
    read,
)
from ami.implementations.mem.redis_util import (
    create_indexes,
    delete_indexes,
    deserialize_data,
    make_index_key,
    make_key,
    make_metadata_key,
    serialize_data,
    update_indexes,
)

_PFX = "mem:items:"
_ID = "item-123"
_TTL = 300
_TWO = 2
_THREE = 3
_FIVE = 5


def _dao(client: AsyncMock | None = None) -> MagicMock:
    """Build a minimal DAO mock."""
    d = MagicMock()
    d._key_prefix = _PFX
    d.collection_name = "items"
    d.client = client
    d.connect = AsyncMock()
    return d


def _scan(keys: list[str]) -> Any:
    """Return an async generator factory that yields keys."""

    async def _gen(**_kw: Any) -> Any:
        for k in keys:
            yield k

    return _gen


async def _bad_scan(**_kw: Any) -> Any:
    """Async generator that raises before yielding."""
    raise ConnectionError
    yield  # pragma: no cover


def _jp(obj: Any) -> str:
    return json.dumps(obj)


class TestKeyBuilders:
    """Verify key construction helpers."""

    def test_make_key(self) -> None:
        assert make_key(_PFX, _ID) == f"{_PFX}{_ID}"

    def test_make_key_empty_prefix(self) -> None:
        assert make_key("", "abc") == "abc"

    def test_make_metadata_key(self) -> None:
        assert make_metadata_key(_PFX, _ID) == f"{_PFX}meta:{_ID}"

    def test_make_index_key(self) -> None:
        assert make_index_key(_PFX, "s", "on") == f"{_PFX}idx:s:on"


class TestSerializeData:
    """Verify JSON serialization with custom handler."""

    def test_dict(self) -> None:
        data: dict[str, Any] = {"name": "t", "value": 1}
        assert json.loads(serialize_data(data)) == data

    def test_datetime_iso(self) -> None:
        now = datetime(2025, 1, 15, 12, 0, 0)
        assert json.loads(serialize_data({"ts": now}))["ts"] == now.isoformat()

    def test_enum_value(self) -> None:
        class Color(Enum):
            RED = "red"

        assert json.loads(serialize_data({"c": Color.RED}))["c"] == "red"

    def test_unserializable_raises(self) -> None:
        with pytest.raises(ValueError, match="serialization failed"):
            serialize_data({"bad": object()})


class TestDeserializeData:
    """Verify JSON deserialization."""

    def test_valid(self) -> None:
        assert deserialize_data('{"a": 1}') == {"a": 1}

    def test_invalid_raises(self) -> None:
        with pytest.raises(ValueError, match="deserialization"):
            deserialize_data("{{{")

    def test_empty_raises(self) -> None:
        with pytest.raises(ValueError, match="deserialization"):
            deserialize_data("")


class TestCreateIndexes:
    """Verify index creation via sadd."""

    @pytest.mark.asyncio
    async def test_matching_fields(self) -> None:
        c = AsyncMock()
        await create_indexes(c, _PFX, _ID, {"s": "on", "k": "d"}, ["s", "k"])
        assert c.sadd.await_count == _TWO

    @pytest.mark.asyncio
    async def test_skips_missing(self) -> None:
        c = AsyncMock()
        await create_indexes(c, _PFX, _ID, {"s": "on"}, ["s", "x"])
        c.sadd.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_empty_fields(self) -> None:
        c = AsyncMock()
        await create_indexes(c, _PFX, _ID, {}, [])
        c.sadd.assert_not_awaited()


class TestUpdateIndexes:
    """Verify index update with old value removal."""

    @pytest.mark.asyncio
    async def test_removes_old_adds_new(self) -> None:
        c = AsyncMock()
        c.get = AsyncMock(return_value=_jp({"s": "draft"}))
        await update_indexes(c, _PFX, _ID, {"s": "pub"}, ["s"])
        c.srem.assert_awaited_once()
        c.sadd.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_existing(self) -> None:
        c = AsyncMock()
        c.get = AsyncMock(return_value=None)
        await update_indexes(c, _PFX, _ID, {"s": "on"}, ["s"])
        c.srem.assert_not_awaited()
        c.sadd.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_corrupted_raises(self) -> None:
        c = AsyncMock()
        c.get = AsyncMock(return_value="{{{")
        with pytest.raises(ValueError, match="deserialization"):
            await update_indexes(c, _PFX, _ID, {"x": 1}, ["x"])


class TestDeleteIndexes:
    """Verify index deletion via scan + srem."""

    @pytest.mark.asyncio
    async def test_removes_all(self) -> None:
        c = AsyncMock()
        c.scan_iter = _scan([f"{_PFX}idx:s:on", f"{_PFX}idx:k:d"])
        await delete_indexes(c, _PFX, _ID)
        assert c.srem.await_count == _TWO

    @pytest.mark.asyncio
    async def test_no_keys(self) -> None:
        c = AsyncMock()
        c.scan_iter = _scan([])
        await delete_indexes(c, _PFX, _ID)
        c.srem.assert_not_awaited()


class TestRead:
    """Verify read() fetch, deserialize, metadata update."""

    @pytest.mark.asyncio
    async def test_success(self) -> None:
        c = AsyncMock()
        c.get = AsyncMock(return_value=_jp({"n": "t"}))
        c.hset = AsyncMock()
        assert await read(_dao(c), _ID) == {"n": "t"}

    @pytest.mark.asyncio
    async def test_missing(self) -> None:
        c = AsyncMock()
        c.get = AsyncMock(return_value=None)
        assert await read(_dao(c), _ID) is None

    @pytest.mark.asyncio
    async def test_auto_connect(self) -> None:
        d = _dao(client=None)
        rc = AsyncMock()
        rc.get = AsyncMock(return_value=None)

        async def _set() -> None:
            d.client = rc

        d.connect.side_effect = _set
        assert await read(d, _ID) is None
        d.connect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_redis_error(self) -> None:
        c = AsyncMock()
        c.get = AsyncMock(side_effect=ConnectionError("x"))
        with pytest.raises(StorageError, match="Failed to read"):
            await read(_dao(c), _ID)

    @pytest.mark.asyncio
    async def test_corrupt_data(self) -> None:
        c = AsyncMock()
        c.get = AsyncMock(return_value="bad")
        with pytest.raises(StorageError, match="Data corruption"):
            await read(_dao(c), _ID)

    @pytest.mark.asyncio
    async def test_metadata_failure_ok(self) -> None:
        c = AsyncMock()
        c.get = AsyncMock(return_value=_jp({"ok": True}))
        c.hset = AsyncMock(side_effect=ConnectionError("x"))
        assert await read(_dao(c), _ID) == {"ok": True}


class TestQuery:
    """Verify query() with and without filters."""

    @pytest.mark.asyncio
    async def test_with_filters(self) -> None:
        c = AsyncMock()
        c.smembers = AsyncMock(return_value={_ID})
        c.get = AsyncMock(return_value=_jp({"s": "on"}))
        c.hset = AsyncMock()
        assert len(await query(_dao(c), {"s": "on"})) == 1

    @pytest.mark.asyncio
    async def test_no_filters_scans(self) -> None:
        c = AsyncMock()
        c.scan_iter = _scan([f"{_PFX}a", f"{_PFX}b"])
        c.get = AsyncMock(return_value=_jp({"v": 1}))
        assert len(await query(_dao(c))) == _TWO

    @pytest.mark.asyncio
    async def test_excludes_meta_idx(self) -> None:
        c = AsyncMock()
        c.scan_iter = _scan([f"{_PFX}a", f"{_PFX}meta:a", f"{_PFX}idx:f:v"])
        c.get = AsyncMock(return_value=_jp({"v": 1}))
        assert len(await query(_dao(c))) == 1

    @pytest.mark.asyncio
    async def test_filter_intersection(self) -> None:
        c = AsyncMock()
        c.smembers = AsyncMock(side_effect=[{"a", "b", "c"}, {"b", "c", "d"}])
        c.get = AsyncMock(return_value=_jp({"v": 1}))
        c.hset = AsyncMock()
        assert len(await query(_dao(c), {"f1": "1", "f2": "2"})) == _TWO

    @pytest.mark.asyncio
    async def test_empty(self) -> None:
        c = AsyncMock()
        c.smembers = AsyncMock(return_value=set())
        assert await query(_dao(c), {"s": "x"}) == []

    @pytest.mark.asyncio
    async def test_index_error(self) -> None:
        c = AsyncMock()
        c.smembers = AsyncMock(side_effect=ConnectionError("x"))
        with pytest.raises(StorageError, match="Failed to query"):
            await query(_dao(c), {"s": "x"})


class TestListAll:
    """Verify list_all() pagination and error handling."""

    @pytest.mark.asyncio
    async def test_paginated(self) -> None:
        c = AsyncMock()
        c.scan_iter = _scan([f"{_PFX}{i}" for i in range(_FIVE)])
        c.get = AsyncMock(return_value=_jp({"v": 1}))
        assert len(await list_all(_dao(c), limit=_THREE)) == _THREE

    @pytest.mark.asyncio
    async def test_offset(self) -> None:
        c = AsyncMock()
        c.scan_iter = _scan([f"{_PFX}{i}" for i in range(_FIVE)])
        c.get = AsyncMock(return_value=_jp({"v": 1}))
        result = await list_all(_dao(c), limit=100, offset=_THREE)
        assert len(result) == _TWO

    @pytest.mark.asyncio
    async def test_skips_corrupted(self) -> None:
        c = AsyncMock()
        c.scan_iter = _scan([f"{_PFX}a", f"{_PFX}b"])
        c.get = AsyncMock(side_effect=["bad", _jp({"ok": True})])
        assert len(await list_all(_dao(c))) == 1

    @pytest.mark.asyncio
    async def test_scan_error(self) -> None:
        c = AsyncMock()
        c.scan_iter = _bad_scan
        with pytest.raises(StorageError, match="Failed to list"):
            await list_all(_dao(c))


class TestCount:
    """Verify count() with and without filters."""

    @pytest.mark.asyncio
    async def test_with_filters(self) -> None:
        c = AsyncMock()
        c.smembers = AsyncMock(return_value={"a", "b", "c"})
        assert await count(_dao(c), {"s": "on"}) == _THREE

    @pytest.mark.asyncio
    async def test_without_filters(self) -> None:
        c = AsyncMock()
        c.scan_iter = _scan([f"{_PFX}a", f"{_PFX}b"])
        assert await count(_dao(c)) == _TWO

    @pytest.mark.asyncio
    async def test_excludes_meta_idx(self) -> None:
        c = AsyncMock()
        c.scan_iter = _scan([f"{_PFX}a", f"{_PFX}meta:a", f"{_PFX}idx:f:v"])
        assert await count(_dao(c)) == 1

    @pytest.mark.asyncio
    async def test_intersection(self) -> None:
        c = AsyncMock()
        c.smembers = AsyncMock(side_effect=[{"a", "b"}, {"b", "c"}])
        assert await count(_dao(c), {"f1": "1", "f2": "2"}) == 1

    @pytest.mark.asyncio
    async def test_index_error(self) -> None:
        c = AsyncMock()
        c.smembers = AsyncMock(side_effect=ConnectionError("x"))
        with pytest.raises(StorageError, match="Failed to count"):
            await count(_dao(c), {"s": "x"})


class TestGetMetadata:
    """Verify get_metadata() retrieval."""

    @pytest.mark.asyncio
    async def test_returns_dict(self) -> None:
        c = AsyncMock()
        c.hgetall = AsyncMock(return_value={"ttl": "300"})
        assert await get_metadata(_dao(c), _ID) == {"ttl": "300"}

    @pytest.mark.asyncio
    async def test_empty_returns_none(self) -> None:
        c = AsyncMock()
        c.hgetall = AsyncMock(return_value={})
        assert await get_metadata(_dao(c), _ID) is None

    @pytest.mark.asyncio
    async def test_failure(self) -> None:
        c = AsyncMock()
        c.hgetall = AsyncMock(side_effect=ConnectionError("x"))
        with pytest.raises(StorageError, match="Failed to get metadata"):
            await get_metadata(_dao(c), _ID)


class TestDelete:
    """Verify delete() removes keys and indexes."""

    @pytest.mark.asyncio
    async def test_true(self) -> None:
        c = AsyncMock()
        c.delete = AsyncMock(return_value=1)
        c.scan_iter = _scan([])
        assert await delete(_dao(c), _ID) is True

    @pytest.mark.asyncio
    async def test_false_when_missing(self) -> None:
        c = AsyncMock()
        c.delete = AsyncMock(return_value=0)
        c.scan_iter = _scan([])
        assert await delete(_dao(c), _ID) is False

    @pytest.mark.asyncio
    async def test_failure(self) -> None:
        c = AsyncMock()
        c.delete = AsyncMock(side_effect=ConnectionError("x"))
        with pytest.raises(StorageError, match="Failed to delete"):
            await delete(_dao(c), _ID)

    @pytest.mark.asyncio
    async def test_auto_connect(self) -> None:
        d = _dao(client=None)
        rc = AsyncMock()
        rc.delete = AsyncMock(return_value=0)
        rc.scan_iter = _scan([])

        async def _set() -> None:
            d.client = rc

        d.connect.side_effect = _set
        await delete(d, _ID)
        d.connect.assert_awaited_once()


class TestClearCollection:
    """Verify clear_collection() scans and deletes all keys."""

    @pytest.mark.asyncio
    async def test_deleted_count(self) -> None:
        c = AsyncMock()
        c.scan_iter = _scan([f"{_PFX}a", f"{_PFX}b"])
        c.delete = AsyncMock(return_value=_TWO)
        assert await clear_collection(_dao(c)) == _TWO

    @pytest.mark.asyncio
    async def test_empty(self) -> None:
        c = AsyncMock()
        c.scan_iter = _scan([])
        assert await clear_collection(_dao(c)) == 0

    @pytest.mark.asyncio
    async def test_failure(self) -> None:
        c = AsyncMock()
        c.scan_iter = _bad_scan
        with pytest.raises(StorageError, match="Failed to clear"):
            await clear_collection(_dao(c))


class TestExpire:
    """Verify expire() sets TTL and updates metadata."""

    @pytest.mark.asyncio
    async def test_true(self) -> None:
        c = AsyncMock()
        c.expire = AsyncMock(return_value=True)
        c.hset = AsyncMock()
        assert await expire(_dao(c), _ID, _TTL) is True
        c.hset.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_false_when_missing(self) -> None:
        c = AsyncMock()
        c.expire = AsyncMock(return_value=False)
        assert await expire(_dao(c), _ID, _TTL) is False

    @pytest.mark.asyncio
    async def test_failure(self) -> None:
        c = AsyncMock()
        c.expire = AsyncMock(side_effect=ConnectionError("x"))
        with pytest.raises(StorageError, match="Failed to set TTL"):
            await expire(_dao(c), _ID, _TTL)


class TestTouch:
    """Verify touch() resets TTL from metadata."""

    @pytest.mark.asyncio
    async def test_resets_ttl(self) -> None:
        c = AsyncMock()
        c.hget = AsyncMock(return_value=str(_TTL))
        c.expire = AsyncMock(return_value=True)
        c.hset = AsyncMock()
        assert await touch(_dao(c), _ID) is True
        c.expire.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_ttl(self) -> None:
        c = AsyncMock()
        c.hget = AsyncMock(return_value=None)
        assert await touch(_dao(c), _ID) is False

    @pytest.mark.asyncio
    async def test_expire_fails(self) -> None:
        c = AsyncMock()
        c.hget = AsyncMock(return_value=str(_TTL))
        c.expire = AsyncMock(return_value=False)
        assert await touch(_dao(c), _ID) is False

    @pytest.mark.asyncio
    async def test_failure(self) -> None:
        c = AsyncMock()
        c.hget = AsyncMock(side_effect=ConnectionError("x"))
        with pytest.raises(StorageError, match="Failed to touch"):
            await touch(_dao(c), _ID)
