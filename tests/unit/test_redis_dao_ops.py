"""Tests for RedisDAO operations with mocked sub-modules."""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.core.exceptions import StorageError
from ami.core.storage_types import StorageType
from ami.implementations.mem.redis_dao import RedisDAO
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

# -- Constants / test model / fixtures --

_VAL = 42
_CLR = 5
_F1 = 7
_BLK = 2

_CFG = StorageConfig(
    storage_type=StorageType.INMEM,
    host="127.0.0.1",
    port=6379,
    database="0",
)


class _TM(StorageModel):
    """Minimal model for DAO tests."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="test_items",
    )
    name: str = "default"
    value: int = 0


@pytest.fixture
def dao() -> RedisDAO:
    """Build a RedisDAO with a mocked async client."""
    d = RedisDAO(_TM, _CFG)
    d.client = AsyncMock()
    return d


_M = "ami.implementations.mem.redis_dao"
_R = f"{_M}.redis"
_CR = f"{_M}.redis_create.create"
_RD = f"{_M}.redis_read.read"
_QR = f"{_M}.redis_read.query"
_UP = f"{_M}.redis_update.update"
_DL = f"{_M}.redis_delete.delete"
_CC = f"{_M}.redis_delete.clear_collection"
_CN = f"{_M}.redis_read.count"


def _am(rv: Any = None) -> AsyncMock:
    """Shorthand for AsyncMock with return_value."""
    return AsyncMock(return_value=rv)


class TestConnect:
    """Verify connect() creates a Redis client and pings."""

    @pytest.mark.asyncio
    async def test_creates_client(self) -> None:
        d = RedisDAO(_TM, _CFG)
        mc = AsyncMock()
        mc.ping = _am(True)
        with patch(f"{_R}.Redis", return_value=mc):
            await d.connect()
        assert d.client is mc

    @pytest.mark.asyncio
    async def test_skips_when_connected(self, dao: RedisDAO) -> None:
        existing = dao.client
        with patch(f"{_R}.Redis") as cm:
            await dao.connect()
        cm.assert_not_called()
        assert dao.client is existing

    @pytest.mark.asyncio
    async def test_awaits_async_ping(self) -> None:
        d = RedisDAO(_TM, _CFG)
        mc = MagicMock()
        mc.ping.return_value = AsyncMock()()
        with patch(f"{_R}.Redis", return_value=mc):
            await d.connect()
        assert d.client is mc


class TestDisconnect:
    """Verify disconnect() closes and clears the client."""

    @pytest.mark.asyncio
    async def test_calls_aclose(self, dao: RedisDAO) -> None:
        await dao.disconnect()
        assert dao.client is None

    @pytest.mark.asyncio
    async def test_noop_without_client(self) -> None:
        d = RedisDAO(_TM, _CFG)
        await d.disconnect()
        assert d.client is None


class TestCreate:
    """Verify create() delegates to redis_create."""

    @pytest.mark.asyncio
    async def test_returns_uid(self, dao: RedisDAO) -> None:
        inst = _TM(name="item-1", value=10)
        with patch(_CR, new_callable=AsyncMock) as m:
            m.return_value = "uid-123"
            result = await dao.create(inst)
        assert result == "uid-123"
        m.assert_awaited_once_with(dao, inst)

    @pytest.mark.asyncio
    async def test_propagates_error(self, dao: RedisDAO) -> None:
        with patch(_CR, new_callable=AsyncMock) as m:
            m.side_effect = StorageError("boom")
            with pytest.raises(StorageError, match="boom"):
                await dao.create(_TM())


class TestRead:
    """Verify read() delegates and wraps result in model."""

    @pytest.mark.asyncio
    async def test_returns_model(self, dao: RedisDAO) -> None:
        raw = {"name": "found", "value": _VAL}
        with patch(_RD, new_callable=AsyncMock, return_value=raw):
            result = await dao.read("id-1")
        assert isinstance(result, _TM)
        assert result.name == "found"
        assert result.value == _VAL

    @pytest.mark.asyncio
    async def test_returns_none(self, dao: RedisDAO) -> None:
        with patch(_RD, new_callable=AsyncMock, return_value=None):
            assert await dao.read("missing") is None


class TestQuery:
    """Verify query() delegates to redis_read.query."""

    @pytest.mark.asyncio
    async def test_returns_list(self, dao: RedisDAO) -> None:
        rows: list[dict[str, Any]] = [
            {"name": "a", "value": 1},
            {"name": "b", "value": _BLK},
        ]
        with patch(_QR, new_callable=AsyncMock, return_value=rows):
            assert await dao.query({"name": "a"}) == rows

    @pytest.mark.asyncio
    async def test_no_filters(self, dao: RedisDAO) -> None:
        with patch(_QR, new_callable=AsyncMock, return_value=[]):
            assert await dao.query() == []


class TestUpdate:
    """Verify update() delegates to redis_update."""

    @pytest.mark.asyncio
    async def test_delegates(self, dao: RedisDAO) -> None:
        data: dict[str, Any] = {"name": "changed"}
        with patch(_UP, new_callable=AsyncMock) as m:
            await dao.update("id-1", data)
        m.assert_awaited_once_with(dao, "id-1", data)


class TestDelete:
    """Verify delete() delegates to redis_delete."""

    @pytest.mark.asyncio
    async def test_returns_true(self, dao: RedisDAO) -> None:
        with patch(_DL, new_callable=AsyncMock, return_value=True):
            assert await dao.delete("id-1") is True

    @pytest.mark.asyncio
    async def test_returns_false(self, dao: RedisDAO) -> None:
        with patch(_DL, new_callable=AsyncMock, return_value=False):
            assert await dao.delete("gone") is False


class TestClearCollection:
    """Verify clear_collection() delegates."""

    @pytest.mark.asyncio
    async def test_returns_count(self, dao: RedisDAO) -> None:
        with patch(_CC, new_callable=AsyncMock, return_value=_CLR):
            assert await dao.clear_collection() == _CLR


class TestFindById:
    """Verify find_by_id() is an alias for read()."""

    @pytest.mark.asyncio
    async def test_delegates_to_read(self, dao: RedisDAO) -> None:
        raw: dict[str, Any] = {"name": "x", "value": 9}
        with patch(_RD, new_callable=AsyncMock, return_value=raw):
            result = await dao.find_by_id("id-1")
        assert isinstance(result, _TM)
        assert result.name == "x"


class TestFind:
    """Verify find() delegates to query and wraps models."""

    @pytest.mark.asyncio
    async def test_wraps_in_model(self, dao: RedisDAO) -> None:
        rows: list[dict[str, Any]] = [{"name": "r", "value": 1}]
        with patch(_QR, new_callable=AsyncMock, return_value=rows):
            result = await dao.find({"name": "r"})
        assert len(result) == 1
        assert isinstance(result[0], _TM)

    @pytest.mark.asyncio
    async def test_empty_returns_empty(self, dao: RedisDAO) -> None:
        with patch(_QR, new_callable=AsyncMock, return_value=[]):
            assert await dao.find({"name": "no"}) == []


class TestFindOne:
    """Verify find_one() returns first or None."""

    @pytest.mark.asyncio
    async def test_returns_model(self, dao: RedisDAO) -> None:
        rows: list[dict[str, Any]] = [
            {"name": "f", "value": _F1},
        ]
        with patch(_QR, new_callable=AsyncMock, return_value=rows):
            result = await dao.find_one({"name": "f"})
        assert isinstance(result, _TM)
        assert result.value == _F1

    @pytest.mark.asyncio
    async def test_returns_none(self, dao: RedisDAO) -> None:
        with patch(_QR, new_callable=AsyncMock, return_value=[]):
            assert await dao.find_one({"name": "x"}) is None


class TestCount:
    """Verify count() delegates to redis_read.count."""

    @pytest.mark.asyncio
    async def test_returns_int(self, dao: RedisDAO) -> None:
        with patch(_CN, new_callable=AsyncMock, return_value=_VAL):
            assert await dao.count() == _VAL


class TestExists:
    """Verify exists() delegates to read()."""

    @pytest.mark.asyncio
    async def test_true_when_found(self, dao: RedisDAO) -> None:
        raw: dict[str, Any] = {"name": "e", "value": 0}
        with patch(_RD, new_callable=AsyncMock, return_value=raw):
            assert await dao.exists("id-1") is True

    @pytest.mark.asyncio
    async def test_false_when_missing(self, dao: RedisDAO) -> None:
        with patch(_RD, new_callable=AsyncMock, return_value=None):
            assert await dao.exists("id-x") is False


class TestBulkCreate:
    """Verify bulk_create() creates each instance."""

    @pytest.mark.asyncio
    async def test_returns_ids(self, dao: RedisDAO) -> None:
        items = [_TM(name="a"), _TM(name="b")]
        with patch(_CR, new_callable=AsyncMock) as m:
            m.side_effect = ["id-a", "id-b"]
            ids = await dao.bulk_create(items)
        assert ids == ["id-a", "id-b"]
        assert m.await_count == _BLK

    @pytest.mark.asyncio
    async def test_raises_on_failure(self, dao: RedisDAO) -> None:
        with patch(_CR, new_callable=AsyncMock) as m:
            m.side_effect = StorageError("fail")
            with pytest.raises(StorageError, match="Bulk create"):
                await dao.bulk_create([_TM()])


class TestBulkDelete:
    """Verify bulk_delete() deletes each and counts."""

    @pytest.mark.asyncio
    async def test_partial_raises(self, dao: RedisDAO) -> None:
        with patch(_DL, new_callable=AsyncMock) as m:
            m.side_effect = [True, True, False]
            with pytest.raises(StorageError, match="Bulk delete"):
                await dao.bulk_delete(["a", "b", "c"])

    @pytest.mark.asyncio
    async def test_all_ok(self, dao: RedisDAO) -> None:
        with patch(_DL, new_callable=AsyncMock) as m:
            m.side_effect = [True, True]
            count = await dao.bulk_delete(["a", "b"])
        assert count == _BLK


class TestRawReadQuery:
    """Verify raw_read_query() validates commands."""

    @pytest.mark.asyncio
    async def test_not_connected_raises(self) -> None:
        d = RedisDAO(_TM, _CFG)
        with pytest.raises(StorageError, match="Not connected"):
            await d.raw_read_query("GET mykey")

    @pytest.mark.asyncio
    async def test_get_command(self, dao: RedisDAO) -> None:
        dao.client.get = _am("val-1")
        result = await dao.raw_read_query("GET mykey")
        assert result == [{"value": "val-1"}]

    @pytest.mark.asyncio
    async def test_get_none_returns_empty(self, dao: RedisDAO) -> None:
        dao.client.get = _am(None)
        assert await dao.raw_read_query("GET mykey") == []

    @pytest.mark.asyncio
    async def test_unsupported_raises(self, dao: RedisDAO) -> None:
        with pytest.raises(StorageError, match="Unsupported"):
            await dao.raw_read_query("FLUSHDB")


class TestRawWriteQuery:
    """Verify raw_write_query() validates and executes."""

    @pytest.mark.asyncio
    async def test_set_command(self, dao: RedisDAO) -> None:
        dao.client.set = _am(True)
        result = await dao.raw_write_query("SET k1 v1")
        assert result == 1
        dao.client.set.assert_awaited_once_with("k1", "v1")

    @pytest.mark.asyncio
    async def test_unsupported_raises(self, dao: RedisDAO) -> None:
        with pytest.raises(StorageError, match="Unsupported"):
            await dao.raw_write_query("LPUSH list val")

    @pytest.mark.asyncio
    async def test_not_connected_raises(self) -> None:
        d = RedisDAO(_TM, _CFG)
        with pytest.raises(StorageError, match="Not connected"):
            await d.raw_write_query("SET k v")


class TestTestConnection:
    """Verify test_connection() pings Redis."""

    @pytest.mark.asyncio
    async def test_ping_returns_true(self, dao: RedisDAO) -> None:
        dao.client.ping = MagicMock(return_value=True)
        assert await dao.test_connection() is True

    @pytest.mark.asyncio
    async def test_no_client_raises(self) -> None:
        d = RedisDAO(_TM, _CFG)
        with pytest.raises(
            StorageError,
            match="Client not initialized",
        ):
            await d.test_connection()
