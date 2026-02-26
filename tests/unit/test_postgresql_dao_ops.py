"""Tests for PostgreSQLDAO CRUD and lifecycle operations."""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.core.exceptions import (
    QueryError,
    StorageValidationError,
)
from ami.core.storage_types import StorageType
from ami.implementations.sql.postgresql_dao import PostgreSQLDAO
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

# ---------------------------------------------------------------
# Constants
# ---------------------------------------------------------------

_UID_A = "uid-aaa-111"
_UID_B = "uid-bbb-222"
_UID_C = "uid-ccc-333"
_PG_PORT = 5432
_THREE = 3
_TWO = 2
_FIVE = 5
_HYDRATED_VALUE = 99

# ---------------------------------------------------------------
# Test model and config
# ---------------------------------------------------------------

_TEST_CONFIG = StorageConfig(
    storage_type=StorageType.RELATIONAL,
    host="localhost",
    port=_PG_PORT,
    database="testdb",
    username="user",
    password="pass",
)


class _TestModel(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="test_items",
    )
    name: str = ""
    value: int = 0


# ---------------------------------------------------------------
# Shared patch paths
# ---------------------------------------------------------------

_MOD = "ami.implementations.sql"
_PG_CREATE = f"{_MOD}.postgresql_create.create"
_PG_READ = f"{_MOD}.postgresql_read.read"
_PG_QUERY = f"{_MOD}.postgresql_read.query"
_PG_COUNT = f"{_MOD}.postgresql_read.count"
_PG_UPDATE = f"{_MOD}.postgresql_update.update"
_PG_DELETE = f"{_MOD}.postgresql_delete.delete"
_ASYNCPG_POOL = "asyncpg.create_pool"


def _make_dao() -> PostgreSQLDAO:
    return PostgreSQLDAO(_TestModel, _TEST_CONFIG)


def _row(uid: str, name: str = "x", val: int = 1) -> dict:
    return {"uid": uid, "name": name, "value": val}


def _pool_with_conn(conn: AsyncMock) -> MagicMock:
    """Build a mock pool whose acquire() yields *conn*."""
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool = MagicMock()
    pool.acquire.return_value = ctx
    pool.close = AsyncMock()
    return pool


# ---------------------------------------------------------------
# TestConnect
# ---------------------------------------------------------------


class TestConnect:
    """Verify pool creation via asyncpg.create_pool."""

    async def test_connect_creates_pool(self) -> None:
        dao = _make_dao()
        mock_pool = AsyncMock()
        with patch(_ASYNCPG_POOL, new=AsyncMock(return_value=mock_pool)):
            await dao.connect()
        assert dao.pool is mock_pool

    async def test_connect_skips_when_pool_exists(self) -> None:
        dao = _make_dao()
        existing = AsyncMock()
        dao.pool = existing
        await dao.connect()
        assert dao.pool is existing


# ---------------------------------------------------------------
# TestDisconnect
# ---------------------------------------------------------------


class TestDisconnect:
    """Verify pool teardown."""

    async def test_disconnect_closes_pool(self) -> None:
        dao = _make_dao()
        mock_pool = AsyncMock()
        dao.pool = mock_pool
        await dao.disconnect()
        mock_pool.close.assert_awaited_once()
        assert dao.pool is None

    async def test_disconnect_noop_without_pool(self) -> None:
        dao = _make_dao()
        await dao.disconnect()
        assert dao.pool is None


# ---------------------------------------------------------------
# TestCreate
# ---------------------------------------------------------------


class TestCreate:
    """Verify create delegates to postgresql_create."""

    async def test_create_with_dict(self) -> None:
        dao = _make_dao()
        with patch(_PG_CREATE, new=AsyncMock(return_value=_UID_A)):
            result = await dao.create({"name": "a", "value": 1})
        assert result == _UID_A

    async def test_create_with_model_instance(self) -> None:
        dao = _make_dao()
        inst = _TestModel(name="b", value=2)
        with patch(_PG_CREATE, new=AsyncMock(return_value=_UID_B)):
            result = await dao.create(inst)
        assert result == _UID_B


# ---------------------------------------------------------------
# TestFindById
# ---------------------------------------------------------------


class TestFindById:
    """Verify find_by_id reads then hydrates."""

    async def test_returns_model_when_found(self) -> None:
        dao = _make_dao()
        row = _row(_UID_A, "found", 10)
        with patch(_PG_READ, new=AsyncMock(return_value=row)):
            obj = await dao.find_by_id(_UID_A)
        assert obj is not None
        assert obj.uid == _UID_A
        assert obj.name == "found"

    async def test_returns_none_when_missing(self) -> None:
        dao = _make_dao()
        with patch(_PG_READ, new=AsyncMock(return_value=None)):
            assert await dao.find_by_id("nope") is None


# ---------------------------------------------------------------
# TestFind
# ---------------------------------------------------------------


class TestFind:
    """Verify find applies skip/limit after query."""

    async def test_find_returns_all(self) -> None:
        dao = _make_dao()
        rows = [_row(_UID_A), _row(_UID_B), _row(_UID_C)]
        with patch(_PG_QUERY, new=AsyncMock(return_value=rows)):
            results = await dao.find({})
        assert len(results) == _THREE

    async def test_find_with_skip(self) -> None:
        dao = _make_dao()
        rows = [_row(_UID_A), _row(_UID_B), _row(_UID_C)]
        with patch(_PG_QUERY, new=AsyncMock(return_value=rows)):
            results = await dao.find({}, skip=1)
        assert len(results) == _TWO
        assert results[0].uid == _UID_B

    async def test_find_with_limit(self) -> None:
        dao = _make_dao()
        rows = [_row(_UID_A), _row(_UID_B), _row(_UID_C)]
        with patch(_PG_QUERY, new=AsyncMock(return_value=rows)):
            results = await dao.find({}, limit=_TWO)
        assert len(results) == _TWO

    async def test_find_with_skip_and_limit(self) -> None:
        dao = _make_dao()
        rows = [_row(_UID_A), _row(_UID_B), _row(_UID_C)]
        with patch(_PG_QUERY, new=AsyncMock(return_value=rows)):
            results = await dao.find({}, skip=1, limit=1)
        assert len(results) == 1
        assert results[0].uid == _UID_B

    async def test_find_empty(self) -> None:
        dao = _make_dao()
        with patch(_PG_QUERY, new=AsyncMock(return_value=[])):
            assert await dao.find({}) == []


# ---------------------------------------------------------------
# TestUpdate
# ---------------------------------------------------------------


class TestUpdate:
    """Verify update delegates to postgresql_update."""

    async def test_update_calls_module(self) -> None:
        dao = _make_dao()
        mock_up = AsyncMock()
        with patch(_PG_UPDATE, new=mock_up):
            await dao.update(_UID_A, {"name": "new"})
        mock_up.assert_awaited_once_with(dao, _UID_A, {"name": "new"})


# ---------------------------------------------------------------
# TestDelete
# ---------------------------------------------------------------


class TestDelete:
    """Verify delete delegates and returns bool."""

    async def test_delete_returns_true(self) -> None:
        dao = _make_dao()
        with patch(_PG_DELETE, new=AsyncMock(return_value=True)):
            assert await dao.delete(_UID_A) is True

    async def test_delete_returns_false(self) -> None:
        dao = _make_dao()
        with patch(_PG_DELETE, new=AsyncMock(return_value=False)):
            assert await dao.delete(_UID_A) is False


# ---------------------------------------------------------------
# TestCount
# ---------------------------------------------------------------


class TestCount:
    """Verify count delegates to postgresql_read.count."""

    async def test_count_with_query(self) -> None:
        dao = _make_dao()
        with patch(_PG_COUNT, new=AsyncMock(return_value=_FIVE)):
            assert await dao.count({"active": True}) == _FIVE

    async def test_count_with_empty_query(self) -> None:
        dao = _make_dao()
        mock_cnt = AsyncMock(return_value=0)
        with patch(_PG_COUNT, new=mock_cnt):
            assert await dao.count({}) == 0
        mock_cnt.assert_awaited_once_with(dao, None)


# ---------------------------------------------------------------
# TestExists
# ---------------------------------------------------------------


class TestExists:
    """Verify exists uses read to check presence."""

    async def test_exists_true(self) -> None:
        dao = _make_dao()
        with patch(_PG_READ, new=AsyncMock(return_value=_row(_UID_A))):
            assert await dao.exists(_UID_A) is True

    async def test_exists_false(self) -> None:
        dao = _make_dao()
        with patch(_PG_READ, new=AsyncMock(return_value=None)):
            assert await dao.exists("missing") is False


# ---------------------------------------------------------------
# TestBulkCreate
# ---------------------------------------------------------------


class TestBulkCreate:
    """Verify bulk_create iterates and collects IDs."""

    async def test_bulk_create_returns_ids(self) -> None:
        dao = _make_dao()
        side = [_UID_A, _UID_B]
        with patch(_PG_CREATE, new=AsyncMock(side_effect=side)):
            ids = await dao.bulk_create(
                [
                    {"name": "a"},
                    {"name": "b"},
                ]
            )
        assert ids == [_UID_A, _UID_B]


# ---------------------------------------------------------------
# TestBulkDelete
# ---------------------------------------------------------------


class TestBulkDelete:
    """Verify bulk_delete counts successful deletions."""

    async def test_bulk_delete_counts(self) -> None:
        dao = _make_dao()
        side = [True, False, True]
        with patch(_PG_DELETE, new=AsyncMock(side_effect=side)):
            count = await dao.bulk_delete([_UID_A, _UID_B, _UID_C])
        assert count == _TWO


# ---------------------------------------------------------------
# TestRawReadQuery
# ---------------------------------------------------------------


class TestRawReadQuery:
    """Verify raw_read_query acquires conn and fetches."""

    async def test_raw_read_returns_rows(self) -> None:
        dao = _make_dao()
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[{"id": 1}])
        dao.pool = _pool_with_conn(conn)
        rows = await dao.raw_read_query("SELECT 1")
        assert rows == [{"id": 1}]

    async def test_raw_read_with_list_params(self) -> None:
        dao = _make_dao()
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[])
        dao.pool = _pool_with_conn(conn)
        rows = await dao.raw_read_query("SELECT $1", [42])
        assert rows == []
        conn.fetch.assert_awaited_once_with("SELECT $1", 42)

    async def test_raw_read_dict_params_raises(self) -> None:
        dao = _make_dao()
        dao.pool = AsyncMock()
        with pytest.raises(StorageValidationError):
            await dao.raw_read_query("SELECT 1", {"a": 1})


# ---------------------------------------------------------------
# TestRawWriteQuery
# ---------------------------------------------------------------


class TestRawWriteQuery:
    """Verify raw_write_query parses affected row count."""

    async def test_raw_write_returns_count(self) -> None:
        dao = _make_dao()
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="DELETE 3")
        dao.pool = _pool_with_conn(conn)
        count = await dao.raw_write_query("DELETE FROM t")
        assert count == _THREE

    async def test_raw_write_empty_result(self) -> None:
        dao = _make_dao()
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="")
        dao.pool = _pool_with_conn(conn)
        count = await dao.raw_write_query("DELETE FROM t")
        assert count == 0


# ---------------------------------------------------------------
# TestTestConnection
# ---------------------------------------------------------------


class TestTestConnection:
    """Verify test_connection acquires and fetchvals."""

    async def test_connection_ok(self) -> None:
        dao = _make_dao()
        conn = AsyncMock()
        conn.fetchval = AsyncMock(return_value=1)
        dao.pool = _pool_with_conn(conn)
        assert await dao.test_connection() is True

    async def test_connection_fail_returns_false(self) -> None:
        dao = _make_dao()
        conn = AsyncMock()
        conn.fetchval = AsyncMock(return_value=0)
        dao.pool = _pool_with_conn(conn)
        assert await dao.test_connection() is False


# ---------------------------------------------------------------
# TestRowToModel
# ---------------------------------------------------------------


class TestRowToModel:
    """Verify _row_to_model calls from_storage_dict."""

    async def test_hydrates_row(self) -> None:
        dao = _make_dao()
        row = _row(_UID_A, "hydrated", _HYDRATED_VALUE)
        model = await dao._row_to_model(row)
        assert model.uid == _UID_A
        assert model.name == "hydrated"
        assert model.value == _HYDRATED_VALUE

    async def test_bad_row_raises_query_error(self) -> None:
        dao = _make_dao()

        async def _boom(_data: dict[str, Any]) -> None:
            msg = "bad data"
            raise ValueError(msg)

        with (
            patch.object(_TestModel, "from_storage_dict", side_effect=_boom),
            pytest.raises(QueryError),
        ):
            await dao._row_to_model({"broken": True})
