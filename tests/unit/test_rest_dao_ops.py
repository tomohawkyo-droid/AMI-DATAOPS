"""Tests for async CRUD operations of RestDAO."""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.core.exceptions import StorageError
from ami.core.storage_types import StorageType
from ami.implementations.rest.rest_dao import RestDAO
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

# ---------------------------------------------------------------
# Test model and shared helpers
# ---------------------------------------------------------------

_PATCH_TARGET = "ami.implementations.rest.rest_dao.request_with_retry"

_EXPECTED_COUNT = 5
_BULK_DELETE_TOTAL = 3
_BULK_DELETE_FOUND = 2
_FIND_LIST_LEN = 2
_RAW_WRITE_AFFECTED = 3
_BULK_CREATE_COUNT = 2


class _TestModel(StorageModel):
    """Minimal model for DAO tests."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="widgets",
    )
    name: str = "default"


def _make_config(**overrides: Any) -> StorageConfig:
    """Build a REST StorageConfig with sensible defaults."""
    defaults: dict[str, Any] = {
        "storage_type": StorageType.REST,
        "host": "api.test.local",
        "port": 8080,
    }
    defaults.update(overrides)
    return StorageConfig(**defaults)


def _make_dao(
    config: StorageConfig | None = None,
) -> RestDAO:
    """Create a RestDAO bound to _TestModel."""
    cfg = config or _make_config()
    return RestDAO(model_cls=_TestModel, config=cfg)


def _mock_response(
    status: int = 200,
    json_data: Any = None,
    text_data: str = "",
) -> MagicMock:
    """Build a mock aiohttp response usable as async ctx mgr."""
    resp = MagicMock()
    resp.status = status
    resp.json = AsyncMock(return_value=json_data)
    resp.text = AsyncMock(return_value=text_data)
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=False)
    return resp


# ---------------------------------------------------------------
# Connection lifecycle
# ---------------------------------------------------------------


class TestConnect:
    """RestDAO.connect creates an aiohttp session."""

    @pytest.mark.asyncio
    async def test_connect_creates_session(self) -> None:
        dao = _make_dao()
        assert dao.session is None
        await dao.connect()
        assert dao.session is not None
        assert dao._connected is True
        await dao.disconnect()

    @pytest.mark.asyncio
    async def test_connect_is_idempotent(self) -> None:
        dao = _make_dao()
        await dao.connect()
        first_session = dao.session
        await dao.connect()
        assert dao.session is first_session
        await dao.disconnect()


class TestDisconnect:
    """RestDAO.disconnect closes the session."""

    @pytest.mark.asyncio
    async def test_disconnect_closes_session(self) -> None:
        dao = _make_dao()
        await dao.connect()
        assert dao.session is not None
        await dao.disconnect()
        assert dao.session is None
        assert dao._connected is False

    @pytest.mark.asyncio
    async def test_disconnect_when_no_session(self) -> None:
        dao = _make_dao()
        await dao.disconnect()
        assert dao.session is None


# ---------------------------------------------------------------
# CRUD operations
# ---------------------------------------------------------------


class TestCreate:
    """RestDAO.create POSTs and returns the new uid."""

    @pytest.mark.asyncio
    @patch(_PATCH_TARGET, new_callable=AsyncMock)
    async def test_create_returns_uid(self, mock_req: AsyncMock) -> None:
        dao = _make_dao()
        await dao.connect()
        resp = _mock_response(status=201, json_data={"uid": "abc"})
        mock_req.return_value = resp

        instance = _TestModel(name="widget-a")
        uid = await dao.create(instance)

        assert uid == "abc"
        mock_req.assert_awaited_once()
        await dao.disconnect()


class TestCreateFailed:
    """RestDAO.create raises StorageError on bad status."""

    @pytest.mark.asyncio
    @patch(_PATCH_TARGET, new_callable=AsyncMock)
    async def test_create_400_raises(self, mock_req: AsyncMock) -> None:
        dao = _make_dao()
        await dao.connect()
        resp = _mock_response(status=400, text_data="Bad Request")
        mock_req.return_value = resp

        instance = _TestModel(name="bad")
        with pytest.raises(StorageError, match="create failed"):
            await dao.create(instance)
        await dao.disconnect()


class TestFindById:
    """RestDAO.find_by_id GETs by ID."""

    @pytest.mark.asyncio
    @patch(_PATCH_TARGET, new_callable=AsyncMock)
    async def test_find_by_id_returns_model(self, mock_req: AsyncMock) -> None:
        dao = _make_dao()
        await dao.connect()
        resp = _mock_response(
            status=200,
            json_data={"uid": "x1", "name": "found"},
        )
        mock_req.return_value = resp

        result = await dao.find_by_id("x1")

        assert result is not None
        assert isinstance(result, _TestModel)
        assert result.name == "found"
        await dao.disconnect()

    @pytest.mark.asyncio
    @patch(_PATCH_TARGET, new_callable=AsyncMock)
    async def test_find_by_id_404_returns_none(self, mock_req: AsyncMock) -> None:
        dao = _make_dao()
        await dao.connect()
        resp = _mock_response(status=404)
        mock_req.return_value = resp

        result = await dao.find_by_id("missing")

        assert result is None
        await dao.disconnect()


class TestFind:
    """RestDAO.find GETs with query params."""

    @pytest.mark.asyncio
    @patch(_PATCH_TARGET, new_callable=AsyncMock)
    async def test_find_returns_list(self, mock_req: AsyncMock) -> None:
        dao = _make_dao()
        await dao.connect()
        items = [
            {"uid": "a", "name": "one"},
            {"uid": "b", "name": "two"},
        ]
        resp = _mock_response(status=200, json_data={"data": items})
        mock_req.return_value = resp

        results = await dao.find({"status": "active"})

        assert len(results) == _FIND_LIST_LEN
        assert all(isinstance(r, _TestModel) for r in results)
        await dao.disconnect()


class TestUpdate:
    """RestDAO.update PATCHes a resource."""

    @pytest.mark.asyncio
    @patch(_PATCH_TARGET, new_callable=AsyncMock)
    async def test_update_succeeds(self, mock_req: AsyncMock) -> None:
        dao = _make_dao()
        await dao.connect()
        resp = _mock_response(status=200)
        mock_req.return_value = resp

        await dao.update("u1", {"name": "updated"})

        mock_req.assert_awaited_once()
        call_args = mock_req.call_args
        assert call_args[0][1] == "PATCH"
        await dao.disconnect()


class TestDelete:
    """RestDAO.delete DELETEs a resource by ID."""

    @pytest.mark.asyncio
    @patch(_PATCH_TARGET, new_callable=AsyncMock)
    async def test_delete_200_returns_true(self, mock_req: AsyncMock) -> None:
        dao = _make_dao()
        await dao.connect()
        resp = _mock_response(status=200)
        mock_req.return_value = resp

        result = await dao.delete("d1")

        assert result is True
        await dao.disconnect()

    @pytest.mark.asyncio
    @patch(_PATCH_TARGET, new_callable=AsyncMock)
    async def test_delete_404_returns_false(self, mock_req: AsyncMock) -> None:
        dao = _make_dao()
        await dao.connect()
        resp = _mock_response(status=404)
        mock_req.return_value = resp

        result = await dao.delete("gone")

        assert result is False
        await dao.disconnect()


class TestCount:
    """RestDAO.count GETs the count endpoint."""

    @pytest.mark.asyncio
    @patch(_PATCH_TARGET, new_callable=AsyncMock)
    async def test_count_returns_int(self, mock_req: AsyncMock) -> None:
        dao = _make_dao()
        await dao.connect()
        resp = _mock_response(
            status=200,
            json_data={"count": _EXPECTED_COUNT},
        )
        mock_req.return_value = resp

        result = await dao.count({"status": "active"})

        assert result == _EXPECTED_COUNT
        await dao.disconnect()


class TestExists:
    """RestDAO.exists uses HEAD to check existence."""

    @pytest.mark.asyncio
    @patch(_PATCH_TARGET, new_callable=AsyncMock)
    async def test_exists_200_returns_true(self, mock_req: AsyncMock) -> None:
        dao = _make_dao()
        await dao.connect()
        resp = _mock_response(status=200)
        mock_req.return_value = resp

        result = await dao.exists("e1")

        assert result is True
        await dao.disconnect()


# ---------------------------------------------------------------
# Raw queries
# ---------------------------------------------------------------


class TestRawReadQuery:
    """RestDAO.raw_read_query issues a raw GET."""

    @pytest.mark.asyncio
    @patch(_PATCH_TARGET, new_callable=AsyncMock)
    async def test_raw_read_returns_data(self, mock_req: AsyncMock) -> None:
        dao = _make_dao()
        await dao.connect()
        payload = [{"id": 1, "val": "x"}]
        resp = _mock_response(status=200, json_data={"data": payload})
        mock_req.return_value = resp

        result = await dao.raw_read_query("custom/path", params={"q": "test"})

        assert result == payload
        await dao.disconnect()


class TestRawWriteQuery:
    """RestDAO.raw_write_query issues a raw POST."""

    @pytest.mark.asyncio
    @patch(_PATCH_TARGET, new_callable=AsyncMock)
    async def test_raw_write_returns_affected(self, mock_req: AsyncMock) -> None:
        dao = _make_dao()
        await dao.connect()
        resp = _mock_response(
            status=201,
            json_data={"affected": 3},
        )
        mock_req.return_value = resp

        result = await dao.raw_write_query("bulk/insert", params={"items": "many"})

        assert result == _RAW_WRITE_AFFECTED
        await dao.disconnect()


# ---------------------------------------------------------------
# Bulk operations
# ---------------------------------------------------------------


class TestBulkCreate:
    """RestDAO.bulk_create creates each and returns ids."""

    @pytest.mark.asyncio
    @patch(_PATCH_TARGET, new_callable=AsyncMock)
    async def test_bulk_create_returns_id_list(self, mock_req: AsyncMock) -> None:
        dao = _make_dao()
        await dao.connect()
        resp_a = _mock_response(status=201, json_data={"uid": "id-1"})
        resp_b = _mock_response(status=201, json_data={"uid": "id-2"})
        mock_req.side_effect = [resp_a, resp_b]

        instances = [
            _TestModel(name="a"),
            _TestModel(name="b"),
        ]
        ids = await dao.bulk_create(instances)

        assert ids == ["id-1", "id-2"]
        assert mock_req.await_count == _BULK_CREATE_COUNT
        await dao.disconnect()


class TestBulkDelete:
    """RestDAO.bulk_delete deletes each, returns count."""

    @pytest.mark.asyncio
    @patch(_PATCH_TARGET, new_callable=AsyncMock)
    async def test_bulk_delete_returns_count(self, mock_req: AsyncMock) -> None:
        dao = _make_dao()
        await dao.connect()
        resp_ok = _mock_response(status=200)
        resp_404 = _mock_response(status=404)
        mock_req.side_effect = [
            resp_ok,
            resp_404,
            resp_ok,
        ]

        deleted = await dao.bulk_delete(["d1", "d2", "d3"])

        assert deleted == _BULK_DELETE_FOUND
        assert mock_req.await_count == _BULK_DELETE_TOTAL
        await dao.disconnect()
