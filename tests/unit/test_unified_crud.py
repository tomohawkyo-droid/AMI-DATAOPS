"""Tests for UnifiedCRUD persistence operations."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, ClassVar
from unittest.mock import AsyncMock, patch

import pytest

from ami.core.exceptions import NotFoundError
from ami.core.storage_types import StorageType
from ami.core.unified_crud import UnifiedCRUD
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

_CONFIG_INDEX = 0
_QUERY_LIMIT = 10
_QUERY_SKIP = 5
_COUNT_RESULT = 42
_OUT_OF_RANGE_INDEX = 99
_EXPECTED_YEAR = 2025

_TEST_CONFIG = StorageConfig(
    storage_type=StorageType.GRAPH,
    host="localhost",
    port=9080,
    database="testdb",
)

_RELATIONAL_CONFIG = StorageConfig(
    storage_type=StorageType.RELATIONAL,
    host="localhost",
    port=5432,
    database="testdb",
)


class _TestModel(StorageModel):
    """Minimal model subclass for unit tests."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="test_models",
        storage_configs={"primary": _TEST_CONFIG},
    )

    name: str = ""


class _RelationalModel(StorageModel):
    """Model configured with relational storage."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="rel_models",
        storage_configs={"pg": _RELATIONAL_CONFIG},
    )

    name: str = ""


def _make_dao() -> AsyncMock:
    """Build an AsyncMock DAO with standard CRUD methods."""
    dao = AsyncMock()
    dao.connect = AsyncMock()
    dao.disconnect = AsyncMock()
    dao.create = AsyncMock(return_value="uid-abc-123")
    dao.find_by_id = AsyncMock(return_value=None)
    dao.find = AsyncMock(return_value=[])
    dao.update = AsyncMock()
    dao.delete = AsyncMock(return_value=True)
    dao.count = AsyncMock(return_value=_COUNT_RESULT)
    dao.raw_read_query = AsyncMock(return_value=[{"x": 1}])
    return dao


@pytest.fixture
def crud() -> UnifiedCRUD:
    """Fresh UnifiedCRUD instance per test."""
    return UnifiedCRUD()


@pytest.fixture
def mock_dao() -> AsyncMock:
    """Fresh mock DAO per test."""
    return _make_dao()


class TestCreate:
    """Verify create delegates to dao.create and registers UID."""

    async def test_returns_uid(self, crud: UnifiedCRUD, mock_dao: AsyncMock) -> None:
        model = _TestModel(name="alice")
        with patch(
            "ami.core.unified_crud.DAOFactory.create",
            return_value=mock_dao,
        ):
            uid = await crud.create(model, _CONFIG_INDEX)

        assert uid == "uid-abc-123"
        mock_dao.create.assert_awaited_once_with(model)

    async def test_sets_uid_on_model(
        self, crud: UnifiedCRUD, mock_dao: AsyncMock
    ) -> None:
        model = _TestModel(name="bob")
        with patch(
            "ami.core.unified_crud.DAOFactory.create",
            return_value=mock_dao,
        ):
            uid = await crud.create(model, _CONFIG_INDEX)

        assert model.uid == uid

    async def test_registers_uid(self, crud: UnifiedCRUD, mock_dao: AsyncMock) -> None:
        model = _TestModel(name="carol")
        with patch(
            "ami.core.unified_crud.DAOFactory.create",
            return_value=mock_dao,
        ):
            uid = await crud.create(model, _CONFIG_INDEX)

        assert uid in crud._uid_registry
        cls, idx = crud._uid_registry[uid]
        assert cls is _TestModel
        assert idx == _CONFIG_INDEX


class TestRead:
    """Verify read delegates to dao.find_by_id."""

    async def test_returns_model(self, crud: UnifiedCRUD, mock_dao: AsyncMock) -> None:
        expected = _TestModel(name="found")
        mock_dao.find_by_id.return_value = expected
        with patch(
            "ami.core.unified_crud.DAOFactory.create",
            return_value=mock_dao,
        ):
            result = await crud.read(_TestModel, "uid-1", _CONFIG_INDEX)

        assert result is expected
        mock_dao.find_by_id.assert_awaited_once_with("uid-1")

    async def test_not_found_raises(
        self, crud: UnifiedCRUD, mock_dao: AsyncMock
    ) -> None:
        mock_dao.find_by_id.return_value = None
        with (
            patch(
                "ami.core.unified_crud.DAOFactory.create",
                return_value=mock_dao,
            ),
            pytest.raises(NotFoundError, match="uid-missing"),
        ):
            await crud.read(_TestModel, "uid-missing", _CONFIG_INDEX)


class TestUpdate:
    """Verify update sets updated_at and delegates to dao.update."""

    async def test_calls_dao_update(
        self, crud: UnifiedCRUD, mock_dao: AsyncMock
    ) -> None:
        model = _TestModel(uid="uid-upd", name="before")
        before = model.updated_at
        with patch(
            "ami.core.unified_crud.DAOFactory.create",
            return_value=mock_dao,
        ):
            await crud.update(model, _CONFIG_INDEX)

        assert model.updated_at is not None
        assert model.updated_at >= (before or datetime.min.replace(tzinfo=UTC))
        mock_dao.update.assert_awaited_once()
        call_args = mock_dao.update.call_args
        assert call_args[0][0] == "uid-upd"

    async def test_no_uid_raises(self, crud: UnifiedCRUD) -> None:
        model = _TestModel(uid=None, name="no-uid")
        with pytest.raises(ValueError, match="without UID"):
            await crud.update(model, _CONFIG_INDEX)


# -- TestDelete --


class TestDelete:
    """Verify delete delegates to dao.delete."""

    async def test_calls_dao_delete(
        self, crud: UnifiedCRUD, mock_dao: AsyncMock
    ) -> None:
        model = _TestModel(uid="uid-del", name="doomed")
        with patch(
            "ami.core.unified_crud.DAOFactory.create",
            return_value=mock_dao,
        ):
            result = await crud.delete(model, _CONFIG_INDEX)

        assert result is True
        mock_dao.delete.assert_awaited_once_with("uid-del")

    async def test_no_uid_raises(self, crud: UnifiedCRUD) -> None:
        model = _TestModel(uid=None, name="no-uid")
        with pytest.raises(ValueError, match="without UID"):
            await crud.delete(model, _CONFIG_INDEX)


# -- TestQuery --


class TestQuery:
    """Verify query delegates to dao.find."""

    async def test_passes_filters(self, crud: UnifiedCRUD, mock_dao: AsyncMock) -> None:
        expected = [_TestModel(name="a"), _TestModel(name="b")]
        mock_dao.find.return_value = expected
        filters: dict[str, Any] = {"name": "a"}
        with patch(
            "ami.core.unified_crud.DAOFactory.create",
            return_value=mock_dao,
        ):
            result = await crud.query(
                _TestModel,
                filters,
                limit=_QUERY_LIMIT,
                skip=_QUERY_SKIP,
                config_index=_CONFIG_INDEX,
            )

        assert result is expected
        mock_dao.find.assert_awaited_once_with(
            filters, limit=_QUERY_LIMIT, skip=_QUERY_SKIP
        )

    async def test_none_query_becomes_empty_dict(
        self, crud: UnifiedCRUD, mock_dao: AsyncMock
    ) -> None:
        with patch(
            "ami.core.unified_crud.DAOFactory.create",
            return_value=mock_dao,
        ):
            await crud.query(_TestModel, None)

        mock_dao.find.assert_awaited_once_with({}, limit=None, skip=0)


# -- TestCount --


class TestCount:
    """Verify count delegates to dao.count."""

    async def test_returns_count(self, crud: UnifiedCRUD, mock_dao: AsyncMock) -> None:
        with patch(
            "ami.core.unified_crud.DAOFactory.create",
            return_value=mock_dao,
        ):
            result = await crud.count(_TestModel, {"active": True}, _CONFIG_INDEX)

        assert result == _COUNT_RESULT
        mock_dao.count.assert_awaited_once_with({"active": True})

    async def test_none_query_becomes_empty_dict(
        self, crud: UnifiedCRUD, mock_dao: AsyncMock
    ) -> None:
        with patch(
            "ami.core.unified_crud.DAOFactory.create",
            return_value=mock_dao,
        ):
            await crud.count(_TestModel, None)

        mock_dao.count.assert_awaited_once_with({})


# -- TestReadByUid --


class TestReadByUid:
    """Verify read_by_uid uses registry then scans."""

    async def test_registry_lookup(
        self, crud: UnifiedCRUD, mock_dao: AsyncMock
    ) -> None:
        expected = _TestModel(uid="uid-reg", name="registered")
        mock_dao.find_by_id.return_value = expected
        crud._uid_registry["uid-reg"] = (
            _TestModel,
            _CONFIG_INDEX,
        )
        with patch(
            "ami.core.unified_crud.DAOFactory.create",
            return_value=mock_dao,
        ):
            result = await crud.read_by_uid("uid-reg")

        assert result is expected

    async def test_scan_when_not_in_registry(
        self, crud: UnifiedCRUD, mock_dao: AsyncMock
    ) -> None:
        found = _TestModel(uid="uid-scan", name="scanned")
        mock_dao.find_by_id.return_value = found
        cache_key = (_TestModel, _CONFIG_INDEX)
        crud._dao_cache[cache_key] = mock_dao
        result = await crud.read_by_uid("uid-scan")

        assert result is found
        assert "uid-scan" in crud._uid_registry

    async def test_not_found_returns_none(self, crud: UnifiedCRUD) -> None:
        result = await crud.read_by_uid("uid-ghost")
        assert result is None


# -- TestDeleteByUid --


class TestDeleteByUid:
    """Verify delete_by_uid uses registry."""

    async def test_deletes_via_registry(
        self, crud: UnifiedCRUD, mock_dao: AsyncMock
    ) -> None:
        crud._uid_registry["uid-bye"] = (
            _TestModel,
            _CONFIG_INDEX,
        )
        with patch(
            "ami.core.unified_crud.DAOFactory.create",
            return_value=mock_dao,
        ):
            result = await crud.delete_by_uid("uid-bye")

        assert result is True
        assert "uid-bye" not in crud._uid_registry
        mock_dao.delete.assert_awaited_once_with("uid-bye")

    async def test_uid_not_in_registry_raises(self, crud: UnifiedCRUD) -> None:
        with pytest.raises(NotFoundError, match="not found"):
            await crud.delete_by_uid("uid-unknown")


# -- TestRawQuery --


class TestRawQuery:
    """Verify raw_query creates dao, runs query, disconnects."""

    async def test_delegates_to_raw_read_query(
        self, crud: UnifiedCRUD, mock_dao: AsyncMock
    ) -> None:
        with patch(
            "ami.core.unified_crud.DAOFactory.create",
            return_value=mock_dao,
        ):
            result = await crud.raw_query(_TestModel, _TEST_CONFIG, "SELECT 1")

        assert result == [{"x": 1}]
        mock_dao.connect.assert_awaited_once()
        mock_dao.raw_read_query.assert_awaited_once_with("SELECT 1")
        mock_dao.disconnect.assert_awaited_once()

    async def test_disconnect_called_on_error(
        self, crud: UnifiedCRUD, mock_dao: AsyncMock
    ) -> None:
        mock_dao.raw_read_query.side_effect = RuntimeError("boom")
        with (
            patch(
                "ami.core.unified_crud.DAOFactory.create",
                return_value=mock_dao,
            ),
            pytest.raises(RuntimeError, match="boom"),
        ):
            await crud.raw_query(_TestModel, _TEST_CONFIG, "BAD")

        mock_dao.disconnect.assert_awaited_once()

    async def test_not_implemented_without_method(self, crud: UnifiedCRUD) -> None:
        dao = AsyncMock(spec=["connect", "disconnect"])
        dao.connect = AsyncMock()
        dao.disconnect = AsyncMock()
        with (
            patch(
                "ami.core.unified_crud.DAOFactory.create",
                return_value=dao,
            ),
            pytest.raises(NotImplementedError, match="not supported"),
        ):
            await crud.raw_query(_TestModel, _TEST_CONFIG, "Q")


# -- TestResolveModelClass --


class TestResolveModelClass:
    """Verify _resolve_model_class static method."""

    def test_type_returns_same(self) -> None:
        result = UnifiedCRUD._resolve_model_class(_TestModel)
        assert result is _TestModel

    def test_instance_returns_class(self) -> None:
        inst = _TestModel(name="x")
        result = UnifiedCRUD._resolve_model_class(inst)
        assert result is _TestModel

    def test_bare_storage_model_raises(self) -> None:
        inst = StorageModel()
        with pytest.raises(ValueError, match="bare StorageModel"):
            UnifiedCRUD._resolve_model_class(inst)


# -- TestSelectConfig --


class TestSelectConfig:
    """Verify _select_config static method."""

    def test_valid_index(self) -> None:
        configs = [_TEST_CONFIG, _RELATIONAL_CONFIG]
        result = UnifiedCRUD._select_config(configs, 1, _TestModel)
        assert result is _RELATIONAL_CONFIG

    def test_out_of_range_raises(self) -> None:
        configs = [_TEST_CONFIG]
        with pytest.raises(ValueError, match="out of range"):
            UnifiedCRUD._select_config(configs, _OUT_OF_RANGE_INDEX, _TestModel)

    def test_negative_index_raises(self) -> None:
        configs = [_TEST_CONFIG]
        with pytest.raises(ValueError, match="out of range"):
            UnifiedCRUD._select_config(configs, -1, _TestModel)


# -- TestMapToStorage --


class TestMapToStorage:
    """Verify _map_to_storage field mapping."""

    def test_graph_assigns_uid_if_missing(self) -> None:
        model = _TestModel(uid=None, name="g", storage_configs=[_TEST_CONFIG])
        data = UnifiedCRUD._map_to_storage(model)
        assert data["uid"] is not None
        assert isinstance(data["uid"], str)
        assert len(data["uid"]) > 0

    def test_relational_renames_uid_to_id(self) -> None:
        model = _RelationalModel(
            uid="rel-uid",
            name="r",
            storage_configs=[_RELATIONAL_CONFIG],
        )
        data = UnifiedCRUD._map_to_storage(model)
        assert "id" in data
        assert data["id"] == "rel-uid"
        assert "uid" not in data

    def test_datetime_to_isoformat(self) -> None:
        model = _TestModel(name="dt", storage_configs=[_TEST_CONFIG])
        data = UnifiedCRUD._map_to_storage(model)
        if "updated_at" in data and data["updated_at"] is not None:
            assert isinstance(data["updated_at"], str)

    def test_no_config_returns_raw_dump(self) -> None:
        model = StorageModel()
        model.__class__._model_meta = ModelMetadata()
        data = UnifiedCRUD._map_to_storage(model)
        assert isinstance(data, dict)


# -- TestMapFromStorage --


class TestMapFromStorage:
    """Verify _map_from_storage field mapping."""

    def test_id_mapped_to_uid(self) -> None:
        data: dict[str, Any] = {
            "id": "mapped-uid",
            "name": "from-db",
        }
        result = UnifiedCRUD._map_from_storage(_TestModel, data)
        assert result.uid == "mapped-uid"

    def test_uid_preserved_if_present(self) -> None:
        data: dict[str, Any] = {
            "uid": "keep-uid",
            "name": "direct",
        }
        result = UnifiedCRUD._map_from_storage(_TestModel, data)
        assert result.uid == "keep-uid"

    def test_datetime_string_parsed(self) -> None:
        ts = "2025-06-15T12:30:00+00:00"
        data: dict[str, Any] = {
            "uid": "dt-uid",
            "name": "ts",
            "updated_at": ts,
        }
        result = UnifiedCRUD._map_from_storage(_TestModel, data)
        assert isinstance(result.updated_at, datetime)
        assert result.updated_at.year == _EXPECTED_YEAR
