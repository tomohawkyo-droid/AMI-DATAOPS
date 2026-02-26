"""Tests for StorageModel and ModelMetadata in base_model.py."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from ami.core.storage_types import StorageType
from ami.models.base_model import (
    ModelMetadata,
    StorageModel,
)
from ami.models.security import SecurityContext
from ami.models.storage_config import StorageConfig


class _TestModel(StorageModel):
    """Model with explicit path in metadata."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="test_items",
    )
    name: str = ""
    tenant_id: str | None = None


class _NoPathModel(StorageModel):
    """Model without path -- falls back to class name."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata()
    name: str = ""


_INMEM_CFG = StorageConfig(storage_type=StorageType.INMEM)
_PG_CFG = StorageConfig(
    storage_type=StorageType.RELATIONAL,
    host="localhost",
    port=5432,
    database="testdb",
    username="u",
    password="p",
)


class _ConfiguredModel(StorageModel):
    """Model with storage configs for DAO tests."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="configured",
        storage_configs={
            "inmem": _INMEM_CFG,
            "pg": _PG_CFG,
        },
    )
    name: str = ""


# -----------------------------------------------------------------
# Collection name
# -----------------------------------------------------------------


class TestGetCollectionName:
    """get_collection_name uses path or class-name + 's'."""

    def test_model_with_path_returns_path(self) -> None:
        inst = _TestModel(name="a")
        assert inst.get_collection_name() == "test_items"

    def test_model_without_path_returns_classname_lower_s(
        self,
    ) -> None:
        inst = _NoPathModel(name="b")
        assert inst.get_collection_name() == "_nopathmodels"


# -----------------------------------------------------------------
# Primary storage config
# -----------------------------------------------------------------


class TestGetPrimaryStorageConfig:
    """get_primary_storage_config returns first config or None."""

    def test_with_configs_returns_first(self) -> None:
        cfg = StorageConfig(storage_type=StorageType.INMEM)
        inst = _TestModel(name="x", storage_configs=[cfg])
        result = inst.get_primary_storage_config()
        assert result is cfg

    def test_without_configs_returns_none(self) -> None:
        inst = _TestModel(name="x")
        assert inst.get_primary_storage_config() is None

    def test_empty_list_returns_none(self) -> None:
        inst = _TestModel(name="x", storage_configs=[])
        assert inst.get_primary_storage_config() is None


# -----------------------------------------------------------------
# get_metadata
# -----------------------------------------------------------------


class TestGetMetadata:
    """get_metadata returns the ClassVar _model_meta."""

    def test_returns_model_meta(self) -> None:
        meta = _TestModel.get_metadata()
        assert isinstance(meta, ModelMetadata)
        assert meta.path == "test_items"


# -----------------------------------------------------------------
# ModelMetadata defaults
# -----------------------------------------------------------------


class TestModelMetadataDefaults:
    """Empty ModelMetadata has correct default values."""

    def test_defaults(self) -> None:
        meta = ModelMetadata()
        assert meta.path is None
        assert meta.storage_configs == {}
        assert meta.indexes == []


# -----------------------------------------------------------------
# to_storage_dict
# -----------------------------------------------------------------


class TestToStorageDict:
    """to_storage_dict serializes fields correctly."""

    @pytest.mark.asyncio
    async def test_basic_fields_present(self) -> None:
        inst = _TestModel(name="hello")
        result = await inst.to_storage_dict()
        assert result["name"] == "hello"
        assert "uid" in result

    @pytest.mark.asyncio
    async def test_storage_configs_excluded(self) -> None:
        cfg = StorageConfig(storage_type=StorageType.INMEM)
        inst = _TestModel(name="a", storage_configs=[cfg])
        result = await inst.to_storage_dict()
        assert "storage_configs" not in result

    @pytest.mark.asyncio
    async def test_path_excluded(self) -> None:
        inst = _TestModel(name="a", path="/some/path")
        result = await inst.to_storage_dict()
        assert "path" not in result

    @pytest.mark.asyncio
    async def test_vault_pointer_cache_excluded(self) -> None:
        inst = _TestModel(name="a")
        inst._vault_pointer_cache["secret"] = "ptr"
        result = await inst.to_storage_dict()
        assert "__vault_pointer_cache__" not in result


# -----------------------------------------------------------------
# from_storage_dict
# -----------------------------------------------------------------


class TestFromStorageDict:
    """from_storage_dict deserializes data correctly."""

    @pytest.mark.asyncio
    async def test_basic_fields(self) -> None:
        data: dict[str, Any] = {"name": "loaded", "uid": "u1"}
        inst = await _TestModel.from_storage_dict(data)
        assert inst.name == "loaded"
        assert inst.uid == "u1"

    @pytest.mark.asyncio
    async def test_datetime_string_parsed(self) -> None:
        ts = "2025-01-15T10:30:00+00:00"
        data: dict[str, Any] = {
            "name": "dt",
            "created_at": ts,
        }
        inst = await _TestModel.from_storage_dict(data)
        assert isinstance(inst.created_at, datetime)

    @pytest.mark.asyncio
    async def test_invalid_datetime_logged(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        data: dict[str, Any] = {
            "name": "bad",
            "created_at": "not-a-date",
        }
        with (
            caplog.at_level(logging.WARNING),
            pytest.raises(ValidationError),
        ):
            await _TestModel.from_storage_dict(data)
        assert any("Failed to parse datetime" in r.message for r in caplog.records)


# -----------------------------------------------------------------
# create_with_security
# -----------------------------------------------------------------


class TestCreateWithSecurity:
    """create_with_security populates security fields."""

    @pytest.mark.asyncio
    async def test_sets_created_modified_owner(self) -> None:
        ctx = SecurityContext(user_id="alice")
        inst = await _TestModel.create_with_security(ctx, name="sec")
        assert inst.created_by == "alice"
        assert inst.modified_by == "alice"
        assert inst.owner_id == "alice"

    @pytest.mark.asyncio
    async def test_tenant_id_included(self) -> None:
        ctx = SecurityContext(user_id="bob", tenant_id="t1")
        inst = await _TestModel.create_with_security(ctx, name="tenant")
        assert inst.tenant_id == "t1"

    @pytest.mark.asyncio
    async def test_tenant_id_absent(self) -> None:
        ctx = SecurityContext(user_id="carol")
        inst = await _TestModel.create_with_security(ctx, name="no_tenant")
        assert inst.tenant_id is None


# -----------------------------------------------------------------
# find_with_security
# -----------------------------------------------------------------


class TestFindWithSecurity:
    """find_with_security applies tenant filter and permissions."""

    @pytest.mark.asyncio
    async def test_tenant_filter_added(self) -> None:
        ctx = SecurityContext(user_id="alice", tenant_id="t1")
        owned = _TestModel(name="ok")
        owned.owner_id = "alice"

        mock_crud = MagicMock()
        mock_crud.query = AsyncMock(return_value=[owned])

        with patch(
            "ami.core.unified_crud.UnifiedCRUD",
            return_value=mock_crud,
        ):
            results = await _TestModel.find_with_security(ctx, {"name": "ok"})

        call_args = mock_crud.query.call_args
        secured_q = call_args[0][1]
        assert "$and" in secured_q
        tenant_part = secured_q["$and"][1]
        assert tenant_part == {"tenant_id": "t1"}
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_results_filtered_by_permission(
        self,
    ) -> None:
        ctx = SecurityContext(user_id="viewer")
        allowed = _TestModel(name="yes")
        allowed.owner_id = "viewer"
        denied = _TestModel(name="no")
        denied.owner_id = "other"

        mock_crud = MagicMock()
        mock_crud.query = AsyncMock(return_value=[allowed, denied])

        with patch(
            "ami.core.unified_crud.UnifiedCRUD",
            return_value=mock_crud,
        ):
            results = await _TestModel.find_with_security(ctx, {})

        names = [r.name for r in results]
        assert "yes" in names
        assert "no" not in names


# -----------------------------------------------------------------
# get_all_daos
# -----------------------------------------------------------------


class TestGetAllDaos:
    """get_all_daos returns dict of name to dao."""

    def test_returns_dao_dict(self) -> None:
        fake_dao = MagicMock()
        with patch(
            "ami.core.dao.DAOFactory.create",
            return_value=fake_dao,
        ):
            result = _ConfiguredModel.get_all_daos()

        assert "inmem" in result
        assert "pg" in result

    def test_empty_configs_raises(self) -> None:
        with pytest.raises(ValueError, match="No storage"):
            _NoPathModel.get_all_daos()


# -----------------------------------------------------------------
# get_dao
# -----------------------------------------------------------------


class TestGetDao:
    """get_dao returns a specific or first dao."""

    def test_returns_named_dao(self) -> None:
        dao_a = MagicMock(name="dao_a")
        dao_b = MagicMock(name="dao_b")

        def _create(cls: type[Any], cfg: StorageConfig) -> MagicMock:
            if cfg is _INMEM_CFG:
                return dao_a
            return dao_b

        with patch(
            "ami.core.dao.DAOFactory.create",
            side_effect=_create,
        ):
            got = _ConfiguredModel.get_dao("pg")

        assert got is dao_b

    def test_returns_first_when_no_name(self) -> None:
        fake_dao = MagicMock()
        with patch(
            "ami.core.dao.DAOFactory.create",
            return_value=fake_dao,
        ):
            got = _ConfiguredModel.get_dao()

        assert got is fake_dao

    def test_missing_name_raises(self) -> None:
        fake_dao = MagicMock()
        with (
            patch(
                "ami.core.dao.DAOFactory.create",
                return_value=fake_dao,
            ),
            pytest.raises(ValueError, match="not found"),
        ):
            _ConfiguredModel.get_dao("nope")
