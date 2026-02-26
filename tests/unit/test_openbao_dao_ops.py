"""Tests for OpenBaoDAO CRUD and lifecycle operations."""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from hvac.exceptions import VaultError as OpenBaoError

from ami.core.exceptions import StorageError
from ami.core.storage_types import StorageType
from ami.implementations.vault.openbao_dao import OpenBaoDAO
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

_FIND_EXPECTED = 2
_COUNT_EXPECTED = 3
_BULK_CREATE_EXPECTED = 2
_BULK_DELETE_SUCCESS = 2
_BULK_DELETE_TOTAL = 3


class _TestModel(StorageModel):
    """Minimal model for OpenBaoDAO tests."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="test_secrets",
    )
    name: str = ""
    value: int = 0


def _make_config(**overrides: Any) -> StorageConfig:
    defaults: dict[str, Any] = {
        "storage_type": StorageType.VAULT,
        "host": "localhost",
        "port": 8200,
        "options": {"token": "test"},
    }
    defaults.update(overrides)
    return StorageConfig(**defaults)


def _make_dao(
    config: StorageConfig | None = None,
) -> OpenBaoDAO:
    cfg = config or _make_config()
    dao = OpenBaoDAO(_TestModel, cfg)
    dao.client = MagicMock()
    return dao


def _vault_response(data: dict[str, Any]) -> dict[str, Any]:
    """Build a nested Vault KV-v2 read response."""
    return {"data": {"data": data}}


# -- TestReference ---------------------------------------------------


class TestReference:
    """Validate _reference path construction and guards."""

    def test_valid_id_builds_correct_path(self) -> None:
        dao = _make_dao()
        path = dao._reference("abc-123")
        assert path == "test_secrets/abc-123"

    def test_path_traversal_rejected(self) -> None:
        dao = _make_dao()
        with pytest.raises(StorageError):
            dao._reference("../etc/passwd")

    def test_leading_slash_rejected(self) -> None:
        dao = _make_dao()
        with pytest.raises(StorageError):
            dao._reference("/absolute")

    def test_control_chars_rejected(self) -> None:
        dao = _make_dao()
        with pytest.raises(StorageError):
            dao._reference("bad\x00id")

    def test_embedded_dotdot_rejected(self) -> None:
        dao = _make_dao()
        with pytest.raises(StorageError):
            dao._reference("foo/../bar")

    def test_tab_char_rejected(self) -> None:
        dao = _make_dao()
        with pytest.raises(StorageError):
            dao._reference("has\ttab")


# -- TestEnsureClient ------------------------------------------------


class TestEnsureClient:
    """Validate _ensure_client guard logic."""

    def test_client_none_raises(self) -> None:
        dao = _make_dao()
        dao.client = None
        with pytest.raises(StorageError, match="not connected"):
            dao._ensure_client()

    def test_client_set_returns_it(self) -> None:
        dao = _make_dao()
        mock_client = MagicMock()
        dao.client = mock_client
        assert dao._ensure_client() is mock_client


# -- TestConnect -----------------------------------------------------


class TestConnect:
    """Validate connect lifecycle."""

    async def test_connect_sets_client(self) -> None:
        cfg = _make_config()
        dao = OpenBaoDAO(_TestModel, cfg)
        dao.client = None
        target = "ami.implementations.vault.openbao_dao.OpenBaoClient"
        with patch(target) as mock_cls:
            mock_cls.return_value = MagicMock()
            await dao.connect()
        assert dao.client is not None

    async def test_connect_idempotent(self) -> None:
        dao = _make_dao()
        original = dao.client
        await dao.connect()
        assert dao.client is original


# -- TestDisconnect --------------------------------------------------


class TestDisconnect:
    """Validate disconnect clears client."""

    async def test_disconnect_clears_client(self) -> None:
        dao = _make_dao()
        assert dao.client is not None
        await dao.disconnect()
        assert dao.client is None


# -- TestCreate ------------------------------------------------------


class TestCreate:
    """Validate create operation."""

    async def test_create_returns_uid(self) -> None:
        dao = _make_dao()
        kv = dao.client.secrets.kv.v2
        kv.create_or_update_secret = MagicMock()
        instance = _TestModel(name="secret1", value=10)
        uid = await dao.create(instance)
        assert isinstance(uid, str)
        assert len(uid) > 0
        kv.create_or_update_secret.assert_called_once()

    async def test_create_with_dict_input(self) -> None:
        dao = _make_dao()
        kv = dao.client.secrets.kv.v2
        kv.create_or_update_secret = MagicMock()
        data: dict[str, Any] = {"name": "raw", "value": 99}
        uid = await dao.create(data)
        assert isinstance(uid, str)
        call_args = kv.create_or_update_secret.call_args
        secret = call_args.kwargs.get(
            "secret",
            call_args[1].get("secret", {}),
        )
        assert secret["name"] == "raw"


# -- TestFindById ----------------------------------------------------


class TestFindById:
    """Validate find_by_id operation."""

    async def test_found_returns_model(self) -> None:
        dao = _make_dao()
        kv = dao.client.secrets.kv.v2
        kv.read_secret_version = MagicMock(
            return_value=_vault_response({"uid": "abc", "name": "s1", "value": 5}),
        )
        result = await dao.find_by_id("abc")
        assert result is not None
        assert result.name == "s1"

    async def test_not_found_returns_none(self) -> None:
        dao = _make_dao()
        kv = dao.client.secrets.kv.v2
        kv.read_secret_version = MagicMock(
            side_effect=OpenBaoError("not found"),
        )
        result = await dao.find_by_id("missing")
        assert result is None


# -- TestFind --------------------------------------------------------


class TestFind:
    """Validate find (list + iterate) operation."""

    async def test_find_lists_then_reads(self) -> None:
        dao = _make_dao()
        kv = dao.client.secrets.kv.v2
        kv.list_secrets = MagicMock(
            return_value={"data": {"keys": ["k1", "k2"]}},
        )

        def _read(path: str, mount_point: str) -> dict[str, Any]:
            key = path.rsplit("/", maxsplit=1)[-1]
            return _vault_response({"uid": key, "name": key, "value": 1})

        kv.read_secret_version = MagicMock(side_effect=_read)
        results = await dao.find({})
        assert len(results) == _FIND_EXPECTED


# -- TestUpdate ------------------------------------------------------


class TestUpdate:
    """Validate update operation."""

    async def test_update_merges_and_writes(self) -> None:
        dao = _make_dao()
        kv = dao.client.secrets.kv.v2
        kv.read_secret_version = MagicMock(
            return_value=_vault_response({"uid": "u1", "name": "old", "value": 1}),
        )
        kv.create_or_update_secret = MagicMock()
        await dao.update("u1", {"name": "new"})
        kv.create_or_update_secret.assert_called_once()

    async def test_update_not_found_raises(self) -> None:
        dao = _make_dao()
        kv = dao.client.secrets.kv.v2
        kv.read_secret_version = MagicMock(
            side_effect=OpenBaoError("gone"),
        )
        with pytest.raises(StorageError, match="not found"):
            await dao.update("missing", {"name": "x"})


# -- TestDelete ------------------------------------------------------


class TestDelete:
    """Validate delete operation."""

    async def test_delete_success_returns_true(self) -> None:
        dao = _make_dao()
        kv = dao.client.secrets.kv.v2
        kv.delete_metadata_and_all_versions = MagicMock()
        result = await dao.delete("d1")
        assert result is True

    async def test_delete_error_returns_false(self) -> None:
        dao = _make_dao()
        kv = dao.client.secrets.kv.v2
        kv.delete_metadata_and_all_versions = MagicMock(
            side_effect=OpenBaoError("fail"),
        )
        result = await dao.delete("d1")
        assert result is False


# -- TestCount -------------------------------------------------------


class TestCount:
    """Validate count delegates to find."""

    async def test_count_returns_length(self) -> None:
        dao = _make_dao()
        dao.find = AsyncMock(return_value=["a", "b", "c"])
        result = await dao.count({})
        assert result == _COUNT_EXPECTED


# -- TestExists ------------------------------------------------------


class TestExists:
    """Validate exists delegates to find_by_id."""

    async def test_exists_true(self) -> None:
        dao = _make_dao()
        dao.find_by_id = AsyncMock(
            return_value=_TestModel(name="x"),
        )
        assert await dao.exists("x1") is True

    async def test_exists_false(self) -> None:
        dao = _make_dao()
        dao.find_by_id = AsyncMock(return_value=None)
        assert await dao.exists("x2") is False


# -- TestBulkCreate --------------------------------------------------


class TestBulkCreate:
    """Validate bulk_create calls create per item."""

    async def test_bulk_create_returns_ids(self) -> None:
        dao = _make_dao()
        dao.create = AsyncMock(side_effect=["id1", "id2"])
        items = [_TestModel(name="a"), _TestModel(name="b")]
        ids = await dao.bulk_create(items)
        assert ids == ["id1", "id2"]
        assert dao.create.call_count == _BULK_CREATE_EXPECTED


# -- TestBulkDelete --------------------------------------------------


class TestBulkDelete:
    """Validate bulk_delete calls delete and counts."""

    async def test_bulk_delete_returns_count(self) -> None:
        dao = _make_dao()
        dao.delete = AsyncMock(
            side_effect=[True, False, True],
        )
        count = await dao.bulk_delete(["d1", "d2", "d3"])
        assert count == _BULK_DELETE_SUCCESS
        assert dao.delete.call_count == _BULK_DELETE_TOTAL


# -- TestRawReadQuery ------------------------------------------------


class TestRawReadQuery:
    """Validate raw_read_query calls kv.v2.read."""

    async def test_raw_read_returns_data(self) -> None:
        dao = _make_dao()
        kv = dao.client.secrets.kv.v2
        kv.read_secret_version = MagicMock(
            return_value=_vault_response({"key": "val"}),
        )
        result = await dao.raw_read_query("some/path")
        assert result == [{"key": "val"}]
        kv.read_secret_version.assert_called_once_with(
            path="some/path",
            mount_point="secret",
        )


# -- TestRawWriteQuery -----------------------------------------------


class TestRawWriteQuery:
    """Validate raw_write_query calls kv.v2 write."""

    async def test_raw_write_returns_one(self) -> None:
        dao = _make_dao()
        kv = dao.client.secrets.kv.v2
        kv.create_or_update_secret = MagicMock()
        params = {"foo": "bar"}
        result = await dao.raw_write_query("some/path", params)
        assert result == 1
        kv.create_or_update_secret.assert_called_once_with(
            path="some/path",
            secret=params,
            mount_point="secret",
        )


# -- TestTestConnection ----------------------------------------------


class TestTestConnection:
    """Validate test_connection health check."""

    async def test_authenticated_returns_true(self) -> None:
        dao = _make_dao()
        client = dao.client
        # Remove sys so the elif branch runs
        if hasattr(client, "sys"):
            del client.sys
        client.is_authenticated = MagicMock(return_value=True)
        result = await dao.test_connection()
        assert result is True
