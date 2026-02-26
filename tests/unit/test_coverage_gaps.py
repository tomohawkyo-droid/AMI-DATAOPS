"""Final coverage gap tests for registry, http_client,
storage_config_factory, and base_model.
"""

from __future__ import annotations

import contextlib
import logging
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from ami.core.exceptions import StorageError
from ami.core.storage_types import StorageType
from ami.models.base_model import (
    _EMPTY_META,
    ModelMetadata,
    StorageModel,
)
from ami.models.storage_config import StorageConfig
from ami.models.storage_config_factory import (
    StorageConfigFactory,
    _yaml_cache,
    invalidate_yaml_cache,
)
from ami.storage.registry import (
    ModelStorageUsage,
    StorageRegistry,
    _iter_storage_models,
)
from ami.utils.http_client import (
    RetryConfig,
    request_with_retry,
)

# -----------------------------------------------------------------
# Constants
# -----------------------------------------------------------------

_HTTP_OK = 200
_HTTP_SERVER_ERROR = 500
_PORT = 5432
_SINGLE_RETRY = 1
_URL = "https://api.test/v1/data"
_METHOD = "GET"


def _make_cfg(**overrides: Any) -> StorageConfig:
    defaults: dict[str, Any] = {
        "storage_type": StorageType.RELATIONAL,
        "host": "localhost",
        "port": _PORT,
        "database": "testdb",
    }
    defaults.update(overrides)
    return StorageConfig(**defaults)


# =================================================================
# A) storage/registry.py
# =================================================================


class TestIterStorageModels:
    """Cover _iter_storage_models walking subclass tree."""

    def test_yields_direct_subclasses(self) -> None:
        results = list(_iter_storage_models())
        assert len(results) > 0
        for cls in results:
            assert issubclass(cls, StorageModel)

    def test_no_duplicates(self) -> None:
        results = list(_iter_storage_models())
        assert len(results) == len(set(results))


class TestModelStorageUsageToDict:
    """Cover ModelStorageUsage.to_dict method."""

    def test_returns_dict_with_model_and_storages(self) -> None:
        usage = ModelStorageUsage(
            model="mod.Widget",
            storages=[{"name": "pg", "storage_type": "postgres"}],
        )
        result = usage.to_dict()
        assert result["model"] == "mod.Widget"
        assert len(result["storages"]) == 1
        assert result["storages"][0]["name"] == "pg"

    def test_empty_storages(self) -> None:
        usage = ModelStorageUsage(model="m.M", storages=[])
        result = usage.to_dict()
        assert result["storages"] == []


class TestListConfigsEmptyCache:
    """Cover list_configs raising StorageError when cache empty."""

    @patch(
        "ami.storage.registry.StorageConfigFactory.get_all_configs",
    )
    def test_raises_when_cache_empty(
        self,
        mock_get_all: MagicMock,
    ) -> None:
        mock_get_all.return_value = []
        reg = StorageRegistry()
        with pytest.raises(
            StorageError,
            match="not initialized",
        ):
            reg.list_configs()


class TestGetConfigUsageIndex:
    """Cover get_config_usage_index reverse-index building."""

    @patch(
        "ami.storage.registry.StorageConfigFactory.get_all_configs",
    )
    @patch("ami.storage.registry._iter_storage_models")
    def test_builds_index_from_usage(
        self,
        mock_iter: MagicMock,
        mock_get_all: MagicMock,
    ) -> None:
        mock_get_all.return_value = []
        cfg = _make_cfg(name="pg")
        model_cls = MagicMock()
        model_cls.__module__ = "app.models"
        model_cls.__name__ = "Item"
        meta = MagicMock()
        meta.storage_configs = {"pg": cfg}
        model_cls.get_metadata.return_value = meta
        mock_iter.return_value = [model_cls]

        reg = StorageRegistry()
        index = reg.get_config_usage_index()

        assert "pg" in index
        assert "app.models.Item" in index["pg"]


class TestResolveModelStoragesList:
    """Cover _resolve_model_storages with list input."""

    def test_list_with_named_config(self) -> None:
        cfg = _make_cfg(name="alpha")
        result = StorageRegistry._resolve_model_storages([cfg])
        assert len(result) == 1
        assert result[0][0] == "alpha"

    def test_list_with_unnamed_typed_config(self) -> None:
        cfg = _make_cfg(name=None)
        result = StorageRegistry._resolve_model_storages([cfg])
        name, _ = result[0]
        assert name == "postgres"

    def test_list_with_no_name_no_type(self) -> None:
        cfg = _make_cfg(name=None, storage_type=None)
        result = StorageRegistry._resolve_model_storages([cfg])
        name, _ = result[0]
        assert name == "storage_0"


# =================================================================
# B) utils/http_client.py
# =================================================================


def _mock_response(
    status: int = _HTTP_OK,
    reason: str = "OK",
    *,
    text_raises: bool = False,
) -> MagicMock:
    resp = MagicMock()
    resp.status = status
    resp.reason = reason
    if text_raises:
        resp.text = AsyncMock(side_effect=OSError("read error"))
    else:
        resp.text = AsyncMock(return_value="error body")
    resp.release = AsyncMock()
    return resp


class TestUnreadableBody:
    """Cover lines 100-101: reading error text fails."""

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_unreadable_body_logs_marker(
        self,
        mock_sleep: AsyncMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        cfg = RetryConfig(max_retries=_SINGLE_RETRY)
        resp = _mock_response(
            _HTTP_SERVER_ERROR,
            "Internal Server Error",
            text_raises=True,
        )
        session = MagicMock(spec=aiohttp.ClientSession)
        session.request = AsyncMock(return_value=resp)

        with (
            caplog.at_level(logging.ERROR),
            pytest.raises(StorageError, match="HTTP"),
        ):
            await request_with_retry(
                session,
                _METHOD,
                _URL,
                retry_cfg=cfg,
            )

        assert any("<unreadable body>" in r.message for r in caplog.records)


class TestPostLoopUnreachable:
    """Cover lines 164-168: post-loop with last_exception."""

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_post_loop_with_last_exception(
        self,
        mock_sleep: AsyncMock,
    ) -> None:
        cfg = RetryConfig(max_retries=_SINGLE_RETRY)
        session = MagicMock(spec=aiohttp.ClientSession)
        session.request = AsyncMock(
            side_effect=TimeoutError("gone"),
        )

        with pytest.raises(StorageError, match="failed after"):
            await request_with_retry(
                session,
                _METHOD,
                _URL,
                retry_cfg=cfg,
            )


# =================================================================
# C) models/storage_config_factory.py
# =================================================================

_FACTORY = "ami.models.storage_config_factory"


class TestLoadYamlNotFound:
    """Cover lines 56-57: file not found."""

    def test_raises_file_not_found(self) -> None:
        invalidate_yaml_cache()
        with (
            patch.dict(
                "os.environ",
                {"AMI_DATAOPS_CONFIG": "/no/such/file.yaml"},
            ),
            pytest.raises(
                FileNotFoundError,
                match="not found",
            ),
        ):
            StorageConfigFactory.from_yaml("anything")
        invalidate_yaml_cache()


class TestGetConfigNotFound:
    """Cover lines 85-86: config name not found in YAML."""

    def test_raises_key_error(self) -> None:
        yaml_data = {
            "storage_configs": {
                "existing": {"type": "graph", "host": "h"},
            },
        }
        invalidate_yaml_cache()
        with (
            patch(
                f"{_FACTORY}._load_yaml",
                return_value=yaml_data,
            ),
            pytest.raises(KeyError, match="not found"),
        ):
            StorageConfigFactory.from_yaml("missing_name")


class TestMissingTypeKey:
    """Cover lines 90-91: missing 'type' key in config."""

    def test_raises_value_error(self) -> None:
        yaml_data = {
            "storage_configs": {
                "bad": {"host": "localhost"},
            },
        }
        invalidate_yaml_cache()
        with (
            patch(
                f"{_FACTORY}._load_yaml",
                return_value=yaml_data,
            ),
            pytest.raises(ValueError, match="missing required"),
        ):
            StorageConfigFactory.from_yaml("bad")


class TestUnknownType:
    """Cover line 95: unknown storage type string."""

    def test_raises_value_error(self) -> None:
        yaml_data = {
            "storage_configs": {
                "alien": {"type": "quantum", "host": "x"},
            },
        }
        invalidate_yaml_cache()
        with (
            patch(
                f"{_FACTORY}._load_yaml",
                return_value=yaml_data,
            ),
            pytest.raises(ValueError, match="Unsupported"),
        ):
            StorageConfigFactory.from_yaml("alien")


class TestInvalidateYamlCache:
    """Cover lines 119-120: invalidate_yaml_cache resets cache."""

    def test_clears_cached_data(self) -> None:
        _yaml_cache["data"] = {"storage_configs": {}}
        invalidate_yaml_cache()
        assert _yaml_cache["data"] is None

    def test_double_invalidate_safe(self) -> None:
        _yaml_cache["data"] = None
        invalidate_yaml_cache()
        assert _yaml_cache["data"] is None


class TestGetAllConfigsSkipsInvalid:
    """Cover lines 119-120 in get_all_configs error branch."""

    def test_skips_bad_entries(self) -> None:
        yaml_data = {
            "storage_configs": {
                "ok": {
                    "type": "graph",
                    "host": "h",
                    "port": 9080,
                },
                "bad_no_type": {"host": "h"},
            },
        }
        invalidate_yaml_cache()
        with patch(
            f"{_FACTORY}._load_yaml",
            return_value=yaml_data,
        ):
            results = StorageConfigFactory.get_all_configs()
        assert len(results) == 1
        assert results[0].name == "ok"


# =================================================================
# D) models/base_model.py
# =================================================================


class _SensitiveModel(StorageModel):
    """Model with sensitive fields for adapter tests."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="sensitive_items",
    )
    _sensitive_fields: ClassVar[dict[str, Any]] = {
        "api_key": MagicMock(),
    }
    name: str = ""
    api_key: str | None = None


class _PlainModel(StorageModel):
    """Model without sensitive fields."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="plain_items",
    )
    name: str = ""


class TestToStorageDictSensitive:
    """Cover lines 138-139: to_storage_dict with sensitive adapter."""

    @pytest.mark.asyncio
    async def test_calls_prepare_for_storage(self) -> None:
        inst = _SensitiveModel(name="s", api_key="secret123")
        expected_data = {"name": "s", "api_key": "vault://ref"}
        with patch(
            "ami.secrets.adapter.prepare_instance_for_storage",
            new_callable=AsyncMock,
            return_value=expected_data,
        ):
            result = await inst.to_storage_dict()
        assert result["api_key"] == "vault://ref"

    @pytest.mark.asyncio
    async def test_import_error_raises(self) -> None:
        inst = _SensitiveModel(name="f", api_key="k")
        with (
            patch(
                "ami.models.base_model.prepare_instance_for_storage",
                side_effect=ImportError("no module"),
                create=True,
            ),
            patch.dict(
                "sys.modules",
                {"ami.secrets.adapter": None},
            ),
            pytest.raises(ImportError, match="sensitive"),
        ):
            await inst.to_storage_dict()


class TestFromStorageDictSensitive:
    """Cover lines 159-164: from_storage_dict with hydration."""

    @pytest.mark.asyncio
    async def test_hydrates_sensitive_fields(self) -> None:
        raw: dict[str, Any] = {
            "name": "loaded",
            "api_key": {"vault_reference": "ref"},
        }
        hydrated: dict[str, Any] = {
            "name": "loaded",
            "api_key": "decrypted_val",
        }
        mock_hydrate = AsyncMock(return_value=hydrated)
        mock_consume = MagicMock(return_value={"api_key": "ptr"})

        with (
            patch(
                "ami.secrets.adapter.hydrate_sensitive_fields",
                mock_hydrate,
            ),
            patch(
                "ami.secrets.adapter.consume_pointer_cache",
                mock_consume,
            ),
        ):
            inst = await _SensitiveModel.from_storage_dict(raw)

        assert inst.name == "loaded"
        assert inst.api_key == "decrypted_val"


class TestFromStorageDictDatetimeError:
    """Cover lines 180-185: datetime parse exception logged."""

    @pytest.mark.asyncio
    async def test_bad_datetime_logs_warning(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        raw: dict[str, Any] = {
            "name": "dt_bad",
            "created_at": "not-a-valid-date",
        }
        with caplog.at_level(logging.WARNING), contextlib.suppress(Exception):
            await _PlainModel.from_storage_dict(raw)
        assert any("Failed to parse datetime" in r.message for r in caplog.records)


class TestGetMetadataReturnsNoneFields:
    """Cover line 265: get_metadata returning empty meta."""

    def test_empty_meta_has_none_path(self) -> None:
        meta = StorageModel.get_metadata()
        assert meta is _EMPTY_META
        assert meta.path is None

    def test_empty_meta_no_storage_configs(self) -> None:
        meta = StorageModel.get_metadata()
        assert meta.storage_configs == {}
