"""Tests for storage configuration registry."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from ami.core.storage_types import StorageType
from ami.models.storage_config import StorageConfig
from ami.storage.registry import (
    ModelStorageUsage,
    StorageRegistry,
    _sanitize_options,
)

REDACTED = "***REDACTED***"
PORT_5432 = 5432
PORT_27017 = 27017
POOL_SIZE_10 = 10
TIMEOUT_30 = 30
RETRIES_3 = 3
ENTRY_COUNT_2 = 2

_CFG_DEFAULTS: dict[str, Any] = {
    "storage_type": StorageType.RELATIONAL,
    "host": "db.local",
    "port": PORT_5432,
    "database": "mydb",
    "username": "admin",
    "password": "s3cret",
    "options": {},
}


def _make_config(**overrides: Any) -> StorageConfig:
    """Build a StorageConfig with sensible defaults."""
    merged = {**_CFG_DEFAULTS, **overrides}
    return StorageConfig(**merged)


# ------------------------------------------------------------------
# _sanitize_options
# ------------------------------------------------------------------


class TestSanitizeOptions:
    """Verify sensitive option keys are redacted."""

    def test_redacts_password_key(self) -> None:
        result = _sanitize_options({"db_password": "abc"})
        assert result["db_password"] == REDACTED

    def test_redacts_secret_key(self) -> None:
        result = _sanitize_options({"client_secret": "xyz"})
        assert result["client_secret"] == REDACTED

    def test_redacts_token_key(self) -> None:
        result = _sanitize_options({"auth_token": "tok"})
        assert result["auth_token"] == REDACTED

    def test_redacts_key_key(self) -> None:
        result = _sanitize_options({"api_key": "k"})
        assert result["api_key"] == REDACTED

    def test_redacts_credential_key(self) -> None:
        result = _sanitize_options({"credential_path": "/x"})
        assert result["credential_path"] == REDACTED

    def test_case_insensitive_redaction(self) -> None:
        result = _sanitize_options({"API_KEY": "val"})
        assert result["API_KEY"] == REDACTED

    def test_safe_key_passes_through(self) -> None:
        result = _sanitize_options({"timeout": TIMEOUT_30})
        assert result["timeout"] == TIMEOUT_30

    def test_multiple_safe_keys(self) -> None:
        opts: dict[str, Any] = {
            "host": "localhost",
            "retries": RETRIES_3,
        }
        result = _sanitize_options(opts)
        assert result == opts

    def test_empty_dict_returns_empty(self) -> None:
        assert _sanitize_options({}) == {}

    def test_none_input_returns_empty(self) -> None:
        assert _sanitize_options(None) == {}


# ------------------------------------------------------------------
# StorageRegistry core methods
# ------------------------------------------------------------------


class TestStorageRegistry:
    """Verify registry listing, lookup, and cache behaviour."""

    @patch(
        "ami.storage.registry.StorageConfigFactory.get_all_configs",
    )
    def test_list_configs_returns_dict(
        self,
        mock_get_all: MagicMock,
    ) -> None:
        cfg = _make_config(name="pg")
        mock_get_all.return_value = [cfg]
        reg = StorageRegistry()
        result = reg.list_configs()
        assert isinstance(result, dict)
        assert "pg" in result
        assert result["pg"] is cfg

    @patch(
        "ami.storage.registry.StorageConfigFactory.get_all_configs",
    )
    def test_get_config_found(
        self,
        mock_get_all: MagicMock,
    ) -> None:
        cfg = _make_config(name="pg")
        mock_get_all.return_value = [cfg]
        reg = StorageRegistry()
        assert reg.get_config("pg") is cfg

    @patch(
        "ami.storage.registry.StorageConfigFactory.get_all_configs",
    )
    def test_get_config_missing_raises(
        self,
        mock_get_all: MagicMock,
    ) -> None:
        mock_get_all.return_value = [_make_config(name="pg")]
        reg = StorageRegistry()
        with pytest.raises(KeyError, match="Unknown storage config"):
            reg.get_config("missing")

    @patch(
        "ami.storage.registry.StorageConfigFactory.get_all_configs",
    )
    def test_list_config_summaries_sanitizes(
        self,
        mock_get_all: MagicMock,
    ) -> None:
        cfg = _make_config(
            name="pg",
            options={
                "ssl_key": "abc",
                "pool_size": POOL_SIZE_10,
            },
        )
        mock_get_all.return_value = [cfg]
        reg = StorageRegistry()
        summaries = reg.list_config_summaries()
        assert len(summaries) == 1
        opts = summaries[0]["options"]
        assert opts["ssl_key"] == REDACTED
        assert opts["pool_size"] == POOL_SIZE_10

    @patch(
        "ami.storage.registry.StorageConfigFactory.get_all_configs",
    )
    def test_refresh_clears_cache(
        self,
        mock_get_all: MagicMock,
    ) -> None:
        cfg_a = _make_config(name="a")
        cfg_b = _make_config(name="b")
        mock_get_all.return_value = [cfg_a]
        reg = StorageRegistry()
        first = reg.list_configs()
        assert "a" in first

        mock_get_all.return_value = [cfg_b]
        reg.refresh()
        second = reg.list_configs()
        assert "b" in second
        assert "a" not in second


# ------------------------------------------------------------------
# get_model_usage
# ------------------------------------------------------------------


class TestGetModelUsage:
    """Verify model storage usage enumeration and caching."""

    @patch(
        "ami.storage.registry.StorageConfigFactory.get_all_configs",
    )
    @patch("ami.storage.registry._iter_storage_models")
    def test_returns_model_storage_usage(
        self,
        mock_iter: MagicMock,
        mock_get_all: MagicMock,
    ) -> None:
        mock_get_all.return_value = []
        cfg = _make_config(name="pg")
        model_cls = MagicMock()
        model_cls.__module__ = "ami.models.widget"
        model_cls.__name__ = "Widget"
        meta = MagicMock()
        meta.storage_configs = {"primary": cfg}
        model_cls.get_metadata.return_value = meta
        mock_iter.return_value = [model_cls]

        reg = StorageRegistry()
        usages = reg.get_model_usage()
        assert len(usages) >= 1
        entry = usages[0]
        assert isinstance(entry, ModelStorageUsage)
        assert entry.model == "ami.models.widget.Widget"

    @patch(
        "ami.storage.registry.StorageConfigFactory.get_all_configs",
    )
    @patch("ami.storage.registry._iter_storage_models")
    def test_caches_results(
        self,
        mock_iter: MagicMock,
        mock_get_all: MagicMock,
    ) -> None:
        mock_get_all.return_value = []
        cfg = _make_config(name="pg")
        model_cls = MagicMock()
        model_cls.__module__ = "mod"
        model_cls.__name__ = "M"
        meta = MagicMock()
        meta.storage_configs = {"db": cfg}
        model_cls.get_metadata.return_value = meta
        mock_iter.return_value = [model_cls]

        reg = StorageRegistry()
        first = reg.get_model_usage()
        second = reg.get_model_usage()
        assert len(first) == len(second)
        # _iter_storage_models called only once due to cache
        mock_iter.assert_called_once()


# ------------------------------------------------------------------
# get_config_usage_index
# ------------------------------------------------------------------


class TestGetConfigUsageIndex:
    """Verify reverse index from config name to model names."""

    @patch(
        "ami.storage.registry.StorageConfigFactory.get_all_configs",
    )
    @patch("ami.storage.registry._iter_storage_models")
    def test_reverse_index(
        self,
        mock_iter: MagicMock,
        mock_get_all: MagicMock,
    ) -> None:
        mock_get_all.return_value = []
        cfg = _make_config(name="pg")
        model_cls = MagicMock()
        model_cls.__module__ = "ami.m"
        model_cls.__name__ = "Foo"
        meta = MagicMock()
        meta.storage_configs = {"pg": cfg}
        model_cls.get_metadata.return_value = meta
        mock_iter.return_value = [model_cls]

        reg = StorageRegistry()
        index = reg.get_config_usage_index()
        assert "pg" in index
        assert "ami.m.Foo" in index["pg"]

    @patch(
        "ami.storage.registry.StorageConfigFactory.get_all_configs",
    )
    @patch("ami.storage.registry._iter_storage_models")
    def test_multiple_models_same_config(
        self,
        mock_iter: MagicMock,
        mock_get_all: MagicMock,
    ) -> None:
        mock_get_all.return_value = []
        cfg = _make_config(name="pg")

        cls_a = MagicMock()
        cls_a.__module__ = "m"
        cls_a.__name__ = "A"
        meta_a = MagicMock()
        meta_a.storage_configs = {"pg": cfg}
        cls_a.get_metadata.return_value = meta_a

        cls_b = MagicMock()
        cls_b.__module__ = "m"
        cls_b.__name__ = "B"
        meta_b = MagicMock()
        meta_b.storage_configs = {"pg": cfg}
        cls_b.get_metadata.return_value = meta_b

        mock_iter.return_value = [cls_a, cls_b]

        reg = StorageRegistry()
        index = reg.get_config_usage_index()
        assert len(index["pg"]) == ENTRY_COUNT_2


# ------------------------------------------------------------------
# _resolve_model_storages (static)
# ------------------------------------------------------------------


class TestResolveModelStorages:
    """Verify dict and list input resolution."""

    def test_dict_input_produces_tuples(self) -> None:
        cfg = _make_config(name="pg")
        result = StorageRegistry._resolve_model_storages(
            {"primary": cfg},
        )
        assert len(result) == 1
        name, config = result[0]
        assert name == "pg"
        assert config is cfg

    def test_dict_uses_key_when_unnamed(self) -> None:
        cfg = _make_config(name=None)
        result = StorageRegistry._resolve_model_storages(
            {"alias": cfg},
        )
        name, _ = result[0]
        assert name == "alias"

    def test_list_input_produces_tuples(self) -> None:
        cfg = _make_config(name="mongo")
        result = StorageRegistry._resolve_model_storages([cfg])
        assert len(result) == 1
        name, config = result[0]
        assert name == "mongo"
        assert config is cfg

    def test_list_derives_name_from_type(self) -> None:
        cfg = _make_config(
            name=None,
            storage_type=StorageType.DOCUMENT,
            port=PORT_27017,
        )
        result = StorageRegistry._resolve_model_storages([cfg])
        name, _ = result[0]
        assert name == "mongodb"

    def test_list_derives_indexed_name(self) -> None:
        cfg = _make_config(name=None, storage_type=None)
        result = StorageRegistry._resolve_model_storages([cfg])
        name, _ = result[0]
        assert name == "storage_0"

    def test_empty_dict_returns_empty(self) -> None:
        assert StorageRegistry._resolve_model_storages({}) == []

    def test_empty_list_returns_empty(self) -> None:
        assert StorageRegistry._resolve_model_storages([]) == []

    def test_multiple_list_entries(self) -> None:
        cfg_a = _make_config(name="a")
        cfg_b = _make_config(name="b")
        result = StorageRegistry._resolve_model_storages(
            [cfg_a, cfg_b],
        )
        assert len(result) == ENTRY_COUNT_2
        assert result[0][0] == "a"
        assert result[1][0] == "b"


# ------------------------------------------------------------------
# _summarize_config (static)
# ------------------------------------------------------------------


class TestSummarizeConfig:
    """Verify config summary structure and sanitization."""

    def test_includes_required_fields(self) -> None:
        cfg = _make_config(
            name="pg",
            host="db.local",
            port=PORT_5432,
            database="mydb",
        )
        summary = StorageRegistry._summarize_config("pg", cfg)
        assert summary["name"] == "pg"
        assert summary["storage_type"] == "postgres"
        assert summary["host"] == "db.local"
        assert summary["port"] == PORT_5432
        assert summary["database"] == "mydb"

    def test_has_credentials_true(self) -> None:
        cfg = _make_config(username="u", password="p")
        summary = StorageRegistry._summarize_config("x", cfg)
        assert summary["has_credentials"] is True

    def test_has_credentials_false(self) -> None:
        cfg = _make_config(username=None, password=None)
        summary = StorageRegistry._summarize_config("x", cfg)
        assert summary["has_credentials"] is False

    def test_has_credentials_username_only(self) -> None:
        cfg = _make_config(username="u", password=None)
        summary = StorageRegistry._summarize_config("x", cfg)
        assert summary["has_credentials"] is True

    def test_options_sanitized(self) -> None:
        cfg = _make_config(
            options={
                "ssl_key": "val",
                "retries": RETRIES_3,
            },
        )
        summary = StorageRegistry._summarize_config("x", cfg)
        assert summary["options"]["ssl_key"] == REDACTED
        assert summary["options"]["retries"] == RETRIES_3

    def test_none_storage_type(self) -> None:
        cfg = _make_config(storage_type=None)
        summary = StorageRegistry._summarize_config("x", cfg)
        assert summary["storage_type"] is None

    def test_empty_options(self) -> None:
        cfg = _make_config(options={})
        summary = StorageRegistry._summarize_config("x", cfg)
        assert summary["options"] == {}
