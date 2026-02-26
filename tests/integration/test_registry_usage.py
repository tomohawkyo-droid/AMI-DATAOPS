"""Integration tests for StorageRegistry with real StorageConfigFactory.

Verifies the registry end-to-end through the factory layer using mocked
YAML data -- no live databases, no direct factory mocks on the registry.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from ami.core.storage_types import StorageType
from ami.models.storage_config import StorageConfig
from ami.models.storage_config_factory import invalidate_yaml_cache
from ami.storage.registry import (
    ModelStorageUsage,
    StorageRegistry,
)

# -- Named constants (ruff: no magic numbers) --------------------------

PORT_PG = 5432
PORT_REDIS = 6379
EXPECTED_CONFIG_COUNT = 2
REDACTED_MARKER = "***REDACTED***"

YAML_FIXTURE: dict[str, Any] = {
    "storage_configs": {
        "testdb": {
            "type": "relational",
            "host": "db.local",
            "port": PORT_PG,
            "database": "mydb",
            "username": "admin",
            "password": "secret123",
        },
        "cache": {
            "type": "inmem",
            "host": "redis.local",
            "port": PORT_REDIS,
        },
    }
}

_YAML_PATCH_TARGET = "ami.models.storage_config_factory._load_yaml"


@pytest.fixture(autouse=True)
def _clear_yaml_cache() -> Any:
    """Ensure YAML cache is fresh before and after every test."""
    invalidate_yaml_cache()
    yield
    invalidate_yaml_cache()


# ======================================================================
# 1. StorageRegistry with mocked YAML
# ======================================================================


class TestRegistryWithMockedYaml:
    """Registry integrates with StorageConfigFactory using mocked YAML."""

    @patch(_YAML_PATCH_TARGET, return_value=YAML_FIXTURE)
    def test_list_configs_returns_all(
        self,
        mock_yaml: MagicMock,
    ) -> None:
        reg = StorageRegistry()
        configs = reg.list_configs()
        assert len(configs) == EXPECTED_CONFIG_COUNT
        assert "testdb" in configs
        assert "cache" in configs

    @patch(_YAML_PATCH_TARGET, return_value=YAML_FIXTURE)
    def test_list_configs_types_correct(
        self,
        mock_yaml: MagicMock,
    ) -> None:
        reg = StorageRegistry()
        configs = reg.list_configs()
        assert configs["testdb"].storage_type == StorageType.RELATIONAL
        assert configs["cache"].storage_type == StorageType.INMEM

    @patch(_YAML_PATCH_TARGET, return_value=YAML_FIXTURE)
    def test_get_config_returns_expected(
        self,
        mock_yaml: MagicMock,
    ) -> None:
        reg = StorageRegistry()
        cfg = reg.get_config("testdb")
        assert cfg.host == "db.local"
        assert cfg.port == PORT_PG
        assert cfg.database == "mydb"

    @patch(_YAML_PATCH_TARGET, return_value=YAML_FIXTURE)
    def test_get_config_missing_raises(
        self,
        mock_yaml: MagicMock,
    ) -> None:
        reg = StorageRegistry()
        with pytest.raises(KeyError, match="Unknown storage config"):
            reg.get_config("nonexistent")

    @patch(_YAML_PATCH_TARGET, return_value=YAML_FIXTURE)
    def test_list_config_summaries_count(
        self,
        mock_yaml: MagicMock,
    ) -> None:
        reg = StorageRegistry()
        summaries = reg.list_config_summaries()
        assert len(summaries) == EXPECTED_CONFIG_COUNT


# ======================================================================
# 2. Config summaries redact credentials
# ======================================================================


class TestSummaryRedaction:
    """Summaries produced via the factory must redact secrets."""

    @patch(_YAML_PATCH_TARGET, return_value=YAML_FIXTURE)
    def test_password_not_in_options(
        self,
        mock_yaml: MagicMock,
    ) -> None:
        """Password lives on the config, not options -- verify
        summary uses has_credentials flag instead."""
        reg = StorageRegistry()
        summaries = reg.list_config_summaries()
        testdb_summary = next(s for s in summaries if s["name"] == "testdb")
        assert testdb_summary["has_credentials"] is True
        assert "password" not in testdb_summary

    @patch(
        _YAML_PATCH_TARGET,
        return_value={
            "storage_configs": {
                "secured": {
                    "type": "relational",
                    "host": "h",
                    "port": PORT_PG,
                    "database": "d",
                    "username": "u",
                    "password": "p",
                    "options": {
                        "ssl_key": "private",
                        "pool_size": 5,
                    },
                },
            },
        },
    )
    def test_sensitive_options_redacted(
        self,
        mock_yaml: MagicMock,
    ) -> None:
        reg = StorageRegistry()
        summaries = reg.list_config_summaries()
        opts = summaries[0]["options"]
        assert opts["ssl_key"] == REDACTED_MARKER
        pool_size_expected = 5
        assert opts["pool_size"] == pool_size_expected

    @patch(_YAML_PATCH_TARGET, return_value=YAML_FIXTURE)
    def test_cache_entry_has_no_credentials(
        self,
        mock_yaml: MagicMock,
    ) -> None:
        reg = StorageRegistry()
        summaries = reg.list_config_summaries()
        cache_summary = next(s for s in summaries if s["name"] == "cache")
        assert cache_summary["has_credentials"] is False


# ======================================================================
# 3. Model usage iteration
# ======================================================================


class TestModelUsageIteration:
    """get_model_usage returns ModelStorageUsage for subclasses."""

    @patch(_YAML_PATCH_TARGET, return_value=YAML_FIXTURE)
    @patch("ami.storage.registry._iter_storage_models")
    def test_returns_model_storage_usage_objects(
        self,
        mock_iter: MagicMock,
        mock_yaml: MagicMock,
    ) -> None:
        cfg = StorageConfig(
            name="pg",
            storage_type=StorageType.RELATIONAL,
            host="h",
            port=PORT_PG,
        )
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
        first = usages[0]
        assert isinstance(first, ModelStorageUsage)
        assert first.model == "ami.models.widget.Widget"
        assert len(first.storages) >= 1
        assert first.storages[0]["primary"] is True

    @patch(_YAML_PATCH_TARGET, return_value=YAML_FIXTURE)
    @patch("ami.storage.registry._iter_storage_models")
    def test_skips_models_without_storage(
        self,
        mock_iter: MagicMock,
        mock_yaml: MagicMock,
    ) -> None:
        model_cls = MagicMock()
        model_cls.__module__ = "m"
        model_cls.__name__ = "Empty"
        meta = MagicMock()
        meta.storage_configs = {}
        model_cls.get_metadata.return_value = meta
        mock_iter.return_value = [model_cls]

        reg = StorageRegistry()
        usages = reg.get_model_usage()
        assert len(usages) == 0


# ======================================================================
# 4. Config usage index (reverse mapping)
# ======================================================================


class TestConfigUsageIndex:
    """get_config_usage_index maps config names back to models."""

    @patch(_YAML_PATCH_TARGET, return_value=YAML_FIXTURE)
    @patch("ami.storage.registry._iter_storage_models")
    def test_reverse_mapping_contains_model(
        self,
        mock_iter: MagicMock,
        mock_yaml: MagicMock,
    ) -> None:
        cfg = StorageConfig(
            name="testdb",
            storage_type=StorageType.RELATIONAL,
            host="h",
            port=PORT_PG,
        )
        cls_a = MagicMock()
        cls_a.__module__ = "ami.models"
        cls_a.__name__ = "Alpha"
        meta_a = MagicMock()
        meta_a.storage_configs = {"testdb": cfg}
        cls_a.get_metadata.return_value = meta_a
        mock_iter.return_value = [cls_a]

        reg = StorageRegistry()
        index = reg.get_config_usage_index()
        assert "testdb" in index
        assert "ami.models.Alpha" in index["testdb"]

    @patch(_YAML_PATCH_TARGET, return_value=YAML_FIXTURE)
    @patch("ami.storage.registry._iter_storage_models")
    def test_multiple_models_same_config(
        self,
        mock_iter: MagicMock,
        mock_yaml: MagicMock,
    ) -> None:
        cfg = StorageConfig(
            name="cache",
            storage_type=StorageType.INMEM,
            host="h",
            port=PORT_REDIS,
        )
        cls_a = MagicMock()
        cls_a.__module__ = "m"
        cls_a.__name__ = "A"
        meta_a = MagicMock()
        meta_a.storage_configs = {"cache": cfg}
        cls_a.get_metadata.return_value = meta_a

        cls_b = MagicMock()
        cls_b.__module__ = "m"
        cls_b.__name__ = "B"
        meta_b = MagicMock()
        meta_b.storage_configs = {"cache": cfg}
        cls_b.get_metadata.return_value = meta_b

        mock_iter.return_value = [cls_a, cls_b]

        reg = StorageRegistry()
        index = reg.get_config_usage_index()
        assert len(index["cache"]) == EXPECTED_CONFIG_COUNT


# ======================================================================
# 5. _resolve_model_storages with dict and list inputs
# ======================================================================


class TestResolveModelStorages:
    """_resolve_model_storages handles dict and list inputs."""

    def test_dict_input_uses_config_name(self) -> None:
        cfg = StorageConfig(
            name="pg",
            storage_type=StorageType.RELATIONAL,
            host="h",
            port=PORT_PG,
        )
        result = StorageRegistry._resolve_model_storages(
            {"primary": cfg},
        )
        assert len(result) == 1
        name, resolved_cfg = result[0]
        assert name == "pg"
        assert resolved_cfg is cfg

    def test_dict_input_falls_back_to_key(self) -> None:
        cfg = StorageConfig(
            name=None,
            storage_type=StorageType.RELATIONAL,
            host="h",
            port=PORT_PG,
        )
        result = StorageRegistry._resolve_model_storages(
            {"alias": cfg},
        )
        name, _ = result[0]
        assert name == "alias"

    def test_list_input_uses_config_name(self) -> None:
        cfg = StorageConfig(
            name="redis",
            storage_type=StorageType.INMEM,
            host="h",
            port=PORT_REDIS,
        )
        result = StorageRegistry._resolve_model_storages([cfg])
        assert len(result) == 1
        name, resolved_cfg = result[0]
        assert name == "redis"
        assert resolved_cfg is cfg

    def test_list_input_derives_from_type(self) -> None:
        cfg = StorageConfig(
            name=None,
            storage_type=StorageType.INMEM,
            host="h",
            port=PORT_REDIS,
        )
        result = StorageRegistry._resolve_model_storages([cfg])
        name, _ = result[0]
        assert name == StorageType.INMEM.value

    def test_list_input_indexed_name_when_no_type(self) -> None:
        cfg = StorageConfig(
            name=None,
            storage_type=None,
            host="h",
        )
        result = StorageRegistry._resolve_model_storages([cfg])
        name, _ = result[0]
        assert name == "storage_0"
