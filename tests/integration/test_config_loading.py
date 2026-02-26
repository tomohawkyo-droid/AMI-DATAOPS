"""Integration smoke tests for configuration loading.

Covers Issue #68 -- populate empty tests/integration/ directory.
These tests verify that YAML config loading and caching works correctly
without requiring live database connections.
"""

import pytest

from ami.models.storage_config_factory import (
    StorageConfigFactory,
    _load_yaml,
    invalidate_yaml_cache,
)


class TestConfigLoading:
    """Verify storage config YAML can be loaded and cached."""

    def test_yaml_file_loads(self) -> None:
        invalidate_yaml_cache()
        raw = _load_yaml()
        assert isinstance(raw, dict)
        assert "storage_configs" in raw

    def test_yaml_has_expected_backends(self) -> None:
        invalidate_yaml_cache()
        raw = _load_yaml()
        configs = raw["storage_configs"]
        expected = {"dgraph", "mongodb", "postgres", "pgvector", "redis"}
        assert expected.issubset(set(configs.keys()))

    def test_yaml_caching(self) -> None:
        invalidate_yaml_cache()
        raw_a = _load_yaml()
        raw_b = _load_yaml()
        assert raw_a is raw_b

    def test_yaml_cache_invalidation(self) -> None:
        invalidate_yaml_cache()
        raw_a = _load_yaml()
        invalidate_yaml_cache()
        raw_b = _load_yaml()
        assert raw_a is not raw_b
        assert raw_a == raw_b

    def test_missing_config_raises(self) -> None:
        invalidate_yaml_cache()

        with pytest.raises(KeyError, match="not found"):
            StorageConfigFactory.from_yaml("nonexistent_backend")

    def test_each_backend_has_type(self) -> None:
        invalidate_yaml_cache()
        raw = _load_yaml()
        for name, cfg in raw["storage_configs"].items():
            assert "type" in cfg, f"Backend '{name}' missing 'type'"
