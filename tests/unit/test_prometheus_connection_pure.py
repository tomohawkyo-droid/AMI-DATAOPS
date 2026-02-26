"""Tests for pure helpers in prometheus_connection."""

from __future__ import annotations

from ami.core.storage_types import StorageType
from ami.implementations.timeseries.prometheus_connection import (
    build_base_url,
)
from ami.models.storage_config import StorageConfig


def _make_config(**kwargs) -> StorageConfig:
    """Build a StorageConfig with timeseries defaults."""
    kwargs.setdefault("storage_type", StorageType.TIMESERIES)
    return StorageConfig(**kwargs)


# ---------------------------------------------------------------
# build_base_url
# ---------------------------------------------------------------


class TestBuildBaseUrlFromConnectionString:
    """When a connection_string is present it takes priority."""

    def test_uses_connection_string(self) -> None:
        cfg = _make_config(
            connection_string="http://vm.internal:8428",
        )
        assert build_base_url(cfg) == "http://vm.internal:8428"

    def test_strips_trailing_slash(self) -> None:
        cfg = _make_config(
            connection_string="http://vm.internal:8428/",
        )
        assert build_base_url(cfg) == "http://vm.internal:8428"

    def test_strips_multiple_trailing_slashes(self) -> None:
        cfg = _make_config(
            connection_string="http://vm.internal:8428///",
        )
        assert build_base_url(cfg) == "http://vm.internal:8428"


class TestBuildBaseUrlFromHostPort:
    """Derived from host / port when no connection_string."""

    def test_host_and_port(self) -> None:
        cfg = _make_config(host="prom.lan", port=9999)
        assert build_base_url(cfg) == "http://prom.lan:9999"

    def test_host_only_uses_default_port(self) -> None:
        cfg = _make_config(host="prom.lan")
        # StorageConfig sets default port 9090 for TIMESERIES
        assert build_base_url(cfg) == "http://prom.lan:9090"

    def test_port_only_uses_localhost(self) -> None:
        cfg = _make_config(port=8428)
        assert build_base_url(cfg) == "http://localhost:8428"


class TestBuildBaseUrlDefaults:
    """Edge cases and defaults."""

    def test_none_config_returns_localhost(self) -> None:
        assert build_base_url(None) == "http://localhost:9090"

    def test_empty_config_uses_defaults(self) -> None:
        cfg = _make_config()
        # TIMESERIES type auto-assigns port 9090
        assert build_base_url(cfg) == "http://localhost:9090"

    def test_connection_string_overrides_host_port(self) -> None:
        cfg = _make_config(
            host="ignored.host",
            port=1234,
            connection_string="http://actual:8428",
        )
        assert build_base_url(cfg) == "http://actual:8428"
