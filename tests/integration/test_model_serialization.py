"""Integration tests for model serialization roundtrips."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import ClassVar

import pytest
from pydantic import Field

from ami.implementations.timeseries.prometheus_models import (
    PrometheusMetric,
    PromQLBuilder,
    _escape_promql_value,
)
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.secret_pointer import SecretPointerRecord
from ami.models.ssh_config import SSHConfig

# -----------------------------------------------------------------
# Named constants (ruff PLR2004)
# -----------------------------------------------------------------

_ROUNDTRIP_COUNT = 5
_SECRET_VERSION_DEFAULT = 1
_SECRET_ROTATION_DEFAULT = 0
_SSH_DEFAULT_PORT = 22
_SSH_DEFAULT_TIMEOUT = 30
_SSH_CUSTOM_PORT = 2222
_SSH_CUSTOM_TIMEOUT = 10
_METRIC_VALUE = 42.0
_METRIC_TS_EPOCH = 1_700_000_000
_METRIC_TS_MS = _METRIC_TS_EPOCH * 1000


# -----------------------------------------------------------------
# Helper model for round-trip tests
# -----------------------------------------------------------------


class _RoundTripModel(StorageModel):
    """Minimal model for serialization round-trips."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(path="rt")
    name: str = ""
    count: int = 0
    tags: list[str] = Field(default_factory=list)


# =================================================================
# StorageModel round-trip
# =================================================================


class TestStorageModelRoundTrip:
    """Verify to_storage_dict / from_storage_dict preserves data."""

    @pytest.mark.asyncio
    async def test_basic_fields_survive(self) -> None:
        inst = _RoundTripModel(
            name="alice",
            count=_ROUNDTRIP_COUNT,
        )
        data = await inst.to_storage_dict()
        restored = await _RoundTripModel.from_storage_dict(data)
        assert restored.name == "alice"
        assert restored.count == _ROUNDTRIP_COUNT

    @pytest.mark.asyncio
    async def test_uid_preserved(self) -> None:
        inst = _RoundTripModel(name="uid-check")
        original_uid = inst.uid
        data = await inst.to_storage_dict()
        restored = await _RoundTripModel.from_storage_dict(data)
        assert restored.uid == original_uid

    @pytest.mark.asyncio
    async def test_updated_at_preserved(self) -> None:
        inst = _RoundTripModel(name="ts-check")
        original_ts = inst.updated_at
        data = await inst.to_storage_dict()
        restored = await _RoundTripModel.from_storage_dict(data)
        assert restored.updated_at == original_ts

    @pytest.mark.asyncio
    async def test_list_field_preserved(self) -> None:
        inst = _RoundTripModel(
            name="tagged",
            tags=["alpha", "beta"],
        )
        data = await inst.to_storage_dict()
        restored = await _RoundTripModel.from_storage_dict(data)
        assert restored.tags == ["alpha", "beta"]

    @pytest.mark.asyncio
    async def test_storage_internals_excluded(self) -> None:
        inst = _RoundTripModel(name="clean")
        data = await inst.to_storage_dict()
        assert "storage_configs" not in data
        assert "path" not in data

    @pytest.mark.asyncio
    async def test_empty_defaults_round_trip(self) -> None:
        inst = _RoundTripModel()
        data = await inst.to_storage_dict()
        restored = await _RoundTripModel.from_storage_dict(data)
        assert restored.name == ""
        assert restored.count == 0
        assert restored.tags == []


# =================================================================
# SecretPointerRecord
# =================================================================


class TestSecretPointerRecord:
    """Field defaults and serialization for SecretPointerRecord."""

    def test_default_field_values(self) -> None:
        record = SecretPointerRecord()
        assert record.vault_reference == ""
        assert record.namespace == ""
        assert record.model_name == ""
        assert record.field_name == ""
        assert record.integrity_hash == ""
        assert record.version == _SECRET_VERSION_DEFAULT
        assert record.rotation_count == _SECRET_ROTATION_DEFAULT
        assert record.secret_last_accessed_at is None
        assert record.status == "active"

    def test_timestamps_auto_populated(self) -> None:
        record = SecretPointerRecord()
        assert isinstance(record.secret_created_at, datetime)
        assert isinstance(record.secret_updated_at, datetime)

    @pytest.mark.asyncio
    async def test_round_trip_preserves_fields(self) -> None:
        record = SecretPointerRecord(
            vault_reference="vault://secrets/db-pass",
            namespace="production",
            model_name="DatabaseConfig",
            field_name="password",
            integrity_hash="sha256:abc123",
        )
        data = await record.to_storage_dict()
        restored = await SecretPointerRecord.from_storage_dict(data)
        assert restored.vault_reference == "vault://secrets/db-pass"
        assert restored.namespace == "production"
        assert restored.model_name == "DatabaseConfig"
        assert restored.field_name == "password"
        assert restored.integrity_hash == "sha256:abc123"

    def test_collection_name(self) -> None:
        record = SecretPointerRecord()
        name = record.get_collection_name()
        assert name == "secret_pointer_records"

    def test_metadata_has_indexes(self) -> None:
        meta = SecretPointerRecord.get_metadata()
        assert len(meta.indexes) > 0
        index_fields = [idx["field"] for idx in meta.indexes]
        assert "vault_reference" in index_fields
        assert "namespace" in index_fields


# =================================================================
# PrometheusMetric
# =================================================================


class TestPrometheusMetric:
    """Creation, selector formatting, and exposition output."""

    def test_basic_creation(self) -> None:
        metric = PrometheusMetric(
            metric_name="http_requests_total",
            labels={"method": "GET"},
            value=_METRIC_VALUE,
        )
        assert metric.metric_name == "http_requests_total"
        assert metric.value == _METRIC_VALUE
        assert metric.labels == {"method": "GET"}

    def test_to_promql_selector_no_labels(self) -> None:
        metric = PrometheusMetric(metric_name="up")
        assert metric.to_promql_selector() == "up"

    def test_to_promql_selector_with_labels(self) -> None:
        metric = PrometheusMetric(
            metric_name="http_requests_total",
            labels={"method": "GET", "status": "200"},
        )
        selector = metric.to_promql_selector()
        assert selector.startswith("http_requests_total{")
        assert 'method="GET"' in selector
        assert 'status="200"' in selector
        assert selector.endswith("}")

    def test_to_promql_selector_labels_sorted(self) -> None:
        metric = PrometheusMetric(
            metric_name="m",
            labels={"z_key": "z", "a_key": "a"},
        )
        selector = metric.to_promql_selector()
        a_pos = selector.index("a_key")
        z_pos = selector.index("z_key")
        assert a_pos < z_pos

    def test_to_exposition_line_without_timestamp(self) -> None:
        metric = PrometheusMetric(
            metric_name="up",
            value=1.0,
        )
        line = metric.to_exposition_line()
        assert line == "up 1.0"

    def test_to_exposition_line_with_timestamp(self) -> None:
        ts = datetime.fromtimestamp(_METRIC_TS_EPOCH, tz=UTC)
        metric = PrometheusMetric(
            metric_name="up",
            value=1.0,
            timestamp=ts,
        )
        line = metric.to_exposition_line()
        assert f"up 1.0 {_METRIC_TS_MS}" == line

    def test_escape_promql_value_backslash(self) -> None:
        escaped = _escape_promql_value('a\\b"c\nd')
        assert escaped == 'a\\\\b\\"c\\nd'


# =================================================================
# PromQLBuilder
# =================================================================


class TestPromQLBuilder:
    """PromQLBuilder query construction helpers."""

    def test_instant_query_no_labels(self) -> None:
        result = PromQLBuilder.instant_query("up")
        assert result == "up"

    def test_instant_query_with_labels(self) -> None:
        result = PromQLBuilder.instant_query(
            "http_requests_total",
            {"method": "POST"},
        )
        assert result == 'http_requests_total{method="POST"}'

    def test_range_query(self) -> None:
        result = PromQLBuilder.range_query(
            "http_requests_total",
            {"method": "GET"},
            duration="10m",
        )
        assert result == 'http_requests_total{method="GET"}[10m]'

    def test_rate_query(self) -> None:
        result = PromQLBuilder.rate_query("http_requests_total")
        assert result == "rate(http_requests_total[5m])"


# =================================================================
# SSHConfig
# =================================================================


class TestSSHConfigValidation:
    """SSHConfig field validation rules."""

    def test_valid_name_accepted(self) -> None:
        cfg = SSHConfig(name="web-server_01")
        assert cfg.name == "web-server_01"

    def test_invalid_name_rejected(self) -> None:
        with pytest.raises(ValueError, match="alphanumeric"):
            SSHConfig(name="bad name!")

    def test_default_port(self) -> None:
        cfg = SSHConfig()
        assert cfg.port == _SSH_DEFAULT_PORT

    def test_default_timeout(self) -> None:
        cfg = SSHConfig()
        assert cfg.timeout == _SSH_DEFAULT_TIMEOUT

    def test_name_none_is_valid(self) -> None:
        cfg = SSHConfig(name=None)
        assert cfg.name is None


class TestSSHConfigParamiko:
    """SSHConfig.to_paramiko_config output mapping."""

    def test_minimal_config(self) -> None:
        cfg = SSHConfig(host="10.0.0.1", username="deploy")
        params = cfg.to_paramiko_config()
        assert params["hostname"] == "10.0.0.1"
        assert params["username"] == "deploy"
        assert params["port"] == _SSH_DEFAULT_PORT
        assert params["timeout"] == _SSH_DEFAULT_TIMEOUT

    def test_custom_port_and_timeout(self) -> None:
        cfg = SSHConfig(
            host="10.0.0.2",
            port=_SSH_CUSTOM_PORT,
            timeout=_SSH_CUSTOM_TIMEOUT,
        )
        params = cfg.to_paramiko_config()
        assert params["port"] == _SSH_CUSTOM_PORT
        assert params["timeout"] == _SSH_CUSTOM_TIMEOUT

    def test_key_filename_included(self) -> None:
        cfg = SSHConfig(
            host="10.0.0.3",
            key_filename="/tmp/test_keys/id_ed25519",
        )
        params = cfg.to_paramiko_config()
        assert params["key_filename"] == "/tmp/test_keys/id_ed25519"

    def test_password_included(self) -> None:
        cfg = SSHConfig(host="10.0.0.4", password="s3cret")
        params = cfg.to_paramiko_config()
        assert params["password"] == "s3cret"

    def test_compression_maps_to_compress(self) -> None:
        cfg = SSHConfig(host="10.0.0.5", compression=True)
        params = cfg.to_paramiko_config()
        assert params["compress"] is True

    def test_known_hosts_maps_to_filename_key(self) -> None:
        cfg = SSHConfig(
            host="10.0.0.6",
            known_hosts_file="/etc/ssh/known_hosts",
        )
        params = cfg.to_paramiko_config()
        key = "known_hosts_filename"
        assert params[key] == "/etc/ssh/known_hosts"

    def test_optional_fields_absent_when_none(self) -> None:
        cfg = SSHConfig(host="10.0.0.7")
        params = cfg.to_paramiko_config()
        assert "password" not in params
        assert "key_filename" not in params
        assert "passphrase" not in params
        assert "known_hosts_filename" not in params
