"""Tests for PrometheusDAO CRUD and lifecycle operations."""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import Field

from ami.core.storage_types import StorageType
from ami.implementations.timeseries.prometheus_dao import (
    PrometheusDAO,
)
from ami.implementations.timeseries.prometheus_models import (
    PrometheusMetric,
)
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

# Patch target prefixes
_CONN = "ami.implementations.timeseries.prometheus_dao"
_WRITE = "ami.implementations.timeseries.prometheus_dao"
_READ = "ami.implementations.timeseries.prometheus_dao"
_HTTP = "ami.implementations.timeseries.prometheus_dao"

# Expected counts used in assertions
_EXPECTED_BULK_COUNT = 2
_EXPECTED_FIND_COUNT = 2
_EXPECTED_COUNT_LEN = 2
_EXPECTED_LINE_COUNT = 2
_EXPECTED_METRIC_VALUE = 99.0


# ------------------------------------------------------------------
# Test model and helpers
# ------------------------------------------------------------------


class _TestModel(StorageModel):
    """Minimal model for PrometheusDAO tests."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="test_metrics",
    )

    name: str = ""
    value: float = 0.0
    labels: dict[str, str] = Field(default_factory=dict)


def _make_config(**overrides: Any) -> StorageConfig:
    """Build a TIMESERIES StorageConfig with defaults."""
    defaults: dict[str, Any] = {
        "storage_type": StorageType.TIMESERIES,
        "host": "localhost",
        "port": 9090,
    }
    defaults.update(overrides)
    return StorageConfig(**defaults)


def _make_dao(
    config: StorageConfig | None = None,
    model_cls: type[Any] | None = None,
) -> PrometheusDAO:
    """Create a PrometheusDAO with a mocked session."""
    cfg = config or _make_config()
    cls = model_cls or _TestModel
    dao = PrometheusDAO(cls, cfg)
    dao.session = AsyncMock()
    dao.session.closed = False
    dao._connected = True
    return dao


# ------------------------------------------------------------------
# TestConnect
# ------------------------------------------------------------------


class TestConnect:
    """Validate connect lifecycle."""

    async def test_connect_stores_session(self) -> None:
        cfg = _make_config()
        dao = PrometheusDAO(_TestModel, cfg)
        mock_session = AsyncMock()
        mock_session.closed = False
        with patch(
            f"{_CONN}.create_session",
            new_callable=AsyncMock,
            return_value=mock_session,
        ):
            await dao.connect()
        assert dao.session is mock_session
        assert dao._connected is True

    async def test_connect_skips_if_open(self) -> None:
        dao = _make_dao()
        original = dao.session
        await dao.connect()
        assert dao.session is original


# ------------------------------------------------------------------
# TestDisconnect
# ------------------------------------------------------------------


class TestDisconnect:
    """Validate disconnect clears session."""

    async def test_disconnect_clears_session(self) -> None:
        dao = _make_dao()
        original_session = dao.session
        with patch(
            f"{_CONN}.close_session",
            new_callable=AsyncMock,
        ) as mock_close:
            await dao.disconnect()
        mock_close.assert_awaited_once_with(original_session)

    async def test_disconnect_sets_none(self) -> None:
        dao = _make_dao()
        with patch(
            f"{_CONN}.close_session",
            new_callable=AsyncMock,
        ):
            await dao.disconnect()
        assert dao.session is None
        assert dao._connected is False


# ------------------------------------------------------------------
# TestCreate
# ------------------------------------------------------------------


class TestCreate:
    """Validate create operation with various input types."""

    async def test_create_with_prometheus_metric(
        self,
    ) -> None:
        dao = _make_dao()
        metric = PrometheusMetric(
            metric_name="http_total",
            labels={"method": "GET"},
            value=42.0,
        )
        uid = "http_total{method=GET}"
        with patch(
            f"{_WRITE}.write_single_metric",
            new_callable=AsyncMock,
            return_value=uid,
        ) as mock_write:
            result = await dao.create(metric)
        assert result == uid
        mock_write.assert_awaited_once()
        call_args = mock_write.call_args
        assert call_args[0][1] == "http_total"

    async def test_create_with_dict(self) -> None:
        dao = _make_dao()
        uid = "req_total{}"
        with patch(
            f"{_WRITE}.write_single_metric",
            new_callable=AsyncMock,
            return_value=uid,
        ):
            result = await dao.create({"metric_name": "req_total", "value": 10})
        assert result == uid

    async def test_create_with_model_instance(
        self,
    ) -> None:
        dao = _make_dao()
        instance = _TestModel(
            name="cpu",
            value=0.85,
            labels={"host": "srv1"},
        )
        uid = "test_metrics{host=srv1}"
        with patch(
            f"{_WRITE}.write_single_metric",
            new_callable=AsyncMock,
            return_value=uid,
        ):
            result = await dao.create(instance)
        assert result == uid


# ------------------------------------------------------------------
# TestBulkCreate
# ------------------------------------------------------------------


class TestBulkCreate:
    """Validate bulk_create writes metrics and returns IDs."""

    async def test_bulk_create_returns_synthetic_ids(
        self,
    ) -> None:
        dao = _make_dao()
        items = [
            {"metric_name": "m1", "labels": {"a": "1"}, "value": 1},
            {"metric_name": "m2", "labels": {}, "value": 2},
        ]
        with patch(
            f"{_WRITE}.write_metrics",
            new_callable=AsyncMock,
            return_value=2,
        ) as mock_wm:
            ids = await dao.bulk_create(items)
        mock_wm.assert_awaited_once()
        assert len(ids) == _EXPECTED_BULK_COUNT
        assert ids[0] == "m1{a=1}"
        assert ids[1] == "m2{}"

    async def test_bulk_create_with_metric_objects(
        self,
    ) -> None:
        dao = _make_dao()
        items = [
            PrometheusMetric(metric_name="x", labels={"k": "v"}, value=1),
        ]
        with patch(
            f"{_WRITE}.write_metrics",
            new_callable=AsyncMock,
            return_value=1,
        ):
            ids = await dao.bulk_create(items)
        assert ids == ["x{k=v}"]


# ------------------------------------------------------------------
# TestFindById
# ------------------------------------------------------------------


class TestFindById:
    """Validate find_by_id via instant_query."""

    async def test_found_returns_model(self) -> None:
        dao = _make_dao()
        record = {
            "metric_name": "test_metrics",
            "labels": {},
            "value": 5.0,
            "name": "cpu",
        }
        with patch(
            f"{_READ}.instant_query",
            new_callable=AsyncMock,
            return_value=[record],
        ):
            result = await dao.find_by_id("test_metrics{}")
        assert result is not None

    async def test_not_found_returns_none(self) -> None:
        dao = _make_dao()
        with patch(
            f"{_READ}.instant_query",
            new_callable=AsyncMock,
            return_value=[],
        ):
            result = await dao.find_by_id("missing{}")
        assert result is None

    async def test_prometheus_metric_model_cls(
        self,
    ) -> None:
        dao = _make_dao(model_cls=PrometheusMetric)
        record = {
            "metric_name": "http_total",
            "labels": {"method": "GET"},
            "value": 99.0,
        }
        with patch(
            f"{_READ}.instant_query",
            new_callable=AsyncMock,
            return_value=[record],
        ):
            result = await dao.find_by_id('http_total{method="GET"}')
        assert isinstance(result, PrometheusMetric)
        assert result.value == _EXPECTED_METRIC_VALUE


# ------------------------------------------------------------------
# TestFind
# ------------------------------------------------------------------


class TestFind:
    """Validate find delegates to find_metrics."""

    async def test_find_returns_list(self) -> None:
        dao = _make_dao()
        records = [
            {
                "metric_name": "test_metrics",
                "labels": {"env": "prod"},
                "value": 1.0,
                "name": "",
            },
            {
                "metric_name": "test_metrics",
                "labels": {"env": "dev"},
                "value": 2.0,
                "name": "",
            },
        ]
        with patch(
            f"{_READ}.find_metrics",
            new_callable=AsyncMock,
            return_value=records,
        ) as mock_fm:
            results = await dao.find({"env": "prod"})
        mock_fm.assert_awaited_once()
        assert len(results) == _EXPECTED_FIND_COUNT


# ------------------------------------------------------------------
# TestUpdate
# ------------------------------------------------------------------


class TestUpdate:
    """Validate update raises NotImplementedError."""

    async def test_update_raises(self) -> None:
        dao = _make_dao()
        with pytest.raises(
            NotImplementedError,
            match="append-only",
        ):
            await dao.update("id", {"value": 1})


# ------------------------------------------------------------------
# TestDelete
# ------------------------------------------------------------------


class TestDelete:
    """Validate delete raises NotImplementedError."""

    async def test_delete_raises(self) -> None:
        dao = _make_dao()
        with pytest.raises(
            NotImplementedError,
            match="append-only",
        ):
            await dao.delete("id")


# ------------------------------------------------------------------
# TestCount
# ------------------------------------------------------------------


class TestCount:
    """Validate count delegates to find."""

    async def test_count_returns_length(self) -> None:
        dao = _make_dao()
        dao.find = AsyncMock(return_value=["a", "b"])
        result = await dao.count({})
        assert result == _EXPECTED_COUNT_LEN

    async def test_count_empty(self) -> None:
        dao = _make_dao()
        dao.find = AsyncMock(return_value=[])
        result = await dao.count({"x": "y"})
        assert result == 0


# ------------------------------------------------------------------
# TestExists
# ------------------------------------------------------------------


class TestExists:
    """Validate exists delegates to find_by_id."""

    async def test_exists_true(self) -> None:
        dao = _make_dao()
        dao.find_by_id = AsyncMock(
            return_value=_TestModel(name="hit"),
        )
        assert await dao.exists("m1{}") is True

    async def test_exists_false(self) -> None:
        dao = _make_dao()
        dao.find_by_id = AsyncMock(return_value=None)
        assert await dao.exists("m2{}") is False


# ------------------------------------------------------------------
# TestRawReadQuery
# ------------------------------------------------------------------


class TestRawReadQuery:
    """Validate raw_read_query delegates to instant_query."""

    async def test_raw_read_returns_results(self) -> None:
        dao = _make_dao()
        expected = [{"metric_name": "up", "value": 1}]
        with patch(
            f"{_READ}.instant_query",
            new_callable=AsyncMock,
            return_value=expected,
        ) as mock_iq:
            result = await dao.raw_read_query("up")
        assert result == expected
        mock_iq.assert_awaited_once()

    async def test_raw_read_with_time_param(self) -> None:
        dao = _make_dao()
        with patch(
            f"{_READ}.instant_query",
            new_callable=AsyncMock,
            return_value=[],
        ) as mock_iq:
            await dao.raw_read_query("up", {"time": "1700000000"})
        call_kwargs = mock_iq.call_args
        assert call_kwargs.kwargs["time"] is not None


# ------------------------------------------------------------------
# TestRawWriteQuery
# ------------------------------------------------------------------


class TestRawWriteQuery:
    """Validate raw_write_query posts exposition lines."""

    async def test_raw_write_returns_line_count(
        self,
    ) -> None:
        dao = _make_dao()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.text = AsyncMock(return_value="ok")
        mock_resp.__aenter__ = AsyncMock(
            return_value=mock_resp,
        )
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        with patch(
            f"{_HTTP}.request_with_retry",
            new_callable=AsyncMock,
            return_value=mock_resp,
        ):
            payload = "metric_a 1\nmetric_b 2"
            result = await dao.raw_write_query(payload)
        assert result == _EXPECTED_LINE_COUNT


# ------------------------------------------------------------------
# TestResolveMetricName
# ------------------------------------------------------------------


class TestResolveMetricName:
    """Validate _resolve_metric_name priority chain."""

    def test_explicit_attribute(self) -> None:
        class _Explicit(StorageModel):
            _model_meta: ClassVar[ModelMetadata] = ModelMetadata()
            metric_name: ClassVar[str] = "explicit_metric"

        dao = PrometheusDAO(_Explicit, _make_config())
        assert dao._metric_name == "explicit_metric"

    def test_metadata_path(self) -> None:
        class _WithPath(StorageModel):
            _model_meta: ClassVar[ModelMetadata] = ModelMetadata(path="from_meta")

        dao = PrometheusDAO(_WithPath, _make_config())
        assert dao._metric_name == "from_meta"

    def test_default_name(self) -> None:
        class _Plain(StorageModel):
            _model_meta: ClassVar[ModelMetadata] = ModelMetadata()

        dao = PrometheusDAO(_Plain, _make_config())
        assert dao._metric_name == "_plain_total"
