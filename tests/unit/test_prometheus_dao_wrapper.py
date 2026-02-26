"""Tests covering uncovered lines in PrometheusDAO.

Targets: _ensure_session branches, bulk_create with generic model,
find_one, find with non-PrometheusMetric model_cls, bulk_update,
bulk_delete, create_indexes, raw_write_query error branch,
and all metadata delegation methods.
"""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.core.exceptions import StorageConnectionError, StorageError
from ami.core.storage_types import StorageType
from ami.implementations.timeseries.prometheus_dao import (
    PrometheusDAO,
)
from ami.implementations.timeseries.prometheus_models import (
    PrometheusMetric,
)
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

_DAO_MOD = "ami.implementations.timeseries.prometheus_dao"
_META_MOD = "ami.implementations.timeseries.prometheus_metadata"

_EXPECTED_TWO = 2
_EXPECTED_STATUS_400 = 400
_EXPECTED_STATUS_500 = 500
_DEFAULT_PORT = 9090


class _NonMetricModel(StorageModel):
    """Model that is NOT PrometheusMetric, for branch coverage."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="non_metric",
    )
    name: str = ""
    value: float = 0.0
    labels: dict[str, str] = MagicMock(default_factory=dict)


def _make_config(**overrides: Any) -> StorageConfig:
    defaults: dict[str, Any] = {
        "storage_type": StorageType.TIMESERIES,
        "host": "localhost",
        "port": _DEFAULT_PORT,
    }
    defaults.update(overrides)
    return StorageConfig(**defaults)


def _make_dao(
    model_cls: type[Any] | None = None,
) -> PrometheusDAO:
    """Create a PrometheusDAO with a pre-attached mock session."""
    cfg = _make_config()
    cls = model_cls or PrometheusMetric
    dao = PrometheusDAO(cls, cfg)
    dao.session = AsyncMock()
    dao.session.closed = False
    dao._connected = True
    return dao


# -- _ensure_session: line 105 (session None or closed) -----------


class TestEnsureSessionConnect:
    """Cover _ensure_session calling connect when no session."""

    async def test_calls_connect_when_session_none(self) -> None:
        dao = _make_dao()
        dao.session = None
        mock_session = AsyncMock()
        mock_session.closed = False
        with patch(
            f"{_DAO_MOD}.create_session",
            new_callable=AsyncMock,
            return_value=mock_session,
        ):
            result = await dao._ensure_session()
        assert result is mock_session

    async def test_calls_connect_when_session_closed(self) -> None:
        dao = _make_dao()
        dao.session.closed = True
        mock_session = AsyncMock()
        mock_session.closed = False
        with patch(
            f"{_DAO_MOD}.create_session",
            new_callable=AsyncMock,
            return_value=mock_session,
        ):
            result = await dao._ensure_session()
        assert result is mock_session


# -- _ensure_session: lines 107-108 (connect fails) ---------------


class TestEnsureSessionFailure:
    """Cover StorageConnectionError when connect fails."""

    async def test_raises_when_connect_leaves_session_none(
        self,
    ) -> None:
        dao = _make_dao()
        dao.session = None

        async def _broken_connect() -> None:
            pass  # Does not set dao.session

        dao.connect = AsyncMock(side_effect=_broken_connect)
        with pytest.raises(
            StorageConnectionError,
            match="Failed to establish",
        ):
            await dao._ensure_session()


# -- bulk_create: lines 169-174 (generic model_dump branch) -------


class TestBulkCreateGenericModel:
    """Cover bulk_create with a non-dict non-PrometheusMetric."""

    async def test_generic_model_with_model_dump(self) -> None:
        dao = _make_dao()
        generic = MagicMock()
        generic.model_dump = MagicMock(
            return_value={
                "metric_name": "gen_metric",
                "labels": {"env": "test"},
                "value": 7.0,
                "timestamp": None,
            },
        )
        with patch(
            f"{_DAO_MOD}.write_metrics",
            new_callable=AsyncMock,
            return_value=1,
        ):
            ids = await dao.bulk_create([generic])
        assert len(ids) == 1
        assert "gen_metric" in ids[0]

    async def test_generic_model_without_model_dump(self) -> None:
        dao = _make_dao()

        class _DictLike:
            """Object convertible via dict()."""

            def __init__(self) -> None:
                self._data = {
                    "metric_name": "dict_metric",
                    "labels": {"a": "b"},
                    "value": 3.0,
                    "timestamp": None,
                }

            def keys(self) -> list[str]:
                return list(self._data.keys())

            def __getitem__(self, key: str) -> Any:
                return self._data[key]

        with patch(
            f"{_DAO_MOD}.write_metrics",
            new_callable=AsyncMock,
            return_value=1,
        ):
            ids = await dao.bulk_create([_DictLike()])
        assert len(ids) == 1
        assert "dict_metric" in ids[0]


# -- find_one: lines 211-212 --------------------------------------


class TestFindOne:
    """Cover find_one delegation."""

    async def test_find_one_returns_first(self) -> None:
        dao = _make_dao()
        record = PrometheusMetric(
            metric_name="m1",
            labels={},
            value=1.0,
        )
        dao.find = AsyncMock(return_value=[record])
        result = await dao.find_one({"metric_name": "m1"})
        assert result is record
        dao.find.assert_awaited_once_with(
            {"metric_name": "m1"},
            limit=1,
        )

    async def test_find_one_returns_none_on_empty(self) -> None:
        dao = _make_dao()
        dao.find = AsyncMock(return_value=[])
        result = await dao.find_one({"x": "y"})
        assert result is None


# -- find: line 234-236 (non-PrometheusMetric from_storage_dict) ---


class TestFindNonMetricModel:
    """Cover from_storage_dict branch in find."""

    async def test_find_uses_from_storage_dict(self) -> None:
        dao = _make_dao(model_cls=_NonMetricModel)
        record = {
            "metric_name": "non_metric",
            "labels": {},
            "value": 5.0,
            "name": "test",
        }
        mock_instance = _NonMetricModel(name="test", value=5.0)
        with (
            patch(
                f"{_DAO_MOD}.find_metrics",
                new_callable=AsyncMock,
                return_value=[record],
            ),
            patch.object(
                _NonMetricModel,
                "from_storage_dict",
                new_callable=AsyncMock,
                return_value=mock_instance,
            ) as mock_fsd,
        ):
            results = await dao.find({"name": "test"})
        assert len(results) == 1
        assert results[0] is mock_instance
        mock_fsd.assert_awaited_once_with(record)


# -- bulk_update / bulk_delete: lines 246-257 ---------------------


class TestBulkUpdate:
    """Cover bulk_update raises NotImplementedError."""

    async def test_bulk_update_raises(self) -> None:
        dao = _make_dao()
        with pytest.raises(NotImplementedError, match="append-only"):
            await dao.bulk_update([{"id": "x", "value": 1}])


class TestBulkDelete:
    """Cover bulk_delete raises NotImplementedError."""

    async def test_bulk_delete_raises(self) -> None:
        dao = _make_dao()
        with pytest.raises(NotImplementedError, match="append-only"):
            await dao.bulk_delete(["id1", "id2"])


# -- create_indexes: line 275 -------------------------------------


class TestCreateIndexes:
    """Cover create_indexes no-op."""

    async def test_create_indexes_is_noop(self) -> None:
        dao = _make_dao()
        result = await dao.create_indexes()
        assert result is None


# -- raw_write_query: lines 308-309 (error status) ----------------


class TestRawWriteQueryError:
    """Cover raw_write_query error response branch."""

    async def test_raises_on_bad_status(self) -> None:
        dao = _make_dao()
        mock_resp = AsyncMock()
        mock_resp.status = _EXPECTED_STATUS_400
        mock_resp.text = AsyncMock(return_value="Bad Request")
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        with (
            patch(
                f"{_DAO_MOD}.request_with_retry",
                new_callable=AsyncMock,
                return_value=mock_resp,
            ),
            pytest.raises(StorageError, match="raw_write_query"),
        ):
            await dao.raw_write_query("bad_metric 1")

    async def test_raises_on_500_status(self) -> None:
        dao = _make_dao()
        mock_resp = AsyncMock()
        mock_resp.status = _EXPECTED_STATUS_500
        mock_resp.text = AsyncMock(return_value="Server Error")
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        with (
            patch(
                f"{_DAO_MOD}.request_with_retry",
                new_callable=AsyncMock,
                return_value=mock_resp,
            ),
            pytest.raises(StorageError, match="raw_write_query"),
        ):
            await dao.raw_write_query("bad 1\nbad 2")


# -- Delegation: list_databases through test_connection ------------


class TestListDatabases:
    """Cover list_databases delegation (lines 318-322)."""

    async def test_delegates_to_metadata(self) -> None:
        dao = _make_dao()
        with patch(
            f"{_META_MOD}.list_databases",
            new_callable=AsyncMock,
            return_value=["prom-db"],
        ) as mock_fn:
            result = await dao.list_databases()
        assert result == ["prom-db"]
        mock_fn.assert_awaited_once_with(dao)


class TestListSchemas:
    """Cover list_schemas delegation (lines 325-329)."""

    async def test_delegates_to_metadata(self) -> None:
        dao = _make_dao()
        with patch(
            f"{_META_MOD}.list_schemas",
            new_callable=AsyncMock,
            return_value=["__name__", "job"],
        ) as mock_fn:
            result = await dao.list_schemas(database="db")
        assert result == ["__name__", "job"]
        mock_fn.assert_awaited_once_with(dao, "db")


class TestListModels:
    """Cover list_models delegation (lines 336-340)."""

    async def test_delegates_to_metadata(self) -> None:
        dao = _make_dao()
        with patch(
            f"{_META_MOD}.list_models",
            new_callable=AsyncMock,
            return_value=["up", "http_requests_total"],
        ) as mock_fn:
            result = await dao.list_models(database="db", schema="s")
        assert result == ["up", "http_requests_total"]
        assert len(result) == _EXPECTED_TWO
        mock_fn.assert_awaited_once_with(dao, "db", "s")


class TestGetModelInfo:
    """Cover get_model_info delegation (lines 348-352)."""

    async def test_delegates_to_metadata(self) -> None:
        dao = _make_dao()
        expected = {"name": "up", "type": "gauge"}
        with patch(
            f"{_META_MOD}.get_model_info",
            new_callable=AsyncMock,
            return_value=expected,
        ) as mock_fn:
            result = await dao.get_model_info("up", "db", "s")
        assert result == expected
        mock_fn.assert_awaited_once_with(dao, "up", "db", "s")


class TestGetModelSchema:
    """Cover get_model_schema delegation (lines 360-364)."""

    async def test_delegates_to_metadata(self) -> None:
        dao = _make_dao()
        expected = {"name": "up", "fields": {}}
        with patch(
            f"{_META_MOD}.get_model_schema",
            new_callable=AsyncMock,
            return_value=expected,
        ) as mock_fn:
            result = await dao.get_model_schema("up", "db", "s")
        assert result == expected
        mock_fn.assert_awaited_once_with(dao, "up", "db", "s")


class TestGetModelFields:
    """Cover get_model_fields delegation (lines 372-376)."""

    async def test_delegates_to_metadata(self) -> None:
        dao = _make_dao()
        expected = [{"name": "__name__", "type": "string"}]
        with patch(
            f"{_META_MOD}.get_model_fields",
            new_callable=AsyncMock,
            return_value=expected,
        ) as mock_fn:
            result = await dao.get_model_fields("up", "db", "s")
        assert result == expected
        mock_fn.assert_awaited_once_with(dao, "up", "db", "s")


class TestGetModelIndexes:
    """Cover get_model_indexes delegation (lines 384-388)."""

    async def test_delegates_to_metadata(self) -> None:
        dao = _make_dao()
        expected = [
            {
                "name": "label_index_job",
                "field": "job",
                "type": "inverted",
            },
        ]
        with patch(
            f"{_META_MOD}.get_model_indexes",
            new_callable=AsyncMock,
            return_value=expected,
        ) as mock_fn:
            result = await dao.get_model_indexes("up", "db", "s")
        assert result == expected
        mock_fn.assert_awaited_once_with(dao, "up", "db", "s")


class TestTestConnection:
    """Cover test_connection delegation (lines 391-395)."""

    async def test_delegates_to_metadata_true(self) -> None:
        dao = _make_dao()
        with patch(
            f"{_META_MOD}.test_connection",
            new_callable=AsyncMock,
            return_value=True,
        ) as mock_fn:
            result = await dao.test_connection()
        assert result is True
        mock_fn.assert_awaited_once_with(dao)

    async def test_delegates_to_metadata_false(self) -> None:
        dao = _make_dao()
        with patch(
            f"{_META_MOD}.test_connection",
            new_callable=AsyncMock,
            return_value=False,
        ) as mock_fn:
            result = await dao.test_connection()
        assert result is False
        mock_fn.assert_awaited_once_with(dao)
