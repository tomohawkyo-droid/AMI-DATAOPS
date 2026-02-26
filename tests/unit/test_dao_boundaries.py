"""Mock-based boundary tests for DAO implementations.

Covers Issue #67 -- verify DAO logic without live backends by mocking
the driver layer (asyncpg pool, redis client, aiohttp session).
"""

import sys
import types
from typing import Any, ClassVar
from unittest.mock import AsyncMock

# Ensure pydgraph stub exists (not always installable in CI).
if "pydgraph" not in sys.modules:
    sys.modules["pydgraph"] = types.ModuleType("pydgraph")

import pytest

from ami.core.exceptions import StorageConnectionError, StorageError
from ami.core.storage_types import StorageType
from ami.implementations.mem.redis_dao import RedisDAO
from ami.implementations.rest.rest_dao import RestDAO
from ami.implementations.sql.postgresql_dao import PostgreSQLDAO
from ami.implementations.vec.pgvector_dao import PgVectorDAO
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig


class _TestModel(StorageModel):
    """Minimal model used across DAO boundary tests."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="boundary_test",
        storage_configs={
            "pg": StorageConfig(storage_type=StorageType.RELATIONAL),
        },
    )

    name: str = ""
    value: int = 0


# ------------------------------------------------------------------
# PostgreSQL DAO
# ------------------------------------------------------------------


class TestPostgreSQLDAOBoundary:
    """Test PostgreSQLDAO delegates to asyncpg pool correctly."""

    def _make_dao(self) -> Any:
        config = StorageConfig(
            storage_type=StorageType.RELATIONAL,
            host="localhost",
            port=5432,
            database="test_db",
            username="user",
            password="pass",
        )
        return PostgreSQLDAO(_TestModel, config)

    @pytest.mark.asyncio
    async def test_find_by_id_no_pool_raises(self) -> None:
        dao = self._make_dao()
        dao.pool = None

        with pytest.raises(StorageConnectionError):
            await dao.find_by_id("some-id")

    @pytest.mark.asyncio
    async def test_count_no_pool_raises(self) -> None:
        dao = self._make_dao()
        dao.pool = None

        with pytest.raises(StorageConnectionError):
            await dao.count({})

    @pytest.mark.asyncio
    async def test_delete_no_pool_raises(self) -> None:
        dao = self._make_dao()
        dao.pool = None

        with pytest.raises(StorageConnectionError):
            await dao.delete("some-id")

    @pytest.mark.asyncio
    async def test_test_connection_false_after_failed_connect(
        self,
    ) -> None:
        dao = self._make_dao()
        dao.pool = None
        # Mock connect so it doesn't hit a real DB but leaves pool None
        dao.connect = AsyncMock()

        result = await dao.test_connection()
        assert result is False


# ------------------------------------------------------------------
# Redis DAO
# ------------------------------------------------------------------


class TestRedisDAOBoundary:
    """Test RedisDAO boundary checks without a live server."""

    def _make_dao(self) -> Any:
        config = StorageConfig(
            storage_type=StorageType.INMEM,
            host="localhost",
            port=6379,
        )
        return RedisDAO(_TestModel, config)

    @pytest.mark.asyncio
    async def test_read_returns_none_for_missing(self) -> None:
        dao = self._make_dao()
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=None)
        dao.client = mock_client
        dao._connected = True

        result = await dao.find_by_id("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_connect_requires_config(self) -> None:
        dao = self._make_dao()
        dao.config = None

        with pytest.raises(StorageError, match="Invalid"):
            await dao.connect()


# ------------------------------------------------------------------
# REST DAO
# ------------------------------------------------------------------


class TestRestDAOBoundary:
    """Test RestDAO HTTP orchestration with mocked aiohttp."""

    def _make_dao(self) -> Any:
        config = StorageConfig(
            storage_type=StorageType.REST,
            connection_string="https://api.example.com",
        )
        return RestDAO(_TestModel, config)

    @pytest.mark.asyncio
    async def test_ensure_session_raises_when_no_session(
        self,
    ) -> None:
        dao = self._make_dao()
        dao.session = None

        # _ensure_session calls connect which creates a real session,
        # so we mock connect to leave session as None.
        dao.connect = AsyncMock()

        with pytest.raises(StorageConnectionError):
            await dao._ensure_session()

    def test_build_url(self) -> None:
        dao = self._make_dao()
        url = dao._build_url()
        assert url == "https://api.example.com/boundary_test"

    def test_build_url_with_item_id(self) -> None:
        dao = self._make_dao()
        url = dao._build_url(item_id="abc-123")
        assert url == "https://api.example.com/boundary_test/abc-123"

    def test_extract_data_uses_config_key(self) -> None:
        dao = self._make_dao()
        dao.config.options = {"response_data_key": "payload"}

        response = {"payload": [{"id": "1"}], "meta": {}}
        result = dao._extract_data(response)
        assert result == [{"id": "1"}]

    def test_extract_data_probes_common_keys(self) -> None:
        dao = self._make_dao()
        dao.config.options = {}

        response = {"data": [{"id": "1"}]}
        result = dao._extract_data(response)
        assert result == [{"id": "1"}]


# ------------------------------------------------------------------
# PgVector DAO
# ------------------------------------------------------------------


class TestPgVectorDAOBoundary:
    """Test PgVectorDAO pool checks."""

    def _make_dao(self) -> Any:
        config = StorageConfig(
            storage_type=StorageType.VECTOR,
            host="localhost",
            port=5432,
            database="test_db",
            username="user",
            password="pass",
        )
        return PgVectorDAO(_TestModel, config)

    @pytest.mark.asyncio
    async def test_find_by_id_no_pool_raises(self) -> None:
        dao = self._make_dao()
        dao.pool = None

        with pytest.raises(StorageConnectionError):
            await dao.find_by_id("some-id")

    @pytest.mark.asyncio
    async def test_test_connection_false_after_failed_connect(
        self,
    ) -> None:
        dao = self._make_dao()
        dao.pool = None
        # Mock connect so it doesn't hit a real DB but leaves pool None
        dao.connect = AsyncMock()

        result = await dao.test_connection()
        assert result is False
