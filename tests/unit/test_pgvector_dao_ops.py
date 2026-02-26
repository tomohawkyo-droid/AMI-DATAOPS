"""Tests for PgVectorDAO async CRUD and vector operations."""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.core.storage_types import StorageType
from ami.implementations.vec.pgvector_dao import PgVectorDAO
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

_EMB_PATCH = "ami.implementations.vec.pgvector_dao.get_embedding_service"
_VEC = "ami.implementations.vec"

_COUNT_EXPECTED = 42
_BULK_COUNT = 3
_BULK_DEL_EXPECTED = 2
_SIM_LIMIT = 5
_EMBED_DIM = 384
_SIM_VEC_LIMIT = 3


class _TestModel(StorageModel):
    """Minimal model for PgVectorDAO tests."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="test_items",
    )
    name: str = "default"


def _make_config(**overrides: Any) -> StorageConfig:
    """Build a VECTOR StorageConfig with defaults."""
    defaults: dict[str, Any] = {
        "storage_type": StorageType.VECTOR,
        "host": "pghost.local",
        "port": 5432,
        "database": "vectordb",
        "username": "vecuser",
        "password": "secret123",
    }
    defaults.update(overrides)
    return StorageConfig(**defaults)


def _mock_emb_svc() -> MagicMock:
    """Build a mock embedding service."""
    svc = MagicMock()
    svc.embedding_dim = _EMBED_DIM
    svc.generate_embedding = AsyncMock(
        return_value=[0.1] * _EMBED_DIM,
    )
    return svc


def _make_dao(
    config: StorageConfig | None = None,
    set_pool: bool = True,
) -> PgVectorDAO:
    """Create a PgVectorDAO with mocked embedding svc."""
    cfg = config or _make_config()
    with patch(_EMB_PATCH, return_value=_mock_emb_svc()):
        dao = PgVectorDAO(model_cls=_TestModel, config=cfg)
    if set_pool:
        dao.pool = AsyncMock()
    return dao


@pytest.fixture
def mock_embedding_svc() -> MagicMock:
    return _mock_emb_svc()


@pytest.fixture
def dao() -> PgVectorDAO:
    """PgVectorDAO with a pre-set mock pool."""
    return _make_dao(set_pool=True)


@pytest.fixture
def dao_no_pool() -> PgVectorDAO:
    """PgVectorDAO without a pre-set pool."""
    return _make_dao(set_pool=False)


class TestBuildDsn:
    """PgVectorDAO._build_dsn constructs a DSN string."""

    def test_from_host_port_components(self, dao: PgVectorDAO) -> None:
        dsn = dao._build_dsn()
        assert dsn == ("postgresql://vecuser:secret123@pghost.local:5432/vectordb")

    def test_from_connection_string(self) -> None:
        conn_str = "postgresql://u:p@h:1234/mydb"
        d = _make_dao(
            config=_make_config(connection_string=conn_str),
        )
        assert d._build_dsn() == conn_str


class TestSafeDsn:
    """PgVectorDAO._safe_dsn redacts the password."""

    def test_password_is_redacted(self, dao: PgVectorDAO) -> None:
        safe = dao._safe_dsn()
        assert "***" in safe
        assert "secret123" not in safe
        assert "vecuser" in safe


class TestConnect:
    """PgVectorDAO.connect creates a connection pool."""

    @pytest.mark.asyncio
    async def test_connect_creates_pool(self) -> None:
        d = _make_dao(set_pool=False)
        mock_pool = AsyncMock()
        with patch(
            "asyncpg.create_pool",
            new_callable=AsyncMock,
            return_value=mock_pool,
        ):
            await d.connect()
        assert d.pool is mock_pool


class TestDisconnect:
    """PgVectorDAO.disconnect closes the pool."""

    @pytest.mark.asyncio
    async def test_disconnect_closes_pool(self, dao: PgVectorDAO) -> None:
        mock_close = AsyncMock()
        dao.pool.close = mock_close
        await dao.disconnect()
        mock_close.assert_awaited_once()
        assert dao.pool is None

    @pytest.mark.asyncio
    async def test_disconnect_clears_ensured_tables(self, dao: PgVectorDAO) -> None:
        dao._ensured_tables.add("some_table")
        dao.pool.close = AsyncMock()
        await dao.disconnect()
        assert len(dao._ensured_tables) == 0


class TestCreate:
    """PgVectorDAO.create delegates to pgvector_create."""

    @pytest.mark.asyncio
    @patch(
        f"{_VEC}.pgvector_create.create",
        new_callable=AsyncMock,
    )
    async def test_create_returns_uid(
        self,
        mock_create: AsyncMock,
        dao: PgVectorDAO,
    ) -> None:
        mock_create.return_value = "new-uid-1"
        inst = _TestModel(name="item-a")
        uid = await dao.create(inst)
        assert uid == "new-uid-1"
        mock_create.assert_awaited_once_with(dao, inst)


class TestFindById:
    """PgVectorDAO.find_by_id delegates to pgvector_read."""

    @pytest.mark.asyncio
    @patch(
        f"{_VEC}.pgvector_read.find_by_id",
        new_callable=AsyncMock,
    )
    async def test_find_by_id_returns_result(
        self,
        mock_find: AsyncMock,
        dao: PgVectorDAO,
    ) -> None:
        expected = _TestModel(uid="abc", name="found")
        mock_find.return_value = expected
        result = await dao.find_by_id("abc")
        assert result is expected
        mock_find.assert_awaited_once_with(dao, "abc")


class TestFind:
    """PgVectorDAO.find delegates to pgvector_read."""

    @pytest.mark.asyncio
    @patch(
        f"{_VEC}.pgvector_read.find",
        new_callable=AsyncMock,
    )
    async def test_find_returns_list(
        self,
        mock_find: AsyncMock,
        dao: PgVectorDAO,
    ) -> None:
        items = [
            _TestModel(name="a"),
            _TestModel(name="b"),
        ]
        mock_find.return_value = items
        result = await dao.find({"name": "a"}, limit=10)
        assert result == items
        mock_find.assert_awaited_once_with(dao, {"name": "a"}, limit=10, skip=0)


class TestUpdate:
    """PgVectorDAO.update delegates to pgvector_update."""

    @pytest.mark.asyncio
    @patch(
        f"{_VEC}.pgvector_update.update",
        new_callable=AsyncMock,
    )
    async def test_update_calls_delegate(
        self,
        mock_update: AsyncMock,
        dao: PgVectorDAO,
    ) -> None:
        await dao.update("u1", {"name": "new-name"})
        mock_update.assert_awaited_once_with(dao, "u1", {"name": "new-name"})


class TestDelete:
    """PgVectorDAO.delete delegates to pgvector_delete."""

    @pytest.mark.asyncio
    @patch(
        f"{_VEC}.pgvector_delete.delete",
        new_callable=AsyncMock,
    )
    async def test_delete_returns_bool(
        self,
        mock_delete: AsyncMock,
        dao: PgVectorDAO,
    ) -> None:
        mock_delete.return_value = True
        result = await dao.delete("d1")
        assert result is True
        mock_delete.assert_awaited_once_with(dao, "d1")


class TestCount:
    """PgVectorDAO.count delegates to pgvector_read."""

    @pytest.mark.asyncio
    @patch(
        f"{_VEC}.pgvector_read.count",
        new_callable=AsyncMock,
    )
    async def test_count_returns_int(
        self,
        mock_count: AsyncMock,
        dao: PgVectorDAO,
    ) -> None:
        mock_count.return_value = _COUNT_EXPECTED
        result = await dao.count({"active": True})
        assert result == _COUNT_EXPECTED
        mock_count.assert_awaited_once_with(dao, {"active": True})


class TestExists:
    """PgVectorDAO.exists delegates to pgvector_read."""

    @pytest.mark.asyncio
    @patch(
        f"{_VEC}.pgvector_read.exists",
        new_callable=AsyncMock,
    )
    async def test_exists_returns_bool(
        self,
        mock_exists: AsyncMock,
        dao: PgVectorDAO,
    ) -> None:
        mock_exists.return_value = True
        result = await dao.exists("e1")
        assert result is True
        mock_exists.assert_awaited_once_with(dao, "e1")


class TestBulkCreate:
    """PgVectorDAO.bulk_create delegates."""

    @pytest.mark.asyncio
    @patch(
        f"{_VEC}.pgvector_create.bulk_create",
        new_callable=AsyncMock,
    )
    async def test_bulk_create_returns_ids(
        self,
        mock_bulk: AsyncMock,
        dao: PgVectorDAO,
    ) -> None:
        expected_ids = ["id-1", "id-2", "id-3"]
        mock_bulk.return_value = expected_ids
        instances = [_TestModel(name=f"item-{i}") for i in range(_BULK_COUNT)]
        ids = await dao.bulk_create(instances)
        assert ids == expected_ids
        mock_bulk.assert_awaited_once_with(dao, instances)


class TestBulkDelete:
    """PgVectorDAO.bulk_delete delegates."""

    @pytest.mark.asyncio
    @patch(
        f"{_VEC}.pgvector_delete.bulk_delete",
        new_callable=AsyncMock,
    )
    async def test_bulk_delete_returns_count(
        self,
        mock_bulk_del: AsyncMock,
        dao: PgVectorDAO,
    ) -> None:
        mock_bulk_del.return_value = _BULK_DEL_EXPECTED
        result = await dao.bulk_delete(["a", "b", "c"])
        assert result == _BULK_DEL_EXPECTED
        mock_bulk_del.assert_awaited_once_with(dao, ["a", "b", "c"])


class TestSimilaritySearch:
    """PgVectorDAO.similarity_search delegates."""

    @pytest.mark.asyncio
    @patch(
        f"{_VEC}.pgvector_vector.similarity_search",
        new_callable=AsyncMock,
    )
    async def test_returns_results(
        self,
        mock_sim: AsyncMock,
        dao: PgVectorDAO,
    ) -> None:
        hits = [
            {"uid": "r1", "score": 0.95},
            {"uid": "r2", "score": 0.80},
        ]
        mock_sim.return_value = hits
        result = await dao.similarity_search(
            "hello world",
            limit=_SIM_LIMIT,
            filters={"active": True},
            metric="cosine",
        )
        assert result == hits
        mock_sim.assert_awaited_once_with(
            dao,
            "hello world",
            limit=_SIM_LIMIT,
            filters={"active": True},
            metric="cosine",
        )


class TestSimilaritySearchByVector:
    """PgVectorDAO.similarity_search_by_vector delegates."""

    @pytest.mark.asyncio
    @patch(
        f"{_VEC}.pgvector_vector.similarity_search_by_vector",
        new_callable=AsyncMock,
    )
    async def test_search_by_vector_returns_results(
        self,
        mock_sim_vec: AsyncMock,
        dao: PgVectorDAO,
    ) -> None:
        hits = [{"uid": "v1", "score": 0.99}]
        mock_sim_vec.return_value = hits
        vec = [0.5] * _EMBED_DIM
        result = await dao.similarity_search_by_vector(vec, limit=_SIM_VEC_LIMIT)
        assert result == hits
        mock_sim_vec.assert_awaited_once_with(
            dao,
            vec,
            limit=_SIM_VEC_LIMIT,
            filters=None,
            metric="cosine",
        )


class TestFetchEmbedding:
    """PgVectorDAO.fetch_embedding delegates."""

    @pytest.mark.asyncio
    @patch(
        f"{_VEC}.pgvector_vector.fetch_embedding",
        new_callable=AsyncMock,
    )
    async def test_fetch_embedding_returns_vector(
        self,
        mock_fetch: AsyncMock,
        dao: PgVectorDAO,
    ) -> None:
        expected = [0.1, 0.2, 0.3]
        mock_fetch.return_value = expected
        result = await dao.fetch_embedding("item-1")
        assert result == expected
        mock_fetch.assert_awaited_once_with(dao, "item-1")


class TestExtractTextForEmbedding:
    """PgVectorDAO._extract_text_for_embedding logic."""

    def test_standard_fields_extracted(self, dao: PgVectorDAO) -> None:
        data = {
            "title": "My Title",
            "content": "Body text here",
            "description": "A desc",
        }
        text = dao._extract_text_for_embedding(data)
        assert "My Title" in text
        assert "Body text here" in text
        assert "A desc" in text

    def test_unknown_fields_skipped(self, dao: PgVectorDAO) -> None:
        data = {
            "unrelated_key": "should not appear",
            "random_col": 12345,
        }
        text = dao._extract_text_for_embedding(data)
        assert text == ""

    def test_empty_data_returns_empty(self, dao: PgVectorDAO) -> None:
        text = dao._extract_text_for_embedding({})
        assert text == ""


class TestTestConnection:
    """PgVectorDAO.test_connection checks the pool."""

    @pytest.mark.asyncio
    async def test_connection_success(self, dao: PgVectorDAO) -> None:
        conn = AsyncMock()
        conn.fetchval = AsyncMock(return_value=1)
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=conn)
        ctx.__aexit__ = AsyncMock(return_value=False)
        dao.pool.acquire = MagicMock(return_value=ctx)
        result = await dao.test_connection()
        assert result is True
        conn.fetchval.assert_awaited_once_with("SELECT 1")
