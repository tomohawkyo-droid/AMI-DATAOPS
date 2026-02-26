"""DgraphDAO delegation wrappers and pgvector_read guard tests."""

from __future__ import annotations

import sys
import types
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

if "pydgraph" not in sys.modules:
    _pdg = types.ModuleType("pydgraph")
    for _n in ("DgraphClient", "DgraphClientStub", "Mutation", "Operation"):
        setattr(_pdg, _n, MagicMock)
    sys.modules["pydgraph"] = _pdg

from ami.core.exceptions import StorageConnectionError, StorageError
from ami.core.storage_types import StorageType
from ami.implementations.graph.dgraph_dao import DgraphDAO
from ami.implementations.vec.pgvector_dao import PgVectorDAO
from ami.implementations.vec.pgvector_read import (
    count,
    exists,
    find,
    find_by_id,
    find_one,
    get_model_fields,
    get_model_indexes,
    get_model_info,
    get_model_schema,
    list_databases,
    list_models,
    list_schemas,
    raw_read_query,
)
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

_DGRAPH_PORT = 9080
_PGVEC_PORT = 5432
_EMBED_DIM = 384
_GR = "ami.implementations.graph"
_EMB_PATCH = "ami.implementations.vec.pgvector_dao.get_embedding_service"


class _DgraphModel(StorageModel):
    """Minimal model for DgraphDAO tests."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="test_nodes",
    )
    name: str = "node"


class _VecModel(StorageModel):
    """Minimal model for pgvector tests."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="test_items",
    )
    name: str = "item"


def _graph_cfg() -> StorageConfig:
    return StorageConfig(
        storage_type=StorageType.GRAPH,
        host="localhost",
        port=_DGRAPH_PORT,
        database="test",
    )


def _make_dao() -> DgraphDAO:
    with patch(f"{_GR}.dgraph_util.ensure_schema"):
        return DgraphDAO(_DgraphModel, _graph_cfg())


def _mock_vec_dao(pool: Any = None) -> MagicMock:
    """Build a mock that looks like PgVectorDAO."""
    dao = MagicMock()
    dao.pool = pool
    dao.collection_name = "test_items"
    return dao


def _mock_pool() -> tuple[MagicMock, AsyncMock]:
    """Return (pool, conn) with working acquire context."""
    pool = MagicMock()
    conn = AsyncMock()
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value = ctx
    pool.close = AsyncMock()
    return pool, conn


def _emb_svc() -> MagicMock:
    svc = MagicMock()
    svc.embedding_dim = _EMBED_DIM
    svc.generate_embedding = AsyncMock(
        return_value=[0.1] * _EMBED_DIM,
    )
    return svc


def _make_pgvec_dao() -> PgVectorDAO:
    """Build a PgVectorDAO with mocked embedding service."""
    cfg = StorageConfig(
        storage_type=StorageType.VECTOR,
        host="h",
        port=_PGVEC_PORT,
        database="db",
    )
    with patch(_EMB_PATCH, return_value=_emb_svc()):
        return PgVectorDAO(_VecModel, cfg)


# ---- A) DgraphDAO ----


class TestDgraphConnect:
    """connect() error branches."""

    @pytest.mark.asyncio
    async def test_no_config_raises(self) -> None:
        with patch(f"{_GR}.dgraph_util.ensure_schema"):
            dao = DgraphDAO.__new__(DgraphDAO)
            dao.model_cls = _DgraphModel
            dao.collection_name = "test_nodes"
            dao.config = None
            dao.client = None
            dao._grpc_client_conn = None
        with pytest.raises(StorageError, match="No configuration"):
            await dao.connect()

    @pytest.mark.asyncio
    async def test_exception_wraps(self) -> None:
        dao = _make_dao()
        with (
            patch(
                "pydgraph.DgraphClientStub",
                side_effect=RuntimeError("boom"),
            ),
            pytest.raises(StorageError, match="Failed to connect"),
        ):
            await dao.connect()


class TestDgraphCreateIndexes:
    """create_indexes() delegates to dgraph_create."""

    @pytest.mark.asyncio
    async def test_delegates(self) -> None:
        dao = _make_dao()
        t = f"{_GR}.dgraph_create.create_indexes"
        with patch(t, new_callable=AsyncMock) as fn:
            await dao.create_indexes()
            fn.assert_awaited_once_with(dao)


class TestDgraphReadDelegation:
    """Read delegation methods on DgraphDAO."""

    @pytest.mark.asyncio
    async def test_list_databases(self) -> None:
        dao = _make_dao()
        t = f"{_GR}.dgraph_read.list_databases"
        with patch(t, new_callable=AsyncMock) as fn:
            fn.return_value = ["db1"]
            result = await dao.list_databases()
            fn.assert_awaited_once_with(dao)
        assert result == ["db1"]

    @pytest.mark.asyncio
    async def test_list_schemas(self) -> None:
        dao = _make_dao()
        t = f"{_GR}.dgraph_read.list_schemas"
        with patch(t, new_callable=AsyncMock) as fn:
            fn.return_value = ["s1"]
            result = await dao.list_schemas("db")
            fn.assert_awaited_once_with(dao, "db")
        assert result == ["s1"]

    @pytest.mark.asyncio
    async def test_list_models(self) -> None:
        dao = _make_dao()
        t = f"{_GR}.dgraph_read.list_models"
        with patch(t, new_callable=AsyncMock) as fn:
            fn.return_value = ["m1"]
            result = await dao.list_models("db", "sch")
            fn.assert_awaited_once_with(dao, "db", "sch")
        assert result == ["m1"]

    @pytest.mark.asyncio
    async def test_get_model_info(self) -> None:
        dao = _make_dao()
        t = f"{_GR}.dgraph_read.get_model_info"
        with patch(t, new_callable=AsyncMock) as fn:
            fn.return_value = {"name": "x"}
            result = await dao.get_model_info("p", "db", "s")
            fn.assert_awaited_once_with(dao, "p", "db", "s")
        assert result == {"name": "x"}

    @pytest.mark.asyncio
    async def test_get_model_schema(self) -> None:
        dao = _make_dao()
        t = f"{_GR}.dgraph_read.get_model_schema"
        with patch(t, new_callable=AsyncMock) as fn:
            fn.return_value = {"fields": []}
            result = await dao.get_model_schema("p", "db", "s")
            fn.assert_awaited_once_with(dao, "p", "db", "s")
        assert result == {"fields": []}

    @pytest.mark.asyncio
    async def test_get_model_fields(self) -> None:
        dao = _make_dao()
        t = f"{_GR}.dgraph_read.get_model_fields"
        with patch(t, new_callable=AsyncMock) as fn:
            fn.return_value = [{"col": "a"}]
            result = await dao.get_model_fields("p", "db", "s")
            fn.assert_awaited_once_with(dao, "p", "db", "s")
        assert result == [{"col": "a"}]

    @pytest.mark.asyncio
    async def test_get_model_indexes(self) -> None:
        dao = _make_dao()
        t = f"{_GR}.dgraph_read.get_model_indexes"
        with patch(t, new_callable=AsyncMock) as fn:
            fn.return_value = [{"idx": "i"}]
            result = await dao.get_model_indexes("p", "db", "s")
            fn.assert_awaited_once_with(dao, "p", "db", "s")
        assert result == [{"idx": "i"}]


class TestDgraphBulkUpdate:
    """bulk_update() delegates to dgraph_update."""

    @pytest.mark.asyncio
    async def test_delegates(self) -> None:
        dao = _make_dao()
        ups = [{"id": "1", "name": "new"}]
        t = f"{_GR}.dgraph_update.bulk_update"
        with patch(t, new_callable=AsyncMock) as fn:
            await dao.bulk_update(ups)
            fn.assert_awaited_once_with(dao, ups)


class TestDgraphGraphDelegation:
    """Graph-specific delegation methods."""

    @pytest.mark.asyncio
    async def test_find_connected_components(self) -> None:
        dao = _make_dao()
        t = f"{_GR}.dgraph_graph.find_connected_components"
        with patch(t, new_callable=AsyncMock) as fn:
            fn.return_value = [["a", "b"]]
            r = await dao.find_connected_components("type1")
            fn.assert_awaited_once_with(dao, "type1")
        assert r == [["a", "b"]]

    @pytest.mark.asyncio
    async def test_get_node_degree(self) -> None:
        dao = _make_dao()
        t = f"{_GR}.dgraph_graph.get_node_degree"
        with patch(t, new_callable=AsyncMock) as fn:
            fn.return_value = {"in": 2, "out": 3}
            r = await dao.get_node_degree("n1", "all")
            fn.assert_awaited_once_with(dao, "n1", "all")
        assert r == {"in": 2, "out": 3}


# ---- B) pgvector_read pool-is-None guards ----


class TestPgvecReadPoolNone:
    """Each function raises StorageConnectionError when pool=None."""

    @pytest.mark.asyncio
    async def test_find_by_id(self) -> None:
        with pytest.raises(StorageConnectionError):
            await find_by_id(_mock_vec_dao(), "id1")

    @pytest.mark.asyncio
    async def test_find_one(self) -> None:
        with pytest.raises(StorageConnectionError):
            await find_one(_mock_vec_dao(), {"k": "v"})

    @pytest.mark.asyncio
    async def test_find(self) -> None:
        with pytest.raises(StorageConnectionError):
            await find(_mock_vec_dao(), {})

    @pytest.mark.asyncio
    async def test_count(self) -> None:
        with pytest.raises(StorageConnectionError):
            await count(_mock_vec_dao(), {})

    @pytest.mark.asyncio
    async def test_exists(self) -> None:
        with pytest.raises(StorageConnectionError):
            await exists(_mock_vec_dao(), "id1")

    @pytest.mark.asyncio
    async def test_raw_read_query(self) -> None:
        with pytest.raises(StorageConnectionError):
            await raw_read_query(_mock_vec_dao(), "SELECT 1")

    @pytest.mark.asyncio
    async def test_list_databases(self) -> None:
        with pytest.raises(StorageConnectionError):
            await list_databases(_mock_vec_dao())

    @pytest.mark.asyncio
    async def test_list_schemas(self) -> None:
        with pytest.raises(StorageConnectionError):
            await list_schemas(_mock_vec_dao())

    @pytest.mark.asyncio
    async def test_list_models(self) -> None:
        with pytest.raises(StorageConnectionError):
            await list_models(_mock_vec_dao())

    @pytest.mark.asyncio
    async def test_get_model_info(self) -> None:
        with pytest.raises(StorageConnectionError):
            await get_model_info(_mock_vec_dao(), "tbl")

    @pytest.mark.asyncio
    async def test_get_model_fields(self) -> None:
        with pytest.raises(StorageConnectionError):
            await get_model_fields(_mock_vec_dao(), "tbl")

    @pytest.mark.asyncio
    async def test_get_model_schema(self) -> None:
        """get_model_schema delegates to get_model_fields."""
        with pytest.raises(StorageConnectionError):
            await get_model_schema(_mock_vec_dao(), "tbl")

    @pytest.mark.asyncio
    async def test_get_model_indexes(self) -> None:
        with pytest.raises(StorageConnectionError):
            await get_model_indexes(_mock_vec_dao(), "tbl")


# ---- B2) pgvector_read pool.acquire paths ----


class TestPgvecReadWithPool:
    """Verify pool.acquire code paths execute correctly."""

    @pytest.mark.asyncio
    async def test_find_by_id_not_found(self) -> None:
        pool, conn = _mock_pool()
        conn.fetchrow.return_value = None
        assert await find_by_id(_mock_vec_dao(pool), "x") is None

    @pytest.mark.asyncio
    async def test_find_empty(self) -> None:
        pool, conn = _mock_pool()
        conn.fetch.return_value = []
        assert await find(_mock_vec_dao(pool), {}) == []

    @pytest.mark.asyncio
    async def test_count_zero(self) -> None:
        pool, conn = _mock_pool()
        conn.fetchval.return_value = 0
        assert await count(_mock_vec_dao(pool), {}) == 0

    @pytest.mark.asyncio
    async def test_exists_true(self) -> None:
        pool, conn = _mock_pool()
        conn.fetchval.return_value = 1
        assert await exists(_mock_vec_dao(pool), "id1") is True

    @pytest.mark.asyncio
    async def test_exists_false(self) -> None:
        pool, conn = _mock_pool()
        conn.fetchval.return_value = None
        assert await exists(_mock_vec_dao(pool), "id1") is False

    @pytest.mark.asyncio
    async def test_raw_read_no_params(self) -> None:
        pool, conn = _mock_pool()
        conn.fetch.return_value = []
        assert await raw_read_query(_mock_vec_dao(pool), "S") == []

    @pytest.mark.asyncio
    async def test_raw_read_with_params(self) -> None:
        pool, conn = _mock_pool()
        conn.fetch.return_value = []
        r = await raw_read_query(_mock_vec_dao(pool), "S $1", {"a": "b"})
        assert r == []

    @pytest.mark.asyncio
    async def test_list_databases_ok(self) -> None:
        pool, conn = _mock_pool()
        conn.fetch.return_value = [{"datname": "db1"}]
        assert await list_databases(_mock_vec_dao(pool)) == ["db1"]

    @pytest.mark.asyncio
    async def test_list_schemas_ok(self) -> None:
        pool, conn = _mock_pool()
        conn.fetch.return_value = [{"schema_name": "pub"}]
        assert await list_schemas(_mock_vec_dao(pool)) == ["pub"]

    @pytest.mark.asyncio
    async def test_list_models_ok(self) -> None:
        pool, conn = _mock_pool()
        conn.fetch.return_value = [{"table_name": "t1"}]
        assert await list_models(_mock_vec_dao(pool)) == ["t1"]

    @pytest.mark.asyncio
    async def test_get_model_info_found(self) -> None:
        pool, conn = _mock_pool()
        conn.fetchrow.return_value = {
            "table_name": "t1",
            "table_type": "BASE TABLE",
        }
        r = await get_model_info(_mock_vec_dao(pool), "t1")
        assert r["name"] == "t1"
        assert r["type"] == "BASE TABLE"

    @pytest.mark.asyncio
    async def test_get_model_info_missing(self) -> None:
        pool, conn = _mock_pool()
        conn.fetchrow.return_value = None
        r = await get_model_info(_mock_vec_dao(pool), "x")
        assert r == {"error": "Table not found"}

    @pytest.mark.asyncio
    async def test_get_model_fields_ok(self) -> None:
        pool, conn = _mock_pool()
        conn.fetch.return_value = [
            {
                "column_name": "uid",
                "data_type": "text",
                "is_nullable": "NO",
                "column_default": None,
            },
        ]
        r = await get_model_fields(_mock_vec_dao(pool), "t1")
        assert len(r) == 1
        assert r[0]["name"] == "uid"
        assert r[0]["nullable"] is False

    @pytest.mark.asyncio
    async def test_get_model_indexes_ok(self) -> None:
        pool, conn = _mock_pool()
        conn.fetch.return_value = [
            {"indexname": "i1", "indexdef": "CREATE INDEX ..."},
        ]
        r = await get_model_indexes(_mock_vec_dao(pool), "t1")
        assert len(r) == 1
        assert r[0]["name"] == "i1"


# ---- B3) PgVectorDAO.test_connection ----


class TestPgVectorTestConnection:
    """PgVectorDAO.test_connection coverage."""

    @pytest.mark.asyncio
    async def test_pool_none_returns_false(self) -> None:
        dao = _make_pgvec_dao()
        dao.pool = None
        object.__setattr__(dao, "_ensure_pool", AsyncMock())
        result = await dao.test_connection()
        assert result is False

    @pytest.mark.asyncio
    async def test_pool_success_returns_true(self) -> None:
        dao = _make_pgvec_dao()
        pool, conn = _mock_pool()
        conn.fetchval.return_value = 1
        dao.pool = pool
        object.__setattr__(dao, "_ensure_pool", AsyncMock())
        result = await dao.test_connection()
        assert result is True
