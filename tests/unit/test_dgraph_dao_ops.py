"""Tests for DgraphDAO CRUD and graph operations.

Each public method on DgraphDAO delegates to a module-level
function in one of the dgraph_* helper modules. These tests
patch each delegate and verify the DAO wiring is correct.
"""

from __future__ import annotations

import sys
import types
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Force-mock pydgraph BEFORE any ami imports so the real
# (possibly broken) native package is never loaded.
# The session-scoped conftest fixture handles this too, but
# this guard ensures the mock is present even when running
# this file in isolation.
if "pydgraph" not in sys.modules:
    _pdg = types.ModuleType("pydgraph")
    _pdg.DgraphClient = MagicMock
    _pdg.DgraphClientStub = MagicMock
    _pdg.Mutation = MagicMock
    _pdg.Operation = MagicMock
    sys.modules["pydgraph"] = _pdg

from ami.core.exceptions import StorageError
from ami.core.storage_types import StorageType
from ami.implementations.graph.dgraph_dao import DgraphDAO
from ami.models.base_model import (
    ModelMetadata,
    StorageModel,
)
from ami.models.storage_config import StorageConfig

_BASE = "ami.implementations.graph"
_EXPECTED_COUNT = 42
_BULK_DELETE_COUNT = 3
_WRITE_AFFECTED = 5
_MAX_HOPS = 5


class _TestModel(StorageModel):
    """Minimal model for DAO tests."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="test_nodes",
    )
    name: str = "default"


def _graph_config(
    host: str | None = "localhost",
    port: int | None = 9080,
) -> StorageConfig:
    return StorageConfig(
        storage_type=StorageType.GRAPH,
        host=host,
        port=port,
    )


@pytest.fixture
def dao() -> DgraphDAO:
    """Return a DgraphDAO wired to _TestModel."""
    cfg = _graph_config()
    d = DgraphDAO(_TestModel, cfg)
    d.client = MagicMock()
    d._grpc_client_conn = MagicMock()
    return d


# ---------------------------------------------------------------
# TestConnect
# ---------------------------------------------------------------


class TestConnect:
    """Verify connect creates client and gRPC channel."""

    @pytest.mark.asyncio
    async def test_creates_client_and_conn(self) -> None:
        cfg = _graph_config()
        d = DgraphDAO(_TestModel, cfg)
        mock_conn = MagicMock()
        mock_client_inst = MagicMock()
        target = f"{_BASE}.dgraph_dao.pydgraph"
        with (
            patch(
                f"{target}.DgraphClientStub",
                return_value=mock_conn,
            ) as conn_cls,
            patch(
                f"{target}.DgraphClient",
                return_value=mock_client_inst,
            ) as client_cls,
            patch(
                f"{_BASE}.dgraph_util.ensure_schema",
            ),
        ):
            await d.connect()
        conn_cls.assert_called_once_with("localhost:9080")
        client_cls.assert_called_once_with(mock_conn)
        assert d.client is mock_client_inst
        assert d._grpc_client_conn is mock_conn

    @pytest.mark.asyncio
    async def test_missing_host_raises(self) -> None:
        cfg = _graph_config(host=None)
        d = DgraphDAO(_TestModel, cfg)
        with pytest.raises(StorageError, match="host"):
            await d.connect()

    @pytest.mark.asyncio
    async def test_missing_port_raises(self) -> None:
        cfg = _graph_config()
        # StorageConfig auto-assigns default port for GRAPH,
        # so force it to None after construction.
        cfg.port = None
        d = DgraphDAO(_TestModel, cfg)
        with pytest.raises(StorageError, match="port"):
            await d.connect()


# ---------------------------------------------------------------
# TestDisconnect
# ---------------------------------------------------------------


class TestDisconnect:
    """Verify disconnect cleans up resources."""

    @pytest.mark.asyncio
    async def test_closes_conn_and_clears_client(self, dao: DgraphDAO) -> None:
        conn = dao._grpc_client_conn
        await dao.disconnect()
        assert conn is not None
        conn.close.assert_called_once()
        assert dao.client is None

    @pytest.mark.asyncio
    async def test_noop_when_no_conn(self) -> None:
        cfg = _graph_config()
        d = DgraphDAO(_TestModel, cfg)
        d._grpc_client_conn = None
        await d.disconnect()


# ---------------------------------------------------------------
# TestCreate
# ---------------------------------------------------------------


class TestCreate:
    """Verify create delegates to dgraph_create.create."""

    @pytest.mark.asyncio
    async def test_returns_uid(self, dao: DgraphDAO) -> None:
        inst = _TestModel(name="alpha")
        with patch(
            f"{_BASE}.dgraph_create.create",
            new_callable=AsyncMock,
            return_value="0xabc",
        ) as mock:
            result = await dao.create(inst)
        mock.assert_awaited_once_with(dao, inst)
        assert result == "0xabc"


# ---------------------------------------------------------------
# TestBulkCreate
# ---------------------------------------------------------------


class TestBulkCreate:
    """Verify bulk_create delegates correctly."""

    @pytest.mark.asyncio
    async def test_returns_uid_list(self, dao: DgraphDAO) -> None:
        items = [
            _TestModel(name="a"),
            _TestModel(name="b"),
        ]
        with patch(
            f"{_BASE}.dgraph_create.bulk_create",
            new_callable=AsyncMock,
            return_value=["0x1", "0x2"],
        ) as mock:
            result = await dao.bulk_create(items)
        mock.assert_awaited_once_with(dao, items)
        assert result == ["0x1", "0x2"]


# ---------------------------------------------------------------
# TestFindById
# ---------------------------------------------------------------


class TestFindById:
    """Verify find_by_id delegates correctly."""

    @pytest.mark.asyncio
    async def test_returns_model(self, dao: DgraphDAO) -> None:
        expected = _TestModel(name="found")
        with patch(
            f"{_BASE}.dgraph_read.find_by_id",
            new_callable=AsyncMock,
            return_value=expected,
        ) as mock:
            result = await dao.find_by_id("0x1")
        mock.assert_awaited_once_with(dao, "0x1")
        assert result is expected

    @pytest.mark.asyncio
    async def test_returns_none(self, dao: DgraphDAO) -> None:
        with patch(
            f"{_BASE}.dgraph_read.find_by_id",
            new_callable=AsyncMock,
            return_value=None,
        ):
            result = await dao.find_by_id("0xmissing")
        assert result is None


# ---------------------------------------------------------------
# TestFind
# ---------------------------------------------------------------


class TestFind:
    """Verify find delegates correctly."""

    @pytest.mark.asyncio
    async def test_returns_list(self, dao: DgraphDAO) -> None:
        items = [_TestModel(name="x")]
        with patch(
            f"{_BASE}.dgraph_read.find",
            new_callable=AsyncMock,
            return_value=items,
        ) as mock:
            result = await dao.find({"name": "x"}, limit=_MAX_HOPS, skip=0)
        mock.assert_awaited_once_with(dao, {"name": "x"}, _MAX_HOPS, 0)
        assert result == items


# ---------------------------------------------------------------
# TestFindOne
# ---------------------------------------------------------------


class TestFindOne:
    """Verify find_one delegates correctly."""

    @pytest.mark.asyncio
    async def test_returns_model(self, dao: DgraphDAO) -> None:
        expected = _TestModel(name="one")
        with patch(
            f"{_BASE}.dgraph_read.find_one",
            new_callable=AsyncMock,
            return_value=expected,
        ) as mock:
            result = await dao.find_one({"name": "one"})
        mock.assert_awaited_once_with(dao, {"name": "one"})
        assert result is expected


# ---------------------------------------------------------------
# TestCount
# ---------------------------------------------------------------


class TestCount:
    """Verify count delegates correctly."""

    @pytest.mark.asyncio
    async def test_returns_int(self, dao: DgraphDAO) -> None:
        with patch(
            f"{_BASE}.dgraph_read.count",
            new_callable=AsyncMock,
            return_value=_EXPECTED_COUNT,
        ) as mock:
            result = await dao.count({"active": True})
        mock.assert_awaited_once_with(dao, {"active": True})
        assert result == _EXPECTED_COUNT


# ---------------------------------------------------------------
# TestExists
# ---------------------------------------------------------------


class TestExists:
    """Verify exists delegates correctly."""

    @pytest.mark.asyncio
    async def test_returns_true(self, dao: DgraphDAO) -> None:
        with patch(
            f"{_BASE}.dgraph_read.exists",
            new_callable=AsyncMock,
            return_value=True,
        ) as mock:
            result = await dao.exists("0x1")
        mock.assert_awaited_once_with(dao, "0x1")
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false(self, dao: DgraphDAO) -> None:
        with patch(
            f"{_BASE}.dgraph_read.exists",
            new_callable=AsyncMock,
            return_value=False,
        ):
            result = await dao.exists("0xnope")
        assert result is False


# ---------------------------------------------------------------
# TestUpdate
# ---------------------------------------------------------------


class TestUpdate:
    """Verify update delegates correctly."""

    @pytest.mark.asyncio
    async def test_calls_delegate(self, dao: DgraphDAO) -> None:
        data: dict[str, Any] = {"name": "new_name"}
        with patch(
            f"{_BASE}.dgraph_update.update",
            new_callable=AsyncMock,
        ) as mock:
            await dao.update("0x1", data)
        mock.assert_awaited_once_with(dao, "0x1", data)


# ---------------------------------------------------------------
# TestDelete
# ---------------------------------------------------------------


class TestDelete:
    """Verify delete delegates correctly."""

    @pytest.mark.asyncio
    async def test_returns_true(self, dao: DgraphDAO) -> None:
        with patch(
            f"{_BASE}.dgraph_delete.delete",
            new_callable=AsyncMock,
            return_value=True,
        ) as mock:
            result = await dao.delete("0x1")
        mock.assert_awaited_once_with(dao, "0x1")
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false(self, dao: DgraphDAO) -> None:
        with patch(
            f"{_BASE}.dgraph_delete.delete",
            new_callable=AsyncMock,
            return_value=False,
        ):
            result = await dao.delete("0xgone")
        assert result is False


# ---------------------------------------------------------------
# TestBulkDelete
# ---------------------------------------------------------------


class TestBulkDelete:
    """Verify bulk_delete delegates and extracts count."""

    @pytest.mark.asyncio
    async def test_returns_count(self, dao: DgraphDAO) -> None:
        with patch(
            f"{_BASE}.dgraph_delete.bulk_delete",
            new_callable=AsyncMock,
            return_value={
                "success_count": _BULK_DELETE_COUNT,
            },
        ) as mock:
            result = await dao.bulk_delete(["0x1", "0x2", "0x3"])
        mock.assert_awaited_once_with(dao, ["0x1", "0x2", "0x3"])
        assert result == _BULK_DELETE_COUNT


# ---------------------------------------------------------------
# TestRawReadQuery
# ---------------------------------------------------------------


class TestRawReadQuery:
    """Verify raw_read_query delegates correctly."""

    @pytest.mark.asyncio
    async def test_returns_results(self, dao: DgraphDAO) -> None:
        rows: list[dict[str, Any]] = [{"uid": "0x1"}]
        query = "{ q(func: uid(0x1)) { uid } }"
        with patch(
            f"{_BASE}.dgraph_read.raw_read_query",
            new_callable=AsyncMock,
            return_value=rows,
        ) as mock:
            result = await dao.raw_read_query(query)
        mock.assert_awaited_once_with(dao, query, None)
        assert result == rows


# ---------------------------------------------------------------
# TestRawWriteQuery
# ---------------------------------------------------------------


class TestRawWriteQuery:
    """Verify raw_write_query delegates correctly."""

    @pytest.mark.asyncio
    async def test_returns_affected(self, dao: DgraphDAO) -> None:
        with patch(
            f"{_BASE}.dgraph_update.raw_write_query",
            new_callable=AsyncMock,
            return_value=_WRITE_AFFECTED,
        ) as mock:
            result = await dao.raw_write_query("mutation { ... }", {"k": "v"})
        mock.assert_awaited_once_with(dao, "mutation { ... }", {"k": "v"})
        assert result == _WRITE_AFFECTED


# ---------------------------------------------------------------
# TestOneHopNeighbors
# ---------------------------------------------------------------


class TestOneHopNeighbors:
    """Verify one_hop_neighbors delegates correctly."""

    @pytest.mark.asyncio
    async def test_returns_dict(self, dao: DgraphDAO) -> None:
        expected: dict[str, Any] = {
            "neighbors": ["0x2", "0x3"],
        }
        with patch(
            f"{_BASE}.dgraph_graph.one_hop_neighbors",
            new_callable=AsyncMock,
            return_value=expected,
        ) as mock:
            result = await dao.one_hop_neighbors("0x1")
        mock.assert_awaited_once_with(dao, "0x1")
        assert result == expected


# ---------------------------------------------------------------
# TestShortestPath
# ---------------------------------------------------------------


class TestShortestPath:
    """Verify shortest_path delegates correctly."""

    @pytest.mark.asyncio
    async def test_returns_path(self, dao: DgraphDAO) -> None:
        path = ["0x1", "0x5", "0x9"]
        with patch(
            f"{_BASE}.dgraph_graph.shortest_path",
            new_callable=AsyncMock,
            return_value=path,
        ) as mock:
            result = await dao.shortest_path("0x1", "0x9", max_depth=_MAX_HOPS)
        mock.assert_awaited_once_with(dao, "0x1", "0x9", _MAX_HOPS)
        assert result == path


# ---------------------------------------------------------------
# TestTestConnection
# ---------------------------------------------------------------


class TestTestConnection:
    """Verify test_connection delegates correctly."""

    @pytest.mark.asyncio
    async def test_returns_true(self, dao: DgraphDAO) -> None:
        with patch(
            f"{_BASE}.dgraph_read.test_connection",
            new_callable=AsyncMock,
            return_value=True,
        ) as mock:
            result = await dao.test_connection()
        mock.assert_awaited_once_with(dao)
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false(self, dao: DgraphDAO) -> None:
        with patch(
            f"{_BASE}.dgraph_read.test_connection",
            new_callable=AsyncMock,
            return_value=False,
        ):
            result = await dao.test_connection()
        assert result is False
