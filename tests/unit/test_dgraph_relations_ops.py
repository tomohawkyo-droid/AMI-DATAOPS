"""Tests for DgraphRelationalMixin operations."""

from __future__ import annotations

import json
import sys
import types
from typing import Any, ClassVar
from unittest.mock import MagicMock, patch

import pytest

# Ensure pydgraph mock is present even when running in isolation.
if "pydgraph" not in sys.modules:
    _pdg = types.ModuleType("pydgraph")
    _pdg.DgraphClient = MagicMock
    _pdg.DgraphClientStub = MagicMock
    _pdg.Mutation = MagicMock
    _pdg.Operation = MagicMock
    sys.modules["pydgraph"] = _pdg

import pydgraph

from ami.core.exceptions import StorageError
from ami.implementations.graph.dgraph_relations import (
    DgraphRelationalMixin,
)
from ami.models.base_model import ModelMetadata, StorageModel

_COLLECTION = "test_nodes"
_EDGE_COUNT_ZERO = 0
_ANALYZER_PATH = (
    "ami.implementations.graph.dgraph_relations.GraphSchemaAnalyzer.analyze_model"
)


class _TestModel(StorageModel):
    """Minimal model for relational mixin tests."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path=_COLLECTION,
    )
    name: str = "default"


class _RelationalDAO(DgraphRelationalMixin):
    """Concrete DAO-like class combining the mixin."""

    def __init__(self) -> None:
        self.client: MagicMock | None = MagicMock()
        self.collection_name: str = _COLLECTION
        self.model_cls: type[StorageModel] = _TestModel


def _make_dao(*, connected: bool = True) -> _RelationalDAO:
    """Build a _RelationalDAO with optional null client."""
    dao = _RelationalDAO()
    if not connected:
        dao.client = None
    return dao


def _schema_with_edges(
    edge_map: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Return a minimal schema dict."""
    return {
        "is_node": True,
        "edges": edge_map or {},
        "properties": {},
        "reverse_edges": {},
        "model_name": _COLLECTION,
    }


def _mock_txn(
    *,
    response: MagicMock | None = None,
    error: Exception | None = None,
) -> MagicMock:
    """Build a mock transaction."""
    txn = MagicMock()
    if error:
        txn.mutate.side_effect = error
    else:
        txn.mutate.return_value = response
    txn.commit.return_value = None
    txn.discard.return_value = None
    return txn


def _attach_txn(
    dao: _RelationalDAO,
    txn: MagicMock,
) -> None:
    """Wire a mock txn into the dao client."""
    assert dao.client is not None
    dao.client.txn.return_value = txn


class TestGetUidReference:
    """Verify _get_uid_reference handles all item shapes."""

    def test_string_item(self) -> None:
        dao = _make_dao()
        assert dao._get_uid_reference("0xabc") == {"uid": "0xabc"}

    def test_item_with_graph_id(self) -> None:
        dao = _make_dao()
        item = MagicMock(spec=["graph_id"])
        item.graph_id = "0x99"
        assert dao._get_uid_reference(item) == {"uid": "0x99"}

    def test_item_with_uid(self) -> None:
        dao = _make_dao()
        item = MagicMock(spec=["uid"])
        item.uid = "myuid"
        assert dao._get_uid_reference(item) == {"uid": "_myuid"}

    def test_unknown_item(self) -> None:
        dao = _make_dao()
        assert dao._get_uid_reference(object()) == {"uid": "_:unknown"}


class TestProcessEdgeValue:
    """Verify _process_edge_value for list and scalar inputs."""

    def test_list_value(self) -> None:
        dao = _make_dao()
        node: dict[str, Any] = {}
        dao._process_edge_value(["0x1", "0x2"], "knows", node)
        assert node["knows"] == [
            {"uid": "0x1"},
            {"uid": "0x2"},
        ]

    def test_scalar_value(self) -> None:
        dao = _make_dao()
        node: dict[str, Any] = {}
        dao._process_edge_value("0xabc", "author", node)
        assert node["author"] == {"uid": "0xabc"}

    def test_falsy_value_ignored(self) -> None:
        dao = _make_dao()
        node: dict[str, Any] = {}
        dao._process_edge_value(None, "empty", node)
        assert "empty" not in node

    def test_empty_string_ignored(self) -> None:
        dao = _make_dao()
        node: dict[str, Any] = {}
        dao._process_edge_value("", "blank", node)
        assert "blank" not in node


class TestPrepareNodeData:
    """Verify _prepare_node_data splits fields vs edges."""

    def test_separates_edges_from_fields(self) -> None:
        dao = _make_dao()
        schema = _schema_with_edges(
            {"author": {"edge_name": "written_by"}},
        )
        instance_dict: dict[str, Any] = {
            "name": "doc1",
            "author": "0x5",
        }
        node, edges = dao._prepare_node_data(instance_dict, schema)
        assert f"{_COLLECTION}.name" in node
        assert "author" in edges
        assert edges["author"]["value"] == "0x5"

    def test_uid_field_prefixed(self) -> None:
        dao = _make_dao()
        schema = _schema_with_edges()
        instance_dict: dict[str, Any] = {
            "uid": "abc-123",
            "name": "item",
        }
        node, edges = dao._prepare_node_data(instance_dict, schema)
        assert node[f"{_COLLECTION}.uid"] == "abc-123"
        assert len(edges) == _EDGE_COUNT_ZERO

    def test_none_values_skipped(self) -> None:
        dao = _make_dao()
        schema = _schema_with_edges()
        instance_dict: dict[str, Any] = {
            "name": None,
            "title": "hello",
        }
        node, _edges = dao._prepare_node_data(instance_dict, schema)
        assert f"{_COLLECTION}.name" not in node
        assert node[f"{_COLLECTION}.title"] == "hello"


class TestAddRelationEdges:
    """Verify _add_relation_edges merges extra relations."""

    def test_adds_known_edge(self) -> None:
        dao = _make_dao()
        schema = _schema_with_edges(
            {"tags": {"edge_name": "tagged_with"}},
        )
        edges: dict[str, Any] = {}
        dao._add_relation_edges(
            {"tags": ["0x10", "0x11"]},
            schema,
            edges,
        )
        assert "tags" in edges
        assert edges["tags"]["value"] == ["0x10", "0x11"]

    def test_ignores_unknown_edge(self) -> None:
        dao = _make_dao()
        schema = _schema_with_edges()
        edges: dict[str, Any] = {}
        dao._add_relation_edges({"bogus": "0x99"}, schema, edges)
        assert len(edges) == _EDGE_COUNT_ZERO

    def test_none_relations_noop(self) -> None:
        dao = _make_dao()
        schema = _schema_with_edges(
            {"tags": {"edge_name": "tagged_with"}},
        )
        edges: dict[str, Any] = {}
        dao._add_relation_edges(None, schema, edges)
        assert len(edges) == _EDGE_COUNT_ZERO


class TestCreateWithRelations:
    """Verify create_with_relations async method."""

    @pytest.mark.asyncio
    async def test_success_returns_uid(self) -> None:
        dao = _make_dao()
        resp = MagicMock()
        resp.uids.get.return_value = "0xnew"
        txn = _mock_txn(response=resp)
        _attach_txn(dao, txn)

        schema = _schema_with_edges()
        with patch(_ANALYZER_PATH, return_value=schema):
            uid = await dao.create_with_relations(
                _TestModel(name="alpha"),
            )

        assert uid == "0xnew"
        txn.mutate.assert_called_once()
        txn.commit.assert_called_once()
        txn.discard.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_uid_raises(self) -> None:
        dao = _make_dao()
        resp = MagicMock()
        resp.uids.get.return_value = None
        txn = _mock_txn(response=resp)
        _attach_txn(dao, txn)

        schema = _schema_with_edges()
        with (
            patch(_ANALYZER_PATH, return_value=schema),
            pytest.raises(StorageError, match="Failed to get UID"),
        ):
            await dao.create_with_relations(
                _TestModel(name="beta"),
            )
        txn.discard.assert_called_once()

    @pytest.mark.asyncio
    async def test_not_connected_raises(self) -> None:
        dao = _make_dao(connected=False)
        with pytest.raises(StorageError, match="Not connected"):
            await dao.create_with_relations(
                _TestModel(name="gamma"),
            )

    @pytest.mark.asyncio
    async def test_mutation_error_raises(self) -> None:
        dao = _make_dao()
        txn = _mock_txn(error=RuntimeError("grpc fail"))
        _attach_txn(dao, txn)

        schema = _schema_with_edges()
        with (
            patch(_ANALYZER_PATH, return_value=schema),
            pytest.raises(
                StorageError,
                match="Failed to create with relations",
            ),
        ):
            await dao.create_with_relations(
                _TestModel(name="delta"),
            )
        txn.discard.assert_called_once()

    @pytest.mark.asyncio
    async def test_with_edges_in_schema(self) -> None:
        dao = _make_dao()
        resp = MagicMock()
        resp.uids.get.return_value = "0xedge"
        txn = _mock_txn(response=resp)
        _attach_txn(dao, txn)

        schema = _schema_with_edges(
            {"author": {"edge_name": "written_by"}},
        )
        with patch(_ANALYZER_PATH, return_value=schema):
            uid = await dao.create_with_relations(
                _TestModel(name="edgy"),
                relations={"author": "0x42"},
            )

        assert uid == "0xedge"
        txn.mutate.assert_called_once()


class TestAddEdge:
    """Verify add_edge async method."""

    @pytest.mark.asyncio
    async def test_simple_edge(self) -> None:
        dao = _make_dao()
        txn = _mock_txn()
        _attach_txn(dao, txn)

        await dao.add_edge("0x1", "0x2", "knows")

        txn.mutate.assert_called_once()
        txn.commit.assert_called_once()
        txn.discard.assert_called_once()

    @pytest.mark.asyncio
    async def test_edge_with_properties(self) -> None:
        dao = _make_dao()
        txn = _mock_txn()
        _attach_txn(dao, txn)

        props = {"weight": 0.8, "since": "2024-01-01"}
        await dao.add_edge(
            "0xa",
            "0xb",
            "collaborates",
            properties=props,
        )

        txn.mutate.assert_called_once()
        txn.commit.assert_called_once()
        txn.discard.assert_called_once()

    @pytest.mark.asyncio
    async def test_not_connected_raises(self) -> None:
        dao = _make_dao(connected=False)
        with pytest.raises(StorageError, match="Not connected"):
            await dao.add_edge("0x1", "0x2", "knows")

    @pytest.mark.asyncio
    async def test_mutation_error_raises(self) -> None:
        dao = _make_dao()
        txn = _mock_txn(error=RuntimeError("timeout"))
        _attach_txn(dao, txn)

        with pytest.raises(StorageError, match="Failed to add edge"):
            await dao.add_edge("0x1", "0x2", "knows")
        txn.discard.assert_called_once()

    @pytest.mark.asyncio
    async def test_simple_edge_mutation_payload(self) -> None:
        """Verify the JSON payload for a simple edge."""
        dao = _make_dao()
        txn = _mock_txn()
        _attach_txn(dao, txn)

        mock_mutation_cls = MagicMock()
        with patch.object(pydgraph, "Mutation", mock_mutation_cls):
            await dao.add_edge("0x1", "0x2", "likes")

        raw = mock_mutation_cls.call_args.kwargs.get("set_json")
        payload = json.loads(raw)
        assert payload == [
            {"uid": "0x1", "likes": {"uid": "0x2"}},
        ]


class TestRemoveEdge:
    """Verify remove_edge async method."""

    @pytest.mark.asyncio
    async def test_success(self) -> None:
        dao = _make_dao()
        txn = _mock_txn()
        _attach_txn(dao, txn)

        await dao.remove_edge("0x1", "0x2", "knows")

        txn.mutate.assert_called_once()
        txn.commit.assert_called_once()
        txn.discard.assert_called_once()

    @pytest.mark.asyncio
    async def test_not_connected_raises(self) -> None:
        dao = _make_dao(connected=False)
        with pytest.raises(StorageError, match="Not connected"):
            await dao.remove_edge("0x1", "0x2", "knows")

    @pytest.mark.asyncio
    async def test_mutation_error_raises(self) -> None:
        dao = _make_dao()
        txn = _mock_txn(error=RuntimeError("boom"))
        _attach_txn(dao, txn)

        with pytest.raises(
            StorageError,
            match="Failed to remove edge",
        ):
            await dao.remove_edge("0x1", "0x2", "knows")
        txn.discard.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_payload(self) -> None:
        """Verify the JSON sent via delete_json."""
        dao = _make_dao()
        txn = _mock_txn()
        _attach_txn(dao, txn)

        mock_mutation_cls = MagicMock()
        with patch.object(pydgraph, "Mutation", mock_mutation_cls):
            await dao.remove_edge("0xa", "0xb", "friend")

        raw = mock_mutation_cls.call_args.kwargs.get("delete_json")
        payload = json.loads(raw)
        assert payload == {
            "uid": "0xa",
            "friend": {"uid": "0xb"},
        }
