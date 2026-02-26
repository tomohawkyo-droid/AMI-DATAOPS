"""Tests for async methods in dgraph_graph and dgraph_traversal."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.core.exceptions import StorageError
from ami.implementations.graph.dgraph_graph import (
    _get_all_nodes,
    _get_node_neighbors,
    find_connected_components,
    get_node_degree,
    one_hop_neighbors,
    shortest_path,
)
from ami.implementations.graph.dgraph_traversal import (
    DgraphTraversalMixin,
)

EXPECT_TWO = 2
EXPECT_THREE = 3
EXPECT_FIVE = 5
_GG = "ami.implementations.graph.dgraph_graph"
_GT = "ami.implementations.graph.dgraph_traversal"


def _dao(connected: bool = True) -> MagicMock:
    d = MagicMock()
    d.collection_name = "TestNode"
    d.connect = AsyncMock()
    d.client = MagicMock() if connected else None
    return d


def _txn(payload: str) -> MagicMock:
    r = MagicMock()
    r.json = payload
    t = MagicMock()
    t.query = MagicMock(return_value=r)
    t.discard = MagicMock()
    return t


def _wire(obj: Any, payload: str) -> MagicMock:
    t = _txn(payload)
    obj.client.txn.return_value = t
    return t


def _reconnect(obj: Any, payload: str) -> None:
    def _w() -> None:
        obj.client = MagicMock()
        _wire(obj, payload)

    obj.connect = AsyncMock(side_effect=_w)


def _mixin(connected: bool = True) -> DgraphTraversalMixin:
    m = DgraphTraversalMixin()
    m.client = MagicMock() if connected else None
    m.collection_name = "TestNode"
    m.model_cls = MagicMock()
    return m


def _j(o: Any) -> str:
    return json.dumps(o)


def _bad_txn() -> MagicMock:
    """Return a txn whose query yields unparseable json."""
    rsp = MagicMock()
    rsp.json = "BAD"
    t = MagicMock()
    t.query = MagicMock(return_value=rsp)
    t.discard = MagicMock()
    return t


class TestOneHopNeighbors:
    """one_hop_neighbors async coverage."""

    @pytest.mark.asyncio
    async def test_hex_uid(self) -> None:
        p = [{"uid": "0x1", "friends": [{"uid": "0x2"}]}]
        d = _dao()
        _wire(d, _j({"path": p}))
        assert await one_hop_neighbors(d, "0x1") == p

    @pytest.mark.asyncio
    async def test_empty_path(self) -> None:
        d = _dao()
        _wire(d, _j({"path": []}))
        assert await one_hop_neighbors(d, "0xabc") == []

    @pytest.mark.asyncio
    async def test_app_uid_found(self) -> None:
        fr, hr = MagicMock(), MagicMock()
        fr.json = _j({"find_node": [{"uid": "0x99"}]})
        hr.json = _j({"path": [{"uid": "0x99"}]})
        t = MagicMock()
        t.query = MagicMock(side_effect=[fr, hr])
        t.discard = MagicMock()
        d = _dao()
        d.client.txn.return_value = t
        r = await one_hop_neighbors(d, "mynode")
        assert r[0]["uid"] == "0x99"

    @pytest.mark.asyncio
    async def test_app_uid_not_found(self) -> None:
        fr = MagicMock()
        fr.json = _j({"find_node": []})
        t = MagicMock()
        t.query = MagicMock(return_value=fr)
        t.discard = MagicMock()
        d = _dao()
        d.client.txn.return_value = t
        with pytest.raises(StorageError, match="not found"):
            await one_hop_neighbors(d, "missing")

    @pytest.mark.asyncio
    async def test_reconnect(self) -> None:
        d = _dao(connected=False)
        _reconnect(d, _j({"path": []}))
        await one_hop_neighbors(d, "0x1")
        d.connect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_bad_uid(self) -> None:
        with pytest.raises(StorageError, match="Invalid"):
            await one_hop_neighbors(_dao(), "bad uid!")


class TestShortestPath:
    """shortest_path async coverage."""

    @pytest.mark.asyncio
    async def test_returns_uids(self) -> None:
        ns = [{"uid": "0x1"}, {"uid": "0x2"}, {"uid": "0x3"}]
        d = _dao()
        _wire(d, _j({"path_nodes": ns}))
        r = await shortest_path(d, "0x1", "0x3")
        assert r == ["0x1", "0x2", "0x3"]

    @pytest.mark.asyncio
    async def test_empty(self) -> None:
        d = _dao()
        _wire(d, _j({"path_nodes": []}))
        assert await shortest_path(d, "0x1", "0x99") == []

    @pytest.mark.asyncio
    async def test_custom_depth(self) -> None:
        d = _dao()
        t = _wire(d, _j({"path_nodes": [{"uid": "0x1"}]}))
        await shortest_path(d, "0x1", "0x2", max_depth=EXPECT_FIVE)
        v = t.query.call_args[1]["variables"]
        assert v["$depth"] == EXPECT_FIVE

    @pytest.mark.asyncio
    async def test_reconnect(self) -> None:
        d = _dao(connected=False)
        _reconnect(d, _j({"path_nodes": []}))
        await shortest_path(d, "0x1", "0x2")
        d.connect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_bad_start(self) -> None:
        with pytest.raises(StorageError, match="Invalid"):
            await shortest_path(_dao(), "bad!", "0x2")

    @pytest.mark.asyncio
    async def test_bad_end(self) -> None:
        with pytest.raises(StorageError, match="Invalid"):
            await shortest_path(_dao(), "0x1", "bad!")

    @pytest.mark.asyncio
    async def test_zero_depth(self) -> None:
        with pytest.raises(StorageError, match="Invalid"):
            await shortest_path(_dao(), "0x1", "0x2", max_depth=0)


class TestGetNodeDegree:
    """get_node_degree async coverage."""

    @pytest.mark.asyncio
    async def test_all(self) -> None:
        nd = {
            "uid": "0x1",
            "dgraph.type": ["P"],
            "friends": [{"uid": "0x2"}, {"uid": "0x3"}],
            "~follows": [{"uid": "0x4"}],
        }
        d = _dao()
        _wire(d, _j({"node": [nd]}))
        r = await get_node_degree(d, "0x1", "all")
        assert r["in"] == 1
        assert r["out"] == EXPECT_TWO
        assert r["total"] == EXPECT_THREE

    @pytest.mark.asyncio
    async def test_in(self) -> None:
        nd = {
            "uid": "0x1",
            "~f": [{"uid": "0x2"}, {"uid": "0x3"}],
        }
        d = _dao()
        _wire(d, _j({"node": [nd]}))
        r = await get_node_degree(d, "0x1", "in")
        assert r == {"in": EXPECT_TWO}

    @pytest.mark.asyncio
    async def test_out(self) -> None:
        nd = {"uid": "0x1", "friends": [{"uid": "0x2"}]}
        d = _dao()
        _wire(d, _j({"node": [nd]}))
        r = await get_node_degree(d, "0x1", "out")
        assert r == {"out": 1}

    @pytest.mark.asyncio
    async def test_empty(self) -> None:
        d = _dao()
        _wire(d, _j({"node": []}))
        r = await get_node_degree(d, "0x1")
        assert r == {"in": 0, "out": 0, "total": 0}

    @pytest.mark.asyncio
    async def test_reconnect(self) -> None:
        d = _dao(connected=False)
        _reconnect(d, _j({"node": []}))
        await get_node_degree(d, "0x1")
        d.connect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_bad_uid(self) -> None:
        with pytest.raises(StorageError, match="Invalid"):
            await get_node_degree(_dao(), "bad!")

    @pytest.mark.asyncio
    async def test_bad_direction(self) -> None:
        with pytest.raises(StorageError, match="Invalid"):
            await get_node_degree(_dao(), "0x1", "sideways")


class TestGetAllNodes:
    """_get_all_nodes async coverage."""

    @pytest.mark.asyncio
    async def test_with_type(self) -> None:
        d = _dao()
        _wire(d, _j({"nodes": [{"uid": "0x1"}]}))
        assert len(await _get_all_nodes(d, "Person")) == 1

    @pytest.mark.asyncio
    async def test_no_type(self) -> None:
        d = _dao()
        ns = [{"uid": "0x1"}, {"uid": "0x2"}]
        _wire(d, _j({"nodes": ns}))
        assert len(await _get_all_nodes(d, None)) == EXPECT_TWO

    @pytest.mark.asyncio
    async def test_empty(self) -> None:
        d = _dao()
        _wire(d, _j({"nodes": []}))
        assert await _get_all_nodes(d, "Ghost") == []


class TestGetNodeNeighbors:
    """_get_node_neighbors async coverage."""

    @pytest.mark.asyncio
    async def test_returns_uids(self) -> None:
        nd = {
            "uid": "0x1",
            "friends": [{"uid": "0x2"}, {"uid": "0x3"}],
        }
        d = _dao()
        _wire(d, _j({"node": [nd]}))
        r = await _get_node_neighbors(d, "0x1")
        assert "0x2" in r
        assert "0x3" in r

    @pytest.mark.asyncio
    async def test_empty(self) -> None:
        d = _dao()
        _wire(d, _j({"node": []}))
        assert await _get_node_neighbors(d, "0x1") == []


class TestFindConnectedComponents:
    """find_connected_components async coverage."""

    @pytest.mark.asyncio
    async def test_single_component(self) -> None:
        d = _dao()
        nds = [{"uid": "0x1"}, {"uid": "0x2"}]
        with (
            patch(
                f"{_GG}._get_all_nodes",
                new_callable=AsyncMock,
                return_value=nds,
            ),
            patch(
                f"{_GG}._find_component_dfs",
                new_callable=AsyncMock,
                side_effect=[["0x1", "0x2"], []],
            ),
        ):
            r = await find_connected_components(d)
        assert len(r) == 1

    @pytest.mark.asyncio
    async def test_empty(self) -> None:
        d = _dao()
        with patch(
            f"{_GG}._get_all_nodes",
            new_callable=AsyncMock,
            return_value=[],
        ):
            r = await find_connected_components(d)
        assert r == []

    @pytest.mark.asyncio
    async def test_reconnect(self) -> None:
        d = _dao(connected=False)
        _reconnect(d, "")
        with patch(
            f"{_GG}._get_all_nodes",
            new_callable=AsyncMock,
            return_value=[],
        ):
            await find_connected_components(d)
        d.connect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_wraps_error(self) -> None:
        d = _dao()
        with (
            patch(
                f"{_GG}._get_all_nodes",
                new_callable=AsyncMock,
                side_effect=RuntimeError("boom"),
            ),
            pytest.raises(StorageError, match="failed"),
        ):
            await find_connected_components(d)

    @pytest.mark.asyncio
    async def test_type_filter(self) -> None:
        d = _dao()
        with patch(
            f"{_GG}._get_all_nodes",
            new_callable=AsyncMock,
            return_value=[],
        ) as mn:
            await find_connected_components(d, "Person")
        mn.assert_awaited_once_with(d, "Person")


class TestGetEdges:
    """get_edges async coverage."""

    @pytest.mark.asyncio
    async def test_all(self) -> None:
        nl = [{"uid": "0x1", "friends": [{"uid": "0x2"}]}]
        m = _mixin()
        _wire(m, _j({"node": nl}))
        assert await m.get_edges("0x1") == nl

    @pytest.mark.asyncio
    @pytest.mark.parametrize("direction", ["out", "in", "both"])
    async def test_direction(self, direction: str) -> None:
        m = _mixin()
        _wire(m, _j({"node": [{"uid": "0x1"}]}))
        r = await m.get_edges("0x1", edge_name="f", direction=direction)
        assert r == [{"uid": "0x1"}]

    @pytest.mark.asyncio
    async def test_not_connected(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await _mixin(connected=False).get_edges("0x1")

    @pytest.mark.asyncio
    async def test_bad_json(self) -> None:
        m = _mixin()
        m.client.txn.return_value = _bad_txn()
        with pytest.raises(StorageError, match="parse"):
            await m.get_edges("0x1")

    @pytest.mark.asyncio
    async def test_missing_key(self) -> None:
        m = _mixin()
        _wire(m, _j({"other": "x"}))
        with pytest.raises(StorageError, match="not found"):
            await m.get_edges("0x1")

    @pytest.mark.asyncio
    async def test_not_list(self) -> None:
        m = _mixin()
        _wire(m, _j({"node": "bad"}))
        with pytest.raises(StorageError, match="Unexpected"):
            await m.get_edges("0x1")


class TestTraverse:
    """traverse async coverage."""

    @pytest.mark.asyncio
    async def test_single_edge(self) -> None:
        p = [{"uid": "0x1", "friends": [{"uid": "0x2"}]}]
        m = _mixin()
        _wire(m, _j({"path": p}))
        r = await m.traverse("0x1", ["friends"])
        assert r[0]["uid"] == "0x2"

    @pytest.mark.asyncio
    async def test_multi_edge(self) -> None:
        inner = {"uid": "0x2", "posts": [{"uid": "0x10"}]}
        p = [{"uid": "0x1", "friends": [inner]}]
        m = _mixin()
        _wire(m, _j({"path": p}))
        r = await m.traverse("0x1", ["friends", "posts"])
        assert r[0]["uid"] == "0x10"

    @pytest.mark.asyncio
    async def test_empty(self) -> None:
        m = _mixin()
        _wire(m, _j({"path": []}))
        assert await m.traverse("0x1", ["friends"]) == []

    @pytest.mark.asyncio
    async def test_not_connected(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await _mixin(connected=False).traverse("0x1", ["f"])

    @pytest.mark.asyncio
    async def test_empty_path(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            await _mixin().traverse("0x1", [])

    @pytest.mark.asyncio
    async def test_bad_json(self) -> None:
        m = _mixin()
        m.client.txn.return_value = _bad_txn()
        with pytest.raises(StorageError, match="parse"):
            await m.traverse("0x1", ["friends"])


class TestLoadWithRelations:
    """load_with_relations async coverage."""

    @pytest.mark.asyncio
    async def test_success(self) -> None:
        fr, nr = MagicMock(), MagicMock()
        fr.json = _j({"items": [{"uid": "0x5"}]})
        nr.json = _j({"node": [{"uid": "0x5", "TestNode.name": "A"}]})
        t = MagicMock()
        t.query = MagicMock(side_effect=[fr, nr])
        t.discard = MagicMock()
        m = _mixin()
        m.client.txn.return_value = t
        mm = MagicMock()
        mm.from_storage_dict = AsyncMock(return_value=MagicMock())
        m.model_cls = mm
        schema: dict[str, Any] = {
            "is_node": True,
            "edges": {},
            "properties": {},
            "reverse_edges": {},
            "model_name": "TestNode",
        }
        with patch(
            f"{_GT}.GraphSchemaAnalyzer.analyze_model",
            return_value=schema,
        ):
            await m.load_with_relations("item-1")
        mm.from_storage_dict.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_not_connected(self) -> None:
        with pytest.raises(StorageError, match="Not connected"):
            await _mixin(connected=False).load_with_relations("x")

    @pytest.mark.asyncio
    async def test_not_found(self) -> None:
        m = _mixin()
        _wire(m, _j({"items": []}))
        with pytest.raises(StorageError, match="not found"):
            await m.load_with_relations("missing")

    @pytest.mark.asyncio
    async def test_items_key_missing(self) -> None:
        m = _mixin()
        _wire(m, _j({"other": "x"}))
        with pytest.raises(StorageError, match="not found"):
            await m.load_with_relations("item-1")

    @pytest.mark.asyncio
    async def test_items_not_list(self) -> None:
        m = _mixin()
        _wire(m, _j({"items": "bad"}))
        with pytest.raises(StorageError, match="not a list"):
            await m.load_with_relations("item-1")
