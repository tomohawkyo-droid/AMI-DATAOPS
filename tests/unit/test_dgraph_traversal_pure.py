"""Tests for pure (non-async) methods on DgraphTraversalMixin."""

from __future__ import annotations

import pytest

from ami.core.exceptions import StorageError
from ami.implementations.graph.dgraph_traversal import (
    DgraphTraversalMixin,
)

_EXPECTED_UID_LINES = 3
_EXPECTED_TWO = 2

# ---- _build_traverse_query ----


class TestBuildTraverseQuerySingleEdge:
    """Single-edge traversal produces a one-level nested query."""

    def setup_method(self) -> None:
        self.mixin = DgraphTraversalMixin()

    def test_contains_uid_func(self) -> None:
        query = self.mixin._build_traverse_query("0x1", ["friends"])
        assert "uid(0x1)" in query

    def test_contains_edge_block(self) -> None:
        query = self.mixin._build_traverse_query("0x1", ["friends"])
        assert "friends {" in query

    def test_contains_expand(self) -> None:
        query = self.mixin._build_traverse_query("0x1", ["friends"])
        assert "expand(_all_)" in query

    def test_contains_uid_field(self) -> None:
        query = self.mixin._build_traverse_query("0x1", ["friends"])
        lines = [ln.strip() for ln in query.splitlines()]
        assert "uid" in lines

    def test_path_func_present(self) -> None:
        query = self.mixin._build_traverse_query("0xabc", ["likes"])
        assert "path(func: uid(0xabc))" in query


class TestBuildTraverseQueryMultiEdge:
    """Multi-edge traversal produces a doubly-nested structure."""

    def setup_method(self) -> None:
        self.mixin = DgraphTraversalMixin()

    def test_two_edges_both_present(self) -> None:
        query = self.mixin._build_traverse_query("0x1", ["friends", "posts"])
        assert "friends {" in query
        assert "posts {" in query

    def test_two_edges_expand_at_leaf_only(self) -> None:
        query = self.mixin._build_traverse_query("0x1", ["friends", "posts"])
        # expand(_all_) should appear exactly once (leaf)
        count = query.count("expand(_all_)")
        assert count == 1

    def test_two_edges_uid_in_both_levels(self) -> None:
        query = self.mixin._build_traverse_query("0x1", ["friends", "posts"])
        lines = [ln.strip() for ln in query.splitlines()]
        uid_lines = [ln for ln in lines if ln == "uid"]
        # root uid + friends uid + posts uid = 3
        assert len(uid_lines) == _EXPECTED_UID_LINES

    def test_three_edges_all_present(self) -> None:
        query = self.mixin._build_traverse_query("0x5", ["a", "b", "c"])
        assert "a {" in query
        assert "b {" in query
        assert "c {" in query

    def test_start_uid_appears_in_func(self) -> None:
        query = self.mixin._build_traverse_query("0xff", ["x", "y"])
        assert "uid(0xff)" in query

    def test_closing_braces_balance(self) -> None:
        query = self.mixin._build_traverse_query("0x1", ["friends", "posts"])
        opens = query.count("{")
        closes = query.count("}")
        assert opens == closes


# ---- _extract_traverse_nodes ----


class TestExtractTraverseNodes:
    """Extraction from traversal result dicts."""

    def setup_method(self) -> None:
        self.mixin = DgraphTraversalMixin()

    def test_single_edge_extraction(self) -> None:
        result = {
            "path": [{"uid": "0x1", "friends": [{"uid": "0x2"}]}],
        }
        nodes = self.mixin._extract_traverse_nodes(result, ["friends"])
        assert len(nodes) == 1
        assert nodes[0]["uid"] == "0x2"

    def test_multi_edge_extraction(self) -> None:
        result = {
            "path": [
                {
                    "uid": "0x1",
                    "friends": [
                        {
                            "uid": "0x2",
                            "posts": [{"uid": "0x10"}],
                        }
                    ],
                }
            ],
        }
        nodes = self.mixin._extract_traverse_nodes(result, ["friends", "posts"])
        assert len(nodes) == 1
        assert nodes[0]["uid"] == "0x10"

    def test_multiple_nodes_at_leaf(self) -> None:
        result = {
            "path": [
                {
                    "uid": "0x1",
                    "friends": [
                        {"uid": "0xa"},
                        {"uid": "0xb"},
                    ],
                }
            ],
        }
        nodes = self.mixin._extract_traverse_nodes(result, ["friends"])
        assert len(nodes) == _EXPECTED_TWO

    def test_empty_path_list_returns_empty(self) -> None:
        result = {"path": []}
        nodes = self.mixin._extract_traverse_nodes(result, ["friends"])
        assert nodes == []


class TestExtractTraverseNodesErrors:
    """Error paths for _extract_traverse_nodes."""

    def setup_method(self) -> None:
        self.mixin = DgraphTraversalMixin()

    def test_missing_path_key_raises(self) -> None:
        with pytest.raises(StorageError, match="path"):
            self.mixin._extract_traverse_nodes({"other": []}, ["friends"])

    def test_path_not_a_list_raises(self) -> None:
        with pytest.raises(StorageError, match="not a list"):
            self.mixin._extract_traverse_nodes({"path": "bad"}, ["friends"])

    def test_path_as_dict_raises(self) -> None:
        with pytest.raises(StorageError, match="not a list"):
            self.mixin._extract_traverse_nodes({"path": {"uid": "0x1"}}, ["friends"])

    def test_path_as_int_raises(self) -> None:
        with pytest.raises(StorageError, match="not a list"):
            self.mixin._extract_traverse_nodes({"path": 42}, ["friends"])


# ---- _follow_edge ----


class TestFollowEdge:
    """Following a single edge through a list of nodes."""

    def setup_method(self) -> None:
        self.mixin = DgraphTraversalMixin()

    def test_edge_as_list_extends(self) -> None:
        nodes = [
            {
                "uid": "0x1",
                "friends": [
                    {"uid": "0x2"},
                    {"uid": "0x3"},
                ],
            }
        ]
        result = self.mixin._follow_edge(nodes, "friends")
        assert len(result) == _EXPECTED_TWO
        uids = [n["uid"] for n in result]
        assert "0x2" in uids
        assert "0x3" in uids

    def test_edge_as_dict_appends(self) -> None:
        nodes = [
            {"uid": "0x1", "author": {"uid": "0x5"}},
        ]
        result = self.mixin._follow_edge(nodes, "author")
        assert len(result) == 1
        assert result[0]["uid"] == "0x5"

    def test_edge_missing_returns_empty(self) -> None:
        nodes = [{"uid": "0x1", "name": "Alice"}]
        result = self.mixin._follow_edge(nodes, "nonexistent")
        assert result == []

    def test_empty_nodes_returns_empty(self) -> None:
        result = self.mixin._follow_edge([], "friends")
        assert result == []

    def test_multiple_nodes_aggregated(self) -> None:
        nodes = [
            {"uid": "0x1", "tags": [{"uid": "0xa"}]},
            {"uid": "0x2", "tags": [{"uid": "0xb"}]},
        ]
        result = self.mixin._follow_edge(nodes, "tags")
        assert len(result) == _EXPECTED_TWO

    def test_mixed_present_and_absent(self) -> None:
        nodes = [
            {"uid": "0x1", "friends": [{"uid": "0x2"}]},
            {"uid": "0x3"},
        ]
        result = self.mixin._follow_edge(nodes, "friends")
        assert len(result) == 1
        assert result[0]["uid"] == "0x2"
