"""Tests for ami.core.graph_relations module."""

from __future__ import annotations

from typing import Annotated

import pytest
from pydantic import BaseModel as PydanticBaseModel

from ami.core.graph_relations import (
    GraphQueryBuilder,
    GraphRelation,
    GraphSchemaAnalyzer,
    RelationalField,
    _escape_dql_value,
)

# ------------------------------------------------------------------
# Test fixtures: lightweight models used across test classes
# ------------------------------------------------------------------


class _SimpleModel(PydanticBaseModel):
    uid: str | None = None
    name: str = ""


class _ModelWithEdge(PydanticBaseModel):
    uid: str | None = None
    name: str = ""
    friends: Annotated[
        list[str],
        GraphRelation(
            edge_name="has_friend",
            reverse_name="friend_of",
        ),
    ] = []
    parent: Annotated[
        str,
        GraphRelation(edge_name="has_parent"),
    ] = ""


class _Obj:
    """Minimal namespace for RelationalField descriptor tests."""

    uid: str | None = None


def _list_edge_config() -> dict:
    return {
        "field_name": "friends",
        "edge_name": "has_friend",
        "is_list": True,
        "target_type": "str",
        "eager_load": False,
    }


def _scalar_edge_config() -> dict:
    return {
        "field_name": "parent",
        "edge_name": "has_parent",
        "is_list": False,
        "target_type": "str",
        "eager_load": False,
    }


# ------------------------------------------------------------------
# _escape_dql_value
# ------------------------------------------------------------------


class TestEscapeDqlValue:
    """Verify DQL string-literal escaping."""

    def test_plain_string_unchanged(self) -> None:
        assert _escape_dql_value("hello") == "hello"

    def test_backslash_escaped(self) -> None:
        assert _escape_dql_value("a\\b") == "a\\\\b"

    def test_double_quote_escaped(self) -> None:
        assert _escape_dql_value('say "hi"') == 'say \\"hi\\"'

    def test_newline_escaped(self) -> None:
        assert _escape_dql_value("line1\nline2") == "line1\\nline2"

    def test_combined_special_chars(self) -> None:
        raw = 'a\\b"c\nd'
        expected = 'a\\\\b\\"c\\nd'
        assert _escape_dql_value(raw) == expected

    def test_non_string_coerced(self) -> None:
        assert _escape_dql_value(42) == "42"


# ------------------------------------------------------------------
# GraphRelation
# ------------------------------------------------------------------


class TestGraphRelation:
    """Verify GraphRelation annotation data class."""

    def test_defaults(self) -> None:
        gr = GraphRelation()
        assert gr.edge_name is None
        assert gr.reverse_name is None
        assert gr.target_type is None
        assert gr.cascade_delete is False
        assert gr.eager_load is False

    def test_all_params(self) -> None:
        gr = GraphRelation(
            edge_name="owns",
            reverse_name="owned_by",
            target_type="Pet",
            cascade_delete=True,
            eager_load=True,
        )
        assert gr.edge_name == "owns"
        assert gr.reverse_name == "owned_by"
        assert gr.target_type == "Pet"
        assert gr.cascade_delete is True
        assert gr.eager_load is True


# ------------------------------------------------------------------
# GraphSchemaAnalyzer
# ------------------------------------------------------------------


class TestGraphSchemaAnalyzer:
    """Verify schema analysis for plain and edge-bearing models."""

    def test_analyze_plain_model(self) -> None:
        schema = GraphSchemaAnalyzer.analyze_model(_SimpleModel)
        assert schema["is_node"] is True
        assert schema["model_name"] == "_SimpleModel"
        assert "uid" in schema["properties"]
        assert "name" in schema["properties"]
        assert schema["edges"] == {}

    def test_analyze_model_with_edges(self) -> None:
        schema = GraphSchemaAnalyzer.analyze_model(_ModelWithEdge)
        assert "friends" in schema["edges"]
        assert "parent" in schema["edges"]
        assert "name" in schema["properties"]
        assert "uid" in schema["properties"]

    def test_edge_config_friends(self) -> None:
        schema = GraphSchemaAnalyzer.analyze_model(_ModelWithEdge)
        cfg = schema["edges"]["friends"]
        assert cfg["edge_name"] == "has_friend"
        assert cfg["is_list"] is True
        assert cfg["reverse_name"] == "friend_of"
        assert cfg["field_name"] == "friends"

    def test_edge_config_parent(self) -> None:
        schema = GraphSchemaAnalyzer.analyze_model(_ModelWithEdge)
        cfg = schema["edges"]["parent"]
        assert cfg["edge_name"] == "has_parent"
        assert cfg["is_list"] is False
        assert cfg["reverse_name"] is None

    def test_reverse_edges_populated(self) -> None:
        schema = GraphSchemaAnalyzer.analyze_model(_ModelWithEdge)
        assert "friend_of" in schema["reverse_edges"]
        rev = schema["reverse_edges"]["friend_of"]
        assert rev["field"] == "friends"
        assert rev["source_type"] == "_ModelWithEdge"

    def test_get_base_type_annotated(self) -> None:
        annotated = Annotated[list[str], GraphRelation()]
        base = GraphSchemaAnalyzer._get_base_type(annotated)
        assert base is not annotated

    def test_get_base_type_plain(self) -> None:
        result = GraphSchemaAnalyzer._get_base_type(str)
        assert result is str

    def test_type_to_string_str_input(self) -> None:
        assert GraphSchemaAnalyzer._type_to_string("Foo") == "Foo"

    def test_type_to_string_type_input(self) -> None:
        assert GraphSchemaAnalyzer._type_to_string(int) == "int"

    def test_type_to_string_unsupported_raises(self) -> None:
        with pytest.raises(TypeError, match="Cannot convert type"):
            GraphSchemaAnalyzer._type_to_string(123)

    def test_is_edge_field_true(self) -> None:
        assert GraphSchemaAnalyzer.is_edge_field(_ModelWithEdge, "friends")

    def test_is_edge_field_false(self) -> None:
        assert not GraphSchemaAnalyzer.is_edge_field(_ModelWithEdge, "name")

    def test_get_edge_config_found(self) -> None:
        cfg = GraphSchemaAnalyzer.get_edge_config(_ModelWithEdge, "friends")
        assert cfg["edge_name"] == "has_friend"

    def test_get_edge_config_missing_raises(self) -> None:
        with pytest.raises(ValueError, match="not an edge field"):
            GraphSchemaAnalyzer.get_edge_config(_ModelWithEdge, "name")


# ------------------------------------------------------------------
# RelationalField
# ------------------------------------------------------------------


class TestRelationalFieldDescriptor:
    """Verify the descriptor protocol for RelationalField."""

    # -- __get__ ------------------------------------------------

    def test_get_from_class_returns_descriptor(self) -> None:
        rf = RelationalField(_list_edge_config())
        assert rf.__get__(None, _Obj) is rf

    def test_get_returns_cached_objects(self) -> None:
        rf = RelationalField(_list_edge_config())
        obj = _Obj()
        cached = ["obj_a", "obj_b"]
        setattr(obj, rf._cache_attr, cached)
        assert rf.__get__(obj) is cached

    def test_get_returns_ids_when_no_cache(self) -> None:
        rf = RelationalField(_list_edge_config())
        obj = _Obj()
        setattr(obj, rf._ids_attr, ["id1", "id2"])
        assert rf.__get__(obj) == ["id1", "id2"]

    def test_get_returns_empty_list_default_for_list(self) -> None:
        rf = RelationalField(_list_edge_config())
        obj = _Obj()
        assert rf.__get__(obj) == []

    def test_get_returns_none_default_for_scalar(self) -> None:
        rf = RelationalField(_scalar_edge_config())
        obj = _Obj()
        assert rf.__get__(obj) is None

    # -- __set__ ------------------------------------------------

    def test_set_none_clears_list(self) -> None:
        rf = RelationalField(_list_edge_config())
        obj = _Obj()
        setattr(obj, rf._cache_attr, ["something"])
        setattr(obj, rf._ids_attr, ["id1"])
        rf.__set__(obj, None)
        assert getattr(obj, rf._ids_attr) == []
        assert not hasattr(obj, rf._cache_attr)

    def test_set_none_clears_scalar(self) -> None:
        rf = RelationalField(_scalar_edge_config())
        obj = _Obj()
        setattr(obj, rf._cache_attr, "cached_val")
        rf.__set__(obj, None)
        assert getattr(obj, rf._ids_attr) is None
        assert not hasattr(obj, rf._cache_attr)

    def test_set_list_of_strings(self) -> None:
        rf = RelationalField(_list_edge_config())
        obj = _Obj()
        rf.__set__(obj, ["uid1", "uid2"])
        assert getattr(obj, rf._ids_attr) == ["uid1", "uid2"]
        assert not hasattr(obj, rf._cache_attr)

    def test_set_list_of_objects(self) -> None:
        rf = RelationalField(_list_edge_config())
        obj = _Obj()
        a = _Obj()
        a.uid = "uid_a"
        b = _Obj()
        b.uid = "uid_b"
        rf.__set__(obj, [a, b])
        assert getattr(obj, rf._ids_attr) == ["uid_a", "uid_b"]
        assert getattr(obj, rf._cache_attr) == [a, b]

    def test_set_scalar_string(self) -> None:
        rf = RelationalField(_scalar_edge_config())
        obj = _Obj()
        rf.__set__(obj, "uid_x")
        assert getattr(obj, rf._ids_attr) == "uid_x"
        assert not hasattr(obj, rf._cache_attr)

    def test_set_scalar_object(self) -> None:
        rf = RelationalField(_scalar_edge_config())
        obj = _Obj()
        related = _Obj()
        related.uid = "uid_r"
        rf.__set__(obj, related)
        assert getattr(obj, rf._ids_attr) == "uid_r"
        assert getattr(obj, rf._cache_attr) is related

    def test_set_string_clears_prior_cache(self) -> None:
        rf = RelationalField(_scalar_edge_config())
        obj = _Obj()
        setattr(obj, rf._cache_attr, "old")
        rf.__set__(obj, "new_uid")
        assert not hasattr(obj, rf._cache_attr)
        assert getattr(obj, rf._ids_attr) == "new_uid"


# ------------------------------------------------------------------
# GraphQueryBuilder
# ------------------------------------------------------------------


class TestGraphQueryBuilder:
    """Verify DQL query construction."""

    def test_build_no_filters_no_edges(self) -> None:
        qb = GraphQueryBuilder(_SimpleModel)
        query = qb.build()
        assert "func: type(_SimpleModel)" in query
        assert "uid" in query
        assert "expand(_all_)" in query

    def test_build_with_filter(self) -> None:
        qb = GraphQueryBuilder(_SimpleModel)
        qb.filter_by(name="Alice")
        query = qb.build()
        assert 'eq(_SimpleModel.name, "Alice")' in query
        assert "type(_SimpleModel)" not in query

    def test_build_with_edges(self) -> None:
        qb = GraphQueryBuilder(_ModelWithEdge)
        qb.with_edges("friends")
        query = qb.build()
        assert "has_friend {" in query

    def test_with_all_edges(self) -> None:
        qb = GraphQueryBuilder(_ModelWithEdge)
        qb.with_all_edges()
        query = qb.build()
        assert "has_friend {" in query
        assert "has_parent {" in query

    def test_filter_by_fluent_chaining(self) -> None:
        qb = GraphQueryBuilder(_SimpleModel)
        ret = qb.filter_by(name="Bob")
        assert ret is qb

    def test_with_edges_fluent_chaining(self) -> None:
        qb = GraphQueryBuilder(_ModelWithEdge)
        ret = qb.with_edges("friends")
        assert ret is qb

    def test_with_all_edges_fluent_chaining(self) -> None:
        qb = GraphQueryBuilder(_ModelWithEdge)
        ret = qb.with_all_edges()
        assert ret is qb

    def test_build_escapes_filter_value(self) -> None:
        qb = GraphQueryBuilder(_SimpleModel)
        qb.filter_by(name='say "hi"')
        query = qb.build()
        assert 'say \\"hi\\"' in query

    def test_build_multiple_filters(self) -> None:
        qb = GraphQueryBuilder(_ModelWithEdge)
        qb.filter_by(name="A", uid="0x1")
        query = qb.build()
        assert " AND " in query
        assert 'eq(_ModelWithEdge.name, "A")' in query
        assert 'eq(_ModelWithEdge.uid, "0x1")' in query

    def test_unknown_edge_ignored(self) -> None:
        qb = GraphQueryBuilder(_ModelWithEdge)
        qb.with_edges("nonexistent")
        query = qb.build()
        assert "nonexistent" not in query
