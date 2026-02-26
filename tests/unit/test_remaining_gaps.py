"""Gap-filling tests for graph_relations, postgresql_util,
prometheus_write, and dgraph_util.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Annotated, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel as PydanticBaseModel
from pydantic import SecretStr

from ami.core.exceptions import StorageError
from ami.core.graph_relations import (
    GraphEdge,
    GraphRelation,
    GraphSchemaAnalyzer,
    RelationalField,
)
from ami.implementations.graph.dgraph_util import (
    _build_operator_filter,
    _collect_indexed_fields,
    _create_model_instance,
    _resolve_generic_type,
    _resolve_union_type,
    commit_with_timeout,
    ensure_schema,
    json_encoder,
    mutate_with_timeout,
    query_with_timeout,
)
from ami.implementations.sql.postgresql_util import (
    create_indexes_for_table,
)
from ami.implementations.timeseries.prometheus_write import (
    _push_to_gateway,
)

_P = 9080
_OK = 200
_BAD = 500
_PW = "ami.implementations.timeseries.prometheus_write.request_with_retry"


class _O:
    """Minimal namespace."""


class _SM(PydanticBaseModel):
    uid: str | None = None
    name: str = ""
    count: int = 0


class _Co(Enum):
    RED = "red"


def _ec(
    fn: str = "i",
    ls: bool = True,
    eg: bool = False,
) -> dict:
    return {
        "field_name": fn,
        "edge_name": "e",
        "is_list": ls,
        "target_type": "str",
        "eager_load": eg,
    }


def _resp(status: int, **kw: Any) -> AsyncMock:
    r = AsyncMock()
    r.status = status
    r.__aenter__ = AsyncMock(return_value=r)
    r.__aexit__ = AsyncMock(return_value=False)
    if "body" in kw:
        r.read = AsyncMock(return_value=kw["body"])
        r.raise_for_status = MagicMock()
    if "txt" in kw:
        r.text = AsyncMock(return_value=kw["txt"])
    return r


class _FC:
    def __init__(
        self,
        host: str = "h",
        options: Any = None,
    ) -> None:
        self.host = host
        self.options = options


class _FD:
    def __init__(
        self,
        s: Any = None,
        config: Any = None,
    ) -> None:
        self.session = s
        self.config = config
        self.base_url = "http://h:9090"


# -- 1. graph_relations gaps --


class TestGrGaps:
    """Cover graph_relations edge cases."""

    def test_edge_none(self) -> None:
        assert GraphEdge(properties=None).properties == {}

    def test_schema_private_skip(self) -> None:
        class _M(PydanticBaseModel):
            model_config = {
                "arbitrary_types_allowed": True,
            }
            _h: str = "x"
            name: str = ""

        props = GraphSchemaAnalyzer.analyze_model(_M)
        assert "_h" not in props["properties"]

    def test_schema_annot_non_rel(self) -> None:
        class _A(PydanticBaseModel):
            n: Annotated[str, "m"] = ""

        s = GraphSchemaAnalyzer.analyze_model(_A)
        assert "n" in s["properties"]
        assert "n" not in s["edges"]

    def test_target_override(self) -> None:
        class _T(PydanticBaseModel):
            lnk: Annotated[
                str | None,
                GraphRelation(edge_name="e", target_type="CT"),
            ] = None

        edges = GraphSchemaAnalyzer.analyze_model(_T)
        assert edges["edges"]["lnk"]["target_type"] == "CT"

    def test_type_to_string_name(self) -> None:
        o = MagicMock()
        o.__name__ = "MT"
        del o.__class__
        r = GraphSchemaAnalyzer._type_to_string(o)
        assert r == "MT"

    def test_set_name(self) -> None:
        rf = RelationalField(_ec())
        rf.__set_name__(_O, "i")
        assert rf.name == "i"
        assert rf.model_cls is _O

    def test_eager_err(self) -> None:
        with pytest.raises(
            RuntimeError,
            match="load_related",
        ):
            RelationalField(
                _ec(eg=True),
            ).__get__(_O(), type(_O))

    def test_set_clears_cache(self) -> None:
        rf = RelationalField(_ec())
        o = _O()
        setattr(o, rf._cache_attr, ["old"])
        rf.__set__(o, ["a"])
        assert not hasattr(o, rf._cache_attr)

    @pytest.mark.asyncio
    async def test_lr_cached(self) -> None:
        rf = RelationalField(_ec())
        o = _O()
        setattr(o, rf._cache_attr, ["c"])
        r = await rf.load_related(o, AsyncMock())
        assert r == ["c"]

    @pytest.mark.asyncio
    async def test_lr_empty_list(self) -> None:
        r = await RelationalField(_ec()).load_related(
            _O(),
            AsyncMock(),
        )
        assert r == []

    @pytest.mark.asyncio
    async def test_lr_empty_scalar(self) -> None:
        r = await RelationalField(
            _ec("p", ls=False),
        ).load_related(_O(), AsyncMock())
        assert r is None

    @pytest.mark.asyncio
    async def test_lr_list_ok(self) -> None:
        rf = RelationalField(_ec())
        o = _O()
        setattr(o, rf._ids_attr, ["1", "2"])
        d = AsyncMock()
        d.find_by_id = AsyncMock(
            side_effect=[MagicMock(), MagicMock()],
        )
        r = await rf.load_related(o, d)
        expected_count = 2
        assert len(r) == expected_count

    @pytest.mark.asyncio
    async def test_lr_list_miss(self) -> None:
        rf = RelationalField(_ec())
        o = _O()
        setattr(o, rf._ids_attr, ["1", "2"])
        d = AsyncMock()
        d.find_by_id = AsyncMock(
            side_effect=[MagicMock(), None],
        )
        with pytest.raises(ValueError, match="Missing"):
            await rf.load_related(o, d)

    @pytest.mark.asyncio
    async def test_lr_scalar_ok(self) -> None:
        rf = RelationalField(_ec("p", ls=False))
        o = _O()
        setattr(o, rf._ids_attr, "x")
        d = AsyncMock()
        d.find_by_id = AsyncMock(
            return_value=MagicMock(),
        )
        assert await rf.load_related(o, d) is not None

    @pytest.mark.asyncio
    async def test_lr_scalar_miss(self) -> None:
        rf = RelationalField(_ec("p", ls=False))
        o = _O()
        setattr(o, rf._ids_attr, "x")
        d = AsyncMock()
        d.find_by_id = AsyncMock(return_value=None)
        with pytest.raises(ValueError, match="Missing"):
            await rf.load_related(o, d)


# -- 2. postgresql_util gaps --


class TestCreateIndexes:
    """Cover create_indexes_for_table branches."""

    @pytest.mark.asyncio
    async def test_gin(self) -> None:
        c = AsyncMock()
        c.execute = AsyncMock()
        await create_indexes_for_table(
            c,
            "t",
            {"j": ["a"]},
        )
        sql = c.execute.call_args_list[0][0][0].lower()
        assert "gin" in sql

    @pytest.mark.asyncio
    async def test_btree(self) -> None:
        c = AsyncMock()
        c.execute = AsyncMock()
        await create_indexes_for_table(
            c,
            "t",
            {"ts": datetime.now(tz=UTC)},
        )
        sql = c.execute.call_args_list[0][0][0]
        assert "ts" in sql

    @pytest.mark.asyncio
    async def test_gin_err(self) -> None:
        c = AsyncMock()
        c.execute = AsyncMock(
            side_effect=RuntimeError("x"),
        )
        with pytest.raises(StorageError, match="GIN"):
            await create_indexes_for_table(
                c,
                "t",
                {"m": {"k": 1}},
            )

    @pytest.mark.asyncio
    async def test_btree_err(self) -> None:
        c = AsyncMock()
        c.execute = AsyncMock(
            side_effect=RuntimeError("x"),
        )
        with pytest.raises(StorageError, match="B-tree"):
            await create_indexes_for_table(
                c,
                "t",
                {"d": datetime.now(tz=UTC)},
            )

    @pytest.mark.asyncio
    async def test_skip_invalid(self) -> None:
        c = AsyncMock()
        c.execute = AsyncMock()
        await create_indexes_for_table(
            c,
            "t",
            {"bad c": {"k": 1}},
        )
        c.execute.assert_not_awaited()


# -- 3. prometheus_write gaps --


class TestPushToGateway:
    """Cover _push_to_gateway branches."""

    @pytest.mark.asyncio
    async def test_no_session(self) -> None:
        with pytest.raises(
            StorageError,
            match="not connected",
        ):
            await _push_to_gateway(
                _FD(),
                [{"metric_name": "x", "value": 1}],
            )

    @pytest.mark.asyncio
    async def test_ok(self) -> None:
        d = _FD(
            AsyncMock(),
            _FC(options={"pushgateway_job": "j"}),
        )
        metrics = [
            {"metric_name": "u", "labels": {}, "value": 1},
        ]
        with patch(_PW, return_value=_resp(_OK)):
            r = await _push_to_gateway(d, metrics)
        assert r == 1

    @pytest.mark.asyncio
    async def test_bad_status(self) -> None:
        d = _FD(AsyncMock(), _FC(options={}))
        metrics = [
            {"metric_name": "x", "labels": {}, "value": 0},
        ]
        with (
            patch(_PW, return_value=_resp(_BAD, txt="e")),
            pytest.raises(StorageError, match="Pushgateway"),
        ):
            await _push_to_gateway(d, metrics)

    @pytest.mark.asyncio
    async def test_exception(self) -> None:
        d = _FD(AsyncMock(), _FC(options={}))
        metrics = [
            {"metric_name": "x", "labels": {}, "value": 0},
        ]
        with (
            patch(_PW, side_effect=OSError("no")),
            pytest.raises(StorageError, match="Pushgateway"),
        ):
            await _push_to_gateway(d, metrics)


# -- 4. dgraph_util gaps --


class TestDgraphUtilGaps:
    """Cover dgraph_util edge cases."""

    @pytest.mark.asyncio
    async def test_query_with_vars(self) -> None:
        t = MagicMock()
        t.query = MagicMock(return_value="{}")
        r = await query_with_timeout(
            t,
            "q",
            variables={"$x": "1"},
        )
        assert r == "{}"

    @pytest.mark.asyncio
    async def test_query_no_vars(self) -> None:
        t = MagicMock()
        t.query = MagicMock(return_value="{}")
        assert await query_with_timeout(t, "q") == "{}"

    @pytest.mark.asyncio
    async def test_mutate_ok(self) -> None:
        t = MagicMock()
        t.mutate = MagicMock(return_value="ok")
        r = await mutate_with_timeout(t, MagicMock())
        assert r == "ok"

    @pytest.mark.asyncio
    async def test_commit_ok(self) -> None:
        t = MagicMock()
        t.commit = MagicMock(return_value=None)
        await commit_with_timeout(t)

    def test_union_multi(self) -> None:
        with pytest.raises(
            StorageError,
            match="multiple",
        ):
            _resolve_union_type(str | int)

    def test_generic_dict(self) -> None:
        r = _resolve_generic_type(dict[str, int])
        assert r == "string"

    def test_json_encoder(self) -> None:
        dt = datetime(2025, 1, 15, 12, 0, 0)
        assert json_encoder(dt) == dt.isoformat()
        assert json_encoder(SecretStr("h")) == "h"
        assert json_encoder(_Co.RED) == "red"
        with pytest.raises(TypeError, match="not JSON"):
            json_encoder(object())

    def test_collect_indexed(self) -> None:
        m = MagicMock()
        m.indexes = [
            {"field": "t", "type": "text"},
            {"field": "s", "type": "hash"},
        ]
        r = _collect_indexed_fields(m)
        assert r["t"] == "fulltext"
        assert r["s"] == "exact"
        none_r = _collect_indexed_fields(None)
        assert none_r == {"app_uid": "exact"}

    def test_create_model_bad(self) -> None:
        with pytest.raises(
            StorageError,
            match="Could not",
        ):
            _create_model_instance(
                "X",
                {"count": "not_a_number"},
                _SM,
            )

    def test_ensure_schema_errs(self) -> None:
        with pytest.raises(
            StorageError,
            match="Not connected",
        ):
            ensure_schema(None, _SM, None, "c")
        c = MagicMock()
        c.alter = MagicMock(
            side_effect=RuntimeError("x"),
        )
        with pytest.raises(StorageError, match="Schema"):
            ensure_schema(c, _SM, None, "c")

    def test_unknown_operator(self) -> None:
        r = _build_operator_filter("$z", [], "c")
        assert r == ""
