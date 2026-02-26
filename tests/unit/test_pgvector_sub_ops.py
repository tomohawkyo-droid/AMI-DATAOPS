"""Tests for pgvector sub-operation modules."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from enum import Enum
from typing import ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

from ami.core.exceptions import StorageConnectionError, StorageError
from ami.core.storage_types import StorageType
from ami.implementations.vec import (
    pgvector_create,
    pgvector_delete,
    pgvector_read,
    pgvector_update,
    pgvector_vector,
)
from ami.implementations.vec.pgvector_dao import PgVectorDAO
from ami.implementations.vec.pgvector_util import (
    build_where_clause,
    create_indexes_for_table,
    create_model_indexes,
    deserialize_row,
    get_safe_table_name,
    infer_column_type,
    is_valid_identifier,
    serialize_value,
)
from ami.implementations.vec.pgvector_vector import (
    _metric_operator,
    _parse_embedding,
    _to_vector_literal,
)
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

_P = "ami.implementations.vec.pgvector_dao.get_embedding_service"
_DIM = 384
_N42 = 42
_N3 = 3
_N2 = 2
_N7 = 7
_VA = 0.1
_VB = 0.2
_DC = 0.15
_SC = 0.85
_DL = 1.25
_DI = -0.9
_SI = 0.9


class _M(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(path="t")
    name: str = "d"


def _svc() -> MagicMock:
    s = MagicMock()
    s.embedding_dim = _DIM
    s.generate_embedding = AsyncMock(return_value=[_VA] * _DIM)
    return s


def _dao(pool: bool = True) -> PgVectorDAO:
    c = StorageConfig(
        storage_type=StorageType.VECTOR,
        host="h",
        port=5432,
        database="db",
        username="u",
        password="p",
    )
    with patch(_P, return_value=_svc()):
        d = PgVectorDAO(model_cls=_M, config=c)
    if pool:
        p = MagicMock()
        cn, ctx = AsyncMock(), MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=cn)
        ctx.__aexit__ = AsyncMock(return_value=False)
        p.acquire = MagicMock(return_value=ctx)
        p.close = AsyncMock()
        d.pool = p
    return d


def _cn(d: PgVectorDAO) -> AsyncMock:
    return d.pool.acquire.return_value.__aenter__.return_value


class TestUtilPure:
    """Pure utility function tests."""

    def test_identifiers(self) -> None:
        assert is_valid_identifier("my_t2") is True
        assert is_valid_identifier("_x") is True
        for bad in ("2t", "a-b", "", "a b"):
            assert is_valid_identifier(bad) is False
        assert get_safe_table_name("items") == "items"
        with pytest.raises(StorageError):
            get_safe_table_name("drop;--")

    def test_infer_types(self) -> None:
        assert infer_column_type(None) == "TEXT"
        assert infer_column_type(True) == "BOOLEAN"
        assert infer_column_type(99) == "BIGINT"
        assert infer_column_type(1.5) == "DOUBLE PRECISION"
        assert "TIMESTAMP" in infer_column_type(datetime.now(UTC))
        assert infer_column_type({"k": 1}) == "JSONB"
        assert infer_column_type([1]) == "JSONB"
        assert infer_column_type("hi") == "TEXT"
        r = infer_column_type("2024-01-15T10:30:00Z", "created_at")
        assert "TIMESTAMP" in r
        assert infer_column_type("nope", "created_at") == "TEXT"

    def test_serialize(self) -> None:
        assert serialize_value({"a": 1}) == json.dumps({"a": 1})
        assert serialize_value([1]) == json.dumps([1])
        now = datetime.now(UTC)
        assert serialize_value(now) is now
        assert serialize_value("s") == "s"
        assert serialize_value(_N42) == _N42

        class C(Enum):
            R = "red"

        assert serialize_value(C.R) == "red"
        with pytest.raises(TypeError):
            serialize_value(SecretStr("x"))

    def test_deserialize(self) -> None:
        r = deserialize_row({"d": '{"k":"v"}', "n": "plain"})
        assert r["d"] == {"k": "v"}
        assert r["n"] == "plain"
        assert deserialize_row({"c": _N42})["c"] == _N42

    def test_where(self) -> None:
        w, p = build_where_clause({"name": "a"})
        assert "name = $1" in w
        assert p == ["a"]
        _, p2 = build_where_clause({"a": "x", "b": 1})
        assert len(p2) == _N2
        w3, p3 = build_where_clause({"x": None})
        assert "IS NULL" in w3
        assert len(p3) == 0
        w4, p4 = build_where_clause({"drop;": "x"})
        assert w4 == ""
        assert p4 == []

    def test_vector_helpers(self) -> None:
        assert _metric_operator("cosine") == "<=>"
        assert _metric_operator("l2") == "<->"
        assert _metric_operator("ip") == "<#>"
        with pytest.raises(ValueError, match="Unsupported"):
            _metric_operator("bad")
        assert _to_vector_literal([_VA, _VB]) == "[0.1,0.2]"
        assert _to_vector_literal([]) == "[]"
        assert _parse_embedding(None) is None
        assert _parse_embedding([_VA]) == [_VA]
        assert _parse_embedding((_VA,)) == [_VA]
        r = _parse_embedding("[0.1,0.2,0.3]")
        assert r is not None
        assert len(r) == _N3
        assert _parse_embedding("nope") is None
        assert _parse_embedding(999) is None


class TestUtilIndexes:
    """Index creation utility tests."""

    @pytest.mark.asyncio
    async def test_table_indexes(self) -> None:
        c = AsyncMock()
        await create_indexes_for_table(c, "t", {"m": {"k": 1}})
        assert "gin" in c.execute.call_args_list[0][0][0].lower()
        c2 = AsyncMock()
        await create_indexes_for_table(c2, "t", {"ts": datetime.now(UTC)})
        assert "btree" in c2.execute.call_args_list[0][0][0].lower()
        c3 = AsyncMock()
        await create_indexes_for_table(c3, "t", {"n": "hi"})
        c3.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_gin_err(self) -> None:
        c = AsyncMock()
        c.execute.side_effect = RuntimeError("f")
        with pytest.raises(StorageError):
            await create_indexes_for_table(c, "t", {"x": [1]})

    @pytest.mark.asyncio
    async def test_model_idx(self) -> None:
        c, m, mt = AsyncMock(), MagicMock(), MagicMock()
        mt.indexes = [{"columns": ["n"], "unique": False, "type": "btree"}]
        m.get_metadata.return_value = mt
        await create_model_indexes(c, "t", m)
        c.execute.assert_awaited_once()
        c2, m2, mt2 = AsyncMock(), MagicMock(), MagicMock()
        mt2.indexes = [{"columns": ["drop;--"], "unique": False}]
        m2.get_metadata.return_value = mt2
        await create_model_indexes(c2, "t", m2)
        c2.execute.assert_not_awaited()
        c3, m3, mt3 = AsyncMock(), MagicMock(), MagicMock()
        mt3.indexes = [{"columns": ["e"], "unique": True, "type": "btree"}]
        m3.get_metadata.return_value = mt3
        await create_model_indexes(c3, "t", m3)
        assert "UNIQUE" in c3.execute.call_args[0][0]


class TestCreateOps:
    """pgvector_create tests."""

    @pytest.mark.asyncio
    async def test_create_single(self) -> None:
        d = _dao()
        d._generate_embedding_for_record = AsyncMock(return_value=None)
        assert await pgvector_create.create(d, {"uid": "a", "name": "t"}) == "a"
        _cn(d).execute.assert_awaited()
        d2 = _dao()
        d2._generate_embedding_for_record = AsyncMock(return_value=None)
        assert len(await pgvector_create.create(d2, {"name": "x"})) > 0
        d3 = _dao()
        d3._generate_embedding_for_record = AsyncMock(return_value=[_VA])
        assert await pgvector_create.create(d3, {"uid": "e", "name": "e"}) == "e"

    @pytest.mark.asyncio
    async def test_no_pool(self) -> None:
        d = _dao(pool=False)
        d.pool, d._ensured_tables = None, {"t"}
        d._generate_embedding_for_record = AsyncMock(return_value=None)
        with pytest.raises(StorageConnectionError):
            await pgvector_create.create(d, {"name": "f"})

    @pytest.mark.asyncio
    async def test_bulk(self) -> None:
        assert await pgvector_create.bulk_create(_dao(), []) == []
        d = _dao()
        d._generate_embedding_for_record = AsyncMock(return_value=None)
        r = await pgvector_create.bulk_create(
            d,
            [{"name": f"i{i}"} for i in range(_N3)],
        )
        assert len(r) == _N3


class TestReadOps:
    """pgvector_read tests."""

    @pytest.mark.asyncio
    async def test_find_by_id(self) -> None:
        d = _dao()
        _cn(d).fetchrow = AsyncMock(return_value={"uid": "r", "name": "a"})
        d.model_cls.from_storage_dict = AsyncMock(return_value=_M(uid="r"))
        assert await pgvector_read.find_by_id(d, "r") is not None
        d2 = _dao()
        _cn(d2).fetchrow = AsyncMock(return_value=None)
        assert await pgvector_read.find_by_id(d2, "x") is None

    @pytest.mark.asyncio
    async def test_find_by_id_no_pool(self) -> None:
        d = _dao(pool=False)
        d.pool = None
        with pytest.raises(StorageConnectionError):
            await pgvector_read.find_by_id(d, "x")

    @pytest.mark.asyncio
    async def test_find_one(self) -> None:
        d = _dao()
        _cn(d).fetchrow = AsyncMock(return_value={"uid": "f", "name": "a"})
        d.model_cls.from_storage_dict = AsyncMock(return_value=_M())
        assert await pgvector_read.find_one(d, {}) is not None
        d2 = _dao()
        _cn(d2).fetchrow = AsyncMock(return_value=None)
        assert await pgvector_read.find_one(d2, {"n": "x"}) is None

    @pytest.mark.asyncio
    async def test_find(self) -> None:
        d = _dao()
        _cn(d).fetch = AsyncMock(return_value=[{"uid": "a"}, {"uid": "b"}])
        d.model_cls.from_storage_dict = AsyncMock(
            side_effect=[_M(uid="a"), _M(uid="b")],
        )
        assert len(await pgvector_read.find(d, {}, limit=10, skip=5)) == _N2
        d2 = _dao()
        _cn(d2).fetch = AsyncMock(return_value=[])
        assert await pgvector_read.find(d2, {"n": "z"}) == []

    @pytest.mark.asyncio
    async def test_count_exists(self) -> None:
        d = _dao()
        _cn(d).fetchval = AsyncMock(return_value=_N42)
        assert await pgvector_read.count(d, {"a": True}) == _N42
        d2 = _dao()
        _cn(d2).fetchval = AsyncMock(return_value=None)
        assert await pgvector_read.count(d2, {}) == 0
        d3 = _dao()
        _cn(d3).fetchval = AsyncMock(return_value=1)
        assert await pgvector_read.exists(d3, "e") is True
        d4 = _dao()
        _cn(d4).fetchval = AsyncMock(return_value=None)
        assert await pgvector_read.exists(d4, "e") is False

    @pytest.mark.asyncio
    async def test_raw_read(self) -> None:
        d = _dao()
        _cn(d).fetch = AsyncMock(return_value=[{"uid": "r"}])
        assert len(await pgvector_read.raw_read_query(d, "S", {"k": "v"})) == 1
        d2 = _dao()
        _cn(d2).fetch = AsyncMock(return_value=[])
        assert await pgvector_read.raw_read_query(d2, "S", None) == []

    @pytest.mark.asyncio
    async def test_schema_introspection(self) -> None:
        d = _dao()
        _cn(d).fetch = AsyncMock(
            return_value=[{"datname": "a"}, {"datname": "b"}],
        )
        assert await pgvector_read.list_databases(d) == ["a", "b"]
        d2 = _dao()
        _cn(d2).fetch = AsyncMock(return_value=[{"schema_name": "public"}])
        assert await pgvector_read.list_schemas(d2) == ["public"]
        d3 = _dao()
        _cn(d3).fetch = AsyncMock(return_value=[{"table_name": "u"}])
        assert await pgvector_read.list_models(d3) == ["u"]

    @pytest.mark.asyncio
    async def test_model_info(self) -> None:
        d = _dao()
        _cn(d).fetchrow = AsyncMock(
            return_value={"table_name": "u", "table_type": "T"},
        )
        assert (await pgvector_read.get_model_info(d, "u"))["name"] == "u"
        d2 = _dao()
        _cn(d2).fetchrow = AsyncMock(return_value=None)
        assert "error" in await pgvector_read.get_model_info(d2, "x")
        r = await pgvector_read.get_model_info(_dao(), "x;")
        assert r == {"error": "Invalid table name"}

    @pytest.mark.asyncio
    async def test_model_fields_indexes(self) -> None:
        d = _dao()
        col = {
            "column_name": "uid",
            "data_type": "text",
            "is_nullable": "NO",
            "column_default": None,
        }
        _cn(d).fetch = AsyncMock(return_value=[col])
        r = await pgvector_read.get_model_fields(d, "u")
        assert r[0]["name"] == "uid"
        assert r[0]["nullable"] is False
        assert await pgvector_read.get_model_fields(_dao(), "x;") == []
        d2 = _dao()
        _cn(d2).fetch = AsyncMock(
            return_value=[{"indexname": "i", "indexdef": "D"}],
        )
        assert (await pgvector_read.get_model_indexes(d2, "u"))[0]["name"] == "i"
        assert await pgvector_read.get_model_indexes(_dao(), "x;") == []


class TestUpdateOps:
    """pgvector_update tests."""

    @pytest.mark.asyncio
    async def test_update_paths(self) -> None:
        d = _dao()
        d._generate_embedding_for_record = AsyncMock(return_value=None)
        await pgvector_update.update(d, "u", {"name": "n"})
        _cn(d).execute.assert_awaited()
        d2 = _dao()
        await pgvector_update.update(d2, "u", {})
        _cn(d2).execute.assert_not_awaited()
        d3 = _dao()
        d3._generate_embedding_for_record = AsyncMock(return_value=None)
        await pgvector_update.update(d3, "u", {"uid": "ig", "name": "n"})
        sql = _cn(d3).execute.call_args[0][0]
        set_part = sql.split("SET")[1].split("WHERE")[0]
        assert "uid =" not in set_part

    @pytest.mark.asyncio
    async def test_no_pool(self) -> None:
        d = _dao(pool=False)
        d.pool = None
        d._generate_embedding_for_record = AsyncMock(return_value=None)
        with pytest.raises(StorageConnectionError):
            await pgvector_update.update(d, "u", {"n": "f"})

    @pytest.mark.asyncio
    async def test_bulk_skip_no_uid(self) -> None:
        d = _dao()
        d._generate_embedding_for_record = AsyncMock(return_value=None)
        await pgvector_update.bulk_update(d, [{"name": "x"}])
        _cn(d).execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_raw_write(self) -> None:
        d = _dao()
        _cn(d).execute = AsyncMock(return_value=f"UPDATE {_N7}")
        assert await pgvector_update.raw_write_query(d, "U", {"n": "x"}) == _N7
        d2 = _dao()
        _cn(d2).execute = AsyncMock(return_value=None)
        assert await pgvector_update.raw_write_query(d2, "T", None) == 0


class TestDeleteOps:
    """pgvector_delete tests."""

    @pytest.mark.asyncio
    async def test_single(self) -> None:
        d = _dao()
        _cn(d).execute = AsyncMock(return_value="DELETE 1")
        assert await pgvector_delete.delete(d, "d") is True
        d2 = _dao()
        _cn(d2).execute = AsyncMock(return_value="DELETE 0")
        assert await pgvector_delete.delete(d2, "m") is False

    @pytest.mark.asyncio
    async def test_no_pool(self) -> None:
        d = _dao(pool=False)
        d.pool = None
        with pytest.raises(StorageConnectionError):
            await pgvector_delete.delete(d, "x")

    @pytest.mark.asyncio
    async def test_bulk(self) -> None:
        assert await pgvector_delete.bulk_delete(_dao(), []) == 0
        d = _dao()
        _cn(d).execute = AsyncMock(return_value=f"DELETE {_N2}")
        assert await pgvector_delete.bulk_delete(d, ["a", "b"]) == _N2


class TestVectorOps:
    """pgvector_vector similarity search tests."""

    @pytest.mark.asyncio
    async def test_sim_delegates(self) -> None:
        d = _dao()
        d._get_query_embedding = AsyncMock(return_value=[_VA] * _DIM)
        with patch(
            "ami.implementations.vec.pgvector_vector.similarity_search_by_vector",
            new_callable=AsyncMock,
            return_value=[{"data": {}, "score": _SC}],
        ) as m:
            r = await pgvector_vector.similarity_search(d, "q")
            m.assert_awaited_once()
            assert len(r) == 1

    @pytest.mark.asyncio
    async def test_metrics(self) -> None:
        for metric, dist, expected in (
            ("cosine", _DC, _SC),
            ("l2", _DL, _DL),
            ("ip", _DI, _SI),
        ):
            d = _dao()
            _cn(d).fetch = AsyncMock(
                return_value=[
                    {"uid": "v", "distance": dist, "embedding": None},
                ]
            )
            r = await pgvector_vector.similarity_search_by_vector(
                d,
                [_VA],
                metric=metric,
            )
            assert r[0]["score"] == pytest.approx(expected)

    @pytest.mark.asyncio
    async def test_filters_empty(self) -> None:
        d = _dao()
        _cn(d).fetch = AsyncMock(return_value=[])
        r = await pgvector_vector.similarity_search_by_vector(
            d,
            [_VA],
            filters={"a": True},
        )
        assert r == []

    @pytest.mark.asyncio
    async def test_none_distance(self) -> None:
        d = _dao()
        row = {"uid": "v", "distance": None, "embedding": None}
        _cn(d).fetch = AsyncMock(return_value=[row])
        r = await pgvector_vector.similarity_search_by_vector(
            d,
            [_VA],
            metric="cosine",
        )
        assert r[0]["score"] == 0.0
        assert r[0]["distance"] is None

    @pytest.mark.asyncio
    async def test_no_pool(self) -> None:
        d = _dao(pool=False)
        d.pool = None
        with pytest.raises(StorageConnectionError):
            await pgvector_vector.similarity_search_by_vector(d, [_VA])

    @pytest.mark.asyncio
    async def test_fetch_embedding(self) -> None:
        d = _dao()
        _cn(d).fetchrow = AsyncMock(return_value={"embedding": [_VA, _VB]})
        assert await pgvector_vector.fetch_embedding(d, "i") == [_VA, _VB]
        d2 = _dao()
        _cn(d2).fetchrow = AsyncMock(return_value=None)
        assert await pgvector_vector.fetch_embedding(d2, "m") is None
        d3 = _dao()
        _cn(d3).fetchrow = AsyncMock(return_value={"embedding": None})
        assert await pgvector_vector.fetch_embedding(d3, "n") is None
