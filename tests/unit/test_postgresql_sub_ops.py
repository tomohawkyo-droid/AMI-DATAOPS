"""Tests for PostgreSQL sub-operation modules."""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock

import pytest
from asyncpg.exceptions import UndefinedTableError

from ami.core.exceptions import (
    StorageConnectionError,
    StorageError,
)
from ami.implementations.sql.postgresql_create import (
    _add_missing_schema_columns,
    _ensure_metadata_indexes,
    convert_datetime_strings_for_timestamps,
    create,
    create_table_from_schema,
    ensure_table_exists,
)
from ami.implementations.sql.postgresql_delete import delete
from ami.implementations.sql.postgresql_read import (
    count,
    get_model_schema,
    list_all,
    query,
    read,
)
from ami.implementations.sql.postgresql_update import (
    _build_set_clause,
    update,
)
from ami.models.base_model import ModelMetadata, StorageModel

_UID_A = "uid-aaa-111"
_UID_B = "uid-bbb-222"
_TBL = "test_items"
_N42 = 42
_TWO = 2
_THREE = 3


class _M(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path=_TBL,
        indexes=[
            {"field": "name", "type": "btree"},
            {"field": "email", "unique": True},
            {"field": "tag", "type": "hash"},
        ],
    )
    name: str = ""
    value: int = 0


def _dao(tc: bool = True, p: Any = None) -> MagicMock:
    d = MagicMock()
    d.collection_name, d.model_cls = _TBL, _M
    d._table_created, d._omit_columns = tc, set()
    d.pool, d.connect = p, AsyncMock()
    return d


def _pc() -> tuple[MagicMock, AsyncMock]:
    cn = AsyncMock()
    cx = AsyncMock()
    cx.__aenter__ = AsyncMock(return_value=cn)
    cx.__aexit__ = AsyncMock(return_value=False)
    pl = MagicMock()
    pl.acquire.return_value = cx
    pl.close = AsyncMock()
    return pl, cn


def _cr(*ns: str) -> list[dict[str, str]]:
    return [{"column_name": n} for n in ns]


class TestCreate:
    async def test_returns_id(self) -> None:
        p, c = _pc()
        d = _dao(p=p)
        c.fetch = AsyncMock(return_value=_cr("id", "name"))
        c.fetchval = AsyncMock(return_value=_UID_A)
        assert await create(d, {"name": "a"}) == _UID_A

    async def test_uid_precedence(self) -> None:
        p, c = _pc()
        d = _dao(p=p)
        c.fetch = AsyncMock(return_value=_cr("id"))
        c.fetchval = AsyncMock(return_value=_UID_A)
        assert await create(d, {"uid": _UID_B}) == _UID_B

    async def test_explicit_id(self) -> None:
        p, c = _pc()
        d = _dao(p=p)
        c.fetch = AsyncMock(return_value=_cr("id"))
        c.fetchval = AsyncMock(return_value="eid")
        assert await create(d, {"id": "eid"}) == "eid"

    async def test_data_col_default(self) -> None:
        p, c = _pc()
        d = _dao(p=p)
        c.fetch = AsyncMock(return_value=_cr("id", "data"))
        c.fetchval = AsyncMock(return_value=_UID_A)
        await create(d, {"name": "t"})
        assert "data" in c.fetchval.call_args[0][0]

    async def test_skips_bad_fields(self) -> None:
        p, c = _pc()
        d = _dao(p=p)
        c.fetch = AsyncMock(return_value=_cr("id"))
        c.fetchval = AsyncMock(return_value=_UID_A)
        assert await create(d, {"n": "ok", "1x": "s"}) == _UID_A

    async def test_error(self) -> None:
        p, c = _pc()
        d = _dao(p=p)
        c.fetch = AsyncMock(return_value=_cr("id"))
        c.fetchval = AsyncMock(side_effect=RuntimeError("x"))
        with pytest.raises(StorageError, match="Failed to create"):
            await create(d, {"n": "f"})


class TestEnsureTable:
    async def test_skip_created(self) -> None:
        p, _ = _pc()
        d = _dao(tc=True, p=p)
        await ensure_table_exists(d)
        p.acquire.assert_not_called()

    async def test_create_new(self) -> None:
        p, c = _pc()
        d = _dao(tc=False, p=p)
        c.fetchval = AsyncMock(return_value=False)
        c.execute = AsyncMock()
        c.fetch = AsyncMock(return_value=[])
        await ensure_table_exists(d)
        assert d._table_created is True

    async def test_exists(self) -> None:
        p, c = _pc()
        d = _dao(tc=False, p=p)
        c.fetchval = AsyncMock(return_value=True)
        c.execute = AsyncMock()
        c.fetch = AsyncMock(return_value=[])
        await ensure_table_exists(d)
        assert d._table_created is True

    async def test_no_model(self) -> None:
        p, _ = _pc()
        d = _dao(tc=False, p=p)
        d.model_cls = MagicMock(model_fields={}, __name__="Empty")
        with pytest.raises(StorageError, match="No model fields"):
            await ensure_table_exists(d)


class TestCreateTableSchema:
    async def test_columns(self) -> None:
        c = AsyncMock()
        cols = {"name": "TEXT", "value": "BIGINT"}
        await create_table_from_schema(c, _TBL, cols)
        sql = c.execute.call_args[0][0]
        assert "name TEXT" in sql
        assert "id TEXT PRIMARY KEY" in sql

    async def test_error(self) -> None:
        c = AsyncMock()
        c.execute = AsyncMock(side_effect=RuntimeError("x"))
        with pytest.raises(StorageError, match="Failed to create"):
            await create_table_from_schema(c, _TBL, {"n": "T"})


class TestAddColumns:
    async def test_adds_missing(self) -> None:
        c = AsyncMock()
        c.fetch = AsyncMock(return_value=_cr("id"))
        c.execute = AsyncMock()
        cols = {"n": "TEXT", "v": "BIGINT"}
        await _add_missing_schema_columns(c, _TBL, cols)
        assert c.execute.await_count == _TWO

    async def test_skips_existing_and_id(self) -> None:
        c = AsyncMock()
        c.fetch = AsyncMock(return_value=_cr("id", "n"))
        c.execute = AsyncMock()
        cols = {"id": "T", "n": "T", "v": "INT"}
        await _add_missing_schema_columns(c, _TBL, cols)
        assert c.execute.await_count == 1

    async def test_empty(self) -> None:
        c = AsyncMock()
        await _add_missing_schema_columns(c, _TBL, {})
        c.fetch.assert_not_awaited()

    async def test_error(self) -> None:
        c = AsyncMock()
        c.fetch = AsyncMock(return_value=_cr("id"))
        c.execute = AsyncMock(side_effect=RuntimeError("x"))
        with pytest.raises(StorageError, match="Failed to add"):
            await _add_missing_schema_columns(c, _TBL, {"z": "T"})


class TestIndexes:
    async def test_all_types(self) -> None:
        c = AsyncMock()
        await _ensure_metadata_indexes(c, _TBL, _dao())
        sqls = [x[0][0] for x in c.execute.call_args_list]
        assert any("UNIQUE" in s for s in sqls)
        assert any("hash" in s for s in sqls)
        assert c.execute.await_count == _THREE

    async def test_no_model(self) -> None:
        c = AsyncMock()
        d = _dao()
        d.model_cls = None
        await _ensure_metadata_indexes(c, _TBL, d)
        c.execute.assert_not_awaited()

    async def test_invalid_field(self) -> None:
        c, d = AsyncMock(), _dao()

        class _B(StorageModel):
            _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
                path="b",
                indexes=[{"field": "1x"}],
            )
            name: str = ""

        d.model_cls = _B
        await _ensure_metadata_indexes(c, _TBL, d)
        c.execute.assert_not_awaited()

    async def test_error(self) -> None:
        c = AsyncMock()
        c.execute = AsyncMock(side_effect=RuntimeError("x"))
        d = _dao()

        class _H(StorageModel):
            _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
                path="h",
                indexes=[{"field": "t", "type": "hash"}],
            )
            t: str = ""

        d.model_cls = _H
        with pytest.raises(StorageError, match="Failed to create"):
            await _ensure_metadata_indexes(c, _TBL, d)


class TestDatetimeConvertError:
    def test_bad_iso(self) -> None:
        bad = {"created_at": "2024-13-99T00:00:00+00:00"}
        with pytest.raises(StorageError, match="Failed to parse"):
            convert_datetime_strings_for_timestamps(bad)


class TestRead:
    async def test_found(self) -> None:
        p, c = _pc()
        d = _dao(p=p)
        c.fetchrow = AsyncMock(return_value={"id": _UID_A})
        r = await read(d, _UID_A)
        assert r is not None
        assert r["uid"] == _UID_A

    async def test_not_found(self) -> None:
        p, c = _pc()
        c.fetchrow = AsyncMock(return_value=None)
        assert await read(_dao(p=p), "x") is None

    async def test_keeps_uid(self) -> None:
        p, c = _pc()
        c.fetchrow = AsyncMock(
            return_value={"id": _UID_A, "uid": _UID_B},
        )
        r = await read(_dao(p=p), _UID_A)
        assert r is not None
        assert r["uid"] == _UID_B

    async def test_undef_table(self) -> None:
        p, c = _pc()
        c.fetchrow = AsyncMock(side_effect=UndefinedTableError("x"))
        with pytest.raises(StorageError, match="Table does not"):
            await read(_dao(p=p), _UID_A)

    async def test_error(self) -> None:
        p, c = _pc()
        c.fetchrow = AsyncMock(side_effect=RuntimeError("x"))
        with pytest.raises(StorageError, match="Failed to read"):
            await read(_dao(p=p), _UID_A)


class TestQuery:
    async def test_no_filters(self) -> None:
        p, c = _pc()
        c.fetch = AsyncMock(
            return_value=[{"id": _UID_A}, {"id": _UID_B}],
        )
        assert len(await query(_dao(p=p))) == _TWO

    async def test_with_filters(self) -> None:
        p, c = _pc()
        c.fetch = AsyncMock(return_value=[{"id": _UID_A}])
        assert len(await query(_dao(p=p), filters={"n": "f"})) == 1

    async def test_empty_where(self) -> None:
        p, c = _pc()
        c.fetch = AsyncMock(return_value=[])
        assert await query(_dao(p=p), filters={"1b": "x"}) == []

    async def test_undef_table(self) -> None:
        p, c = _pc()
        c.fetch = AsyncMock(side_effect=UndefinedTableError("x"))
        with pytest.raises(StorageError, match="Table does not"):
            await query(_dao(p=p))

    async def test_error(self) -> None:
        p, c = _pc()
        c.fetch = AsyncMock(side_effect=RuntimeError("x"))
        with pytest.raises(StorageError, match="Failed to query"):
            await query(_dao(p=p))


class TestListAll:
    async def test_rows(self) -> None:
        p, c = _pc()
        c.fetch = AsyncMock(return_value=[{"id": _UID_A}])
        assert len(await list_all(_dao(p=p))) == 1

    async def test_undef_table(self) -> None:
        p, c = _pc()
        c.fetch = AsyncMock(side_effect=UndefinedTableError("x"))
        with pytest.raises(StorageError, match="Table does not"):
            await list_all(_dao(p=p))

    async def test_error(self) -> None:
        p, c = _pc()
        c.fetch = AsyncMock(side_effect=RuntimeError("x"))
        with pytest.raises(StorageError, match="Failed to list"):
            await list_all(_dao(p=p))


class TestCount:
    async def test_no_filters(self) -> None:
        p, c = _pc()
        c.fetchval = AsyncMock(return_value=_N42)
        assert await count(_dao(p=p)) == _N42

    async def test_with_filters(self) -> None:
        p, c = _pc()
        c.fetchval = AsyncMock(return_value=_THREE)
        assert await count(_dao(p=p), filters={"a": True}) == _THREE

    async def test_none_result(self) -> None:
        p, c = _pc()
        c.fetchval = AsyncMock(return_value=None)
        with pytest.raises(StorageError, match="COUNT query"):
            await count(_dao(p=p))

    async def test_undef_table(self) -> None:
        p, c = _pc()
        c.fetchval = AsyncMock(side_effect=UndefinedTableError("x"))
        with pytest.raises(StorageError, match="Table does not"):
            await count(_dao(p=p))

    async def test_type_error(self) -> None:
        p, c = _pc()
        c.fetchval = AsyncMock(side_effect=TypeError("x"))
        with pytest.raises(StorageError, match="Invalid count"):
            await count(_dao(p=p))

    async def test_generic_error(self) -> None:
        p, c = _pc()
        c.fetchval = AsyncMock(side_effect=RuntimeError("x"))
        with pytest.raises(StorageError, match="Failed to count"):
            await count(_dao(p=p))


class TestGetModelSchema:
    async def test_schema(self) -> None:
        p, c = _pc()
        c.fetch = AsyncMock(
            return_value=[
                {
                    "column_name": "id",
                    "data_type": "text",
                    "is_nullable": "NO",
                    "column_default": None,
                },
                {
                    "column_name": "n",
                    "data_type": "text",
                    "is_nullable": "YES",
                    "column_default": None,
                },
            ]
        )
        r = await get_model_schema(_dao(p=p), _TBL)
        assert len(r["fields"]) == _TWO
        assert r["fields"][0]["nullable"] is False

    async def test_no_rows(self) -> None:
        p, c = _pc()
        c.fetch = AsyncMock(return_value=[])
        with pytest.raises(StorageError, match="schema not found"):
            await get_model_schema(_dao(p=p), _TBL)

    async def test_error(self) -> None:
        p, c = _pc()
        c.fetch = AsyncMock(side_effect=RuntimeError("x"))
        with pytest.raises(StorageError, match="Failed to get"):
            await get_model_schema(_dao(p=p), _TBL)


class TestBuildSetClause:
    def test_builds(self) -> None:
        c, _v = _build_set_clause({"name": "n", "value": 10})
        assert len(c) == _TWO
        assert "name = $2" in c

    def test_skips_id(self) -> None:
        c, _v = _build_set_clause({"id": "s", "name": "k"})
        assert len(c) == 1

    def test_skips_invalid(self) -> None:
        c, _ = _build_set_clause({"1b": "s", "good": "k"})
        assert len(c) == 1

    def test_empty(self) -> None:
        c, v = _build_set_clause({})
        assert c == []
        assert v == []


class TestUpdate:
    async def test_success(self) -> None:
        p, c = _pc()
        d = _dao(p=p)
        c.execute = AsyncMock(return_value="UPDATE 1")
        await update(d, _UID_A, {"name": "u"})
        c.execute.assert_awaited_once()

    async def test_not_found(self) -> None:
        p, c = _pc()
        c.execute = AsyncMock(return_value="UPDATE 0")
        with pytest.raises(StorageError, match="Record not found"):
            await update(_dao(p=p), "x", {"name": "n"})

    async def test_only_id_still_updates_timestamp(self) -> None:
        p, c = _pc()
        c.execute = AsyncMock(return_value="UPDATE 1")
        await update(_dao(p=p), _UID_A, {"id": "only"})
        c.execute.assert_awaited_once()

    async def test_error(self) -> None:
        p, c = _pc()
        c.execute = AsyncMock(side_effect=RuntimeError("x"))
        with pytest.raises(StorageError, match="Failed to update"):
            await update(_dao(p=p), _UID_A, {"name": "f"})

    async def test_reraise(self) -> None:
        p, c = _pc()
        c.execute = AsyncMock(side_effect=StorageError("direct"))
        with pytest.raises(StorageError, match="direct"):
            await update(_dao(p=p), _UID_A, {"name": "f"})


class TestDelete:
    async def test_true(self) -> None:
        p, c = _pc()
        c.execute = AsyncMock(return_value="DELETE 1")
        assert await delete(_dao(p=p), _UID_A) is True

    async def test_false(self) -> None:
        p, c = _pc()
        c.execute = AsyncMock(return_value="DELETE 0")
        assert await delete(_dao(p=p), "x") is False

    async def test_error(self) -> None:
        p, c = _pc()
        c.execute = AsyncMock(side_effect=RuntimeError("x"))
        with pytest.raises(StorageError, match="Failed to delete"):
            await delete(_dao(p=p), _UID_A)

    async def test_pool_none(self) -> None:
        d = _dao(p=None)
        d.pool = None

        async def sc() -> None:
            pass

        d.connect = AsyncMock(side_effect=sc)
        with pytest.raises(StorageConnectionError, match="pool"):
            await delete(d, _UID_A)

    async def test_connect(self) -> None:
        p, c = _pc()
        d = _dao(p=None)
        d.pool = None

        async def sc() -> None:
            d.pool = p

        d.connect = AsyncMock(side_effect=sc)
        c.execute = AsyncMock(return_value="DELETE 1")
        assert await delete(d, _UID_A) is True
        d.connect.assert_awaited_once()
