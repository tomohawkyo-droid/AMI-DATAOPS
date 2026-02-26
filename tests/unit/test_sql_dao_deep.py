"""Deep coverage for PostgreSQLDAO and PgVectorDAO wrapper methods."""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest

from ami.core.exceptions import (
    QueryError,
    StorageConnectionError,
    StorageError,
)
from ami.core.storage_types import StorageType
from ami.implementations.sql.postgresql_dao import PostgreSQLDAO
from ami.implementations.vec.pgvector_dao import PgVectorDAO
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

_PORT = 5432
_DIM = 384
_EST = 500
_UID = "uid-aaa-111"
_TWO = 2
_THREE = 3
_EP = "ami.implementations.vec.pgvector_dao.get_embedding_service"
_PR = "ami.implementations.sql.postgresql_read"
_PC = "ami.implementations.sql.postgresql_create"
_AP = "asyncpg.create_pool"
_VR = "ami.implementations.vec.pgvector_read"
_VU = "ami.implementations.vec.pgvector_update"
_VT = "ami.implementations.vec.pgvector_util"
_PU = "ami.implementations.sql.postgresql_update.update"
_PG = StorageConfig(
    storage_type=StorageType.RELATIONAL,
    host="h",
    port=_PORT,
    database="d",
    username="u",
    password="p",
)
_VC = StorageConfig(
    storage_type=StorageType.VECTOR,
    host="h",
    port=_PORT,
    database="d",
    username="u",
    password="p",
)


class _M(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="test_items",
    )
    name: str = ""
    value: int = 0


def _es() -> MagicMock:
    s = MagicMock()
    s.embedding_dim = _DIM
    s.generate_embedding = AsyncMock(return_value=[0.1] * _DIM)
    return s


def _pd(cfg: StorageConfig | None = _PG) -> PostgreSQLDAO:
    return PostgreSQLDAO(_M, cfg)


def _vd(
    cfg: StorageConfig | None = _VC,
    pool: bool = False,
) -> PgVectorDAO:
    with patch(_EP, return_value=_es()):
        d = PgVectorDAO(_M, cfg)
    if pool:
        d.pool = AsyncMock()
    return d


def _mp(cn: AsyncMock) -> MagicMock:
    cx = AsyncMock()
    cx.__aenter__ = AsyncMock(return_value=cn)
    cx.__aexit__ = AsyncMock(return_value=False)
    p = MagicMock()
    p.acquire.return_value = cx
    p.close = AsyncMock()
    return p


def _ac() -> tuple[AsyncMock, MagicMock]:
    c = AsyncMock()
    return c, _mp(c)


def _sp(d: Any, p: Any) -> Any:
    async def _fn() -> None:
        d.pool = p

    return _fn


class TestPgConnect:
    async def test_no_config(self) -> None:
        with pytest.raises(StorageError, match="required"):
            await _pd(cfg=None).connect()

    async def test_oserror(self) -> None:
        with (
            patch(_AP, new=AsyncMock(side_effect=OSError("x"))),
            pytest.raises(StorageConnectionError),
        ):
            await _pd().connect()

    async def test_test_exc(self) -> None:
        d = _pd()
        c, p = _ac()
        c.fetchval = AsyncMock(side_effect=asyncpg.InterfaceError("x"))
        d.pool = p
        assert await d.test_connection() is False

    async def test_test_pool_none(self) -> None:
        d = _pd()
        d.pool = None
        with patch(_AP, new=AsyncMock(return_value=None)):
            assert await d.test_connection() is False


class TestPgCrud:
    async def test_create_bad(self) -> None:
        with pytest.raises(StorageError, match="StorageModel"):
            await _pd().create(12345)

    async def test_find_one_found(self) -> None:
        r = {"uid": _UID, "name": "f", "value": 1}
        with patch(f"{_PR}.query", new=AsyncMock(return_value=[r])):
            assert (await _pd().find_one({"n": "f"})).uid == _UID

    async def test_find_one_empty(self) -> None:
        with patch(f"{_PR}.query", new=AsyncMock(return_value=[])):
            assert await _pd().find_one({"n": "x"}) is None

    async def test_bulk_upd_id(self) -> None:
        d, m = _pd(), AsyncMock()
        with patch(_PU, new=m):
            await d.bulk_update([{"id": "a", "name": "n"}])
        m.assert_awaited_once_with(d, "a", {"name": "n"})

    async def test_bulk_upd_uid(self) -> None:
        d, m = _pd(), AsyncMock()
        with patch(_PU, new=m):
            await d.bulk_update([{"uid": "u", "name": "n"}])
        m.assert_awaited_once_with(d, "u", {"name": "n"})

    async def test_bulk_upd_no_id(self) -> None:
        with pytest.raises(StorageError, match="id"):
            await _pd().bulk_update([{"name": "x"}])

    async def test_create_idx(self) -> None:
        d, m = _pd(), AsyncMock()
        with patch(f"{_PC}.ensure_table_exists", new=m):
            await d.create_indexes()
        m.assert_awaited_once_with(d)


class TestPgRaw:
    async def test_read_auto(self) -> None:
        d, (c, p) = _pd(), _ac()
        c.fetch = AsyncMock(return_value=[{"v": 1}])
        d.pool = None
        with patch.object(d, "connect", side_effect=_sp(d, p)):
            assert await d.raw_read_query("S") == [{"v": 1}]

    async def test_read_none(self) -> None:
        d = _pd()
        d.pool = None
        with (
            patch.object(d, "connect", new=AsyncMock()),
            pytest.raises(StorageConnectionError),
        ):
            await d.raw_read_query("S")

    async def test_read_err(self) -> None:
        d, (c, p) = _pd(), _ac()
        c.fetch = AsyncMock(side_effect=RuntimeError("x"))
        d.pool = p
        with pytest.raises(StorageError, match="raw read"):
            await d.raw_read_query("S")

    async def test_write_auto(self) -> None:
        d, (c, p) = _pd(), _ac()
        c.execute = AsyncMock(return_value="UPDATE 2")
        d.pool = None
        with patch.object(d, "connect", side_effect=_sp(d, p)):
            assert await d.raw_write_query("U") == _TWO

    async def test_write_none(self) -> None:
        d = _pd()
        d.pool = None
        with (
            patch.object(d, "connect", new=AsyncMock()),
            pytest.raises(StorageConnectionError),
        ):
            await d.raw_write_query("U")

    async def test_write_params(self) -> None:
        d, (c, p) = _pd(), _ac()
        c.execute = AsyncMock(return_value="UPDATE 1")
        d.pool = p
        assert await d.raw_write_query("U", {"n": "v"}) == 1

    async def test_write_err(self) -> None:
        d, (c, p) = _pd(), _ac()
        c.execute = AsyncMock(side_effect=RuntimeError("x"))
        d.pool = p
        with pytest.raises(StorageError, match="raw write"):
            await d.raw_write_query("D")


class TestPgIntro:
    async def test_list_db(self) -> None:
        d, (c, p) = _pd(), _ac()
        c.fetch = AsyncMock(return_value=[{"datname": "a"}, {"datname": "b"}])
        d.pool = p
        assert await d.list_databases() == ["a", "b"]

    async def test_list_db_err(self) -> None:
        d, (c, p) = _pd(), _ac()
        c.fetch = AsyncMock(side_effect=RuntimeError("x"))
        d.pool = p
        with pytest.raises(StorageError, match="list databases"):
            await d.list_databases()

    async def test_schemas(self) -> None:
        d, (c, p) = _pd(), _ac()
        c.fetch = AsyncMock(return_value=[{"schema_name": "public"}])
        d.pool = p
        assert await d.list_schemas() == ["public"]

    async def test_schemas_err(self) -> None:
        d, (c, p) = _pd(), _ac()
        c.fetch = AsyncMock(side_effect=RuntimeError("x"))
        d.pool = p
        with pytest.raises(StorageError, match="list schemas"):
            await d.list_schemas()

    async def test_models(self) -> None:
        d, (c, p) = _pd(), _ac()
        c.fetch = AsyncMock(return_value=[{"table_name": "t"}])
        d.pool = p
        assert await d.list_models() == ["t"]

    async def test_models_err(self) -> None:
        d, (c, p) = _pd(), _ac()
        c.fetch = AsyncMock(side_effect=RuntimeError("x"))
        d.pool = p
        with pytest.raises(StorageError, match="list tables"):
            await d.list_models()

    async def test_info(self) -> None:
        d, (c, p) = _pd(), _ac()
        c.fetchval = AsyncMock(return_value=_EST)
        d.pool = p
        with patch(
            f"{_PR}.get_model_schema",
            new=AsyncMock(return_value={"fields": []}),
        ):
            assert (await d.get_model_info("t"))["estimated_rows"] == _EST

    async def test_info_null(self) -> None:
        d, (c, p) = _pd(), _ac()
        c.fetchval = AsyncMock(return_value=None)
        d.pool = p
        with patch(f"{_PR}.get_model_schema", new=AsyncMock(return_value={})):
            assert (await d.get_model_info("t"))["estimated_rows"] == 0

    async def test_info_exc(self) -> None:
        d, (c, p) = _pd(), _ac()
        c.fetchval = AsyncMock(side_effect=RuntimeError("x"))
        d.pool = p
        with pytest.raises(StorageError, match="model info"):
            await d.get_model_info("t")

    async def test_info_reraise(self) -> None:
        d, (c, p) = _pd(), _ac()
        c.fetchval = AsyncMock(return_value=0)
        d.pool = p
        with (
            patch(
                f"{_PR}.get_model_schema",
                new=AsyncMock(side_effect=StorageError("inner")),
            ),
            pytest.raises(StorageError, match="inner"),
        ):
            await d.get_model_info("t")

    async def test_schema(self) -> None:
        with patch(f"{_PR}.get_model_schema", new=AsyncMock(return_value={})):
            assert await _pd().get_model_schema("t") == {}

    async def test_fields(self) -> None:
        with patch(
            f"{_PR}.get_model_schema",
            new=AsyncMock(return_value={"fields": [{"n": "id"}]}),
        ):
            assert await _pd().get_model_fields("t") == [{"n": "id"}]

    async def test_fields_empty(self) -> None:
        with patch(f"{_PR}.get_model_schema", new=AsyncMock(return_value={})):
            assert await _pd().get_model_fields("t") == []

    async def test_idx(self) -> None:
        d, (c, p) = _pd(), _ac()
        c.fetch = AsyncMock(
            return_value=[
                {
                    "index_name": "i",
                    "column_name": "n",
                    "is_unique": True,
                    "index_type": "btree",
                }
            ]
        )
        d.pool = p
        r = await d.get_model_indexes("t")
        assert r[0]["name"] == "i"
        assert r[0]["unique"] is True

    async def test_idx_err(self) -> None:
        d, (c, p) = _pd(), _ac()
        c.fetch = AsyncMock(side_effect=RuntimeError("x"))
        d.pool = p
        with pytest.raises(StorageError, match="indexes"):
            await d.get_model_indexes("t")

    async def test_idx_auto(self) -> None:
        d, (c, p) = _pd(), _ac()
        c.fetch = AsyncMock(return_value=[])
        d.pool = None
        with patch.object(d, "connect", side_effect=_sp(d, p)):
            assert await d.get_model_indexes("t") == []


class TestVecLifecycle:
    async def test_connect_skips(self) -> None:
        d = _vd(pool=True)
        old = d.pool
        await d.connect()
        assert d.pool is old

    async def test_test_conn_exc(self) -> None:
        d = _vd(pool=True)
        c = AsyncMock()
        c.fetchval = AsyncMock(side_effect=asyncpg.InterfaceError("x"))
        cx = MagicMock()
        cx.__aenter__ = AsyncMock(return_value=c)
        cx.__aexit__ = AsyncMock(return_value=False)
        d.pool.acquire = MagicMock(return_value=cx)
        assert await d.test_connection() is False

    async def test_find_one(self) -> None:
        d = _vd(pool=True)
        e = _M(uid="f1", name="f")
        with patch(f"{_VR}.find_one", new=AsyncMock(return_value=e)):
            assert await d.find_one({"n": "f"}) is e

    async def test_bulk_update(self) -> None:
        d, m = _vd(pool=True), AsyncMock()
        with patch(f"{_VU}.bulk_update", new=m):
            await d.bulk_update([{"uid": "a", "name": "n"}])
        m.assert_awaited_once()


class TestVecIdx:
    async def test_ok(self) -> None:
        d, (c, p) = _vd(), _ac()
        d.pool = p
        with patch(f"{_VT}.create_model_indexes", new=AsyncMock()):
            await d.create_indexes()
        assert c.execute.await_count >= 1

    async def test_hnsw_skip(self) -> None:
        d, (c, p) = _vd(), _ac()
        c.execute = AsyncMock(side_effect=RuntimeError("x"))
        d.pool = p
        with patch(f"{_VT}.create_model_indexes", new=AsyncMock()):
            await d.create_indexes()

    async def test_pool_none(self) -> None:
        d = _vd()
        d.pool = None
        with (
            patch.object(d, "connect", new=AsyncMock()),
            pytest.raises(StorageConnectionError),
        ):
            await d.create_indexes()


class TestVecDelegates:
    async def test_raw_read(self) -> None:
        d = _vd(pool=True)
        with patch(f"{_VR}.raw_read_query", new=AsyncMock(return_value=[])):
            assert await d.raw_read_query("S") == []

    async def test_raw_write(self) -> None:
        d = _vd(pool=True)
        with patch(f"{_VU}.raw_write_query", new=AsyncMock(return_value=_THREE)):
            assert await d.raw_write_query("U") == _THREE

    async def test_list_dbs(self) -> None:
        d = _vd(pool=True)
        with patch(f"{_VR}.list_databases", new=AsyncMock(return_value=["d"])):
            assert await d.list_databases() == ["d"]

    async def test_list_schemas(self) -> None:
        d = _vd(pool=True)
        with patch(f"{_VR}.list_schemas", new=AsyncMock(return_value=["p"])):
            assert await d.list_schemas() == ["p"]

    async def test_list_models(self) -> None:
        d = _vd(pool=True)
        with patch(f"{_VR}.list_models", new=AsyncMock(return_value=["t"])):
            assert await d.list_models() == ["t"]

    async def test_info(self) -> None:
        d = _vd(pool=True)
        with patch(f"{_VR}.get_model_info", new=AsyncMock(return_value={"n": "t"})):
            assert (await d.get_model_info("t"))["n"] == "t"

    async def test_schema(self) -> None:
        d = _vd(pool=True)
        with patch(f"{_VR}.get_model_schema", new=AsyncMock(return_value={})):
            assert await d.get_model_schema("t") == {}

    async def test_fields(self) -> None:
        d = _vd(pool=True)
        with patch(f"{_VR}.get_model_fields", new=AsyncMock(return_value=[])):
            assert await d.get_model_fields("t") == []

    async def test_indexes(self) -> None:
        d = _vd(pool=True)
        with patch(f"{_VR}.get_model_indexes", new=AsyncMock(return_value=[])):
            assert await d.get_model_indexes("t") == []


class TestVecEmb:
    async def test_err(self) -> None:
        d = _vd(pool=True)
        d._embedding_service.generate_embedding = AsyncMock(
            side_effect=RuntimeError("x"),
        )
        with pytest.raises(QueryError, match="Embedding"):
            await d._generate_embedding_for_record({"title": "t"})

    async def test_empty(self) -> None:
        assert await _vd(pool=True)._generate_embedding_for_record({}) is None

    async def test_ws(self) -> None:
        r = await _vd(pool=True)._generate_embedding_for_record({"title": "  "})
        assert r is None

    async def _fe(self, raw: Any) -> Any:
        d, (c, p) = _vd(), _ac()
        c.fetchrow = AsyncMock(return_value={"embedding": raw})
        d.pool = p
        return await d._fetch_embedding("a")

    async def test_fetch_list(self) -> None:
        assert await self._fe([0.1]) == [0.1]

    async def test_fetch_tuple(self) -> None:
        assert await self._fe((0.3,)) == [0.3]

    async def test_fetch_mv(self) -> None:
        assert await self._fe(memoryview(bytes([1]))) == [1.0]

    async def test_fetch_str(self) -> None:
        assert await self._fe("[0.5]") == [0.5]

    async def test_fetch_bad(self) -> None:
        assert await self._fe("nope") is None

    async def test_fetch_unknown(self) -> None:
        assert await self._fe(999) is None

    async def test_fetch_none_row(self) -> None:
        d, (c, p) = _vd(), _ac()
        c.fetchrow = AsyncMock(return_value=None)
        d.pool = p
        assert await d._fetch_embedding("a") is None

    async def test_fetch_none_emb(self) -> None:
        assert await self._fe(None) is None

    async def test_fetch_pool_none(self) -> None:
        d = _vd()
        d.pool = None
        with pytest.raises(StorageConnectionError):
            await d._fetch_embedding("x")

    def test_dsn_no_cfg(self) -> None:
        d = _vd(cfg=None)
        d.config = None
        with pytest.raises(StorageError, match="No storage"):
            d._build_dsn()
        assert d._safe_dsn() == "<no config>"
