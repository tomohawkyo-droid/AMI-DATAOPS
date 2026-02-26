"""Deep coverage tests for redis_dao, redis_create, redis_update."""

from __future__ import annotations

import json
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.core.exceptions import (
    StorageConnectionError,
    StorageError,
    StorageValidationError,
)
from ami.core.storage_types import StorageType
from ami.implementations.mem.redis_create import (
    _create_indexes,
    _store_data_and_metadata,
    create,
)
from ami.implementations.mem.redis_dao import RedisDAO
from ami.implementations.mem.redis_update import update
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

_TTL = 300
_TWO = 2
_M = "ami.implementations.mem.redis_dao"
_CFG = StorageConfig(
    storage_type=StorageType.INMEM,
    host="127.0.0.1",
    port=6379,
    database="0",
)


class _TM(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="test_items",
    )
    name: str = "default"
    value: int = 0


def _am(rv: Any = None) -> AsyncMock:
    return AsyncMock(return_value=rv)


@pytest.fixture
def dao() -> RedisDAO:
    d = RedisDAO(_TM, _CFG)
    d.client = AsyncMock()
    return d


def _dm(client: AsyncMock | None = None) -> MagicMock:
    d = MagicMock()
    d._key_prefix = "test_items:"
    d.collection_name = "test_items"
    d.client = client
    d.connect = AsyncMock()
    d.DEFAULT_TTL = RedisDAO.DEFAULT_TTL
    return d


def _sg(*keys: str) -> Any:
    async def _gen(**_kw: Any) -> Any:
        for k in keys:
            yield k

    return _gen


def _fc() -> AsyncMock:
    c = AsyncMock()
    for a in ("setex", "hset", "expire", "sadd"):
        setattr(c, a, _am())
    return c


def _rerr() -> type:
    return __import__("redis.exceptions", fromlist=["RedisError"]).RedisError


class TestConnectErrors:
    @pytest.mark.asyncio
    async def test_no_config(self) -> None:
        with pytest.raises(StorageError, match="Invalid Redis"):
            await RedisDAO(_TM, None).connect()

    @pytest.mark.asyncio
    async def test_no_host(self) -> None:
        cfg = StorageConfig(
            storage_type=StorageType.INMEM,
            host=None,
            port=6379,
            database="0",
        )
        with pytest.raises(StorageError, match="host not config"):
            await RedisDAO(_TM, cfg).connect()

    @pytest.mark.asyncio
    async def test_no_port(self) -> None:
        cfg = StorageConfig(
            storage_type=StorageType.INMEM,
            host="127.0.0.1",
            port=6379,
            database="0",
        )
        cfg.port = None
        with pytest.raises(StorageError, match="port not config"):
            await RedisDAO(_TM, cfg).connect()

    @pytest.mark.asyncio
    async def test_redis_error(self) -> None:
        err = _rerr()("no")
        with (
            patch(f"{_M}.redis.Redis", side_effect=err),
            pytest.raises(StorageConnectionError),
        ):
            await RedisDAO(_TM, _CFG).connect()


class TestDisconnectError:
    @pytest.mark.asyncio
    async def test_aclose_error(self, dao: RedisDAO) -> None:
        dao.client.aclose.side_effect = _rerr()("x")
        with pytest.raises(StorageError, match="disconnection"):
            await dao.disconnect()


class TestDaoWrappers:
    @pytest.mark.asyncio
    async def test_list_all_wraps(self, dao: RedisDAO) -> None:
        rows = [{"name": "a", "value": 1}, {"name": "b", "value": _TWO}]
        with patch(f"{_M}.redis_read.list_all", _am(rows)):
            r = await dao.list_all(limit=10, offset=0)
        assert len(r) == _TWO
        assert all(isinstance(x, _TM) for x in r)

    @pytest.mark.asyncio
    async def test_list_all_no_model(self) -> None:
        d = RedisDAO(_TM, _CFG)
        d.client, d.model_cls = AsyncMock(), None
        with patch(f"{_M}.redis_read.list_all", _am([{"x": 1}])):
            assert await d.list_all() == [{"x": 1}]

    @pytest.mark.asyncio
    async def test_find_by_field_wraps(self, dao: RedisDAO) -> None:
        with patch(
            f"{_M}.redis_read.query",
            _am([{"name": "m", "value": 1}]),
        ):
            r = await dao.find_by_field("name", "m")
        assert isinstance(r[0], _TM)

    @pytest.mark.asyncio
    async def test_find_by_field_no_model(self) -> None:
        d = RedisDAO(_TM, _CFG)
        d.client, d.model_cls = AsyncMock(), None
        with patch(f"{_M}.redis_read.query", _am([{"n": 1}])):
            assert await d.find_by_field("n", 1) == [{"n": 1}]

    @pytest.mark.asyncio
    async def test_bulk_update_missing_id(self, dao: RedisDAO) -> None:
        with pytest.raises(StorageValidationError, match="missing"):
            await dao.bulk_update([{"name": "x"}])

    @pytest.mark.asyncio
    async def test_bulk_update_ok(self, dao: RedisDAO) -> None:
        with patch(f"{_M}.redis_update.update", new_callable=AsyncMock) as m:
            await dao.bulk_update([{"id": "a", "v": 1}, {"id": "b", "v": _TWO}])
        assert m.await_count == _TWO

    @pytest.mark.asyncio
    async def test_bulk_delete_exc(self, dao: RedisDAO) -> None:
        with patch(f"{_M}.redis_delete.delete", new_callable=AsyncMock) as m:
            m.side_effect = [True, RuntimeError("boom")]
            with pytest.raises(StorageError, match="Bulk delete"):
                await dao.bulk_delete(["ok", "fail"])

    @pytest.mark.asyncio
    async def test_noop_methods(self, dao: RedisDAO) -> None:
        await dao.create_indexes()
        assert await dao.get_model_indexes("p") == []


class TestIntrospection:
    """list_databases, list_schemas, list_models, model info."""

    @pytest.mark.asyncio
    async def test_list_databases_parses(self, dao: RedisDAO) -> None:
        dao.client.info = _am("db0:keys=10\ndb1:keys=5\no")
        r = await dao.list_databases()
        assert "db0" in r
        assert "db1" in r

    @pytest.mark.asyncio
    async def test_list_databases_default(self, dao: RedisDAO) -> None:
        dao.client.info = _am("")
        assert await dao.list_databases() == ["db0"]

    @pytest.mark.asyncio
    async def test_list_schemas(self, dao: RedisDAO) -> None:
        dao.client.keys = _am(["u:1", "u:2", "i:a"])
        assert set(await dao.list_schemas()) == {"u", "i"}

    @pytest.mark.asyncio
    async def test_list_schemas_no_colon(self, dao: RedisDAO) -> None:
        dao.client.keys = _am(["plain", "a:1"])
        assert await dao.list_schemas() == ["a"]

    @pytest.mark.asyncio
    async def test_list_models_schema(self, dao: RedisDAO) -> None:
        dao.client.keys = _am(["ns:1", "ns:2"])
        assert "ns" in await dao.list_models(schema="ns")

    @pytest.mark.asyncio
    async def test_list_models_all(self, dao: RedisDAO) -> None:
        dao.client.keys = _am(["a:1", "b:2", "plain"])
        assert set(await dao.list_models()) == {"a", "b"}

    @pytest.mark.asyncio
    async def test_model_info_keys(self, dao: RedisDAO) -> None:
        dao.client.keys = _am(["p:a", "p:b"])
        dao.client.type = _am("hash")
        r = await dao.get_model_info("p")
        assert r["key_count"] == _TWO
        assert r["key_type"] == "hash"

    @pytest.mark.asyncio
    async def test_model_info_empty(self, dao: RedisDAO) -> None:
        dao.client.keys = _am([])
        r = await dao.get_model_info("e")
        assert r["key_count"] == 0
        assert r["key_type"] is None

    @pytest.mark.asyncio
    async def test_not_connected(self) -> None:
        d = RedisDAO(_TM, _CFG)
        with pytest.raises(StorageError):
            await d.list_databases()
        with pytest.raises(StorageError):
            await d.list_schemas()
        with pytest.raises(StorageError):
            await d.list_models()
        with pytest.raises(StorageError):
            await d.get_model_info("p")
        with pytest.raises(StorageError):
            await d.get_model_fields("p")

    @pytest.mark.asyncio
    async def test_schema_delegates(self, dao: RedisDAO) -> None:
        dao.client.keys = _am(["x:1"])
        dao.client.type = _am("string")
        assert (await dao.get_model_schema("x"))["path"] == "x"

    @pytest.mark.asyncio
    async def test_hash_fields(self, dao: RedisDAO) -> None:
        dao.client.keys = _am(["p:1"])
        dao.client.type = _am("hash")
        dao.client.hkeys = _am(["name", "status"])
        assert len(await dao.get_model_fields("p")) == _TWO

    @pytest.mark.asyncio
    async def test_dedup_fields(self, dao: RedisDAO) -> None:
        dao.client.keys = _am(["p:1", "p:2"])
        dao.client.type = _am("hash")
        dao.client.hkeys = _am(["name"])
        assert len(await dao.get_model_fields("p")) == 1


class TestRawQueries:
    @pytest.mark.asyncio
    async def test_keys_pattern(self, dao: RedisDAO) -> None:
        dao.client.scan_iter = _sg("a:1", "a:2")
        assert len(await dao.raw_read_query("KEYS a:*")) == _TWO

    @pytest.mark.asyncio
    async def test_keys_default(self, dao: RedisDAO) -> None:
        dao.client.scan_iter = _sg("x:1")
        assert len(await dao.raw_read_query("KEYS")) == 1

    @pytest.mark.asyncio
    async def test_info(self, dao: RedisDAO) -> None:
        dao.client.info = _am({"server": "redis"})
        assert "info" in (await dao.raw_read_query("INFO"))[0]

    @pytest.mark.asyncio
    async def test_empty_and_get_no_key(self, dao: RedisDAO) -> None:
        assert await dao.raw_read_query("") == []
        assert await dao.raw_read_query("GET") == []

    @pytest.mark.asyncio
    async def test_read_exception(self, dao: RedisDAO) -> None:
        dao.client.info.side_effect = RuntimeError("x")
        with pytest.raises(StorageError, match="Raw read"):
            await dao.raw_read_query("INFO")

    @pytest.mark.asyncio
    async def test_del(self, dao: RedisDAO) -> None:
        dao.client.delete = _am(_TWO)
        assert await dao.raw_write_query("DEL k1 k2") == _TWO

    @pytest.mark.asyncio
    async def test_insufficient_args(self, dao: RedisDAO) -> None:
        assert await dao.raw_write_query("SET k1") == 0
        assert await dao.raw_write_query("DEL") == 0

    @pytest.mark.asyncio
    async def test_write_exception(self, dao: RedisDAO) -> None:
        dao.client.set = _am()
        dao.client.set.side_effect = RuntimeError("x")
        with pytest.raises(StorageError, match="Raw write"):
            await dao.raw_write_query("SET k v")


class TestTestConnectionExt:
    @pytest.mark.asyncio
    async def test_async_ping(self, dao: RedisDAO) -> None:
        dao.client.ping = MagicMock(return_value=AsyncMock()())
        assert await dao.test_connection() is True

    @pytest.mark.asyncio
    async def test_error(self, dao: RedisDAO) -> None:
        dao.client.ping = MagicMock(side_effect=RuntimeError("x"))
        with pytest.raises(StorageError, match="connection test"):
            await dao.test_connection()


class TestStoreDataMeta:
    def _d(self, **kw: Any) -> dict[str, Any]:
        return {"created_at": "t", "updated_at": "t", **kw}

    @pytest.mark.asyncio
    async def test_no_ttl(self) -> None:
        with pytest.raises(StorageError, match="must define a TTL"):
            await _store_data_and_metadata(_dm(AsyncMock()), self._d(name="x"), "i")

    @pytest.mark.asyncio
    async def test_zero_ttl(self) -> None:
        with pytest.raises(StorageError, match="positive TTL"):
            await _store_data_and_metadata(_dm(AsyncMock()), self._d(_ttl=0), "i")

    @pytest.mark.asyncio
    async def test_setex_fail(self) -> None:
        c = AsyncMock()
        c.setex.side_effect = RuntimeError("disk")
        with pytest.raises(StorageError, match="store data"):
            await _store_data_and_metadata(_dm(c), self._d(_ttl=_TTL, name="v"), "i")

    @pytest.mark.asyncio
    async def test_meta_fail_cleans(self) -> None:
        c = AsyncMock()
        c.setex, c.delete = _am(), _am()
        c.hset.side_effect = RuntimeError("meta")
        with pytest.raises(StorageError, match="metadata"):
            await _store_data_and_metadata(_dm(c), self._d(_ttl=_TTL, name="v"), "i")
        c.delete.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_meta_and_cleanup_fail(self) -> None:
        c = AsyncMock()
        c.setex = _am()
        c.hset.side_effect = RuntimeError("meta")
        c.delete.side_effect = RuntimeError("cleanup")
        with pytest.raises(StorageError, match=r"cleanup.*also failed"):
            await _store_data_and_metadata(_dm(c), self._d(_ttl=_TTL, name="v"), "i")

    @pytest.mark.asyncio
    async def test_success(self) -> None:
        c = _fc()
        await _store_data_and_metadata(_dm(c), self._d(_ttl=_TTL, name="v"), "i")
        c.setex.assert_awaited_once()


class TestCreateIdxSub:
    @pytest.mark.asyncio
    async def test_explicit(self) -> None:
        c = AsyncMock()
        await _create_indexes(_dm(c), {"name": "v", "_index_fields": ["name"]}, "i")
        c.sadd.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_auto(self) -> None:
        c = AsyncMock()
        d: dict[str, Any] = {
            "name": "v",
            "status": "on",
            "uid": "i",
            "created_at": "t",
            "updated_at": "t",
        }
        await _create_indexes(_dm(c), d, "i")
        assert c.sadd.await_count == _TWO

    @pytest.mark.asyncio
    async def test_error(self) -> None:
        c = AsyncMock()
        c.sadd.side_effect = RuntimeError("x")
        with pytest.raises(StorageError, match="Failed to create"):
            await _create_indexes(_dm(c), {"name": "v"}, "i")


class TestCreateFull:
    @pytest.mark.asyncio
    async def test_ttl_field(self) -> None:
        r = await create(_dm(_fc()), {"uid": "a", "ttl": _TTL})
        assert r == "a"

    @pytest.mark.asyncio
    async def test_underscore_ttl(self) -> None:
        r = await create(_dm(_fc()), {"uid": "b", "_ttl": _TTL})
        assert r == "b"

    @pytest.mark.asyncio
    async def test_no_ttl(self) -> None:
        with pytest.raises(StorageError, match="explicitly define"):
            await create(_dm(AsyncMock()), {"uid": "x"})

    @pytest.mark.asyncio
    async def test_bad_ttl(self) -> None:
        with pytest.raises(StorageError, match="positive TTL"):
            await create(_dm(AsyncMock()), {"uid": "x", "ttl": -10})
        with pytest.raises(StorageError, match="positive TTL"):
            await create(_dm(AsyncMock()), {"uid": "x", "ttl": "no"})

    @pytest.mark.asyncio
    async def test_auto_connect(self) -> None:
        c = _fc()
        d = _dm(client=None)

        async def _wire() -> None:
            d.client = c

        d.connect.side_effect = _wire
        assert await create(d, {"uid": "c", "ttl": _TTL}) == "c"
        d.connect.assert_awaited_once()


class TestUpdateFull:
    @pytest.mark.asyncio
    async def test_merge(self) -> None:
        c = _fc()
        c.get = _am(json.dumps({"name": "old", "_ttl": _TTL}))
        await update(_dm(c), "i1", {"name": "new"})
        c.setex.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_miss(self) -> None:
        c = _fc()
        c.get = _am(None)
        await update(_dm(c), "i1", {"name": "x", "_ttl": _TTL})
        c.setex.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_auto_connect(self) -> None:
        c = _fc()
        c.get = _am(None)
        d = _dm(client=None)

        async def _wire() -> None:
            d.client = c

        d.connect.side_effect = _wire
        await update(d, "i1", {"name": "x", "_ttl": _TTL})
        d.connect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_error(self) -> None:
        c = AsyncMock()
        c.get.side_effect = RuntimeError("boom")
        with pytest.raises(StorageError, match="Failed to update"):
            await update(_dm(c), "i1", {"_ttl": _TTL})

    @pytest.mark.asyncio
    async def test_no_indexable(self) -> None:
        c = _fc()
        c.get = _am(None)
        await update(_dm(c), "i1", {"_ttl": _TTL})
        c.sadd.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_explicit_index(self) -> None:
        c = _fc()
        c.get = _am(None)
        d: dict[str, Any] = {
            "name": "v",
            "_ttl": _TTL,
            "_index_fields": ["name"],
        }
        await update(_dm(c), "i1", d)
        c.sadd.assert_awaited()
