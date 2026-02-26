"""Tests for REST discovery helpers and operation wrappers.

Covers rest_discovery.py and rest_operations.py.
"""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.core.exceptions import StorageError
from ami.core.storage_types import StorageType
from ami.implementations.rest.rest_dao import RestDAO
from ami.implementations.rest.rest_discovery import (
    HTTP_OK,
    _try_discovery_endpoint,
    get_model_fields,
    get_model_indexes,
    get_model_info,
    get_model_schema,
    list_databases,
    list_models,
    list_schemas,
)
from ami.implementations.rest.rest_discovery import (
    test_connection as check_rest_connection,
)
from ami.implementations.rest.rest_operations import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    batch_create,
    batch_delete,
    batch_update,
    create_rest_dao,
    paginated_fetch,
    upsert,
)
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

_DP = "ami.implementations.rest.rest_discovery.request_with_retry"
_NOT_FOUND = 404
_TWO = 2
_THREE = 3
_FOUR = 4
_OVER_MAX = 1500


class _W(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(path="widgets")
    name: str = "default"


def _cfg(**kw: Any) -> StorageConfig:
    d: dict[str, Any] = {
        "storage_type": StorageType.REST,
        "host": "api.test.local",
        "port": 8080,
    }
    d.update(kw)
    return StorageConfig(**d)


def _resp(status: int = 200, json_data: Any = None) -> MagicMock:
    r = MagicMock()
    r.status = status
    r.json = AsyncMock(return_value=json_data)
    r.text = AsyncMock(return_value="")
    r.__aenter__ = AsyncMock(return_value=r)
    r.__aexit__ = AsyncMock(return_value=False)
    return r


def _dao(
    config: StorageConfig | None = None,
    model_cls: type[Any] | None = None,
) -> MagicMock:
    d = MagicMock()
    d.base_url = "http://api.test.local:8080"
    d.collection_name = "widgets"
    d.config = config
    d.model_cls = model_cls or _W
    d._ensure_session = AsyncMock(return_value=MagicMock())
    d._extract_data = RestDAO._extract_data.__get__(d)
    return d


def _dao_with_opts() -> MagicMock:
    d = _dao(config=_cfg())
    d.config.options = {}
    return d


class TestTryDiscoveryEndpoint:
    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_list_data(self, m: AsyncMock) -> None:
        d = _dao_with_opts()
        m.return_value = _resp(json_data={"data": [{"name": "x"}]})
        assert await _try_discovery_endpoint(d, "db") == [{"name": "x"}]

    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_dict_wrapped(self, m: AsyncMock) -> None:
        d = _dao_with_opts()
        m.return_value = _resp(json_data={"data": {"name": "s"}})
        assert await _try_discovery_endpoint(d, "i") == [{"name": "s"}]

    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_non_200_none(self, m: AsyncMock) -> None:
        m.return_value = _resp(status=_NOT_FOUND)
        assert await _try_discovery_endpoint(_dao(), "x") is None

    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_error_none(self, m: AsyncMock) -> None:
        m.side_effect = StorageError("down")
        assert await _try_discovery_endpoint(_dao(), "x") is None

    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_scalar_none(self, m: AsyncMock) -> None:
        d = _dao_with_opts()
        m.return_value = _resp(json_data="str")
        assert await _try_discovery_endpoint(d, "x") is None


class TestListDatabases:
    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_names(self, m: AsyncMock) -> None:
        d = _dao_with_opts()
        m.return_value = _resp(json_data={"data": [{"name": "a"}, {"name": "b"}]})
        assert await list_databases(d) == ["a", "b"]

    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_config_db(self, m: AsyncMock) -> None:
        m.return_value = _resp(status=_NOT_FOUND)
        assert await list_databases(_dao(config=_cfg(database="fb"))) == ["fb"]

    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_empty(self, m: AsyncMock) -> None:
        m.return_value = _resp(status=_NOT_FOUND)
        assert await list_databases(_dao(config=None)) == []


class TestListSchemas:
    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_names(self, m: AsyncMock) -> None:
        d = _dao_with_opts()
        m.return_value = _resp(json_data={"data": [{"name": "pub"}]})
        assert await list_schemas(d, database="db") == ["pub"]

    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_default(self, m: AsyncMock) -> None:
        m.return_value = _resp(status=_NOT_FOUND)
        assert await list_schemas(_dao()) == ["widgets"]


class TestListModels:
    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_names(self, m: AsyncMock) -> None:
        d = _dao_with_opts()
        m.return_value = _resp(json_data={"data": [{"name": "u"}]})
        assert await list_models(d, database="d", schema="s") == ["u"]

    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_default(self, m: AsyncMock) -> None:
        m.return_value = _resp(status=_NOT_FOUND)
        assert await list_models(_dao()) == ["widgets"]


class TestGetModelInfo:
    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_single(self, m: AsyncMock) -> None:
        d = _dao_with_opts()
        m.return_value = _resp(json_data={"data": {"name": "w"}})
        assert (await get_model_info(d, "w"))["name"] == "w"

    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_default(self, m: AsyncMock) -> None:
        m.return_value = _resp(status=_NOT_FOUND)
        r = await get_model_info(_dao(), "w")
        assert r["type"] == "rest_resource"

    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_multi(self, m: AsyncMock) -> None:
        d = _dao_with_opts()
        m.return_value = _resp(json_data={"data": [{"n": "a"}, {"n": "b"}]})
        assert "items" in await get_model_info(d, "r", database="d", schema="s")


class TestGetModelSchema:
    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_endpoint(self, m: AsyncMock) -> None:
        d = _dao_with_opts()
        s = {"type": "object"}
        m.return_value = _resp(json_data={"data": s})
        assert await get_model_schema(d, "w") == s

    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_derived(self, m: AsyncMock) -> None:
        m.return_value = _resp(status=_NOT_FOUND)
        r = await get_model_schema(_dao(), "w")
        assert "name" in r["fields"]


class TestGetModelFields:
    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_dict_expanded(self, m: AsyncMock) -> None:
        m.return_value = _resp(status=_NOT_FOUND)
        names = [f["name"] for f in await get_model_fields(_dao(), "w")]
        assert "name" in names

    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_list_passthrough(self, m: AsyncMock) -> None:
        d = _dao_with_opts()
        m.return_value = _resp(json_data={"data": [{"name": "id"}]})
        assert isinstance(await get_model_fields(d, "w"), list)


class TestGetModelIndexes:
    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_returns(self, m: AsyncMock) -> None:
        d = _dao_with_opts()
        ix = [{"field": "name"}]
        m.return_value = _resp(json_data={"data": ix})
        assert await get_model_indexes(d, "w") == ix

    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_empty(self, m: AsyncMock) -> None:
        m.return_value = _resp(status=_NOT_FOUND)
        assert await get_model_indexes(_dao(), "w") == []


class TestTestConnection:
    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_healthy(self, m: AsyncMock) -> None:
        m.return_value = _resp(status=HTTP_OK)
        assert await check_rest_connection(_dao()) is True

    @pytest.mark.asyncio
    @patch(_DP, new_callable=AsyncMock)
    async def test_probes_fail(self, m: AsyncMock) -> None:
        m.side_effect = StorageError("down")
        assert await check_rest_connection(_dao()) is False

    @pytest.mark.asyncio
    async def test_session_error(self) -> None:
        d = _dao()
        d._ensure_session = AsyncMock(side_effect=RuntimeError("x"))
        assert await check_rest_connection(d) is False


class TestCreateRestDao:
    @pytest.mark.asyncio
    async def test_connects(self) -> None:
        dao = await create_rest_dao(_W, _cfg())
        assert dao._connected is True
        await dao.disconnect()

    @pytest.mark.asyncio
    async def test_no_auto(self) -> None:
        dao = await create_rest_dao(_W, _cfg(), auto_connect=False)
        assert dao._connected is False


class TestPaginatedFetch:
    @pytest.mark.asyncio
    async def test_pages(self) -> None:
        d = MagicMock(spec=RestDAO)
        d.find = AsyncMock(side_effect=[[1, 2, 3], [4]])
        r = await paginated_fetch(d, {"q": "x"}, page_size=_THREE)
        assert len(r) == _FOUR

    @pytest.mark.asyncio
    async def test_empty(self) -> None:
        d = MagicMock(spec=RestDAO)
        d.find = AsyncMock(return_value=[])
        assert await paginated_fetch(d) == []

    @pytest.mark.asyncio
    async def test_clamp(self) -> None:
        d = MagicMock(spec=RestDAO)
        d.find = AsyncMock(return_value=[])
        await paginated_fetch(d, page_size=_OVER_MAX)
        assert d.find.call_args[1]["limit"] == MAX_PAGE_SIZE

    @pytest.mark.asyncio
    async def test_max_records(self) -> None:
        d = MagicMock(spec=RestDAO)
        d.find = AsyncMock(return_value=list(range(DEFAULT_PAGE_SIZE)))
        r = await paginated_fetch(d, max_records=_TWO)
        assert len(r) == _TWO


class TestBatchCreate:
    @pytest.mark.asyncio
    async def test_ids(self) -> None:
        d = MagicMock(spec=RestDAO)
        d.create = AsyncMock(side_effect=["a", "b"])
        assert await batch_create(d, [1, 2]) == ["a", "b"]

    @pytest.mark.asyncio
    async def test_skip_err(self) -> None:
        d = MagicMock(spec=RestDAO)
        d.create = AsyncMock(side_effect=[StorageError("x"), "b"])
        assert await batch_create(d, [1, 2]) == ["b"]

    @pytest.mark.asyncio
    async def test_stop_err(self) -> None:
        d = MagicMock(spec=RestDAO)
        d.create = AsyncMock(side_effect=StorageError("x"))
        with pytest.raises(StorageError):
            await batch_create(d, [1], stop_on_error=True)


class TestBatchUpdate:
    @pytest.mark.asyncio
    async def test_count(self) -> None:
        d = MagicMock(spec=RestDAO)
        d.update = AsyncMock()
        r = await batch_update(d, [{"uid": "a", "v": 1}, {"id": "b", "v": 2}])
        assert r == _TWO

    @pytest.mark.asyncio
    async def test_skip_no_id(self) -> None:
        d = MagicMock(spec=RestDAO)
        d.update = AsyncMock()
        assert await batch_update(d, [{"v": 1}]) == 0

    @pytest.mark.asyncio
    async def test_stop_err(self) -> None:
        d = MagicMock(spec=RestDAO)
        d.update = AsyncMock(side_effect=StorageError("x"))
        with pytest.raises(StorageError):
            await batch_update(d, [{"uid": "a"}], stop_on_error=True)

    @pytest.mark.asyncio
    async def test_tolerate(self) -> None:
        d = MagicMock(spec=RestDAO)
        d.update = AsyncMock(side_effect=StorageError("x"))
        assert await batch_update(d, [{"uid": "a"}]) == 0


class TestBatchDelete:
    @pytest.mark.asyncio
    async def test_count(self) -> None:
        d = MagicMock(spec=RestDAO)
        d.delete = AsyncMock(side_effect=[True, False, True])
        assert await batch_delete(d, ["a", "b", "c"]) == _TWO

    @pytest.mark.asyncio
    async def test_stop_err(self) -> None:
        d = MagicMock(spec=RestDAO)
        d.delete = AsyncMock(side_effect=StorageError("x"))
        with pytest.raises(StorageError):
            await batch_delete(d, ["a"], stop_on_error=True)


class TestUpsert:
    @pytest.mark.asyncio
    async def test_create_new(self) -> None:
        d = MagicMock(spec=RestDAO)
        d.exists = AsyncMock(return_value=False)
        d.create = AsyncMock(return_value="new")
        uid, created = await upsert(d, {"uid": "x", "name": "n"})
        assert uid == "new"
        assert created is True

    @pytest.mark.asyncio
    async def test_update_dict(self) -> None:
        d = MagicMock(spec=RestDAO)
        d.exists = AsyncMock(return_value=True)
        d.update = AsyncMock()
        uid, created = await upsert(d, {"uid": "x1", "name": "u"})
        assert uid == "x1"
        assert created is False

    @pytest.mark.asyncio
    async def test_update_model(self) -> None:
        d = MagicMock(spec=RestDAO)
        d.exists = AsyncMock(return_value=True)
        d.update = AsyncMock()
        uid, created = await upsert(d, _W(uid="m1", name="m"))
        assert uid == "m1"
        assert created is False

    @pytest.mark.asyncio
    async def test_no_uid(self) -> None:
        d = MagicMock(spec=RestDAO)
        d.create = AsyncMock(return_value="g")
        o = MagicMock(spec=[])
        o.uid, o.id = None, None
        uid, created = await upsert(d, o)
        assert uid == "g"
        assert created is True

    @pytest.mark.asyncio
    async def test_update_generic(self) -> None:
        d = MagicMock(spec=RestDAO)
        d.exists = AsyncMock(return_value=True)
        d.update = AsyncMock()
        o = MagicMock()
        o.uid = "g1"
        o.model_dump = MagicMock(return_value={"uid": "g1"})
        uid, created = await upsert(d, o)
        assert uid == "g1"
        assert created is False
