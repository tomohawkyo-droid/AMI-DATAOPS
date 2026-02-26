"""Deep coverage for RestDAO edges and EmbeddingService sync paths."""

from __future__ import annotations

import sys
import types
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.core.exceptions import QueryError, StorageError
from ami.core.storage_types import StorageType
from ami.implementations.embedding_service import EmbeddingService
from ami.implementations.rest.rest_dao import RestDAO
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

_PT = "ami.implementations.rest.rest_dao.request_with_retry"
_DP = "ami.implementations.rest.rest_discovery"
_OK = 200
_CREATED = 201
_NO_CONTENT = 204
_BAD_REQ = 400
_NOT_FOUND = 404
_SRV_ERR = 500
_EDIM = 3
_TWO = 2
_SEVEN = 7
_FIVE = 5


class _W(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(path="w")
    name: str = "default"


def _cfg(**kw: Any) -> StorageConfig:
    d: dict[str, Any] = {
        "storage_type": StorageType.REST,
        "host": "h",
        "port": 8080,
    }
    d.update(kw)
    return StorageConfig(**d)


def _dao(cfg: StorageConfig | None = None) -> RestDAO:
    return RestDAO(model_cls=_W, config=cfg or _cfg())


def _r(status: int = _OK, js: Any = None) -> MagicMock:
    r = MagicMock()
    r.status = status
    r.json = AsyncMock(return_value=js)
    r.text = AsyncMock(return_value="err")
    r.__aenter__ = AsyncMock(return_value=r)
    r.__aexit__ = AsyncMock(return_value=False)
    return r


class TestCreateBranches:
    @patch(_PT, new_callable=AsyncMock)
    async def test_dict(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_CREATED, {"uid": "d1"})
        assert await dao.create({"name": "x"}) == "d1"
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_generic(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_CREATED, {"uid": "g1"})
        obj = MagicMock()
        obj.model_dump = MagicMock(return_value={"name": "g"})
        assert await dao.create(obj) == "g1"
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_scalar_id(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_CREATED, "sc-99")
        assert await dao.create({"name": "x"}) == "sc-99"
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_no_id(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_CREATED, {"other": "v"})
        with pytest.raises(QueryError, match="no ID"):
            await dao.create({"name": "x"})
        await dao.disconnect()


class TestFindBranches:
    @patch(_PT, new_callable=AsyncMock)
    async def test_by_id_500(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_SRV_ERR)
        with pytest.raises(StorageError, match="find_by_id"):
            await dao.find_by_id("x")
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_by_id_non_dict(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_OK, "plain")
        assert await dao.find_by_id("s") == "plain"
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_one_first(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_OK, {"data": [{"uid": "f", "name": "a"}]})
        assert isinstance(await dao.find_one({"q": "x"}), _W)
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_one_empty(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_OK, {"data": []})
        assert await dao.find_one({"q": "x"}) is None
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_limit_skip(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_OK, {"data": [{"uid": "a", "name": "a"}]})
        await dao.find({"q": "x"}, limit=10, skip=5)
        p = m.call_args.kwargs.get("params", {})
        assert p["limit"] == "10"
        assert p["offset"] == "5"
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_error(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_BAD_REQ)
        with pytest.raises(StorageError, match="find failed"):
            await dao.find({"q": "x"})
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_non_list_wrap(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_OK, {"uid": "s", "name": "solo"})
        assert len(await dao.find({"q": "x"})) == 1
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_non_dict_item(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_OK, {"data": ["raw"]})
        assert await dao.find({"q": "x"}) == ["raw"]
        await dao.disconnect()


class TestMutationErrors:
    @patch(_PT, new_callable=AsyncMock)
    async def test_update_err(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_SRV_ERR)
        with pytest.raises(StorageError, match="update failed"):
            await dao.update("u", {"n": "x"})
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_update_put(self, m: AsyncMock) -> None:
        dao = _dao(_cfg(options={"update_method": "PUT"}))
        await dao.connect()
        m.return_value = _r(_OK)
        await dao.update("u", {"n": "x"})
        assert m.call_args[0][1] == "PUT"
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_delete_err(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_SRV_ERR)
        with pytest.raises(StorageError, match="delete failed"):
            await dao.delete("d")
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_count_find(self, m: AsyncMock) -> None:
        m.side_effect = [
            _r(_NOT_FOUND),
            _r(_OK, {"data": [{"uid": "a"}, {"uid": "b"}]}),
        ]
        dao = _dao()
        await dao.connect()
        assert await dao.count({"q": "x"}) == _TWO
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_count_int(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_OK, {"data": 7})
        assert await dao.count({"q": "x"}) == _SEVEN
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_exists_head_fail(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.side_effect = [StorageError("H"), _r(_OK, {"uid": "e", "name": "f"})]
        assert await dao.exists("e") is True
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_bulk_update_skip(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_OK)
        await dao.bulk_update(
            [
                {"uid": "a", "name": "u"},
                {"no_id": "s"},
                {"id": "b", "name": "v"},
            ]
        )
        assert m.await_count == _TWO
        await dao.disconnect()

    async def test_create_indexes(self) -> None:
        await _dao().create_indexes()


class TestRawQueryEdges:
    @patch(_PT, new_callable=AsyncMock)
    async def test_read_err(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_SRV_ERR)
        with pytest.raises(StorageError, match="raw_read"):
            await dao.raw_read_query("p")
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_read_non_list(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_OK, {"id": 1})
        assert await dao.raw_read_query("s") == [{"id": 1}]
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_read_empty(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_OK, None)
        assert await dao.raw_read_query("e") == []
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_write_err(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_SRV_ERR)
        with pytest.raises(StorageError, match="raw_write"):
            await dao.raw_write_query("p")
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_write_204(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_NO_CONTENT)
        assert await dao.raw_write_query("d") == 1
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_write_int(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_OK, {"data": 5})
        assert await dao.raw_write_query("q") == _FIVE
        await dao.disconnect()

    @patch(_PT, new_callable=AsyncMock)
    async def test_write_non_int(self, m: AsyncMock) -> None:
        dao = _dao()
        await dao.connect()
        m.return_value = _r(_OK, "ok")
        assert await dao.raw_write_query("q") == 1
        await dao.disconnect()


class TestDiscovery:
    @patch(f"{_DP}.list_databases", new_callable=AsyncMock)
    async def test_databases(self, m: AsyncMock) -> None:
        m.return_value = ["db1"]
        assert await _dao().list_databases() == ["db1"]

    @patch(f"{_DP}.list_schemas", new_callable=AsyncMock)
    async def test_schemas(self, m: AsyncMock) -> None:
        m.return_value = ["pub"]
        assert await _dao().list_schemas(database="d") == ["pub"]

    @patch(f"{_DP}.list_models", new_callable=AsyncMock)
    async def test_models(self, m: AsyncMock) -> None:
        m.return_value = ["u"]
        assert await _dao().list_models("d", "s") == ["u"]

    @patch(f"{_DP}.get_model_info", new_callable=AsyncMock)
    async def test_info(self, m: AsyncMock) -> None:
        m.return_value = {"name": "w"}
        assert (await _dao().get_model_info("w", "d", "s"))["name"] == "w"

    @patch(f"{_DP}.get_model_schema", new_callable=AsyncMock)
    async def test_schema(self, m: AsyncMock) -> None:
        m.return_value = {"type": "object"}
        assert (await _dao().get_model_schema("w", "d", "s"))["type"] == "object"

    @patch(f"{_DP}.get_model_fields", new_callable=AsyncMock)
    async def test_fields(self, m: AsyncMock) -> None:
        m.return_value = [{"name": "id"}]
        assert await _dao().get_model_fields("w", "d", "s") == [{"name": "id"}]

    @patch(f"{_DP}.get_model_indexes", new_callable=AsyncMock)
    async def test_indexes(self, m: AsyncMock) -> None:
        m.return_value = [{"field": "n"}]
        assert await _dao().get_model_indexes("w", "d", "s") == [{"field": "n"}]

    @patch(f"{_DP}.test_connection", new_callable=AsyncMock)
    async def test_conn(self, m: AsyncMock) -> None:
        m.return_value = True
        assert await _dao().test_connection() is True


# -- Embedding --


def _torch_mod() -> types.ModuleType:
    mod = types.ModuleType("torch")
    t = MagicMock()
    t.__getitem__ = MagicMock(
        return_value=MagicMock(tolist=MagicMock(return_value=[0.1, 0.2, 0.3])),
    )
    t.__iter__ = MagicMock(
        return_value=iter(
            [
                MagicMock(tolist=MagicMock(return_value=[0.1, 0.2, 0.3])),
                MagicMock(tolist=MagicMock(return_value=[0.4, 0.5, 0.6])),
            ]
        )
    )
    nn = MagicMock()
    nn.functional.normalize = MagicMock(return_value=t)
    mod.nn = nn
    mod.sum = MagicMock(return_value=t)
    mod.clamp = MagicMock(return_value=t)
    return mod


def _mt() -> tuple[MagicMock, MagicMock]:
    tens = MagicMock()
    tens.size = MagicMock(return_value=(_TWO, 10, _EDIM))
    tens.float = MagicMock(return_value=tens)
    out = MagicMock()
    out.__getitem__ = MagicMock(return_value=tens)
    model = MagicMock(return_value=out)
    mask = MagicMock()
    mask.unsqueeze = MagicMock(
        return_value=MagicMock(
            expand=MagicMock(
                return_value=MagicMock(
                    float=MagicMock(return_value=tens),
                    sum=MagicMock(return_value=tens),
                )
            ),
        )
    )
    tok_r = MagicMock()
    tok_r.__getitem__ = MagicMock(return_value=mask)
    return model, MagicMock(return_value=tok_r)


def _svc() -> EmbeddingService:
    s = EmbeddingService()
    s._model, s._tokenizer = _mt()
    return s


@pytest.fixture(autouse=True)
def _reset_emb() -> None:
    EmbeddingService._instance = None


@pytest.fixture
def _torch() -> None:
    sys.modules["torch"] = _torch_mod()


class TestEmbGetModel:
    def test_loads(self) -> None:
        svc = EmbeddingService()
        model, tok = _mt()
        ort = MagicMock()
        ort.from_pretrained = MagicMock(return_value=model)
        at = MagicMock()
        at.from_pretrained = MagicMock(return_value=tok)
        with patch.dict(
            sys.modules,
            {
                "optimum": MagicMock(),
                "optimum.onnxruntime": MagicMock(ORTModelForFeatureExtraction=ort),
                "transformers": MagicMock(AutoTokenizer=at),
            },
        ):
            m, t = svc._get_model()
            assert m is model
            assert t is tok

    def test_cached(self) -> None:
        svc = _svc()
        assert svc._get_model()[0] is svc._model


class TestEmbPoolAndGen:
    @pytest.mark.usefixtures("_torch")
    def test_mean_pooling(self) -> None:
        svc = EmbeddingService()
        mout, te = MagicMock(), MagicMock()
        te.size = MagicMock(return_value=(1, 5, _EDIM))
        mout.__getitem__ = MagicMock(return_value=te)
        am, ex = MagicMock(), MagicMock()
        ex.float = MagicMock(return_value=ex)
        ex.sum = MagicMock(return_value=ex)
        am.unsqueeze = MagicMock(
            return_value=MagicMock(expand=MagicMock(return_value=ex)),
        )
        assert svc._mean_pooling(mout, am) is not None

    @pytest.mark.usefixtures("_torch")
    def test_sync_single(self) -> None:
        r = _svc()._generate_embedding_sync("hi")
        assert isinstance(r, list)
        assert len(r) == _EDIM

    @pytest.mark.usefixtures("_torch")
    async def test_async_single(self) -> None:
        r = await _svc().generate_embedding("hi")
        assert isinstance(r, list)
        assert len(r) == _EDIM

    @pytest.mark.usefixtures("_torch")
    def test_sync_batch(self) -> None:
        r = _svc()._generate_embeddings_sync(["a", "b"])
        assert isinstance(r, list)
        assert len(r) == _TWO

    @pytest.mark.usefixtures("_torch")
    async def test_async_batch(self) -> None:
        r = await _svc().generate_embeddings(["a", "b"])
        assert isinstance(r, list)
        assert len(r) == _TWO


class TestEmbFromDict:
    @pytest.mark.usefixtures("_torch")
    async def test_strings(self) -> None:
        assert isinstance(await _svc().generate_from_dict({"t": "h"}), list)

    @pytest.mark.usefixtures("_torch")
    async def test_list_vals(self) -> None:
        assert isinstance(await _svc().generate_from_dict({"x": ["a"]}), list)

    @pytest.mark.usefixtures("_torch")
    async def test_list_dicts(self) -> None:
        r = await _svc().generate_from_dict({"i": [{"text": "n"}]})
        assert isinstance(r, list)

    @pytest.mark.usefixtures("_torch")
    async def test_nested(self) -> None:
        assert isinstance(await _svc().generate_from_dict({"m": {"a": "A"}}), list)

    async def test_empty_raises(self) -> None:
        with pytest.raises(ValueError, match="No text"):
            await EmbeddingService().generate_from_dict({"n": 42})
