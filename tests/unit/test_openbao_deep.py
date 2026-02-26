"""Deep coverage tests for OpenBaoDAO uncovered branches."""

from __future__ import annotations

from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from hvac.exceptions import VaultError as OpenBaoError

from ami.core.exceptions import StorageConnectionError, StorageError
from ami.core.storage_types import StorageType
from ami.implementations.vault.openbao_dao import OpenBaoDAO
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig

_ONE = 1
_TWO = 2
_BAO = "ami.implementations.vault.openbao_dao.OpenBaoClient"


class _M(StorageModel):
    """Minimal model for tests."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="deep_secrets",
    )
    name: str = ""
    value: int = 0


class _HasDump:
    """Object with model_dump but no to_storage_dict."""

    def __init__(self, uid: str, name: str) -> None:
        self.uid = uid
        self.name = name

    def model_dump(self, mode: str = "json") -> dict[str, Any]:
        return {"uid": self.uid, "name": self.name}


class _Bare:
    """Object convertible via dict() only."""

    def __init__(self, uid: str, name: str) -> None:
        self.uid = uid
        self.name = name

    def __iter__(self):
        yield "uid", self.uid
        yield "name", self.name


def _cfg(**kw: Any) -> StorageConfig:
    d: dict[str, Any] = {
        "storage_type": StorageType.VAULT,
        "host": "127.0.0.1",
        "port": 8200,
        "options": {"token": "t"},
    }
    d.update(kw)
    return StorageConfig(**d)


def _dao(config: StorageConfig | None = None) -> OpenBaoDAO:
    d = OpenBaoDAO(_M, config or _cfg())
    d.client = MagicMock()
    return d


def _vr(data: dict[str, Any]) -> dict[str, Any]:
    return {"data": {"data": data}}


class TestConnectError:
    """Cover connect() error and config branches."""

    async def test_openbao_error_raises(self) -> None:
        dao = OpenBaoDAO(_M, _cfg())
        with (
            patch(_BAO, side_effect=OpenBaoError("boom")),
            pytest.raises(StorageError, match="Failed to connect"),
        ):
            await dao.connect()

    async def test_connection_string_override(self) -> None:
        dao = OpenBaoDAO(_M, _cfg(connection_string="http://c:9"))
        with patch(_BAO) as mc:
            mc.return_value = MagicMock()
            await dao.connect()
        mc.assert_called_once_with(url="http://c:9", token="t")

    async def test_tls_false(self) -> None:
        dao = OpenBaoDAO(_M, _cfg(options={"tls": False, "token": "tk"}))
        with patch(_BAO) as mc:
            mc.return_value = MagicMock()
            await dao.connect()
        assert mc.call_args.kwargs["url"].startswith("http://")

    async def test_password_as_token(self) -> None:
        dao = OpenBaoDAO(_M, _cfg(password="pw", options=None))
        with patch(_BAO) as mc:
            mc.return_value = MagicMock()
            await dao.connect()
        assert mc.call_args.kwargs["token"] == "pw"


class TestTestConnection:
    """Cover test_connection branches."""

    async def test_client_none_triggers_connect(self) -> None:
        dao = OpenBaoDAO(_M, _cfg())
        dao.client = None
        with patch(_BAO) as mc:
            mock_client = MagicMock()
            mock_client.sys.read_health_status = MagicMock()
            mc.return_value = mock_client
            result = await dao.test_connection()
        assert result is True

    async def test_sys_health_status(self) -> None:
        dao = _dao()
        dao.client.sys.read_health_status = MagicMock()
        assert await dao.test_connection() is True

    async def test_error_returns_false(self) -> None:
        dao = _dao()
        dao.client.sys.read_health_status = MagicMock(
            side_effect=OpenBaoError("down"),
        )
        assert await dao.test_connection() is False


class TestCreateEdgeCases:
    """Cover create() model_dump, bare-dict, and error paths."""

    async def test_model_dump_object(self) -> None:
        dao = _dao()
        dao.client.secrets.kv.v2.create_or_update_secret = MagicMock()
        assert await dao.create(_HasDump("p1", "x")) == "p1"

    async def test_bare_dict_conversion(self) -> None:
        dao = _dao()
        dao.client.secrets.kv.v2.create_or_update_secret = MagicMock()
        assert await dao.create(_Bare("b1", "x")) == "b1"

    async def test_openbao_error(self) -> None:
        dao = _dao()
        dao.client.secrets.kv.v2.create_or_update_secret = MagicMock(
            side_effect=OpenBaoError("fail"),
        )
        with pytest.raises(StorageError, match="Failed to create"):
            await dao.create({"uid": "x1", "name": "n"})


class TestFindByIdEdge:
    """Cover find_by_id empty-data and non-dict response."""

    async def test_empty_data_returns_none(self) -> None:
        dao = _dao()
        dao.client.secrets.kv.v2.read_secret_version = MagicMock(
            return_value={"data": {"data": {}}},
        )
        assert await dao.find_by_id("empty") is None

    async def test_non_dict_response_returns_none(self) -> None:
        dao = _dao()
        dao.client.secrets.kv.v2.read_secret_version = MagicMock(
            return_value=None,
        )
        assert await dao.find_by_id("x") is None


class TestFindOneBranches:
    """Cover find_one uid/id shortcuts and find delegation."""

    async def test_uid_key(self) -> None:
        dao = _dao()
        dao.find_by_id = AsyncMock(return_value=_M(name="f"))
        r = await dao.find_one({"uid": "u1"})
        dao.find_by_id.assert_called_once_with("u1")
        assert r is not None

    async def test_id_key(self) -> None:
        dao = _dao()
        dao.find_by_id = AsyncMock(return_value=_M(name="f"))
        r = await dao.find_one({"id": "i1"})
        dao.find_by_id.assert_called_once_with("i1")
        assert r is not None

    async def test_via_find_empty(self) -> None:
        dao = _dao()
        dao.find = AsyncMock(return_value=[])
        assert await dao.find_one({"name": "no"}) is None

    async def test_via_find_hit(self) -> None:
        dao = _dao()
        m = _M(name="hit")
        dao.find = AsyncMock(return_value=[m])
        assert await dao.find_one({"name": "hit"}) is m


def _mock_list_and_read(
    dao: OpenBaoDAO,
    keys: list[str],
    val: int = 0,
) -> None:
    kv = dao.client.secrets.kv.v2
    kv.list_secrets = MagicMock(
        return_value={"data": {"keys": keys}},
    )

    def _read(path: str, mount_point: str) -> dict[str, Any]:
        k = path.rsplit("/", maxsplit=1)[-1]
        return _vr({"uid": k, "name": k, "value": val})

    kv.read_secret_version = MagicMock(side_effect=_read)


class TestFindFilter:
    """Cover find query filtering, skip, limit, error."""

    async def test_skips_none_items(self) -> None:
        dao = _dao()
        kv = dao.client.secrets.kv.v2
        kv.list_secrets = MagicMock(
            return_value={"data": {"keys": ["k1"]}},
        )
        kv.read_secret_version = MagicMock(
            side_effect=OpenBaoError("gone"),
        )
        assert await dao.find({}) == []

    async def test_query_filters_match(self) -> None:
        dao = _dao()
        kv = dao.client.secrets.kv.v2
        kv.list_secrets = MagicMock(
            return_value={"data": {"keys": ["a", "b"]}},
        )

        def _rd(path: str, mount_point: str) -> dict[str, Any]:
            k = path.rsplit("/", maxsplit=1)[-1]
            v = 10 if k == "b" else 5
            return _vr({"uid": k, "name": k, "value": v})

        kv.read_secret_version = MagicMock(side_effect=_rd)
        r = await dao.find({"value": 10})
        assert len(r) == _ONE
        assert r[0].name == "b"

    async def test_skip(self) -> None:
        dao = _dao()
        _mock_list_and_read(dao, ["a", "b", "c"])
        assert len(await dao.find({}, skip=2)) == _ONE

    async def test_limit(self) -> None:
        dao = _dao()
        _mock_list_and_read(dao, ["a", "b", "c"])
        assert len(await dao.find({}, limit=1)) == _ONE

    async def test_list_error_raises(self) -> None:
        dao = _dao()
        dao.client.secrets.kv.v2.list_secrets = MagicMock(
            side_effect=OpenBaoError("err"),
        )
        with pytest.raises(StorageConnectionError):
            await dao.find({})

    async def test_query_on_model_item(self) -> None:
        dao = _dao()
        _mock_list_and_read(dao, ["x"])
        r = await dao.find({"name": "x"})
        assert len(r) == _ONE


class TestUpdateEdge:
    """Cover update dict/else merge and write error."""

    async def test_update_dict_existing(self) -> None:
        dao = _dao()
        kv = dao.client.secrets.kv.v2
        kv.read_secret_version = MagicMock(
            return_value=_vr({"uid": "d1", "name": "o", "value": 1}),
        )
        kv.create_or_update_secret = MagicMock()
        await dao.update("d1", {"name": "n"})
        kv.create_or_update_secret.assert_called_once()

    async def test_update_write_error(self) -> None:
        dao = _dao()
        kv = dao.client.secrets.kv.v2
        kv.read_secret_version = MagicMock(
            return_value=_vr({"uid": "e1", "name": "o", "value": 0}),
        )
        kv.create_or_update_secret = MagicMock(
            side_effect=OpenBaoError("err"),
        )
        with pytest.raises(StorageError, match="Failed to update"):
            await dao.update("e1", {"name": "bad"})


class TestBulkUpdate:
    """Cover bulk_update logic."""

    async def test_processes_entries(self) -> None:
        dao = _dao()
        dao.update = AsyncMock()
        await dao.bulk_update(
            [
                {"uid": "u1", "name": "a"},
                {"id": "u2", "name": "b"},
            ]
        )
        assert dao.update.call_count == _TWO

    async def test_skips_no_id(self) -> None:
        dao = _dao()
        dao.update = AsyncMock()
        await dao.bulk_update([{"name": "orphan"}])
        dao.update.assert_not_called()


class TestCreateIndexes:
    """Cover create_indexes no-op."""

    async def test_noop(self) -> None:
        await _dao().create_indexes()


class TestRawQueryErrors:
    """Cover raw_read_query and raw_write_query error paths."""

    async def test_raw_read_error(self) -> None:
        dao = _dao()
        dao.client.secrets.kv.v2.read_secret_version = MagicMock(
            side_effect=OpenBaoError("err"),
        )
        with pytest.raises(StorageError, match="Raw read failed"):
            await dao.raw_read_query("bad")

    async def test_raw_write_error(self) -> None:
        dao = _dao()
        dao.client.secrets.kv.v2.create_or_update_secret = MagicMock(
            side_effect=OpenBaoError("err"),
        )
        with pytest.raises(StorageError, match="Raw write failed"):
            await dao.raw_write_query("bad", {"k": "v"})


class TestListDatabases:
    """Cover list_databases branches."""

    async def test_returns_mounts(self) -> None:
        dao = _dao()
        dao.client.sys.list_mounted_secrets_engines = MagicMock(
            return_value={"data": {"secret/": {}, "pki/": {}}},
        )
        r = await dao.list_databases()
        assert "secret" in r
        assert "pki" in r

    async def test_no_data_key(self) -> None:
        dao = _dao()
        dao.client.sys.list_mounted_secrets_engines = MagicMock(
            return_value={"kv/": {}, "transit/": {}},
        )
        assert "kv" in await dao.list_databases()

    async def test_error_returns_mount(self) -> None:
        dao = _dao()
        dao.client.sys.list_mounted_secrets_engines = MagicMock(
            side_effect=OpenBaoError("denied"),
        )
        assert await dao.list_databases() == ["secret"]

    async def test_no_sys_attr(self) -> None:
        dao = _dao()
        del dao.client.sys
        assert await dao.list_databases() == ["secret"]


class TestListSchemas:
    """Cover list_schemas branches."""

    async def test_returns_keys(self) -> None:
        dao = _dao()
        dao.client.secrets.kv.v2.list_secrets = MagicMock(
            return_value={"data": {"keys": ["apps/", "cfg/"]}},
        )
        assert await dao.list_schemas() == ["apps", "cfg"]

    async def test_custom_database(self) -> None:
        dao = _dao()
        dao.client.secrets.kv.v2.list_secrets = MagicMock(
            return_value={"data": {"keys": ["ns/"]}},
        )
        assert await dao.list_schemas(database="x") == ["ns"]

    async def test_error_raises(self) -> None:
        dao = _dao()
        dao.client.secrets.kv.v2.list_secrets = MagicMock(
            side_effect=OpenBaoError("err"),
        )
        with pytest.raises(StorageConnectionError):
            await dao.list_schemas()


class TestListModels:
    """Cover list_models branches."""

    async def test_returns_keys(self) -> None:
        dao = _dao()
        dao.client.secrets.kv.v2.list_secrets = MagicMock(
            return_value={"data": {"keys": ["i1", "i2/"]}},
        )
        assert await dao.list_models() == ["i1", "i2"]

    async def test_custom_db_schema(self) -> None:
        dao = _dao()
        dao.client.secrets.kv.v2.list_secrets = MagicMock(
            return_value={"data": {"keys": ["m1"]}},
        )
        r = await dao.list_models(database="db", schema="sc")
        assert r == ["m1"]

    async def test_error_raises(self) -> None:
        dao = _dao()
        dao.client.secrets.kv.v2.list_secrets = MagicMock(
            side_effect=OpenBaoError("err"),
        )
        with pytest.raises(StorageConnectionError):
            await dao.list_models()


class TestGetModelInfo:
    """Cover get_model_info."""

    async def test_default(self) -> None:
        info = await _dao().get_model_info("sec/path")
        assert info["name"] == "sec/path"
        assert info["type"] == "vault_secret"
        assert info["mount"] == "secret"

    async def test_custom_db_schema(self) -> None:
        info = await _dao().get_model_info("p", database="db", schema="sc")
        assert info["mount"] == "db"
        assert info["schema"] == "sc"


class TestGetModelSchema:
    """Cover get_model_schema field iteration."""

    async def test_returns_fields(self) -> None:
        s = await _dao().get_model_schema("test")
        assert s["name"] == "test"
        assert "name" in s["fields"]
        assert "value" in s["fields"]

    async def test_field_has_type_and_required(self) -> None:
        s = await _dao().get_model_schema("test")
        assert "type" in s["fields"]["name"]
        assert "required" in s["fields"]["name"]


class TestGetModelFields:
    """Cover get_model_fields."""

    async def test_returns_list(self) -> None:
        fields = await _dao().get_model_fields("test")
        names = [f["name"] for f in fields]
        assert "name" in names
        assert "value" in names


class TestGetModelIndexes:
    """Cover get_model_indexes empty return."""

    async def test_empty(self) -> None:
        assert await _dao().get_model_indexes("test") == []


class TestReferenceEdge:
    """Additional _reference validation edge cases."""

    def test_space_rejected(self) -> None:
        with pytest.raises(StorageError):
            _dao()._reference("has space")

    def test_nested_path_valid(self) -> None:
        assert _dao()._reference("sub/i_1") == "deep_secrets/sub/i_1"

    def test_empty_rejected(self) -> None:
        with pytest.raises(StorageError):
            _dao()._reference("")


class TestInitOptions:
    """Cover __init__ mount extraction."""

    def test_custom_mount(self) -> None:
        c = _cfg(options={"mount": "kv", "token": "t"})
        assert OpenBaoDAO(_M, c)._mount == "kv"

    def test_no_options(self) -> None:
        assert OpenBaoDAO(_M, _cfg(options=None))._mount == "secret"

    def test_no_config(self) -> None:
        assert OpenBaoDAO(_M, config=None)._mount == "secret"
