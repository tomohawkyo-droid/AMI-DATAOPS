"""Tests for types, secured_mixin, storage_config, and dao modules."""

from __future__ import annotations

from typing import ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.core.dao import (
    BaseDAO,
    DAOFactory,
    _merge_with_yaml_defaults,
    get_dao,
    get_dao_class,
    register_dao,
)
from ami.core.exceptions import StorageError
from ami.core.storage_types import StorageType
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.security import (
    ACLEntry,
    DataClassification,
    Permission,
    SecurityContext,
)
from ami.models.storage_config import StorageConfig
from ami.models.types import AuthProviderType, TokenType

_PG = 5432
_MONGO = 27017
_PROM = 9090
_DG = 9080
_RD = 6379
_REST_P = 443
_VLT = 8200
_CUST = 9999
_AUTH_CT = 8
_TOKEN_CT = 4


class _TM(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="test_items",
    )
    name: str = "default"


def _uctx(
    uid: str = "user_a",
    roles: list[str] | None = None,
    groups: list[str] | None = None,
    tenant_id: str | None = None,
) -> SecurityContext:
    return SecurityContext(
        user_id=uid,
        roles=roles or [],
        groups=groups or [],
        tenant_id=tenant_id,
    )


def _acl(
    pid: str,
    ptype: str = "user",
    perms: list[Permission] | None = None,
    deny: bool = False,
) -> ACLEntry:
    return ACLEntry(
        principal_id=pid,
        principal_type=ptype,
        permissions=perms or [Permission.READ],
        granted_by="admin",
        is_deny_rule=deny,
    )


# =============== types.py ===============


class TestTypes:
    def test_auth_provider(self) -> None:
        assert len(AuthProviderType) == _AUTH_CT
        assert AuthProviderType.GOOGLE == "google"
        assert AuthProviderType.SSH == "ssh"

    def test_token(self) -> None:
        assert len(TokenType) == _TOKEN_CT
        assert TokenType.ACCESS == "access"
        assert TokenType.ID_TOKEN == "id_token"


# =============== secured_mixin.py ===============


class TestCheckPermissionRaises:
    @pytest.mark.asyncio
    async def test_deny_raises(self) -> None:
        m = _TM(name="s")
        m.owner_id = "other"
        m.acl = [
            _acl("user_a", perms=[Permission.WRITE], deny=True),
        ]
        with pytest.raises(PermissionError, match="denied"):
            await m.check_permission(
                _uctx(),
                Permission.WRITE,
                raise_on_deny=True,
            )

    @pytest.mark.asyncio
    async def test_no_match_raises(self) -> None:
        m = _TM(name="e")
        m.owner_id = "other"
        with pytest.raises(PermissionError, match="No perm"):
            await m.check_permission(
                _uctx(),
                Permission.DELETE,
                raise_on_deny=True,
            )


class TestMatchesPrincipal:
    @pytest.mark.asyncio
    async def test_role(self) -> None:
        m = _TM(name="r")
        m.owner_id = "other"
        m.acl = [_acl("editor", "role", [Permission.WRITE])]
        r = await m.check_permission(
            _uctx(roles=["editor"]),
            Permission.WRITE,
            raise_on_deny=False,
        )
        assert r is True

    @pytest.mark.asyncio
    async def test_group(self) -> None:
        m = _TM(name="g")
        m.owner_id = "other"
        m.acl = [_acl("eng", "group")]
        r = await m.check_permission(
            _uctx(groups=["eng"]),
            Permission.READ,
            raise_on_deny=False,
        )
        assert r is True

    @pytest.mark.asyncio
    async def test_service(self) -> None:
        m = _TM(name="sv")
        m.owner_id = "other"
        m.acl = [_acl("svc", "service", [Permission.EXECUTE])]
        ctx = _uctx("svc", roles=["svc"])
        r = await m.check_permission(
            ctx,
            Permission.EXECUTE,
            raise_on_deny=False,
        )
        assert r is True


class TestGrantRevoke:
    @pytest.mark.asyncio
    async def test_grants(self) -> None:
        m = _TM(name="g")
        m.owner_id = "adm"
        e = await m.grant_permission(
            _uctx("adm"),
            "r1",
            [Permission.READ],
        )
        assert e.principal_id == "r1"

    @pytest.mark.asyncio
    async def test_grant_denied(self) -> None:
        m = _TM(name="n")
        m.owner_id = "other"
        with pytest.raises(PermissionError, match="admin"):
            await m.grant_permission(
                _uctx("out"),
                "r2",
                [Permission.READ],
            )

    @pytest.mark.asyncio
    async def test_revokes(self) -> None:
        m = _TM(name="rv")
        m.owner_id = "adm"
        m.acl = [_acl("r1")]
        r = await m.revoke_permission(_uctx("adm"), "r1")
        assert r is True

    @pytest.mark.asyncio
    async def test_revoke_miss(self) -> None:
        m = _TM(name="ne")
        m.owner_id = "adm"
        r = await m.revoke_permission(_uctx("adm"), "ghost")
        assert r is False

    @pytest.mark.asyncio
    async def test_revoke_denied(self) -> None:
        m = _TM(name="n")
        m.owner_id = "other"
        with pytest.raises(PermissionError):
            await m.revoke_permission(_uctx("out"), "x")


class TestSetOwnerAndClassification:
    def test_adds_admin(self) -> None:
        m = _TM(name="o")
        m.set_owner("u1")
        assert m.owner_id == "u1"
        assert any(
            a.principal_id == "u1" and Permission.ADMIN in a.permissions for a in m.acl
        )

    def test_skip_dup(self) -> None:
        m = _TM(name="d")
        m.acl = [_acl("u1", perms=[Permission.ADMIN])]
        m.set_owner("u1")
        ct = sum(
            1
            for a in m.acl
            if a.principal_id == "u1" and Permission.ADMIN in a.permissions
        )
        assert ct == 1

    def test_classification(self) -> None:
        m = _TM(name="c")
        m.set_classification(DataClassification.RESTRICTED)
        assert m.classification == DataClassification.RESTRICTED.value


class TestRowLevelSecurity:
    def test_tenant(self) -> None:
        r = _TM().apply_row_level_security(
            {"s": "a"},
            _uctx(tenant_id="t"),
        )
        assert "$and" in r

    def test_empty(self) -> None:
        r = _TM().apply_row_level_security(
            {},
            _uctx(tenant_id="t"),
        )
        assert r == {"tenant_id": "t"}

    def test_no_tenant(self) -> None:
        q = {"s": "a"}
        assert _TM().apply_row_level_security(q, _uctx()) == q


# =============== storage_config.py ===============


class TestFromDict:
    def test_valid(self) -> None:
        c = StorageConfig.from_dict(
            {"type": "document", "host": "m", "database": "d"},
        )
        assert c.storage_type == StorageType.DOCUMENT

    def test_unknown(self) -> None:
        with pytest.raises(ValueError, match="Unknown"):
            StorageConfig.from_dict({"type": "alien"})

    def test_db_str(self) -> None:
        c = StorageConfig.from_dict(
            {"type": "inmem", "host": "r", "database": 5},
        )
        assert c.database == "5"


class TestDefaultPorts:
    @pytest.mark.parametrize(
        ("stype", "expected"),
        [
            (StorageType.RELATIONAL, _PG),
            (StorageType.DOCUMENT, _MONGO),
            (StorageType.TIMESERIES, _PROM),
            (StorageType.VECTOR, _PG),
            (StorageType.GRAPH, _DG),
            (StorageType.INMEM, _RD),
            (StorageType.REST, _REST_P),
            (StorageType.VAULT, _VLT),
        ],
    )
    def test_default(
        self,
        stype: StorageType,
        expected: int,
    ) -> None:
        cfg = StorageConfig(storage_type=stype)
        assert cfg.port == expected

    def test_explicit_and_none(self) -> None:
        c = StorageConfig(
            storage_type=StorageType.INMEM,
            port=_CUST,
        )
        assert c.port == _CUST
        assert StorageConfig().port is None


class TestGetConnectionString:
    def test_relational(self) -> None:
        c = StorageConfig(
            storage_type=StorageType.RELATIONAL,
            host="h",
            port=_PG,
            database="d",
            username="u",
            password="p",
        )
        assert "postgresql+asyncpg://" in c.get_connection_string()

    def test_inmem_and_graph(self) -> None:
        c1 = StorageConfig(
            storage_type=StorageType.INMEM,
            host="h",
            port=_RD,
        )
        assert c1.get_connection_string().startswith("redis://")
        c2 = StorageConfig(
            storage_type=StorageType.GRAPH,
            host="h",
            port=_DG,
        )
        assert c2.get_connection_string() == f"h:{_DG}"

    def test_rest_variants(self) -> None:
        c1 = StorageConfig(
            storage_type=StorageType.REST,
            host="h",
            port=_REST_P,
            database="v1",
        )
        cs1 = c1.get_connection_string()
        assert cs1.startswith("https://")
        assert "v1" in cs1
        c2 = StorageConfig(
            storage_type=StorageType.REST,
            host="h",
            port=8080,
        )
        assert c2.get_connection_string().startswith("http://")

    def test_override(self) -> None:
        c = StorageConfig(
            storage_type=StorageType.RELATIONAL,
            connection_string="custom://x",
        )
        assert c.get_connection_string() == "custom://x"

    def test_errors(self) -> None:
        with pytest.raises(ValueError, match="type not set"):
            StorageConfig().get_connection_string()
        c = StorageConfig(
            storage_type=StorageType.VAULT,
            host="h",
            port=_VLT,
        )
        with pytest.raises(ValueError, match="Unsupported"):
            c.get_connection_string()


# =============== dao.py ===============

# Create a concrete BaseDAO subclass by clearing abstract methods.
_FakeDAO = type("_FakeDAO", (BaseDAO,), {})
_FakeDAO.__abstractmethods__ = frozenset()


class TestDaoRegistry:
    def test_register_get(self) -> None:
        register_dao(StorageType.FILE, _FakeDAO)
        assert get_dao_class(StorageType.FILE) is _FakeDAO

    def test_unregistered(self) -> None:
        with pytest.raises(StorageError, match="No DAO"):
            get_dao_class(StorageType.VAULT)

    def test_factory_creates(self) -> None:
        register_dao(StorageType.FILE, _FakeDAO)
        cfg = StorageConfig(
            storage_type=StorageType.FILE,
            host="h",
        )
        assert isinstance(DAOFactory.create(_TM, cfg), _FakeDAO)

    def test_factory_none_type(self) -> None:
        with pytest.raises(StorageError, match="cannot be None"):
            DAOFactory.create(_TM, StorageConfig())

    def test_get_dao(self) -> None:
        register_dao(StorageType.FILE, _FakeDAO)
        cfg = StorageConfig(
            storage_type=StorageType.FILE,
            host="h",
        )
        assert isinstance(get_dao(_TM, cfg), _FakeDAO)


class TestMergeWithYamlDefaults:
    def test_rest_passthrough(self) -> None:
        c = StorageConfig(
            storage_type=StorageType.REST,
            host="h",
        )
        assert _merge_with_yaml_defaults(StorageType.REST, c) is c

    @patch("ami.core.dao.StorageConfigFactory.from_yaml")
    def test_merge(self, mock_yaml: MagicMock) -> None:
        mock_yaml.return_value = StorageConfig(
            storage_type=StorageType.INMEM,
            host="yh",
            port=_RD,
            username="yu",
            password="yp",
            database="0",
        )
        r = _merge_with_yaml_defaults(
            StorageType.INMEM,
            StorageConfig(storage_type=StorageType.INMEM),
        )
        assert r.host == "yh"


class TestFindOrCreate:
    @pytest.mark.asyncio
    async def test_finds(self) -> None:
        d = _FakeDAO(_TM)
        d.find_one = AsyncMock(
            return_value=_TM(uid="e", name="f"),
        )
        r, created = await d.find_or_create({"name": "f"})
        assert not created
        assert r.uid == "e"

    @pytest.mark.asyncio
    async def test_creates(self) -> None:
        d = _FakeDAO(_TM)
        d.find_one = AsyncMock(return_value=None)
        d.create = AsyncMock(return_value="n")
        r, created = await d.find_or_create({"name": "n"})
        assert created
        assert r.uid == "n"


class TestUpdateOrCreate:
    @pytest.mark.asyncio
    async def test_updates(self) -> None:
        d = _FakeDAO(_TM)
        d.find_one = AsyncMock(
            return_value=_TM(uid="u1", name="old"),
        )
        d.update = AsyncMock()
        d.find_by_id = AsyncMock(
            return_value=_TM(uid="u1", name="new"),
        )
        r, created = await d.update_or_create(
            {"name": "old"},
            {"name": "new"},
        )
        assert not created
        assert r.name == "new"

    @pytest.mark.asyncio
    async def test_creates(self) -> None:
        d = _FakeDAO(_TM)
        d.find_one = AsyncMock(return_value=None)
        d.create = AsyncMock(return_value="c1")
        _, created = await d.update_or_create({"name": "x"})
        assert created

    @pytest.mark.asyncio
    async def test_find_by_id_none(self) -> None:
        d = _FakeDAO(_TM)
        d.find_one = AsyncMock(
            return_value=_TM(uid="u2", name="o"),
        )
        d.update = AsyncMock()
        d.find_by_id = AsyncMock(return_value=None)
        d.create = AsyncMock(return_value="c2")
        _, created = await d.update_or_create(
            {"name": "o"},
            {"name": "n"},
        )
        assert created


class TestCollectionName:
    def test_from_meta(self) -> None:
        assert _FakeDAO(_TM).collection_name == "test_items"

    def test_from_class(self) -> None:
        class Gadget(StorageModel):
            _model_meta: ClassVar[ModelMetadata] = ModelMetadata()

        assert _FakeDAO(Gadget).collection_name == "gadgets"
