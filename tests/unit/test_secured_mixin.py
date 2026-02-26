"""Tests for SecuredModelMixin: access log, deny-first, expired rules.

Covers Issue #21 -- secured_mixin test coverage, and Issue #35 -- access
log records permission and result.
"""

from datetime import UTC, datetime, timedelta
from typing import ClassVar

import pytest

from ami.core.storage_types import StorageType
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.security import (
    ACLEntry,
    Permission,
    SecurityContext,
)
from ami.models.storage_config import StorageConfig

_MAX_ACCESS_ENTRIES = 100


class _SecuredModel(StorageModel):
    """Minimal secured model for mixin tests."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="secured_test",
        storage_configs={
            "inmem": StorageConfig(storage_type=StorageType.INMEM),
        },
    )

    name: str = ""


class TestOwnerAlwaysGranted:
    """Owner must always pass permission checks."""

    @pytest.fixture
    def ctx(self) -> SecurityContext:
        return SecurityContext(user_id="owner_1", roles=[], groups=[])

    @pytest.mark.asyncio
    async def test_owner_granted_for_every_permission(
        self,
        ctx: SecurityContext,
    ) -> None:
        model = _SecuredModel(name="owned")
        model.owner_id = "owner_1"

        for perm in Permission:
            result = await model.check_permission(
                ctx,
                perm,
                raise_on_deny=False,
            )
            assert result is True, f"Owner should be granted {perm}"


class TestDenyOverridesAllow:
    """DENY rules must be evaluated before ALLOW rules."""

    @pytest.fixture
    def ctx(self) -> SecurityContext:
        return SecurityContext(
            user_id="user_a",
            roles=["viewer"],
            groups=[],
        )

    @pytest.mark.asyncio
    async def test_deny_blocks_despite_allow(
        self,
        ctx: SecurityContext,
    ) -> None:
        model = _SecuredModel(name="contested")
        model.owner_id = "other"
        model.acl = [
            ACLEntry(
                principal_id="user_a",
                principal_type="user",
                permissions=[Permission.READ],
                granted_by="admin",
                is_deny_rule=False,
            ),
            ACLEntry(
                principal_id="user_a",
                principal_type="user",
                permissions=[Permission.READ],
                granted_by="admin",
                is_deny_rule=True,
            ),
        ]

        result = await model.check_permission(
            ctx,
            Permission.READ,
            raise_on_deny=False,
        )
        assert result is False


class TestExpiredRuleIgnored:
    """Expired ACL entries must not affect permission checks."""

    @pytest.fixture
    def ctx(self) -> SecurityContext:
        return SecurityContext(user_id="user_b", roles=[], groups=[])

    @pytest.mark.asyncio
    async def test_expired_deny_is_ignored(
        self,
        ctx: SecurityContext,
    ) -> None:
        model = _SecuredModel(name="doc")
        model.owner_id = "other"
        model.acl = [
            ACLEntry(
                principal_id="user_b",
                principal_type="user",
                permissions=[Permission.READ],
                granted_by="admin",
                is_deny_rule=True,
                expires_at=datetime.now(UTC) - timedelta(hours=1),
            ),
            ACLEntry(
                principal_id="user_b",
                principal_type="user",
                permissions=[Permission.READ],
                granted_by="admin",
                is_deny_rule=False,
            ),
        ]

        result = await model.check_permission(
            ctx,
            Permission.READ,
            raise_on_deny=False,
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_expired_allow_is_ignored(
        self,
        ctx: SecurityContext,
    ) -> None:
        model = _SecuredModel(name="doc")
        model.owner_id = "other"
        model.acl = [
            ACLEntry(
                principal_id="user_b",
                principal_type="user",
                permissions=[Permission.READ],
                granted_by="admin",
                is_deny_rule=False,
                expires_at=datetime.now(UTC) - timedelta(hours=1),
            ),
        ]

        result = await model.check_permission(
            ctx,
            Permission.READ,
            raise_on_deny=False,
        )
        assert result is False


class TestAccessLogRecordsPermissionAndResult:
    """_log_access must store user_id, permission, result, timestamp."""

    @pytest.fixture
    def ctx(self) -> SecurityContext:
        return SecurityContext(user_id="user_c", roles=[], groups=[])

    @pytest.mark.asyncio
    async def test_log_entry_on_grant(
        self,
        ctx: SecurityContext,
    ) -> None:
        model = _SecuredModel(name="logged")
        model.owner_id = "user_c"

        await model.check_permission(ctx, Permission.READ)

        assert len(model.access_log) == 1
        entry = model.access_log[0]
        assert entry["user_id"] == "user_c"
        assert entry["permission"] == Permission.READ.value
        assert entry["result"] == "GRANTED"
        assert "timestamp" in entry

    @pytest.mark.asyncio
    async def test_log_entry_on_deny(
        self,
        ctx: SecurityContext,
    ) -> None:
        model = _SecuredModel(name="logged")
        model.owner_id = "other"
        model.acl = [
            ACLEntry(
                principal_id="user_c",
                principal_type="user",
                permissions=[Permission.READ],
                granted_by="admin",
                is_deny_rule=True,
            ),
        ]

        await model.check_permission(
            ctx,
            Permission.READ,
            raise_on_deny=False,
        )

        assert len(model.access_log) == 1
        entry = model.access_log[0]
        assert entry["result"] == "DENIED"

    @pytest.mark.asyncio
    async def test_log_entry_on_no_match(
        self,
        ctx: SecurityContext,
    ) -> None:
        model = _SecuredModel(name="logged")
        model.owner_id = "other"

        await model.check_permission(
            ctx,
            Permission.READ,
            raise_on_deny=False,
        )

        assert len(model.access_log) == 1
        entry = model.access_log[0]
        assert entry["result"] == "NO_MATCH"


class TestMaxEntriesTrimmed:
    """Access log must be trimmed at _MAX_ACCESS_ENTRIES."""

    @pytest.fixture
    def ctx(self) -> SecurityContext:
        return SecurityContext(user_id="user_d", roles=[], groups=[])

    @pytest.mark.asyncio
    async def test_log_trimmed_to_max(
        self,
        ctx: SecurityContext,
    ) -> None:
        model = _SecuredModel(name="busy")
        model.owner_id = "user_d"

        for _ in range(_MAX_ACCESS_ENTRIES + 10):
            await model.check_permission(ctx, Permission.READ)

        assert len(model.access_log) == _MAX_ACCESS_ENTRIES
