"""Integration tests for secrets management lifecycle.

Exercises InMemorySecretsBackend (real implementation, not mocked),
SecretPointerRepository (with mock UnifiedCRUD), integrity hashing,
and environment-based key/salt resolution.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from ami.models.secret_pointer import SecretPointerRecord
from ami.models.security import DataClassification
from ami.secrets.client import (
    InMemorySecretsBackend,
    _get_integrity_salt,
    _get_master_key,
    compute_integrity_hash,
)
from ami.secrets.pointer import VaultFieldPointer
from ami.secrets.repository import (
    EnsureRecordParams,
    SecretPointerRepository,
    parse_classification,
)

# Named constants to satisfy ruff (no magic numbers).
VERSION_ONE = 1
VERSION_TWO = 2
ROTATION_ZERO = 0
ROTATION_ONE = 1
HASH_HEX_LENGTH = 64


# -- helpers ---------------------------------------------------------


def _backend() -> InMemorySecretsBackend:
    """Create a fresh in-memory backend with a fixed key."""
    return InMemorySecretsBackend(master_key=b"integration-key")


def _make_params(**overrides: object) -> EnsureRecordParams:
    """Build EnsureRecordParams with sensible defaults."""
    defaults: dict[str, object] = {
        "reference": "vault/integration/api-key",
        "namespace": "integration",
        "model_name": "Credential",
        "field_name": "api_key",
        "integrity_hash": "hash-aaa",
    }
    defaults.update(overrides)
    return EnsureRecordParams(**defaults)


def _make_record(**overrides: object) -> SecretPointerRecord:
    """Build a SecretPointerRecord with sensible defaults."""
    defaults: dict[str, object] = {
        "uid": "rec-int-001",
        "vault_reference": "vault/integration/api-key",
        "namespace": "integration",
        "model_name": "Credential",
        "field_name": "api_key",
        "integrity_hash": "hash-aaa",
        "version": 1,
        "rotation_count": 0,
        "secret_created_at": datetime(2025, 6, 1, tzinfo=UTC),
        "secret_updated_at": datetime(2025, 6, 1, tzinfo=UTC),
    }
    defaults.update(overrides)
    return SecretPointerRecord(**defaults)


# ================================================================
# InMemorySecretsBackend full lifecycle
# ================================================================


class TestInMemoryLifecycle:
    """Store, retrieve, delete -- end-to-end with real backend."""

    @pytest.mark.asyncio
    async def test_ensure_then_retrieve(self) -> None:
        backend = _backend()
        ptr = await backend.ensure_secret(
            namespace="ns",
            model="mdl",
            field="fld",
            value="super-secret",
        )
        value, integrity = await backend.retrieve_secret(
            ptr.vault_reference,
        )
        assert value == "super-secret"
        assert integrity == ptr.integrity_hash

    @pytest.mark.asyncio
    async def test_delete_then_retrieve_raises(self) -> None:
        backend = _backend()
        ptr = await backend.ensure_secret(
            namespace="ns",
            model="mdl",
            field="fld",
            value="ephemeral",
        )
        await backend.delete_secret(ptr.vault_reference)
        with pytest.raises(KeyError):
            await backend.retrieve_secret(ptr.vault_reference)

    @pytest.mark.asyncio
    async def test_ensure_returns_vault_field_pointer(self) -> None:
        backend = _backend()
        ptr = await backend.ensure_secret(
            namespace="ns",
            model="mdl",
            field="fld",
            value="typed-check",
        )
        assert isinstance(ptr, VaultFieldPointer)
        assert ptr.version == VERSION_ONE
        assert len(ptr.vault_reference) == HASH_HEX_LENGTH


# ================================================================
# Idempotent ensure
# ================================================================


class TestIdempotentEnsure:
    """Calling ensure_secret twice with identical data."""

    @pytest.mark.asyncio
    async def test_same_reference_returned(self) -> None:
        backend = _backend()
        kwargs = {
            "namespace": "ns",
            "model": "mdl",
            "field": "fld",
            "value": "constant",
        }
        p1 = await backend.ensure_secret(**kwargs)
        p2 = await backend.ensure_secret(**kwargs)
        assert p1.vault_reference == p2.vault_reference
        assert p1.integrity_hash == p2.integrity_hash

    @pytest.mark.asyncio
    async def test_version_stays_at_one(self) -> None:
        backend = _backend()
        kwargs = {
            "namespace": "ns",
            "model": "mdl",
            "field": "fld",
            "value": "stable",
        }
        await backend.ensure_secret(**kwargs)
        p2 = await backend.ensure_secret(**kwargs)
        assert p2.version == VERSION_ONE


# ================================================================
# Ensure with changed hash (value mutation)
# ================================================================


class TestEnsureChangedHash:
    """Calling ensure_secret with different data bumps version."""

    @pytest.mark.asyncio
    async def test_version_incremented(self) -> None:
        backend = _backend()
        p1 = await backend.ensure_secret(
            namespace="ns",
            model="mdl",
            field="fld",
            value="original",
        )
        assert p1.version == VERSION_ONE

        # Tamper stored hash so the backend detects a change.
        backend._records[p1.vault_reference].integrity_hash = "tampered"

        p2 = await backend.ensure_secret(
            namespace="ns",
            model="mdl",
            field="fld",
            value="original",
        )
        assert p2.version == VERSION_TWO

    @pytest.mark.asyncio
    async def test_integrity_hash_updated(self) -> None:
        backend = _backend()
        p1 = await backend.ensure_secret(
            namespace="ns",
            model="mdl",
            field="fld",
            value="v1-data",
        )
        backend._records[p1.vault_reference].integrity_hash = "outdated"
        p2 = await backend.ensure_secret(
            namespace="ns",
            model="mdl",
            field="fld",
            value="v1-data",
        )
        assert p2.integrity_hash == p1.integrity_hash
        assert p2.integrity_hash != "outdated"


# ================================================================
# SecretPointerRepository with mock UnifiedCRUD
# ================================================================


class TestPointerRepositoryEnsure:
    """Repository ensure_record creates, updates, or skips."""

    @pytest.fixture
    def mock_crud(self) -> AsyncMock:
        crud = AsyncMock()
        crud.query = AsyncMock(return_value=[])
        crud.create = AsyncMock(return_value="uid-new")
        crud.update = AsyncMock(return_value=None)
        crud.read = AsyncMock(return_value=None)
        crud.delete = AsyncMock(return_value=True)
        return crud

    @pytest.fixture
    def repo(self, mock_crud: AsyncMock) -> SecretPointerRepository:
        return SecretPointerRepository(crud=mock_crud)

    @pytest.mark.asyncio
    async def test_creates_new_record(
        self,
        repo: SecretPointerRepository,
        mock_crud: AsyncMock,
    ) -> None:
        mock_crud.query.return_value = []
        params = _make_params()
        result = await repo.ensure_record(params)

        assert isinstance(result, SecretPointerRecord)
        assert result.version == VERSION_ONE
        assert result.rotation_count == ROTATION_ZERO
        mock_crud.create.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_updates_when_hash_changed(
        self,
        repo: SecretPointerRepository,
        mock_crud: AsyncMock,
    ) -> None:
        existing = _make_record(integrity_hash="old-hash")
        refreshed = _make_record(
            integrity_hash="hash-aaa",
            version=VERSION_TWO,
            rotation_count=ROTATION_ONE,
        )
        mock_crud.query.return_value = [existing]
        mock_crud.read.return_value = refreshed

        params = _make_params(integrity_hash="hash-aaa")
        result = await repo.ensure_record(params)

        assert result.version == VERSION_TWO
        assert result.rotation_count == ROTATION_ONE
        mock_crud.update.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_skips_rotation_when_same_hash(
        self,
        repo: SecretPointerRepository,
        mock_crud: AsyncMock,
    ) -> None:
        existing = _make_record(
            integrity_hash="hash-aaa",
            version=VERSION_ONE,
        )
        refreshed = _make_record(
            integrity_hash="hash-aaa",
            version=VERSION_ONE,
        )
        mock_crud.query.return_value = [existing]
        mock_crud.read.return_value = refreshed

        params = _make_params(integrity_hash="hash-aaa")
        result = await repo.ensure_record(params)

        assert result.version == VERSION_ONE
        mock_crud.update.assert_awaited_once()
        mock_crud.create.assert_not_awaited()


# ================================================================
# compute_integrity_hash determinism
# ================================================================


class TestIntegrityHashDeterminism:
    """Same input always produces same hash; different inputs differ."""

    def test_same_input_same_hash(self) -> None:
        h1 = compute_integrity_hash("deterministic-input")
        h2 = compute_integrity_hash("deterministic-input")
        assert h1 == h2
        assert len(h1) == HASH_HEX_LENGTH

    def test_different_inputs_produce_different_hashes(self) -> None:
        h1 = compute_integrity_hash("input-alpha")
        h2 = compute_integrity_hash("input-bravo")
        assert h1 != h2


# ================================================================
# _get_master_key / _get_integrity_salt from env vars
# ================================================================


class TestEnvKeyResolution:
    """Verify key helpers read from environment variables."""

    def test_master_key_returns_bytes(self) -> None:
        result = _get_master_key()
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_integrity_salt_returns_bytes(self) -> None:
        result = _get_integrity_salt()
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_master_key_matches_env_value(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DATAOPS_MASTER_KEY", "env-key-42")
        assert _get_master_key() == b"env-key-42"

    def test_integrity_salt_matches_env_value(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(
            "DATAOPS_INTEGRITY_SALT",
            "env-salt-99",
        )
        assert _get_integrity_salt() == b"env-salt-99"


# ================================================================
# parse_classification round-trip
# ================================================================


class TestParseClassificationIntegration:
    """Verify parse_classification handles various input forms."""

    def test_string_value_resolves(self) -> None:
        result = parse_classification("confidential")
        assert result == DataClassification.CONFIDENTIAL

    def test_enum_member_passes_through(self) -> None:
        member = DataClassification.RESTRICTED
        assert parse_classification(member) is member

    def test_none_returns_none(self) -> None:
        assert parse_classification(None) is None
