"""Unit tests for ami.secrets.repository."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from ami.models.secret_pointer import SecretPointerRecord
from ami.models.security import DataClassification
from ami.secrets.repository import (
    EnsureRecordParams,
    SecretPointerRepository,
    parse_classification,
)

EXPECTED_VERSION_AFTER_ROTATION = 3
EXPECTED_ROTATION_COUNT_TWO = 2
EXPECTED_ROTATION_COUNT_SIX = 6


def _make_params(**overrides: object) -> EnsureRecordParams:
    """Build an EnsureRecordParams with sensible defaults."""
    defaults = {
        "reference": "vault/secret/api-key",
        "namespace": "prod",
        "model_name": "Credential",
        "field_name": "api_key",
        "integrity_hash": "abc123",
    }
    defaults.update(overrides)
    return EnsureRecordParams(**defaults)


def _make_record(**overrides: object) -> SecretPointerRecord:
    """Build a SecretPointerRecord with sensible defaults."""
    defaults = {
        "uid": "rec-001",
        "vault_reference": "vault/secret/api-key",
        "namespace": "prod",
        "model_name": "Credential",
        "field_name": "api_key",
        "integrity_hash": "abc123",
        "version": 1,
        "rotation_count": 0,
        "secret_created_at": datetime(2025, 1, 1, tzinfo=UTC),
        "secret_updated_at": datetime(2025, 1, 1, tzinfo=UTC),
    }
    defaults.update(overrides)
    return SecretPointerRecord(**defaults)


@pytest.fixture
def mock_crud() -> AsyncMock:
    """Return a fully mocked UnifiedCRUD instance."""
    crud = AsyncMock()
    crud.query = AsyncMock(return_value=[])
    crud.create = AsyncMock(return_value="new-uid")
    crud.update = AsyncMock(return_value=None)
    crud.read = AsyncMock(return_value=None)
    crud.delete = AsyncMock(return_value=True)
    return crud


@pytest.fixture
def repo(mock_crud: AsyncMock) -> SecretPointerRepository:
    """Build a repository backed by the mocked CRUD."""
    return SecretPointerRepository(crud=mock_crud)


# ----------------------------------------------------------------
# ensure_record: new record
# ----------------------------------------------------------------


class TestEnsureRecordNew:
    """When no existing record is found, create a fresh one."""

    @pytest.mark.asyncio
    async def test_creates_record_with_version_one(
        self,
        repo: SecretPointerRepository,
        mock_crud: AsyncMock,
    ) -> None:
        mock_crud.query.return_value = []
        params = _make_params()

        result = await repo.ensure_record(params)

        assert isinstance(result, SecretPointerRecord)
        assert result.version == 1
        assert result.rotation_count == 0

    @pytest.mark.asyncio
    async def test_calls_crud_create(
        self,
        repo: SecretPointerRepository,
        mock_crud: AsyncMock,
    ) -> None:
        mock_crud.query.return_value = []
        params = _make_params()

        await repo.ensure_record(params)

        mock_crud.create.assert_awaited_once()
        created = mock_crud.create.call_args[0][0]
        assert isinstance(created, SecretPointerRecord)
        assert created.vault_reference == params.reference

    @pytest.mark.asyncio
    async def test_new_record_uses_internal_classification(
        self,
        repo: SecretPointerRepository,
        mock_crud: AsyncMock,
    ) -> None:
        mock_crud.query.return_value = []
        params = _make_params(classification=None)

        result = await repo.ensure_record(params)

        assert result.classification == DataClassification.INTERNAL.value


# ----------------------------------------------------------------
# ensure_record: hash changed
# ----------------------------------------------------------------


class TestEnsureRecordUpdateHashChanged:
    """Existing record with a different hash triggers rotation."""

    @pytest.mark.asyncio
    async def test_version_incremented(
        self,
        repo: SecretPointerRepository,
        mock_crud: AsyncMock,
    ) -> None:
        existing = _make_record(
            integrity_hash="old-hash",
            version=2,
            rotation_count=1,
        )
        refreshed = _make_record(
            integrity_hash="abc123",
            version=3,
            rotation_count=2,
        )
        mock_crud.query.return_value = [existing]
        mock_crud.read.return_value = refreshed
        params = _make_params(integrity_hash="abc123")

        result = await repo.ensure_record(params)

        assert result.version == EXPECTED_VERSION_AFTER_ROTATION
        assert result.rotation_count == EXPECTED_ROTATION_COUNT_TWO

    @pytest.mark.asyncio
    async def test_calls_crud_update_then_read(
        self,
        repo: SecretPointerRepository,
        mock_crud: AsyncMock,
    ) -> None:
        existing = _make_record(integrity_hash="old-hash")
        refreshed = _make_record(
            integrity_hash="abc123",
            version=2,
            rotation_count=1,
        )
        mock_crud.query.return_value = [existing]
        mock_crud.read.return_value = refreshed
        params = _make_params(integrity_hash="abc123")

        await repo.ensure_record(params)

        mock_crud.update.assert_awaited_once()
        mock_crud.read.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_rotation_count_incremented(
        self,
        repo: SecretPointerRepository,
        mock_crud: AsyncMock,
    ) -> None:
        existing = _make_record(
            integrity_hash="old-hash",
            rotation_count=5,
        )
        refreshed = _make_record(
            integrity_hash="abc123",
            rotation_count=6,
            version=2,
        )
        mock_crud.query.return_value = [existing]
        mock_crud.read.return_value = refreshed
        params = _make_params(integrity_hash="abc123")

        result = await repo.ensure_record(params)

        assert result.rotation_count == EXPECTED_ROTATION_COUNT_SIX


# ----------------------------------------------------------------
# ensure_record: same hash
# ----------------------------------------------------------------


class TestEnsureRecordSameHash:
    """Existing record with same hash still updates timestamps."""

    @pytest.mark.asyncio
    async def test_version_unchanged(
        self,
        repo: SecretPointerRepository,
        mock_crud: AsyncMock,
    ) -> None:
        existing = _make_record(
            version=EXPECTED_VERSION_AFTER_ROTATION,
        )
        refreshed = _make_record(
            version=EXPECTED_VERSION_AFTER_ROTATION,
        )
        mock_crud.query.return_value = [existing]
        mock_crud.read.return_value = refreshed
        params = _make_params()

        result = await repo.ensure_record(params)

        assert result.version == EXPECTED_VERSION_AFTER_ROTATION

    @pytest.mark.asyncio
    async def test_calls_update(
        self,
        repo: SecretPointerRepository,
        mock_crud: AsyncMock,
    ) -> None:
        existing = _make_record()
        refreshed = _make_record()
        mock_crud.query.return_value = [existing]
        mock_crud.read.return_value = refreshed
        params = _make_params()

        await repo.ensure_record(params)

        mock_crud.update.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_secret_updated_at_set(
        self,
        repo: SecretPointerRepository,
        mock_crud: AsyncMock,
    ) -> None:
        old_time = datetime(2024, 1, 1, tzinfo=UTC)
        existing = _make_record(secret_updated_at=old_time)
        refreshed = _make_record()
        mock_crud.query.return_value = [existing]
        mock_crud.read.return_value = refreshed
        params = _make_params()

        frozen = datetime(2025, 6, 15, 12, 0, 0, tzinfo=UTC)
        with patch(
            "ami.secrets.repository.datetime",
        ) as mock_dt:
            mock_dt.now.return_value = frozen
            mock_dt.side_effect = datetime
            await repo.ensure_record(params)

        updated_record = mock_crud.update.call_args[0][0]
        assert updated_record.secret_updated_at == frozen


# ----------------------------------------------------------------
# get_by_reference
# ----------------------------------------------------------------


class TestGetByReference:
    """Retrieve a pointer record by vault reference."""

    @pytest.mark.asyncio
    async def test_found_returns_record(
        self,
        repo: SecretPointerRepository,
        mock_crud: AsyncMock,
    ) -> None:
        record = _make_record()
        mock_crud.query.return_value = [record]

        result = await repo.get_by_reference("vault/secret/api-key")

        assert result is record

    @pytest.mark.asyncio
    async def test_not_found_returns_none(
        self,
        repo: SecretPointerRepository,
        mock_crud: AsyncMock,
    ) -> None:
        mock_crud.query.return_value = []

        result = await repo.get_by_reference("nonexistent")

        assert result is None


# ----------------------------------------------------------------
# mark_accessed
# ----------------------------------------------------------------


class TestMarkAccessed:
    """Update last-accessed timestamp on a pointer record."""

    @pytest.mark.asyncio
    async def test_sets_last_accessed(
        self,
        repo: SecretPointerRepository,
        mock_crud: AsyncMock,
    ) -> None:
        record = _make_record()
        mock_crud.query.return_value = [record]

        await repo.mark_accessed("vault/secret/api-key")

        assert record.secret_last_accessed_at is not None
        mock_crud.update.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_not_found_returns_silently(
        self,
        repo: SecretPointerRepository,
        mock_crud: AsyncMock,
    ) -> None:
        mock_crud.query.return_value = []

        await repo.mark_accessed("nonexistent")

        mock_crud.update.assert_not_awaited()


# ----------------------------------------------------------------
# delete
# ----------------------------------------------------------------


class TestDelete:
    """Remove pointer metadata by vault reference."""

    @pytest.mark.asyncio
    async def test_found_record_calls_crud_delete(
        self,
        repo: SecretPointerRepository,
        mock_crud: AsyncMock,
    ) -> None:
        record = _make_record(uid="del-001")
        mock_crud.query.return_value = [record]

        await repo.delete("vault/secret/api-key")

        mock_crud.delete.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_not_found_returns_silently(
        self,
        repo: SecretPointerRepository,
        mock_crud: AsyncMock,
    ) -> None:
        mock_crud.query.return_value = []

        await repo.delete("nonexistent")

        mock_crud.delete.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_record_with_none_uid_returns_silently(
        self,
        repo: SecretPointerRepository,
        mock_crud: AsyncMock,
    ) -> None:
        record = _make_record(uid=None)
        mock_crud.query.return_value = [record]

        await repo.delete("vault/secret/api-key")

        mock_crud.delete.assert_not_awaited()


# ----------------------------------------------------------------
# list_by_namespace
# ----------------------------------------------------------------


class TestListByNamespace:
    """List pointer records filtered by namespace."""

    @pytest.mark.asyncio
    async def test_returns_query_results(
        self,
        repo: SecretPointerRepository,
        mock_crud: AsyncMock,
    ) -> None:
        records = [
            _make_record(uid="r1", namespace="prod"),
            _make_record(uid="r2", namespace="prod"),
        ]
        mock_crud.query.return_value = records

        result = await repo.list_by_namespace("prod")

        assert list(result) == records
        mock_crud.query.assert_awaited_once()
        call_kwargs = mock_crud.query.call_args
        assert call_kwargs[0][1] == {"namespace": "prod"}


# ----------------------------------------------------------------
# parse_classification (module-level function)
# ----------------------------------------------------------------


class TestParseClassification:
    """Convert arbitrary input into a DataClassification enum."""

    def test_valid_enum_value(self) -> None:
        result = parse_classification("internal")
        assert result == DataClassification.INTERNAL

    def test_valid_string(self) -> None:
        result = parse_classification("confidential")
        assert result == DataClassification.CONFIDENTIAL

    def test_case_insensitive_string(self) -> None:
        result = parse_classification("RESTRICTED")
        assert result == DataClassification.RESTRICTED

    def test_none_returns_none(self) -> None:
        result = parse_classification(None)
        assert result is None

    def test_invalid_returns_none(self) -> None:
        result = parse_classification("not-a-real-level")
        assert result is None

    def test_already_enum_returns_same(self) -> None:
        member = DataClassification.TOP_SECRET
        result = parse_classification(member)
        assert result is member
