"""Shared test fixtures for the AMI-DATAOPS test suite."""

from __future__ import annotations

import os
import sys
import types
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from ami.core.storage_types import StorageType
from ami.models.security import Permission, SecurityContext
from ami.models.storage_config import StorageConfig


def pytest_configure(config: pytest.Config) -> None:
    """Inject a minimal pydgraph mock before test collection.

    The real pydgraph may be installed but broken due to a
    protobuf version mismatch; always replace it.
    """
    mod = types.ModuleType("pydgraph")
    for name in ("DgraphClient", "DgraphClientStub", "Mutation", "Operation"):
        setattr(mod, name, MagicMock)
    sys.modules["pydgraph"] = mod


# --- Environment variables for encryption ---


@pytest.fixture(autouse=True, scope="session")
def _master_key_env() -> None:
    """Ensure encryption env vars are set for the test session."""
    os.environ.setdefault("DATAOPS_MASTER_KEY", "test-master-key-for-ci")
    os.environ.setdefault(
        "DATAOPS_INTEGRITY_SALT",
        "test-integrity-salt-for-ci",
    )


# --- Reusable factory fixtures ---


@pytest.fixture
def make_storage_config() -> Any:
    """Return a factory that builds StorageConfig with sensible defaults."""

    def _factory(
        storage_type: StorageType = StorageType.RELATIONAL,
        **overrides: Any,
    ) -> StorageConfig:
        defaults: dict[str, Any] = {
            "storage_type": storage_type,
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "username": "user",
            "password": "pass",
        }
        defaults.update(overrides)
        return StorageConfig(**defaults)

    return _factory


@pytest.fixture
def security_ctx() -> SecurityContext:
    """Default security context for tests."""
    return SecurityContext(
        user_id="test_user",
        roles=["member"],
        groups=["dev"],
    )


@pytest.fixture
def admin_ctx() -> SecurityContext:
    """Admin security context with DECRYPT permission."""
    return SecurityContext(
        user_id="admin_user",
        roles=["admin"],
        groups=["administrators"],
        permissions=[Permission.ADMIN, Permission.DECRYPT],
    )


# --- Mock helpers ---


@pytest.fixture
def mock_asyncpg_pool() -> AsyncMock:
    """AsyncMock asyncpg connection pool."""
    pool = AsyncMock()
    conn = AsyncMock()
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value = ctx
    pool.close = AsyncMock()
    return pool


@pytest.fixture
def mock_aiohttp_session() -> AsyncMock:
    """AsyncMock aiohttp client session."""
    session = AsyncMock()
    session.closed = False
    session.close = AsyncMock()
    return session


@pytest.fixture
def mock_redis_client() -> AsyncMock:
    """AsyncMock redis.asyncio client."""
    client = AsyncMock()
    client.ping = AsyncMock(return_value=True)
    client.close = AsyncMock()
    return client


@pytest.fixture
def mock_hvac_client() -> MagicMock:
    """MagicMock hvac.Client."""
    client = MagicMock()
    client.is_authenticated.return_value = True
    return client
