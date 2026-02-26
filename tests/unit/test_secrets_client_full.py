"""Tests for ami.secrets.client module."""

from __future__ import annotations

from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.core.exceptions import ConfigurationError
from ami.secrets.client import (
    _CLIENT_STATE,
    HTTPSecretsBrokerBackend,
    InMemorySecretsBackend,
    SecretsBrokerClient,
    _build_default_backend,
    _get_integrity_salt,
    _get_master_key,
    compute_integrity_hash,
    get_secrets_broker_client,
    reset_secrets_broker_client,
    set_secrets_broker_client,
)
from ami.secrets.pointer import VaultFieldPointer

_VERSION_BUMPED = 2


# ----------------------------------------------------------
# TestGetMasterKey
# ----------------------------------------------------------


class TestGetMasterKey:
    """Verify _get_master_key reads env and raises."""

    def test_returns_bytes_when_set(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DATAOPS_MASTER_KEY", "abc123")
        result = _get_master_key()
        assert result == b"abc123"
        assert isinstance(result, bytes)

    def test_raises_when_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("DATAOPS_MASTER_KEY", raising=False)
        with pytest.raises(ConfigurationError):
            _get_master_key()


# ----------------------------------------------------------
# TestGetIntegritySalt
# ----------------------------------------------------------


class TestGetIntegritySalt:
    """Verify _get_integrity_salt reads env and raises."""

    def test_returns_bytes_when_set(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DATAOPS_INTEGRITY_SALT", "salt99")
        result = _get_integrity_salt()
        assert result == b"salt99"
        assert isinstance(result, bytes)

    def test_raises_when_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv(
            "DATAOPS_INTEGRITY_SALT",
            raising=False,
        )
        with pytest.raises(ConfigurationError):
            _get_integrity_salt()


# ----------------------------------------------------------
# TestComputeIntegrityHash
# ----------------------------------------------------------


class TestComputeIntegrityHash:
    """Verify compute_integrity_hash behaviour."""

    def test_deterministic(self) -> None:
        h1 = compute_integrity_hash("test-value")
        h2 = compute_integrity_hash("test-value")
        assert h1 == h2

    def test_different_inputs_different_hashes(self) -> None:
        h1 = compute_integrity_hash("alpha")
        h2 = compute_integrity_hash("bravo")
        assert h1 != h2


# ----------------------------------------------------------
# TestInMemorySecretsBackend
# ----------------------------------------------------------


class TestInMemorySecretsBackend:
    """In-memory backend for development use."""

    @pytest.fixture
    def backend(self) -> InMemorySecretsBackend:
        return InMemorySecretsBackend(master_key=b"test-key")

    @pytest.mark.asyncio
    async def test_ensure_creates_new_pointer(
        self,
        backend: InMemorySecretsBackend,
    ) -> None:
        ptr = await backend.ensure_secret(
            namespace="ns",
            model="mdl",
            field="fld",
            value="secret-val",
        )
        assert isinstance(ptr, VaultFieldPointer)
        assert ptr.version == 1
        assert ptr.vault_reference
        assert ptr.integrity_hash

    @pytest.mark.asyncio
    async def test_ensure_same_value_idempotent(
        self,
        backend: InMemorySecretsBackend,
    ) -> None:
        p1 = await backend.ensure_secret(
            namespace="ns",
            model="mdl",
            field="fld",
            value="same-val",
        )
        p2 = await backend.ensure_secret(
            namespace="ns",
            model="mdl",
            field="fld",
            value="same-val",
        )
        assert p1.vault_reference == p2.vault_reference
        assert p1.integrity_hash == p2.integrity_hash
        assert p2.version == 1

    @pytest.mark.asyncio
    async def test_ensure_changed_value_bumps_version(
        self,
        backend: InMemorySecretsBackend,
    ) -> None:
        p1 = await backend.ensure_secret(
            namespace="ns",
            model="mdl",
            field="fld",
            value="same-val",
        )
        assert p1.version == 1

        # Tamper with the stored hash so the backend sees
        # an integrity mismatch on the next call, triggering
        # a version bump.
        rec = backend._records[p1.vault_reference]
        rec.integrity_hash = "stale"

        p2 = await backend.ensure_secret(
            namespace="ns",
            model="mdl",
            field="fld",
            value="same-val",
        )
        assert p2.version == _VERSION_BUMPED

    @pytest.mark.asyncio
    async def test_retrieve_returns_value_and_hash(
        self,
        backend: InMemorySecretsBackend,
    ) -> None:
        ptr = await backend.ensure_secret(
            namespace="ns",
            model="mdl",
            field="fld",
            value="retrieve-me",
        )
        value, ihash = await backend.retrieve_secret(
            ptr.vault_reference,
        )
        assert value == "retrieve-me"
        assert ihash == ptr.integrity_hash

    @pytest.mark.asyncio
    async def test_retrieve_unknown_raises_key_error(
        self,
        backend: InMemorySecretsBackend,
    ) -> None:
        with pytest.raises(KeyError):
            await backend.retrieve_secret("nonexistent-ref")

    @pytest.mark.asyncio
    async def test_delete_removes_record(
        self,
        backend: InMemorySecretsBackend,
    ) -> None:
        ptr = await backend.ensure_secret(
            namespace="ns",
            model="mdl",
            field="fld",
            value="delete-me",
        )
        await backend.delete_secret(ptr.vault_reference)
        with pytest.raises(KeyError):
            await backend.retrieve_secret(ptr.vault_reference)


# ----------------------------------------------------------
# TestHTTPSecretsBrokerBackend
# ----------------------------------------------------------


class TestHTTPSecretsBrokerBackend:
    """HTTP backend construction and request building."""

    def test_init_empty_url_raises(self) -> None:
        with pytest.raises(
            ValueError,
            match="base URL must be provided",
        ):
            HTTPSecretsBrokerBackend(base_url="")

    @pytest.mark.asyncio
    async def test_request_builds_correct_url_and_headers(
        self,
    ) -> None:
        backend = HTTPSecretsBrokerBackend(
            base_url="https://broker.example.com/",
            token="tok-abc",
        )

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.read = AsyncMock(
            return_value=b'{"ok": true}',
        )
        mock_resp.raise_for_status = MagicMock()
        mock_resp.__aenter__ = AsyncMock(
            return_value=mock_resp,
        )
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.request = MagicMock(
            return_value=mock_resp,
        )
        mock_session.__aenter__ = AsyncMock(
            return_value=mock_session,
        )
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "ami.secrets.client.aiohttp.ClientSession",
            return_value=mock_session,
        ):
            result = await backend._request(
                "POST",
                "/v1/secrets/ensure",
                {"key": "val"},
            )

        assert result == {"ok": True}
        mock_session.request.assert_called_once()
        call_args = mock_session.request.call_args
        assert call_args[0][0] == "POST"
        url = call_args[0][1]
        assert url == ("https://broker.example.com/v1/secrets/ensure")
        headers = call_args[1]["headers"]
        assert headers["Authorization"] == "Bearer tok-abc"
        assert headers["Content-Type"] == "application/json"


# ----------------------------------------------------------
# TestSecretsBrokerClient
# ----------------------------------------------------------


class TestSecretsBrokerClient:
    """High-level client delegates to backend."""

    @pytest.fixture
    def mock_backend(self) -> AsyncMock:
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_ensure_delegates(
        self,
        mock_backend: AsyncMock,
    ) -> None:
        client = SecretsBrokerClient(backend=mock_backend)
        await client.ensure_secret(
            namespace="ns",
            model="mdl",
            field="fld",
            value="val",
        )
        mock_backend.ensure_secret.assert_awaited_once_with(
            namespace="ns",
            model="mdl",
            field="fld",
            value="val",
            classification=None,
        )

    @pytest.mark.asyncio
    async def test_retrieve_delegates(
        self,
        mock_backend: AsyncMock,
    ) -> None:
        mock_backend.retrieve_secret.return_value = (
            "v",
            "h",
        )
        client = SecretsBrokerClient(backend=mock_backend)
        val, hsh = await client.retrieve_secret("ref-1")
        mock_backend.retrieve_secret.assert_awaited_once_with(
            "ref-1",
        )
        assert val == "v"
        assert hsh == "h"

    @pytest.mark.asyncio
    async def test_delete_delegates(
        self,
        mock_backend: AsyncMock,
    ) -> None:
        client = SecretsBrokerClient(backend=mock_backend)
        await client.delete_secret("ref-2")
        mock_backend.delete_secret.assert_awaited_once_with(
            "ref-2",
        )


# ----------------------------------------------------------
# TestBuildDefaultBackend
# ----------------------------------------------------------


class TestBuildDefaultBackend:
    """_build_default_backend selects HTTP or in-memory."""

    def test_returns_http_when_broker_url_set(self) -> None:
        with (
            patch(
                "ami.secrets.client._DEFAULT_BROKER_URL",
                "https://broker.test",
            ),
            patch(
                "ami.secrets.client._DEFAULT_BROKER_TOKEN",
                "tok",
            ),
            patch(
                "ami.secrets.client._DEFAULT_BROKER_TIMEOUT",
                "3.0",
            ),
        ):
            backend = _build_default_backend()
        assert isinstance(backend, HTTPSecretsBrokerBackend)

    def test_returns_in_memory_when_no_url(self) -> None:
        with patch(
            "ami.secrets.client._DEFAULT_BROKER_URL",
            None,
        ):
            backend = _build_default_backend()
        assert isinstance(backend, InMemorySecretsBackend)


# ----------------------------------------------------------
# TestClientState
# ----------------------------------------------------------


class TestClientState:
    """Singleton management for the broker client."""

    @pytest.fixture(autouse=True)
    def _reset_state(self) -> Generator[None, None, None]:
        _CLIENT_STATE.client = None
        yield
        _CLIENT_STATE.client = None

    def test_get_returns_singleton(self) -> None:
        c1 = get_secrets_broker_client()
        c2 = get_secrets_broker_client()
        assert c1 is c2

    def test_reset_clears_singleton(self) -> None:
        c1 = get_secrets_broker_client()
        reset_secrets_broker_client()
        c2 = get_secrets_broker_client()
        assert c1 is not c2

    def test_set_overrides_singleton(self) -> None:
        original = get_secrets_broker_client()
        custom = SecretsBrokerClient(backend=AsyncMock())
        set_secrets_broker_client(custom)
        assert get_secrets_broker_client() is custom
        assert get_secrets_broker_client() is not original
