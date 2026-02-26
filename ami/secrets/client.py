"""Secrets Broker client abstractions for DataOps."""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Protocol, cast

import aiohttp
from pydantic import BaseModel, ConfigDict

from ami.core.exceptions import ConfigurationError
from ami.secrets.pointer import VaultFieldPointer

logger = logging.getLogger(__name__)


def _get_master_key() -> bytes:
    """Read master key from environment. Raises on missing."""
    value = os.getenv("DATAOPS_MASTER_KEY")
    if not value:
        msg = "Required: DATAOPS_MASTER_KEY environment variable"
        raise ConfigurationError(msg)
    return value.encode()


def _get_integrity_salt() -> bytes:
    """Read integrity salt from environment. Raises on missing."""
    value = os.getenv("DATAOPS_INTEGRITY_SALT")
    if not value:
        msg = "Required: DATAOPS_INTEGRITY_SALT environment variable"
        raise ConfigurationError(msg)
    return value.encode()


_DEFAULT_BROKER_URL = os.getenv("SECRETS_BROKER_URL") or os.getenv(
    "DATAOPS_SECRETS_BROKER_URL"
)
_DEFAULT_BROKER_TOKEN = os.getenv("SECRETS_BROKER_TOKEN") or os.getenv(
    "DATAOPS_INTERNAL_TOKEN"
)
_DEFAULT_BROKER_TIMEOUT = os.getenv("SECRETS_BROKER_TIMEOUT", "5.0")

_HTTP_NOT_FOUND = 404
_HTTP_UNAUTHORIZED = 401


def compute_integrity_hash(value: str) -> str:
    """Compute the public integrity hash for a secret value."""
    digest = hmac.new(_get_integrity_salt(), value.encode(), "sha256")
    return digest.hexdigest()


class SecretsBrokerBackend(Protocol):
    """Protocol describing broker backend operations."""

    async def ensure_secret(
        self,
        *,
        namespace: str,
        model: str,
        field: str,
        value: str,
        classification: Any | None = None,
    ) -> VaultFieldPointer: ...

    async def retrieve_secret(self, reference: str) -> tuple[str, str]:
        """Return secret value and integrity hash."""

    async def delete_secret(self, reference: str) -> None: ...


class _SecretRecord(BaseModel):
    """Internal record for stored secrets in the in-memory backend."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    value: str
    integrity_hash: str
    version: int
    updated_at: datetime


class InMemorySecretsBackend:
    """Development backend that simulates broker-side behaviour."""

    def __init__(self, master_key: bytes | None = None) -> None:
        self._master_key = master_key or _get_master_key()
        self._records: dict[str, _SecretRecord] = {}
        self._lock = asyncio.Lock()

    async def ensure_secret(
        self,
        *,
        namespace: str,
        model: str,
        field: str,
        value: str,
        classification: Any | None = None,
    ) -> VaultFieldPointer:
        reference = self._derive_reference(namespace, model, field, value)
        integrity_hash = compute_integrity_hash(value)

        if classification is not None:
            logger.debug(
                "Received classification metadata for %s.%s.%s",
                namespace,
                model,
                field,
            )

        async with self._lock:
            record = self._records.get(reference)
            if record and record.integrity_hash == integrity_hash:
                record.updated_at = datetime.now(tz=UTC)
                return VaultFieldPointer(
                    vault_reference=reference,
                    integrity_hash=record.integrity_hash,
                    version=record.version,
                    updated_at=record.updated_at,
                )

            version = 1
            if record:
                version = record.version + 1
            updated_at = datetime.now(tz=UTC)
            self._records[reference] = _SecretRecord(
                value=value,
                integrity_hash=integrity_hash,
                version=version,
                updated_at=updated_at,
            )
            logger.debug(
                "Stored secret in in-memory broker: ref=%s version=%d",
                reference,
                version,
            )
            return VaultFieldPointer(
                vault_reference=reference,
                integrity_hash=integrity_hash,
                version=version,
                updated_at=updated_at,
            )

    async def retrieve_secret(self, reference: str) -> tuple[str, str]:
        async with self._lock:
            record = self._records.get(reference)
            if not record:
                msg = f"Unknown vault reference: {reference}"
                raise KeyError(msg)
            return record.value, record.integrity_hash

    async def delete_secret(self, reference: str) -> None:
        async with self._lock:
            self._records.pop(reference, None)

    def _derive_reference(
        self,
        namespace: str,
        model: str,
        field: str,
        value: str,
    ) -> str:
        payload = f"{namespace}|{model}|{field}|{value}".encode()
        digest = hmac.new(self._master_key, payload, "sha256")
        return digest.hexdigest()


class HTTPSecretsBrokerBackend:
    """Async HTTP client that talks to the broker service."""

    def __init__(
        self,
        base_url: str,
        token: str | None = None,
        *,
        timeout: float = 5.0,
    ) -> None:
        if not base_url:
            msg = "Secrets broker base URL must be provided"
            raise ValueError(msg)
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._timeout = aiohttp.ClientTimeout(total=timeout)

    async def ensure_secret(
        self,
        *,
        namespace: str,
        model: str,
        field: str,
        value: str,
        classification: Any | None = None,
    ) -> VaultFieldPointer:
        payload: dict[str, Any] = {
            "namespace": namespace,
            "model": model,
            "field": field,
            "value": value,
        }
        if classification is not None:
            payload["classification"] = getattr(
                classification,
                "value",
                str(classification),
            )
        data = await self._request("POST", "/v1/secrets/ensure", payload)
        return VaultFieldPointer.model_validate(data)

    async def retrieve_secret(self, reference: str) -> tuple[str, str]:
        payload: dict[str, str] = {"vault_reference": reference}
        data = await self._request("POST", "/v1/secrets/retrieve", payload)
        value = data.get("value")
        integrity_hash = data.get("integrity_hash")
        if not isinstance(value, str) or not isinstance(
            integrity_hash,
            str,
        ):
            msg = "Secrets broker returned malformed secret payload"
            raise TypeError(msg)
        return value, integrity_hash

    async def delete_secret(self, reference: str) -> None:
        await self._request("DELETE", f"/v1/secrets/{reference}")

    async def _request(
        self,
        method: str,
        path: str,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{self._base_url}{path}"
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"

        json_payload = dict(payload) if payload is not None else None

        async with aiohttp.ClientSession(
            timeout=self._timeout,
        ) as session:
            try:
                async with session.request(
                    method,
                    url,
                    json=json_payload,
                    headers=headers,
                ) as resp:
                    if (
                        resp.status == _HTTP_NOT_FOUND
                        and method == "POST"
                        and path == "/v1/secrets/retrieve"
                        and payload is not None
                    ):
                        msg = str(payload["vault_reference"])
                        raise KeyError(msg)
                    if resp.status == _HTTP_UNAUTHORIZED:
                        msg = "Secrets broker rejected credentials"
                        raise PermissionError(msg)
                    resp.raise_for_status()
                    raw = await resp.read()
            except aiohttp.ClientError as exc:
                msg = "Unable to reach secrets broker"
                raise ConnectionError(msg) from exc

        if not raw:
            return {}
        parsed_json = json.loads(raw.decode())
        if not isinstance(parsed_json, dict):
            msg = "Secrets broker returned a non-object payload"
            raise TypeError(msg)
        return cast(dict[str, Any], parsed_json)


class SecretsBrokerClient:
    """High-level client that delegates to a broker backend."""

    def __init__(
        self,
        backend: SecretsBrokerBackend | None = None,
    ) -> None:
        self._backend = backend or _build_default_backend()

    async def ensure_secret(
        self,
        *,
        namespace: str,
        model: str,
        field: str,
        value: str,
        classification: Any | None = None,
    ) -> VaultFieldPointer:
        return await self._backend.ensure_secret(
            namespace=namespace,
            model=model,
            field=field,
            value=value,
            classification=classification,
        )

    async def retrieve_secret(self, reference: str) -> tuple[str, str]:
        return await self._backend.retrieve_secret(reference)

    async def delete_secret(self, reference: str) -> None:
        await self._backend.delete_secret(reference)


class _ClientState:
    """Mutable holder for the broker client singleton."""

    def __init__(self) -> None:
        self.client: SecretsBrokerClient | None = None


_CLIENT_STATE = _ClientState()


def _build_default_backend() -> SecretsBrokerBackend:
    """Create a backend based on environment configuration."""
    if _DEFAULT_BROKER_URL:
        try:
            timeout = float(_DEFAULT_BROKER_TIMEOUT)
        except ValueError:
            timeout = 5.0
        logger.debug(
            "Using HTTP secrets broker backend: %s",
            _DEFAULT_BROKER_URL,
        )
        return HTTPSecretsBrokerBackend(
            _DEFAULT_BROKER_URL,
            _DEFAULT_BROKER_TOKEN,
            timeout=timeout,
        )

    logger.debug("Switching to in-memory secrets backend")
    return InMemorySecretsBackend()


def get_secrets_broker_client() -> SecretsBrokerClient:
    """Return the configured secrets broker client."""
    if _CLIENT_STATE.client is None:
        _CLIENT_STATE.client = SecretsBrokerClient()
    return _CLIENT_STATE.client


def set_secrets_broker_client(client: SecretsBrokerClient) -> None:
    """Override the global secrets broker client."""
    _CLIENT_STATE.client = client


def reset_secrets_broker_client() -> None:
    """Reset the global client to its default state."""
    _CLIENT_STATE.client = None
