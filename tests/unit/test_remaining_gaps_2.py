"""Gap-filling tests for unified_crud, secrets/client,
and secrets/adapter.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

from ami.core.storage_types import StorageType
from ami.core.unified_crud import UnifiedCRUD
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.security import DataClassification
from ami.models.storage_config import StorageConfig
from ami.secrets.adapter import (
    _POINTER_CONTEXT,
    consume_pointer_cache,
    hydrate_sensitive_fields,
    prepare_instance_for_storage,
)
from ami.secrets.client import (
    HTTPSecretsBrokerBackend,
    InMemorySecretsBackend,
    _build_default_backend,
)
from ami.secrets.config import SensitiveFieldConfig
from ami.secrets.pointer import VaultFieldPointer

_V1 = 1
_P = 9080
_OK = 200
_NF = 404
_UA = 401
_CFG = StorageConfig(
    storage_type=StorageType.GRAPH,
    host="localhost",
    port=_P,
    database="testdb",
)
_CS = "ami.secrets.client"
_SA = "ami.secrets.adapter"


class _TM(StorageModel):
    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="t",
        storage_configs={"p": _CFG},
    )
    name: str = ""


def _resp(status: int, **kw: Any) -> AsyncMock:
    r = AsyncMock()
    r.status = status
    r.__aenter__ = AsyncMock(return_value=r)
    r.__aexit__ = AsyncMock(return_value=False)
    if "body" in kw:
        r.read = AsyncMock(return_value=kw["body"])
        r.raise_for_status = MagicMock()
    if "txt" in kw:
        r.text = AsyncMock(return_value=kw["txt"])
    return r


def _ms(resp: AsyncMock) -> MagicMock:
    ms = MagicMock()
    ms.request = MagicMock(return_value=resp)
    ms.__aenter__ = AsyncMock(return_value=ms)
    ms.__aexit__ = AsyncMock(return_value=False)
    return ms


# -- 5. unified_crud gaps --


class TestUnifiedCrudGaps:
    """Cover unified_crud edge cases."""

    def test_no_storage_config(self) -> None:
        class _NC(StorageModel):
            _model_meta: ClassVar[ModelMetadata] = ModelMetadata()

        with pytest.raises(
            ValueError,
            match="No storage",
        ):
            UnifiedCRUD()._resolve_storage_configs(
                _NC,
                _NC,
            )

    def test_dict_storage_config(self) -> None:
        class _DC(StorageModel):
            _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
                path="p",
                storage_configs={"a": _CFG},
            )

        r = UnifiedCRUD()._resolve_storage_configs(
            _DC,
            _DC,
        )
        assert isinstance(r, list)
        assert len(r) == 1

    @pytest.mark.asyncio
    async def test_evict_stale_loop(self) -> None:
        crud = UnifiedCRUD()
        ol = MagicMock()
        ol.is_closed = MagicMock(return_value=True)
        dao = AsyncMock()
        dao.disconnect = AsyncMock()
        k = (_TM, 0)
        crud._dao_cache[k] = dao
        crud._dao_loop_cache[k] = ol
        await crud._evict_if_loop_changed(
            k,
            asyncio.get_running_loop(),
        )
        assert k not in crud._dao_cache

    def test_map_from_storage_dt(self) -> None:
        d: dict[str, Any] = {
            "uid": "u",
            "name": "n",
            "updated_at": "2025-06-15T12:30:00+00:00",
        }
        result = UnifiedCRUD._map_from_storage(_TM, d)
        assert isinstance(result.updated_at, datetime)


# -- 6. secrets/client gaps --


class TestSecretsClientGaps:
    """Cover secrets client edge cases."""

    @pytest.mark.asyncio
    async def test_inmem_ensure(self) -> None:
        b = InMemorySecretsBackend(
            master_key=b"test-key",
        )
        p = await b.ensure_secret(
            namespace="n",
            model="m",
            field="f",
            value="s",
            classification=DataClassification.CONFIDENTIAL,
        )
        assert p.version == _V1

    @pytest.mark.asyncio
    async def test_http_ensure(self) -> None:
        b = HTTPSecretsBrokerBackend(
            base_url="https://b.t",
            token="t",
        )
        fd = {
            "vault_reference": "r",
            "integrity_hash": "h",
            "version": 1,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        with patch.object(
            b,
            "_request",
            new_callable=AsyncMock,
            return_value=fd,
        ) as mr:
            r = await b.ensure_secret(
                namespace="n",
                model="m",
                field="f",
                value="v",
                classification=DataClassification.INTERNAL,
            )
        assert isinstance(r, VaultFieldPointer)
        cls = mr.call_args[0][2]["classification"]
        assert cls == "internal"

    @pytest.mark.asyncio
    async def test_http_retrieve(self) -> None:
        b = HTTPSecretsBrokerBackend(
            base_url="https://b.t",
        )
        with patch.object(
            b,
            "_request",
            new_callable=AsyncMock,
            return_value={
                "value": "s",
                "integrity_hash": "h",
            },
        ):
            r = await b.retrieve_secret("r")
        assert r == ("s", "h")

    @pytest.mark.asyncio
    async def test_http_retrieve_bad(self) -> None:
        b = HTTPSecretsBrokerBackend(
            base_url="https://b.t",
        )
        with (
            patch.object(
                b,
                "_request",
                new_callable=AsyncMock,
                return_value={
                    "value": 1,
                    "integrity_hash": None,
                },
            ),
            pytest.raises(TypeError, match="malformed"),
        ):
            await b.retrieve_secret("r")

    @pytest.mark.asyncio
    async def test_http_delete(self) -> None:
        b = HTTPSecretsBrokerBackend(
            base_url="https://b.t",
        )
        with patch.object(
            b,
            "_request",
            new_callable=AsyncMock,
            return_value={},
        ) as mr:
            await b.delete_secret("r")
        mr.assert_awaited_once_with(
            "DELETE",
            "/v1/secrets/r",
        )

    @pytest.mark.asyncio
    async def test_req_404(self) -> None:
        b = HTTPSecretsBrokerBackend(
            base_url="https://b.t",
        )
        with (
            patch(
                f"{_CS}.aiohttp.ClientSession",
                return_value=_ms(_resp(_NF)),
            ),
            pytest.raises(KeyError),
        ):
            await b._request(
                "POST",
                "/v1/secrets/retrieve",
                {"vault_reference": "r"},
            )

    @pytest.mark.asyncio
    async def test_req_401(self) -> None:
        b = HTTPSecretsBrokerBackend(
            base_url="https://b.t",
        )
        with (
            patch(
                f"{_CS}.aiohttp.ClientSession",
                return_value=_ms(_resp(_UA)),
            ),
            pytest.raises(
                PermissionError,
                match="rejected",
            ),
        ):
            await b._request("GET", "/v1/s", None)

    @pytest.mark.asyncio
    async def test_req_empty_body(self) -> None:
        b = HTTPSecretsBrokerBackend(
            base_url="https://b.t",
        )
        with patch(
            f"{_CS}.aiohttp.ClientSession",
            return_value=_ms(_resp(_OK, body=b"")),
        ):
            r = await b._request("D", "/x", None)
        assert r == {}

    @pytest.mark.asyncio
    async def test_req_non_dict(self) -> None:
        b = HTTPSecretsBrokerBackend(
            base_url="https://b.t",
        )
        with (
            patch(
                f"{_CS}.aiohttp.ClientSession",
                return_value=_ms(
                    _resp(_OK, body=b"[1]"),
                ),
            ),
            pytest.raises(TypeError, match="non-object"),
        ):
            await b._request("G", "/l", None)

    def test_bad_timeout(self) -> None:
        with (
            patch(
                f"{_CS}._DEFAULT_BROKER_URL",
                "https://b.t",
            ),
            patch(f"{_CS}._DEFAULT_BROKER_TOKEN", "t"),
            patch(
                f"{_CS}._DEFAULT_BROKER_TIMEOUT",
                "bad",
            ),
        ):
            result = _build_default_backend()
            assert isinstance(
                result,
                HTTPSecretsBrokerBackend,
            )


# -- 7. secrets/adapter gaps --


class TestSecretsAdapterGaps:
    """Cover secrets adapter edge cases."""

    @pytest.mark.asyncio
    async def test_no_sensitive_fields(self) -> None:
        i = MagicMock()
        i.__class__._sensitive_fields = None
        r = await prepare_instance_for_storage(
            i,
            {"n": "v"},
        )
        assert r == {"n": "v"}

    @pytest.mark.asyncio
    async def test_ptr_value(self) -> None:
        ptr = VaultFieldPointer(
            vault_reference="r",
            integrity_hash="h",
            version=1,
        )

        class _M:
            _sensitive_fields: ClassVar[dict[str, Any]] = {
                "s": SensitiveFieldConfig(mask_pattern="*"),
            }
            _vault_pointer_cache: ClassVar[dict[str, Any]] = {}
            s = ptr

        i = _M()
        i.__class__.__module__ = "m"
        i.__class__.__name__ = "M"
        r = await prepare_instance_for_storage(
            i,
            {"s": "x"},
        )
        assert "vault_reference" in r["s"]

    @pytest.mark.asyncio
    async def test_dict_ptr(self) -> None:
        pd = {
            "vault_reference": "r",
            "integrity_hash": "h",
            "version": 1,
            "updated_at": datetime.now(UTC).isoformat(),
        }

        class _D:
            _sensitive_fields: ClassVar[dict[str, Any]] = {
                "t": SensitiveFieldConfig(mask_pattern="*"),
            }
            _vault_pointer_cache: ClassVar[dict[str, Any]] = {}
            t = pd

        i = _D()
        i.__class__.__module__ = "m"
        i.__class__.__name__ = "D"
        r = await prepare_instance_for_storage(
            i,
            {"t": "x"},
        )
        assert "vault_reference" in r["t"]

    @pytest.mark.asyncio
    async def test_secret_str(self) -> None:
        class _S:
            _sensitive_fields: ClassVar[dict[str, Any]] = {
                "p": SensitiveFieldConfig(mask_pattern="*", namespace="n"),
            }
            _vault_pointer_cache: ClassVar[dict[str, Any]] = {}
            p = SecretStr("pw")

        i = _S()
        i.__class__.__module__ = "m"
        i.__class__.__name__ = "S"
        mp = VaultFieldPointer(
            vault_reference="r",
            integrity_hash="h",
            version=1,
        )
        mc = AsyncMock()
        mc.ensure_secret = AsyncMock(return_value=mp)
        with patch(
            f"{_SA}.get_secrets_broker_client",
            return_value=mc,
        ):
            r = await prepare_instance_for_storage(
                i,
                {"p": "x"},
            )
        assert "vault_reference" in r["p"]

    @pytest.mark.asyncio
    async def test_hydrate_none(self) -> None:
        class _P:
            _sensitive_fields = None

        r = await hydrate_sensitive_fields(
            _P,
            {"n": "v"},
        )
        assert r == {"n": "v"}

    @pytest.mark.asyncio
    async def test_hydrate_plain(self) -> None:
        class _W:
            _sensitive_fields: ClassVar[dict[str, Any]] = {
                "k": SensitiveFieldConfig(mask_pattern="*"),
            }

        r = await hydrate_sensitive_fields(
            _W,
            {"k": "plain"},
        )
        assert r["k"] == "plain"

    @pytest.mark.asyncio
    async def test_hydrate_mismatch(self) -> None:
        class _I:
            _sensitive_fields: ClassVar[dict[str, Any]] = {
                "k": SensitiveFieldConfig(mask_pattern="*"),
            }
            __name__ = "_I"

        pd = {
            "vault_reference": "r",
            "integrity_hash": "exp",
            "version": 1,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        mc = AsyncMock()
        mc.retrieve_secret = AsyncMock(
            return_value=("v", "wrong"),
        )
        with (
            patch(
                f"{_SA}.get_secrets_broker_client",
                return_value=mc,
            ),
            pytest.raises(
                ValueError,
                match="Integrity",
            ),
        ):
            await hydrate_sensitive_fields(
                _I,
                {"k": pd},
            )

    def test_consume_pointer_cache(self) -> None:
        _POINTER_CONTEXT.set(None)
        assert consume_pointer_cache() is None
        c: dict[str, Any] = {"f": MagicMock()}
        _POINTER_CONTEXT.set(c)
        assert consume_pointer_cache() is c
        assert _POINTER_CONTEXT.get() is None
