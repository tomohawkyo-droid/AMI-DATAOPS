"""Tests for DataOps decorators."""

from typing import Any, ClassVar

import pytest

from ami.core.storage_types import StorageType
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig
from ami.secrets.client import (
    InMemorySecretsBackend,
    SecretsBrokerClient,
    reset_secrets_broker_client,
    set_secrets_broker_client,
)
from ami.secrets.pointer import VaultFieldPointer
from ami.services.decorators import (
    sanitize_for_mcp,
    sensitive_field,
)

TEST_PASSWORD = "secret123"
TEST_API_KEY = "key_abc123"

EXPECTED_ENSURE_CALLS = 2


@sensitive_field("password", mask_pattern="pwd_masked")
@sensitive_field("api_key", mask_pattern="{field}_hidden")
class SampleUser(StorageModel):
    """Test user model with sensitive fields."""

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="test_users",
        storage_configs={
            "memory": StorageConfig(storage_type=StorageType.INMEM),
        },
    )

    username: str
    password: str
    api_key: str = "secret_key_123"
    email: str = "test@example.com"


class TestDecorators:
    """Test decorator functionality."""

    def test_sensitive_field_decorator(self) -> None:
        assert hasattr(SampleUser, "_sensitive_fields")
        assert "password" in SampleUser._sensitive_fields
        assert "api_key" in SampleUser._sensitive_fields

        password_config = SampleUser._sensitive_fields["password"]
        api_key_config = SampleUser._sensitive_fields["api_key"]

        assert password_config.mask_pattern == "pwd_masked"
        assert api_key_config.mask_pattern == "{field}_hidden"

    def test_sanitize_for_mcp(self) -> None:
        user = SampleUser(
            username="john",
            password=TEST_PASSWORD,
            api_key=TEST_API_KEY,
        )
        sanitized = sanitize_for_mcp(user, caller="mcp")
        assert sanitized["username"] == "john"
        assert sanitized["password"] == "pwd_masked"
        assert "api_key_hidden" in sanitized["api_key"]
        assert sanitized["email"] == "test@example.com"

    @pytest.mark.asyncio
    async def test_sensitive_field_storage_and_hydration(self) -> None:
        class CountingBackend(InMemorySecretsBackend):
            def __init__(self) -> None:
                super().__init__()
                self.ensure_calls = 0

            async def ensure_secret(
                self,
                *,
                namespace: str,
                model: str,
                field: str,
                value: str,
                classification: Any | None = None,
            ) -> VaultFieldPointer:
                self.ensure_calls += 1
                return await super().ensure_secret(
                    namespace=namespace,
                    model=model,
                    field=field,
                    value=value,
                    classification=classification,
                )

        backend = CountingBackend()
        client = SecretsBrokerClient(backend=backend)
        set_secrets_broker_client(client)
        try:
            user = SampleUser(
                username="john",
                password=TEST_PASSWORD,
                api_key=TEST_API_KEY,
            )
            payload = await user.to_storage_dict()

            pw_pointer = payload["password"]
            ak_pointer = payload["api_key"]
            assert isinstance(pw_pointer, dict)
            assert isinstance(ak_pointer, dict)
            assert "vault_reference" in pw_pointer
            assert "integrity_hash" in pw_pointer
            assert backend.ensure_calls == EXPECTED_ENSURE_CALLS

            await user.to_storage_dict()
            assert backend.ensure_calls == EXPECTED_ENSURE_CALLS

            hydrated = await SampleUser.from_storage_dict(payload)
            assert hydrated.password == TEST_PASSWORD
            assert hydrated.api_key == TEST_API_KEY
        finally:
            reset_secrets_broker_client()
