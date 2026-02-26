"""Integration tests for encryption roundtrips without mocks.

Verifies that KeyManager, TokenEncryption, FieldEncryption,
TransparentEncryption, and PIIEncryption work end-to-end using
real cryptographic operations.
"""

from __future__ import annotations

import types

import pytest

from ami.core.exceptions import DecryptionError
from ami.models.security import (
    DataClassification,
    Permission,
    SecurityContext,
)
from ami.security.encryption import (
    FieldEncryption,
    KeyManager,
    PIIEncryption,
    TokenEncryption,
    TransparentEncryption,
)

# Named constants to satisfy PLR2004 (no magic numbers).
_SSN_LAST_FOUR = 4
_PHONE_AREA_CODE_LEN = 3
_BCRYPT_PREFIX = "$2b$"


class _FakeModel:
    """Minimal attribute-bag for TransparentEncryption."""

    def __init__(self, **kwargs: object) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _ctx_with_decrypt() -> types.SimpleNamespace:
    """Build a context that carries DECRYPT permission."""
    return types.SimpleNamespace(
        user_id="integration-tester",
        permissions=[Permission.DECRYPT],
    )


def _ctx_read_only() -> types.SimpleNamespace:
    """Build a context that only has READ permission."""
    return types.SimpleNamespace(
        user_id="viewer",
        permissions=[Permission.READ],
    )


def _ctx_no_perms() -> SecurityContext:
    """Build a SecurityContext with no permissions attribute."""
    return SecurityContext(
        user_id="unprivileged",
        roles=["guest"],
    )


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_key_manager():
    """Reset KeyManager state before and after each test."""
    KeyManager._fernet = None
    yield
    KeyManager._fernet = None


# ------------------------------------------------------------------
# 1. KeyManager -> TokenEncryption roundtrip
# ------------------------------------------------------------------


class TestKeyManagerTokenRoundtrip:
    """Initialize KeyManager, encrypt via TokenEncryption, decrypt."""

    def test_encrypt_then_decrypt_returns_original(self) -> None:
        test_key = "integration-test-master-key-001"
        KeyManager.initialize(test_key)

        enc = TokenEncryption()
        original = "top-secret-api-token-xyz"

        token = enc.encrypt(original)
        assert token != original
        assert isinstance(token, str)
        assert len(token) > 0

        recovered = enc.decrypt(token)
        assert recovered == original

    def test_different_keys_cannot_cross_decrypt(self) -> None:
        KeyManager.initialize("key-alpha")
        enc_a = TokenEncryption()
        token_a = enc_a.encrypt("sensitive-data")

        KeyManager._fernet = None
        KeyManager.initialize("key-beta")
        enc_b = TokenEncryption()

        with pytest.raises(DecryptionError):
            enc_b.decrypt(token_a)


# ------------------------------------------------------------------
# 2. FieldEncryption encrypt -> decrypt with SecurityContext
# ------------------------------------------------------------------


class TestFieldEncryptionRoundtrip:
    """FieldEncryption with CONFIDENTIAL classification."""

    def test_confidential_roundtrip_with_decrypt_perm(self) -> None:
        KeyManager.initialize("field-encryption-key-99")

        original = "secret-medical-record-12345"
        encrypted = FieldEncryption.encrypt_field(
            original,
            "medical_id",
            DataClassification.CONFIDENTIAL,
        )

        assert encrypted != original
        assert isinstance(encrypted, str)

        ctx = _ctx_with_decrypt()
        decrypted = FieldEncryption.decrypt_field(encrypted, "medical_id", ctx)
        assert decrypted == original

    def test_restricted_roundtrip_with_decrypt_perm(self) -> None:
        KeyManager.initialize("field-encryption-key-99")

        original = "ultra-classified-data"
        encrypted = FieldEncryption.encrypt_field(
            original,
            "classified_note",
            DataClassification.RESTRICTED,
        )

        assert encrypted != original

        ctx = _ctx_with_decrypt()
        decrypted = FieldEncryption.decrypt_field(encrypted, "classified_note", ctx)
        assert decrypted == original


# ------------------------------------------------------------------
# 3. FieldEncryption without DECRYPT permission
# ------------------------------------------------------------------


class TestFieldEncryptionNoPermission:
    """Decrypt without DECRYPT permission returns masked sentinel."""

    def test_no_perms_returns_encrypted_sentinel(self) -> None:
        KeyManager.initialize("no-perm-test-key-42")

        encrypted = FieldEncryption.encrypt_field(
            "hidden-value",
            "secret_field",
            DataClassification.CONFIDENTIAL,
        )

        ctx = _ctx_no_perms()
        result = FieldEncryption.decrypt_field(encrypted, "secret_field", ctx)
        assert result == "[ENCRYPTED]"

    def test_read_only_perm_returns_encrypted_sentinel(self) -> None:
        KeyManager.initialize("no-perm-test-key-42")

        encrypted = FieldEncryption.encrypt_field(
            "another-secret",
            "api_key",
            DataClassification.CONFIDENTIAL,
        )

        ctx = _ctx_read_only()
        result = FieldEncryption.decrypt_field(encrypted, "api_key", ctx)
        assert result == "[ENCRYPTED]"


# ------------------------------------------------------------------
# 4. TransparentEncryption encrypt_model -> decrypt_model
# ------------------------------------------------------------------


class TestTransparentEncryptionRoundtrip:
    """Encrypt and decrypt model instances with sensitive fields."""

    def test_roundtrip_preserves_sensitive_fields(self) -> None:
        KeyManager.initialize("transparent-enc-key-77")

        te = TransparentEncryption(
            model_class=_FakeModel,
            encrypted_fields=["ssn", "email", "api_key"],
        )

        model = _FakeModel(
            ssn="123-45-6789",
            email="alice@example.com",
            api_key="sk-abc123",
            name="Alice",
        )

        te.encrypt_model(model)

        assert model.ssn.startswith("[ENC:")
        assert model.ssn.endswith("]")
        assert model.email.startswith("[ENC:")
        assert model.api_key.startswith("[ENC:")
        assert model.name == "Alice"

        ctx = _ctx_with_decrypt()
        te.decrypt_model(model, ctx)

        assert model.ssn == "123-45-6789"
        assert model.email == "alice@example.com"
        assert model.api_key == "sk-abc123"
        assert model.name == "Alice"

    def test_no_permission_masks_encrypted_fields(self) -> None:
        KeyManager.initialize("transparent-enc-key-77")

        te = TransparentEncryption(
            model_class=_FakeModel,
            encrypted_fields=["email", "token"],
        )

        model = _FakeModel(email="bob@corp.io", token="t-secret")
        te.encrypt_model(model)

        ctx = _ctx_no_perms()
        te.decrypt_model(model, ctx)

        # "email" is a PII field, so it gets the PII sentinel.
        assert model.email == "[PII_ENCRYPTED]"
        # "token" is not recognized as PII.
        assert model.token == "[ENCRYPTED]"


# ------------------------------------------------------------------
# 5. PIIEncryption masking
# ------------------------------------------------------------------


class TestPIIMasking:
    """Verify SSN, email, and phone masking output."""

    def test_mask_ssn(self) -> None:
        masked = PIIEncryption.mask_pii("123456789", "ssn")
        assert masked == "***-**-6789"
        assert masked[-_SSN_LAST_FOUR:] == "6789"

    def test_mask_email(self) -> None:
        masked = PIIEncryption.mask_pii("alice@example.com", "email")
        assert masked == "a***@example.com"
        assert "@example.com" in masked

    def test_mask_phone(self) -> None:
        masked = PIIEncryption.mask_pii("5551234567", "phone")
        assert masked == "(555) ***-****"
        assert masked[1 : 1 + _PHONE_AREA_CODE_LEN] == "555"

    def test_mask_ssn_short_value_uses_generic(self) -> None:
        masked = PIIEncryption.mask_pii("12", "ssn")
        # Short SSN (< 4 chars) cannot mask last 4, dispatches generic.
        assert masked == "**"

    def test_mask_empty_string_is_passthrough(self) -> None:
        assert PIIEncryption.mask_pii("", "ssn") == ""
        assert PIIEncryption.mask_pii("", "email") == ""
        assert PIIEncryption.mask_pii("", "phone") == ""


# ------------------------------------------------------------------
# 6. bcrypt hash -> verify roundtrip
# ------------------------------------------------------------------


class TestBcryptHashRoundtrip:
    """FieldEncryption.hash_field and verify_hash end-to-end."""

    def test_hash_and_verify_correct_value(self) -> None:
        original = "super-secret-password-2024"
        hashed = FieldEncryption.hash_field(original)

        assert hashed.startswith(_BCRYPT_PREFIX)
        assert hashed != original
        assert FieldEncryption.verify_hash(original, hashed)

    def test_verify_rejects_wrong_value(self) -> None:
        hashed = FieldEncryption.hash_field("correct-password")
        assert not FieldEncryption.verify_hash("wrong-password", hashed)

    def test_same_input_produces_different_hashes(self) -> None:
        value = "deterministic-input"
        hash_a = FieldEncryption.hash_field(value)
        hash_b = FieldEncryption.hash_field(value)

        # bcrypt uses random salts, so two hashes differ.
        assert hash_a != hash_b
        # Both still verify against the original value.
        assert FieldEncryption.verify_hash(value, hash_a)
        assert FieldEncryption.verify_hash(value, hash_b)

    def test_verify_malformed_hash_returns_false(self) -> None:
        assert not FieldEncryption.verify_hash("anything", "not-a-bcrypt-hash")
