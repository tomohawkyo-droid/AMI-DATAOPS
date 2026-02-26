"""Tests for ami.security.encryption module."""

from __future__ import annotations

import types

import pytest

from ami.core.exceptions import (
    ConfigurationError,
    DecryptionError,
)
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


class _FakeModel:
    """Minimal model stand-in for TransparentEncryption."""

    def __init__(self, **kwargs: object) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)


def _ctx_with_decrypt() -> types.SimpleNamespace:
    """Return a context object that carries DECRYPT permission."""
    return types.SimpleNamespace(
        user_id="tester",
        permissions=[Permission.DECRYPT],
    )


def _ctx_without_decrypt() -> types.SimpleNamespace:
    """Return a context object without DECRYPT permission."""
    return types.SimpleNamespace(
        user_id="viewer",
        permissions=[Permission.READ],
    )


def _ctx_no_perms() -> SecurityContext:
    """Return a plain SecurityContext (no permissions attr)."""
    return SecurityContext(
        user_id="nobody",
        roles=["guest"],
    )


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_key_manager():
    KeyManager._fernet = None
    yield
    KeyManager._fernet = None


# ------------------------------------------------------------------
# KeyManager
# ------------------------------------------------------------------


class TestKeyManager:
    """KeyManager initialization and retrieval."""

    def test_initialize_with_explicit_key(self) -> None:
        KeyManager.initialize("my-explicit-key")
        fernet = KeyManager.get_fernet()
        assert fernet is not None

    def test_initialize_from_env(self) -> None:
        KeyManager.initialize()
        fernet = KeyManager.get_fernet()
        assert fernet is not None

    def test_initialize_no_key_env_cleared(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("DATAOPS_MASTER_KEY", raising=False)
        with pytest.raises(ConfigurationError):
            KeyManager.initialize()

    def test_get_fernet_after_init(self) -> None:
        KeyManager.initialize("test-key-abc")
        fernet = KeyManager.get_fernet()
        assert fernet is not None
        assert fernet is KeyManager._fernet

    def test_get_fernet_auto_initializes_from_env(self) -> None:
        assert KeyManager._fernet is None
        fernet = KeyManager.get_fernet()
        assert fernet is not None
        assert KeyManager._fernet is not None

    def test_same_key_produces_same_fernet_output(self) -> None:
        KeyManager.initialize("deterministic-key")
        f1 = KeyManager.get_fernet()
        plaintext = b"hello world"
        token = f1.encrypt(plaintext)

        KeyManager._fernet = None
        KeyManager.initialize("deterministic-key")
        f2 = KeyManager.get_fernet()
        assert f2.decrypt(token) == plaintext


# ------------------------------------------------------------------
# TokenEncryption
# ------------------------------------------------------------------


class TestTokenEncryption:
    """Encrypt / decrypt round-trip via TokenEncryption."""

    def test_roundtrip(self) -> None:
        enc = TokenEncryption()
        original = "sensitive-value-42"
        token = enc.encrypt(original)
        assert token != original
        assert enc.decrypt(token) == original

    def test_encrypt_empty_returns_empty(self) -> None:
        enc = TokenEncryption()
        assert enc.encrypt("") == ""

    def test_decrypt_invalid_token_raises(self) -> None:
        enc = TokenEncryption()
        with pytest.raises(DecryptionError):
            enc.decrypt("not-a-valid-token!!!")


# ------------------------------------------------------------------
# FieldEncryption
# ------------------------------------------------------------------


class TestFieldEncryption:
    """Static encrypt / decrypt / hash helpers."""

    def test_encrypt_field_confidential(self) -> None:
        encrypted = FieldEncryption.encrypt_field(
            "secret-data",
            "api_key",
            DataClassification.CONFIDENTIAL,
        )
        assert encrypted != "secret-data"
        assert isinstance(encrypted, str)

    def test_encrypt_field_below_confidential(self) -> None:
        result = FieldEncryption.encrypt_field(
            "public-data",
            "title",
            DataClassification.INTERNAL,
        )
        assert result == "public-data"

    def test_encrypt_field_public_returns_str(self) -> None:
        result = FieldEncryption.encrypt_field(
            12345,
            "count",
            DataClassification.PUBLIC,
        )
        assert result == "12345"

    def test_decrypt_field_with_permission(self) -> None:
        encrypted = FieldEncryption.encrypt_field(
            "unlock-me",
            "secret",
            DataClassification.CONFIDENTIAL,
        )
        ctx = _ctx_with_decrypt()
        decrypted = FieldEncryption.decrypt_field(encrypted, "secret", ctx)
        assert decrypted == "unlock-me"

    def test_decrypt_field_without_permission(self) -> None:
        encrypted = FieldEncryption.encrypt_field(
            "hidden",
            "secret",
            DataClassification.CONFIDENTIAL,
        )
        ctx = _ctx_no_perms()
        result = FieldEncryption.decrypt_field(encrypted, "secret", ctx)
        assert result == "[ENCRYPTED]"

    def test_decrypt_field_invalid_data_raises(self) -> None:
        ctx = _ctx_with_decrypt()
        with pytest.raises(DecryptionError):
            FieldEncryption.decrypt_field("garbled-ciphertext", "field_x", ctx)

    def test_hash_field_bcrypt(self) -> None:
        hashed = FieldEncryption.hash_field("password123")
        assert hashed.startswith("$2b$")

    def test_verify_hash_correct(self) -> None:
        hashed = FieldEncryption.hash_field("correct-horse")
        assert FieldEncryption.verify_hash("correct-horse", hashed)

    def test_verify_hash_incorrect(self) -> None:
        hashed = FieldEncryption.hash_field("right-value")
        assert not FieldEncryption.verify_hash("wrong-value", hashed)

    def test_verify_hash_malformed(self) -> None:
        assert not FieldEncryption.verify_hash("anything", "not-a-bcrypt-hash")


# ------------------------------------------------------------------
# PIIEncryption
# ------------------------------------------------------------------


class TestPIIEncryption:
    """PII detection and masking."""

    @pytest.mark.parametrize(
        "field",
        sorted(PIIEncryption.PII_FIELDS),
    )
    def test_is_pii_field_matches_all_entries(self, field: str) -> None:
        assert PIIEncryption.is_pii_field(field)

    def test_is_pii_field_non_pii(self) -> None:
        assert not PIIEncryption.is_pii_field("created_at")
        assert not PIIEncryption.is_pii_field("record_count")

    def test_is_pii_field_case_insensitive(self) -> None:
        assert PIIEncryption.is_pii_field("user_email")
        assert PIIEncryption.is_pii_field("USER_EMAIL")
        assert PIIEncryption.is_pii_field("Email_Address")

    def test_mask_ssn(self) -> None:
        assert PIIEncryption._mask_ssn("123456789") == "***-**-6789"

    def test_mask_credit_card(self) -> None:
        assert (
            PIIEncryption._mask_credit_card("4111111111111111") == "****-****-****-1111"
        )

    def test_mask_email(self) -> None:
        assert PIIEncryption._mask_email("user@example.com") == "u***@example.com"

    def test_mask_phone(self) -> None:
        assert PIIEncryption._mask_phone("5551234567") == "(555) ***-****"

    def test_mask_generic(self) -> None:
        assert PIIEncryption._mask_generic("secret") == "s****t"

    def test_mask_generic_short(self) -> None:
        assert PIIEncryption._mask_generic("ab") == "**"

    def test_mask_pii_dispatches_ssn(self) -> None:
        result = PIIEncryption.mask_pii("123456789", "ssn")
        assert result == "***-**-6789"

    def test_mask_pii_dispatches_email(self) -> None:
        result = PIIEncryption.mask_pii("user@example.com", "email")
        assert result == "u***@example.com"

    def test_mask_pii_dispatches_credit_card(self) -> None:
        result = PIIEncryption.mask_pii("4111111111111111", "credit_card")
        assert result == "****-****-****-1111"

    def test_mask_pii_dispatches_phone(self) -> None:
        result = PIIEncryption.mask_pii("5551234567", "phone")
        assert result == "(555) ***-****"

    def test_mask_pii_dispatches_generic(self) -> None:
        result = PIIEncryption.mask_pii("anything", "generic")
        assert result == "a******g"

    def test_mask_pii_empty_string(self) -> None:
        assert PIIEncryption.mask_pii("") == ""


# ------------------------------------------------------------------
# TransparentEncryption
# ------------------------------------------------------------------


class TestTransparentEncryption:
    """Automatic encrypt / decrypt on model fields."""

    def _make_te(self) -> TransparentEncryption:
        return TransparentEncryption(
            model_class=_FakeModel,
            encrypted_fields=["secret", "token"],
        )

    def test_encrypt_model_marks_fields(self) -> None:
        te = self._make_te()
        model = _FakeModel(secret="abc", token="xyz", name="ok")
        te.encrypt_model(model)
        assert model.secret.startswith("[ENC:")
        assert model.secret.endswith("]")
        assert model.token.startswith("[ENC:")
        assert model.name == "ok"

    def test_encrypt_model_skips_already_encrypted(self) -> None:
        te = self._make_te()
        already = "[ENC:some-cipher-text]"
        model = _FakeModel(secret=already, token="plain")
        te.encrypt_model(model)
        assert model.secret == already
        assert model.token.startswith("[ENC:")

    def test_decrypt_model_with_permission(self) -> None:
        te = self._make_te()
        model = _FakeModel(secret="my-secret", token="my-token", name="ok")
        te.encrypt_model(model)
        ctx = _ctx_with_decrypt()
        te.decrypt_model(model, ctx)
        assert model.secret == "my-secret"
        assert model.token == "my-token"
        assert model.name == "ok"

    def test_decrypt_model_without_permission(self) -> None:
        te = self._make_te()
        model = _FakeModel(secret="hidden", token="also-hidden")
        te.encrypt_model(model)
        ctx = _ctx_no_perms()
        te.decrypt_model(model, ctx)
        assert model.secret == "[ENCRYPTED]"
        assert model.token == "[ENCRYPTED]"

    def test_decrypt_model_pii_without_permission(self) -> None:
        te = TransparentEncryption(
            model_class=_FakeModel,
            encrypted_fields=["email", "ssn"],
        )
        model = _FakeModel(email="a@b.com", ssn="123456789")
        te.encrypt_model(model)
        ctx = _ctx_no_perms()
        te.decrypt_model(model, ctx)
        assert model.email == "[PII_ENCRYPTED]"
        assert model.ssn == "[PII_ENCRYPTED]"
