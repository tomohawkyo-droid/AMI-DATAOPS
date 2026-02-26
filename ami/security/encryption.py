"""Field-level encryption for PII and sensitive data."""

import base64
import logging
import os
from typing import Any, ClassVar

import bcrypt
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from ami.core.exceptions import ConfigurationError, DecryptionError
from ami.models.security import (
    DataClassification,
    Permission,
    SecurityContext,
)

logger = logging.getLogger(__name__)

# Fixed application salt for PBKDF2 key derivation (not a secret)
_APP_KDF_SALT = b"ami-dataops-field-encryption-v1"
_KDF_ITERATIONS = 600_000


class KeyManager:
    """Manage encryption keys via a single Fernet derived from the master key."""

    _fernet: ClassVar[Fernet | None] = None

    @classmethod
    def initialize(cls, master_key: str | None = None) -> None:
        """Derive a Fernet key from the master key."""
        raw = master_key or os.getenv("DATAOPS_MASTER_KEY")
        if not raw:
            msg = (
                "Master encryption key required. "
                "Set DATAOPS_MASTER_KEY or pass master_key."
            )
            raise ConfigurationError(msg)
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=_APP_KDF_SALT,
            iterations=_KDF_ITERATIONS,
        )
        derived = kdf.derive(raw.encode())
        cls._fernet = Fernet(base64.urlsafe_b64encode(derived))

    @classmethod
    def get_fernet(cls) -> Fernet:
        """Return the initialized Fernet instance."""
        if cls._fernet is None:
            cls.initialize()
        if cls._fernet is None:
            msg = "Encryption not initialized"
            raise ConfigurationError(msg)
        return cls._fernet


class TokenEncryption:
    """Token-based encryption for fields."""

    def __init__(self, fernet: Fernet | None = None) -> None:
        self.cipher = fernet or KeyManager.get_fernet()

    def encrypt(self, value: str) -> str:
        """Encrypt value and return base64-encoded token."""
        if not value:
            return value
        encrypted = self.cipher.encrypt(value.encode())
        return base64.urlsafe_b64encode(encrypted).decode()

    def decrypt(self, token: str) -> str:
        """Decrypt token and return original value."""
        if not token:
            return token
        try:
            encrypted = base64.urlsafe_b64decode(token.encode())
            decrypted: bytes = self.cipher.decrypt(encrypted)
            return decrypted.decode()
        except Exception as exc:
            msg = "Decryption failed"
            raise DecryptionError(msg) from exc


class FieldEncryption:
    """Encrypt specific fields in models."""

    @staticmethod
    def encrypt_field(
        value: Any,
        field_name: str,
        classification: DataClassification,
    ) -> str:
        """Encrypt field based on classification."""
        if classification >= DataClassification.CONFIDENTIAL:
            fernet = KeyManager.get_fernet()
            return fernet.encrypt(str(value).encode()).decode()
        return str(value)

    @staticmethod
    def decrypt_field(
        encrypted: str,
        field_name: str,
        context: SecurityContext,
    ) -> Any:
        """Decrypt field with permission check."""
        if (
            not hasattr(context, "permissions")
            or Permission.DECRYPT not in context.permissions
        ):
            return "[ENCRYPTED]"
        fernet = KeyManager.get_fernet()
        try:
            return fernet.decrypt(encrypted.encode()).decode()
        except Exception as exc:
            msg = f"Decryption failed for field {field_name}"
            raise DecryptionError(msg) from exc

    @staticmethod
    def hash_field(value: str) -> str:
        """One-way hash using bcrypt."""
        return bcrypt.hashpw(value.encode(), bcrypt.gensalt()).decode()

    @staticmethod
    def verify_hash(value: str, hashed: str) -> bool:
        """Verify value against bcrypt hash."""
        try:
            return bcrypt.checkpw(value.encode(), hashed.encode())
        except ValueError:
            return False


class PIIEncryption:
    """Special handling for PII fields."""

    PII_FIELDS: ClassVar[set[str]] = {
        "ssn",
        "social_security",
        "tax_id",
        "passport",
        "driver_license",
        "credit_card",
        "bank_account",
        "email",
        "phone",
        "address",
        "date_of_birth",
        "medical_record",
        "health_info",
    }

    @classmethod
    def is_pii_field(cls, field_name: str) -> bool:
        """Check if field contains PII."""
        field_lower = field_name.lower()
        return any(pii in field_lower for pii in cls.PII_FIELDS)

    @classmethod
    def _mask_ssn(cls, value: str) -> str | None:
        """Mask SSN showing last 4 digits."""
        mask_last_digits = 4
        if len(value) >= mask_last_digits:
            return f"***-**-{value[-mask_last_digits:]}"
        return None

    @classmethod
    def _mask_credit_card(cls, value: str) -> str | None:
        """Mask credit card showing last 4 digits."""
        mask_last_digits = 4
        if len(value) >= mask_last_digits:
            return f"****-****-****-{value[-mask_last_digits:]}"
        return None

    @classmethod
    def _mask_email(cls, value: str) -> str | None:
        """Mask email showing first char and domain."""
        if "@" in value:
            local, domain = value.split("@", 1)
            if len(local) > 1:
                return f"{local[0]}***@{domain}"
        return None

    @classmethod
    def _mask_phone(cls, value: str) -> str | None:
        """Mask phone showing area code."""
        phone_min_length = 10
        if len(value) >= phone_min_length:
            return f"({value[:3]}) ***-****"
        return None

    @classmethod
    def _mask_generic(cls, value: str) -> str:
        """Generic masking showing first and last char."""
        min_length_for_partial = 2
        if len(value) > min_length_for_partial:
            return f"{value[0]}{'*' * (len(value) - 2)}{value[-1]}"
        return "*" * len(value)

    @classmethod
    def mask_pii(cls, value: str, field_type: str = "generic") -> str:
        """Mask PII for display."""
        if not value:
            return value
        maskers = {
            "ssn": cls._mask_ssn,
            "credit_card": cls._mask_credit_card,
            "email": cls._mask_email,
            "phone": cls._mask_phone,
        }
        if field_type in maskers:
            result = maskers[field_type](value)
            if result is not None:
                return result
        return cls._mask_generic(value)


class TransparentEncryption:
    """Transparent encryption for database fields.

    Automatically encrypts/decrypts on save/load.
    """

    def __init__(
        self,
        model_class: type,
        encrypted_fields: list[str],
    ) -> None:
        self.model_class = model_class
        self.encrypted_fields = encrypted_fields

    def encrypt_model(self, instance: Any) -> Any:
        """Encrypt fields in model instance."""
        fernet = KeyManager.get_fernet()
        encryptor = TokenEncryption(fernet)
        for field in self.encrypted_fields:
            if hasattr(instance, field):
                value = getattr(instance, field)
                if value and not value.startswith("[ENC:"):
                    encrypted = encryptor.encrypt(str(value))
                    setattr(instance, field, f"[ENC:{encrypted}]")
        return instance

    def decrypt_model(
        self,
        instance: Any,
        context: SecurityContext,
    ) -> Any:
        """Decrypt fields in model instance."""
        for field in self.encrypted_fields:
            if hasattr(instance, field):
                value = getattr(instance, field)
                if value and value.startswith("[ENC:"):
                    encrypted = value[5:-1]
                    if (
                        hasattr(context, "permissions")
                        and Permission.DECRYPT in context.permissions
                    ):
                        fernet = KeyManager.get_fernet()
                        decryptor = TokenEncryption(fernet)
                        decrypted = decryptor.decrypt(encrypted)
                        setattr(instance, field, decrypted)
                    elif PIIEncryption.is_pii_field(field):
                        setattr(instance, field, "[PII_ENCRYPTED]")
                    else:
                        setattr(instance, field, "[ENCRYPTED]")
        return instance
