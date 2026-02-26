"""Decorators for sensitive field handling."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, TypeVar

from uuid_utils import uuid7

from ami.models.base_model import StorageModel
from ami.models.security import DataClassification
from ami.secrets.config import SensitiveFieldConfig

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=StorageModel)


def sensitive_field(
    field_name: str,
    mask_pattern: str = "{field}_uid",
    *,
    classification: DataClassification | None = None,
    namespace: str | None = None,
    auto_rotate_days: int | None = None,
) -> Callable[[type], type]:
    """Mark a field as sensitive for vault-backed persistence."""
    config = SensitiveFieldConfig(
        mask_pattern=mask_pattern,
        classification=classification,
        namespace=namespace,
        auto_rotate_days=auto_rotate_days,
    )

    def decorator(cls: type[T]) -> type[T]:
        attr = "_sensitive_fields"
        if not hasattr(cls, attr):
            setattr(cls, attr, {})
        sensitive_fields = getattr(cls, attr)
        if isinstance(sensitive_fields, dict):
            sensitive_fields[field_name] = config
        return cls

    return decorator


def sanitize_for_mcp(
    instance: StorageModel,
    caller: str = "mcp",
) -> dict[str, Any]:
    """Sanitize model instance for MCP server output.

    Replaces sensitive field values with masked versions.
    """
    data = instance.model_dump()

    if hasattr(instance.__class__, "_sensitive_fields"):
        for field_name, config in instance.__class__._sensitive_fields.items():
            if field_name in data:
                mask_value = config.mask_value(field_name)
                if "uid" in mask_value.lower():
                    mask_value = f"{mask_value}_{uuid7()}"
                data[field_name] = mask_value
                logger.debug(
                    "Masked sensitive field '%s' for %s",
                    field_name,
                    caller,
                )

    return data
