"""Tests for ami.storage.validator module."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

from ami.core.exceptions import StorageError
from ami.core.storage_types import StorageType
from ami.models.storage_config import StorageConfig
from ami.storage.registry import ModelStorageUsage
from ami.storage.validator import (
    StorageValidationResult,
    StorageValidator,
)

# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------

_CFG_DEFAULTS: dict[str, Any] = {
    "storage_type": StorageType.RELATIONAL,
    "host": "db.local",
    "port": 5432,
    "database": "mydb",
    "username": "user",
    "password": "pass",
    "options": {},
    "name": None,
}


def _make_config(**overrides: Any) -> StorageConfig:
    merged = {**_CFG_DEFAULTS, **overrides}
    return StorageConfig(**merged)


def _mock_registry(
    configs: dict[str, StorageConfig] | None = None,
    usage: list[ModelStorageUsage] | None = None,
    usage_index: dict[str, list[str]] | None = None,
) -> MagicMock:
    registry = MagicMock()
    registry.list_configs.return_value = configs or {}
    registry.get_model_usage.return_value = usage or []
    registry.get_config_usage_index.return_value = usage_index or {}
    return registry


def _mock_dao(
    connect_ok: bool = True,
    test_result: bool = True,
) -> AsyncMock:
    dao = AsyncMock()
    if connect_ok:
        dao.connect.return_value = None
    else:
        dao.connect.side_effect = StorageError("refused")
    dao.test_connection.return_value = test_result
    dao.disconnect.return_value = None
    return dao


# ---------------------------------------------------------------
# TestStorageValidationResult
# ---------------------------------------------------------------


class TestStorageValidationResult:
    """StorageValidationResult.to_dict tests."""

    def test_to_dict_keys(self) -> None:
        result = StorageValidationResult(
            name="pg",
            storage_type="postgres",
            status="ok",
            details=None,
            missing_fields=[],
            models=["app.User"],
        )
        d = result.to_dict()
        expected_keys = {
            "name",
            "storage_type",
            "status",
            "details",
            "missing_fields",
            "models",
        }
        assert set(d.keys()) == expected_keys

    def test_to_dict_values(self) -> None:
        result = StorageValidationResult(
            name="redis",
            storage_type="redis",
            status="error",
            details="Connection refused",
            missing_fields=["host"],
            models=[],
        )
        d = result.to_dict()
        assert d["name"] == "redis"
        assert d["storage_type"] == "redis"
        assert d["status"] == "error"
        assert d["details"] == "Connection refused"
        assert d["missing_fields"] == ["host"]
        assert d["models"] == []

    def test_to_dict_defaults(self) -> None:
        result = StorageValidationResult(
            name="x",
            storage_type=None,
            status="ok",
        )
        d = result.to_dict()
        assert d["details"] is None
        assert d["missing_fields"] == []
        assert d["models"] == []


# ---------------------------------------------------------------
# TestIsFieldMissing
# ---------------------------------------------------------------


class TestIsFieldMissing:
    """StorageValidator._is_field_missing tests."""

    def test_standard_field_present(self) -> None:
        cfg = _make_config(host="db.local")
        assert not StorageValidator._is_field_missing(cfg, "host")

    def test_standard_field_none(self) -> None:
        cfg = _make_config(host=None)
        assert StorageValidator._is_field_missing(cfg, "host")

    def test_standard_field_empty(self) -> None:
        cfg = _make_config(username="")
        assert StorageValidator._is_field_missing(cfg, "username")

    def test_options_token_present(self) -> None:
        cfg = _make_config(options={"token": "abc123"})
        assert not StorageValidator._is_field_missing(cfg, "options.token")

    def test_options_token_missing(self) -> None:
        cfg = _make_config(options={})
        assert StorageValidator._is_field_missing(cfg, "options.token")

    def test_options_token_none_value(self) -> None:
        cfg = _make_config(options={"token": None})
        assert StorageValidator._is_field_missing(cfg, "options.token")

    def test_options_none_dict(self) -> None:
        cfg = _make_config(options=None)
        assert StorageValidator._is_field_missing(cfg, "options.token")


# ---------------------------------------------------------------
# TestMissingRequiredFields
# ---------------------------------------------------------------


class TestMissingRequiredFields:
    """StorageValidator._missing_required_fields tests."""

    def test_relational_all_present(self) -> None:
        cfg = _make_config(
            storage_type=StorageType.RELATIONAL,
        )
        result = StorageValidator._missing_required_fields(cfg)
        assert result == []

    def test_relational_missing_host(self) -> None:
        cfg = _make_config(storage_type=StorageType.RELATIONAL, host=None)
        result = StorageValidator._missing_required_fields(cfg)
        assert result == ["host"]

    def test_graph_missing_host(self) -> None:
        cfg = _make_config(
            storage_type=StorageType.GRAPH,
            host=None,
        )
        result = StorageValidator._missing_required_fields(cfg)
        assert "host" in result

    def test_vault_missing_options_token(self) -> None:
        cfg = _make_config(
            storage_type=StorageType.VAULT,
            host="vault.local",
            port=8200,
            options={},
        )
        result = StorageValidator._missing_required_fields(cfg)
        assert "options.token" in result

    def test_none_storage_type(self) -> None:
        cfg = _make_config(storage_type=None, port=None)
        result = StorageValidator._missing_required_fields(cfg)
        assert result == ["storage_type"]


# ---------------------------------------------------------------
# TestValidateAll
# ---------------------------------------------------------------


class TestValidateAll:
    """StorageValidator.validate_all tests."""

    async def test_success(self) -> None:
        cfg = _make_config(name="pg")
        dao = _mock_dao(connect_ok=True, test_result=True)
        factory = MagicMock(return_value=dao)
        registry = _mock_registry(
            configs={"pg": cfg},
            usage_index={"pg": ["app.User"]},
        )
        validator = StorageValidator(registry=registry, dao_factory=factory)
        results = await validator.validate_all()
        assert len(results) == 1
        assert results[0].status == "ok"
        assert results[0].name == "pg"
        assert results[0].models == ["app.User"]
        dao.connect.assert_awaited_once()
        dao.test_connection.assert_awaited_once()

    async def test_connection_failure(self) -> None:
        cfg = _make_config(name="pg")
        dao = _mock_dao(connect_ok=False)
        factory = MagicMock(return_value=dao)
        registry = _mock_registry(configs={"pg": cfg})
        validator = StorageValidator(registry=registry, dao_factory=factory)
        results = await validator.validate_all()
        assert len(results) == 1
        assert results[0].status == "error"
        assert "refused" in (results[0].details or "")

    async def test_unknown_config_name(self) -> None:
        registry = _mock_registry(configs={})
        validator = StorageValidator(registry=registry)
        results = await validator.validate_all(names=["missing"])
        assert len(results) == 1
        assert results[0].status == "error"
        assert results[0].storage_type is None
        assert "Unknown" in (results[0].details or "")

    async def test_missing_fields_skips_dao(self) -> None:
        cfg = _make_config(storage_type=StorageType.RELATIONAL, host=None)
        factory = MagicMock()
        registry = _mock_registry(configs={"pg": cfg})
        validator = StorageValidator(registry=registry, dao_factory=factory)
        results = await validator.validate_all()
        assert len(results) == 1
        assert results[0].status == "error"
        assert "host" in results[0].missing_fields
        factory.assert_not_called()


# ---------------------------------------------------------------
# TestValidateForModel
# ---------------------------------------------------------------


class TestValidateForModel:
    """StorageValidator.validate_for_model tests."""

    async def test_found_model(self) -> None:
        cfg = _make_config(name="pg")
        dao = _mock_dao()
        factory = MagicMock(return_value=dao)
        usage = [
            ModelStorageUsage(
                model="app.models.User",
                storages=[
                    {
                        "name": "pg",
                        "storage_type": "postgres",
                        "primary": True,
                    }
                ],
            ),
        ]
        registry = _mock_registry(
            configs={"pg": cfg},
            usage=usage,
            usage_index={"pg": ["app.models.User"]},
        )
        validator = StorageValidator(registry=registry, dao_factory=factory)
        results = await validator.validate_for_model("User")
        assert len(results) == 1
        assert results[0].name == "pg"

    async def test_unknown_model(self) -> None:
        registry = _mock_registry(usage=[])
        validator = StorageValidator(registry=registry)
        results = await validator.validate_for_model("NoSuch")
        assert results == []


# ---------------------------------------------------------------
# TestBuildValidationDao
# ---------------------------------------------------------------


class TestBuildValidationDao:
    """StorageValidator._build_validation_dao tests."""

    def test_calls_factory_with_correct_args(self) -> None:
        cfg = _make_config(name="pg")
        factory = MagicMock(return_value=MagicMock())
        validator = StorageValidator(dao_factory=factory)
        validator._build_validation_dao(cfg)
        factory.assert_called_once()
        call_args = factory.call_args
        model_cls = call_args[0][0]
        passed_config = call_args[0][1]
        assert passed_config is cfg
        meta = model_cls.get_metadata()
        assert "validation" in meta.storage_configs
        assert meta.storage_configs["validation"] is cfg
