"""Tests for SSHConfig model."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from ami.models.ssh_config import SSHConfig

_DEFAULT_PORT = 22
_DEFAULT_TIMEOUT = 30
_CUSTOM_PORT = 2222
_CUSTOM_TIMEOUT = 60
_VALID_PORT = 8022
_INVALID_PORT_HIGH = 65536
_POSITIVE_TIMEOUT = 120


class TestValidateName:
    """Name field validation."""

    def test_valid_alphanumeric(self) -> None:
        cfg = SSHConfig(name="server01")
        assert cfg.name == "server01"

    def test_hyphens_accepted(self) -> None:
        cfg = SSHConfig(name="web-prod-01")
        assert cfg.name == "web-prod-01"

    def test_underscores_accepted(self) -> None:
        cfg = SSHConfig(name="db_backup_02")
        assert cfg.name == "db_backup_02"

    def test_none_accepted(self) -> None:
        cfg = SSHConfig(name=None)
        assert cfg.name is None

    def test_whitespace_only_raises(self) -> None:
        with pytest.raises(ValidationError, match="Server name"):
            SSHConfig(name="   ")

    def test_spaces_raise(self) -> None:
        with pytest.raises(ValidationError, match="alphanumeric"):
            SSHConfig(name="my server")

    def test_dots_raise(self) -> None:
        with pytest.raises(ValidationError, match="alphanumeric"):
            SSHConfig(name="prod.web.01")

    def test_strips_whitespace_on_valid_name(self) -> None:
        """Strip is applied after regex on valid names."""
        cfg = SSHConfig(name="nodeA")
        assert cfg.name == "nodeA"


class TestDefaults:
    """Default values for SSHConfig fields."""

    def test_port_defaults_to_22(self) -> None:
        cfg = SSHConfig()
        assert cfg.port == _DEFAULT_PORT

    def test_timeout_defaults_to_30(self) -> None:
        cfg = SSHConfig()
        assert cfg.timeout == _DEFAULT_TIMEOUT

    def test_allow_agent_true(self) -> None:
        cfg = SSHConfig()
        assert cfg.allow_agent is True

    def test_look_for_keys_true(self) -> None:
        cfg = SSHConfig()
        assert cfg.look_for_keys is True

    def test_compression_false(self) -> None:
        cfg = SSHConfig()
        assert cfg.compression is False


class TestToParamikoConfig:
    """to_paramiko_config with all fields populated."""

    def test_all_fields_produce_complete_dict(self) -> None:
        cfg = SSHConfig(
            host="10.0.0.1",
            port=_CUSTOM_PORT,
            username="deploy",
            password="s3cret",
            timeout=_CUSTOM_TIMEOUT,
            key_filename="/tmp/test_keys/id_rsa",
            passphrase="kpass",
            known_hosts_file="/etc/ssh/known_hosts",
            allow_agent=False,
            look_for_keys=False,
            compression=True,
        )
        result = cfg.to_paramiko_config()
        assert result["hostname"] == "10.0.0.1"
        assert result["port"] == _CUSTOM_PORT
        assert result["username"] == "deploy"
        assert result["timeout"] == _CUSTOM_TIMEOUT
        assert result["compress"] is True
        assert result["allow_agent"] is False
        assert result["look_for_keys"] is False
        assert result["password"] == "s3cret"
        assert result["key_filename"] == "/tmp/test_keys/id_rsa"
        assert result["passphrase"] == "kpass"
        assert result["known_hosts_filename"] == ("/etc/ssh/known_hosts")


class TestToParamikoConfigMinimal:
    """to_paramiko_config with only required fields."""

    def test_optional_keys_absent(self) -> None:
        cfg = SSHConfig(host="10.0.0.1", username="admin")
        result = cfg.to_paramiko_config()
        assert result["hostname"] == "10.0.0.1"
        assert result["username"] == "admin"
        assert "password" not in result
        assert "key_filename" not in result
        assert "passphrase" not in result
        assert "known_hosts_filename" not in result


class TestPortValidation:
    """Port validation inherited from IPConfig."""

    def test_zero_raises(self) -> None:
        with pytest.raises(ValidationError, match="Port must be between"):
            SSHConfig(port=0)

    def test_above_max_raises(self) -> None:
        with pytest.raises(ValidationError, match="Port must be between"):
            SSHConfig(port=_INVALID_PORT_HIGH)

    def test_valid_port_accepted(self) -> None:
        cfg = SSHConfig(port=_VALID_PORT)
        assert cfg.port == _VALID_PORT


class TestTimeoutValidation:
    """Timeout validation inherited from IPConfig."""

    def test_zero_raises(self) -> None:
        with pytest.raises(ValidationError, match="Timeout must be positive"):
            SSHConfig(timeout=0)

    def test_negative_raises(self) -> None:
        with pytest.raises(ValidationError, match="Timeout must be positive"):
            SSHConfig(timeout=-5)

    def test_positive_accepted(self) -> None:
        cfg = SSHConfig(timeout=_POSITIVE_TIMEOUT)
        assert cfg.timeout == _POSITIVE_TIMEOUT
