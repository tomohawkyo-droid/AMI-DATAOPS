"""Unit tests for backup CLI functionality."""

import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from ami.dataops.backup.backup_exceptions import (
    ArchiveError,
    BackupConfigError,
    BackupError,
    UploadError,
)
from ami.dataops.backup.create.cli import BackupCLI
from ami.dataops.backup.restore.cli import RestoreCLI


class TestBackupCLI:
    """Test backup CLI argument parsing and initialization."""

    def test_create_parser(self):
        """Test that parser is created with all required options."""
        service = MagicMock()
        cli = BackupCLI(service)
        parser = cli.create_parser()

        assert parser is not None
        assert parser.prog == "backup_to_gdrive"

    def test_parse_help_flag(self):
        """Test parsing --help doesn't raise."""
        service = MagicMock()
        cli = BackupCLI(service)

        with pytest.raises(SystemExit) as exc_info:
            cli.parse_arguments(["--help"])
        assert exc_info.value.code == 0

    def test_parse_name_argument(self):
        """Test parsing --name argument."""
        service = MagicMock()
        cli = BackupCLI(service)

        args = cli.parse_arguments(["--name", "my-backup"])
        assert args.name == "my-backup"

    def test_parse_keep_local_flag(self):
        """Test parsing --keep-local flag."""
        service = MagicMock()
        cli = BackupCLI(service)

        args = cli.parse_arguments(["--keep-local"])
        assert args.keep_local is True

    def test_parse_verbose_flag(self):
        """Test parsing --verbose flag."""
        service = MagicMock()
        cli = BackupCLI(service)

        args = cli.parse_arguments(["-v"])
        assert args.verbose is True

    def test_parse_source_directory(self):
        """Test parsing source directory argument."""
        service = MagicMock()
        cli = BackupCLI(service)

        args = cli.parse_arguments(["/tmp/test"])
        assert args.source == Path("/tmp/test")

    def test_default_source_is_cwd(self):
        """Test default source is current working directory."""
        service = MagicMock()
        cli = BackupCLI(service)

        args = cli.parse_arguments([])
        assert args.source == Path.cwd()


class TestRestoreCLI:
    """Test restore CLI argument parsing and initialization."""

    def test_create_parser(self):
        """Test that parser is created with all required options."""
        service = MagicMock()
        cli = RestoreCLI(service)
        parser = cli.create_parser()

        assert parser is not None
        assert parser.prog == "backup_restore"

    def test_parse_help_flag(self):
        """Test parsing --help doesn't raise."""
        service = MagicMock()
        cli = RestoreCLI(service)

        with pytest.raises(SystemExit) as exc_info:
            cli.parse_arguments(["--help"])
        assert exc_info.value.code == 0

    def test_parse_file_id_argument(self):
        """Test parsing --file-id argument."""
        service = MagicMock()
        cli = RestoreCLI(service)

        args = cli.parse_arguments(["--file-id", "abc123"])
        assert args.file_id == "abc123"

    def test_parse_latest_local_flag(self):
        """Test parsing --latest-local flag."""
        service = MagicMock()
        cli = RestoreCLI(service)

        args = cli.parse_arguments(["--latest-local"])
        assert args.latest_local is True

    def test_parse_local_path_argument(self):
        """Test parsing --local-path argument."""
        service = MagicMock()
        cli = RestoreCLI(service)

        args = cli.parse_arguments(["--local-path", "/tmp/backup.tar.zst"])
        assert args.local_path == Path("/tmp/backup.tar.zst")

    def test_mutually_exclusive_sources(self):
        """Test that source options are mutually exclusive."""
        service = MagicMock()
        cli = RestoreCLI(service)

        with pytest.raises(SystemExit):
            cli.parse_arguments(["--file-id", "abc", "--latest-local"])


class TestBackupCLIRun:
    """Tests for BackupCLI.run method."""

    @pytest.mark.asyncio
    async def test_run_no_service_returns_error(self):
        """Test run returns 1 when service is None."""
        cli = BackupCLI(service=None)
        args = cli.parse_arguments([])

        result = await cli.run(args)

        assert result == 1

    @pytest.mark.asyncio
    async def test_run_setup_auth(self):
        """Test run calls setup_auth when flag is set."""
        service = MagicMock()
        service.setup_auth = AsyncMock(return_value=0)
        cli = BackupCLI(service)
        args = cli.parse_arguments(["--setup-auth"])

        result = await cli.run(args)

        assert result == 0
        service.setup_auth.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_success(self):
        """Test successful backup run."""
        service = MagicMock()
        service.run_backup = AsyncMock(return_value="file_id_123")
        cli = BackupCLI(service)
        args = cli.parse_arguments([])

        result = await cli.run(args)

        assert result == 0
        service.run_backup.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_with_auth_mode(self, monkeypatch):
        """Test run sets GDRIVE_AUTH_METHOD when auth_mode provided."""
        # Clear any existing env var
        monkeypatch.delenv("GDRIVE_AUTH_METHOD", raising=False)

        service = MagicMock()
        service.run_backup = AsyncMock(return_value="file_id_123")
        cli = BackupCLI(service)
        args = cli.parse_arguments(["--auth-mode", "impersonation"])

        result = await cli.run(args)

        assert result == 0
        # The env var should be restored (deleted since it wasn't set before)
        assert "GDRIVE_AUTH_METHOD" not in os.environ

    @pytest.mark.asyncio
    async def test_run_restores_auth_env_on_success(self, monkeypatch):
        """Test run restores original GDRIVE_AUTH_METHOD on success."""
        monkeypatch.setenv("GDRIVE_AUTH_METHOD", "oauth")

        service = MagicMock()
        service.run_backup = AsyncMock(return_value="file_id_123")
        cli = BackupCLI(service)
        args = cli.parse_arguments(["--auth-mode", "key"])

        result = await cli.run(args)

        assert result == 0
        assert os.environ.get("GDRIVE_AUTH_METHOD") == "oauth"

    @pytest.mark.asyncio
    async def test_run_backup_config_error(self):
        """Test run handles BackupConfigError."""
        service = MagicMock()
        service.run_backup = AsyncMock(side_effect=BackupConfigError("Config error"))
        cli = BackupCLI(service)
        args = cli.parse_arguments([])

        result = await cli.run(args)

        assert result == 1

    @pytest.mark.asyncio
    async def test_run_upload_error(self):
        """Test run handles UploadError."""
        service = MagicMock()
        service.run_backup = AsyncMock(side_effect=UploadError("Upload failed"))
        cli = BackupCLI(service)
        args = cli.parse_arguments([])

        result = await cli.run(args)

        assert result == 1

    @pytest.mark.asyncio
    async def test_run_archive_error(self):
        """Test run handles ArchiveError."""
        service = MagicMock()
        service.run_backup = AsyncMock(side_effect=ArchiveError("Archive failed"))
        cli = BackupCLI(service)
        args = cli.parse_arguments([])

        result = await cli.run(args)

        assert result == 1

    @pytest.mark.asyncio
    async def test_run_backup_error(self):
        """Test run handles BackupError."""
        service = MagicMock()
        service.run_backup = AsyncMock(side_effect=BackupError("Backup failed"))
        cli = BackupCLI(service)
        args = cli.parse_arguments([])

        result = await cli.run(args)

        assert result == 1

    @pytest.mark.asyncio
    async def test_run_unexpected_error(self):
        """Test run handles unexpected exceptions."""
        service = MagicMock()
        service.run_backup = AsyncMock(side_effect=Exception("Unexpected"))
        cli = BackupCLI(service)
        args = cli.parse_arguments([])

        result = await cli.run(args)

        assert result == 1


class TestBackupCLIHelpers:
    """Tests for BackupCLI helper methods."""

    def test_restore_auth_env_no_auth_mode(self):
        """Test _restore_auth_env does nothing when no auth_mode."""
        cli = BackupCLI(MagicMock())
        cli._restore_auth_env(None, "oauth")
        # Should not raise or change anything

    def test_restore_auth_env_restores_original(self, monkeypatch):
        """Test _restore_auth_env restores original value."""
        monkeypatch.setenv("GDRIVE_AUTH_METHOD", "key")
        cli = BackupCLI(MagicMock())

        cli._restore_auth_env("impersonation", "oauth")

        assert os.environ["GDRIVE_AUTH_METHOD"] == "oauth"

    def test_restore_auth_env_deletes_when_original_none(self, monkeypatch):
        """Test _restore_auth_env deletes env var when original was None."""
        monkeypatch.setenv("GDRIVE_AUTH_METHOD", "key")
        cli = BackupCLI(MagicMock())

        cli._restore_auth_env("key", None)

        assert "GDRIVE_AUTH_METHOD" not in os.environ

    def test_log_error_suggestions_credentials_error(self, capsys):
        """Test _log_error_suggestions for credentials error."""
        cli = BackupCLI(MagicMock())
        error = BackupConfigError("Credentials not found")

        cli._log_error_suggestions(error, True)

        # Loguru writes to stderr by default, but we check it ran without error

    def test_log_error_suggestions_auth_method_error(self, capsys):
        """Test _log_error_suggestions for auth method error."""
        cli = BackupCLI(MagicMock())
        error = BackupConfigError("Invalid GDRIVE_AUTH_METHOD")

        cli._log_error_suggestions(error, True)

    def test_log_error_suggestions_upload_auth_error(self, capsys):
        """Test _log_error_suggestions for upload auth error."""
        cli = BackupCLI(MagicMock())
        error = UploadError("reauthentication required")

        cli._log_error_suggestions(error, True)

    def test_log_error_suggestions_upload_auth_error_retry_failed(self, capsys):
        """Test _log_error_suggestions when auth retry was attempted."""
        cli = BackupCLI(MagicMock())
        error = UploadError("Not authenticated")

        cli._log_error_suggestions(error, True)

    def test_setup_logging_verbose(self):
        """Test _setup_logging with verbose=True."""
        cli = BackupCLI(MagicMock())
        # Should not raise
        cli._setup_logging(verbose=True)

    def test_setup_logging_non_verbose(self):
        """Test _setup_logging with verbose=False."""
        cli = BackupCLI(MagicMock())
        # Should not raise
        cli._setup_logging(verbose=False)

    def test_parse_no_auth_retry(self):
        """Test parsing --no-auth-retry flag."""
        cli = BackupCLI(MagicMock())
        args = cli.parse_arguments(["--no-auth-retry"])
        assert args.no_auth_retry is True

    def test_parse_include_all(self):
        """Test parsing --include-all flag."""
        cli = BackupCLI(MagicMock())
        args = cli.parse_arguments(["--include-all"])
        assert args.include_all is True

    def test_parse_config_path(self):
        """Test parsing --config-path argument."""
        cli = BackupCLI(MagicMock())
        args = cli.parse_arguments(["--config-path", "/custom/path"])
        assert args.config_path == Path("/custom/path")

    def test_parse_auth_mode(self):
        """Test parsing --auth-mode argument."""
        cli = BackupCLI(MagicMock())
        args = cli.parse_arguments(["--auth-mode", "key"])
        assert args.auth_mode == "key"
