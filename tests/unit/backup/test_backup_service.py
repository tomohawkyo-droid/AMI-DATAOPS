"""Unit tests for backup service."""

import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.dataops.backup.create.cli import BackupCLI
from ami.dataops.backup.create.service import BackupOptions, BackupService


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """Clear GDRIVE env vars before each test."""
    for key in list(os.environ.keys()):
        if key.startswith("GDRIVE_"):
            monkeypatch.delenv(key, raising=False)


class TestBackupOptions:
    """Test BackupOptions dataclass."""

    def test_default_values(self):
        """Test default values are set correctly."""
        options = BackupOptions()

        assert options.keep_local is False
        assert options.retry_auth is True
        assert options.source_dir is None
        assert options.output_filename is None
        assert options.ignore_exclusions is False
        assert options.config_path is None

    def test_custom_values(self):
        """Test custom values can be set."""
        options = BackupOptions(
            keep_local=True,
            retry_auth=False,
            source_dir=Path("/tmp"),
            output_filename="my-backup",
            ignore_exclusions=True,
            config_path=Path("/config"),
        )

        assert options.keep_local is True
        assert options.retry_auth is False
        assert options.source_dir == Path("/tmp")
        assert options.output_filename == "my-backup"
        assert options.ignore_exclusions is True
        assert options.config_path == Path("/config")


class TestBackupService:
    """Test BackupService."""

    def test_service_initialization(self):
        """Test service can be initialized with dependencies."""
        uploader = MagicMock()
        auth_manager = MagicMock()

        service = BackupService(uploader, auth_manager)

        assert service.uploader is uploader
        assert service.auth_manager is auth_manager

    @pytest.mark.asyncio
    async def test_run_backup_accepts_options(self, tmp_path):
        """Test that run_backup accepts BackupOptions with all fields."""
        # Create .env file
        env_file = tmp_path / ".env"
        env_file.write_text("GDRIVE_AUTH_METHOD=oauth\n")

        # Create source directory
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        (source_dir / "test.txt").write_text("test content")

        uploader = MagicMock()
        uploader.upload_to_gdrive = AsyncMock(return_value="file_id_123")

        auth_manager = MagicMock()

        service = BackupService(uploader, auth_manager)

        options = BackupOptions(
            keep_local=True,
            retry_auth=False,
            source_dir=source_dir,
            output_filename="test-backup",
            ignore_exclusions=False,
            config_path=tmp_path,
        )

        # This should not raise - if it does, the interface is broken
        with patch(
            "ami.dataops.backup.create.service.create_zip_archive"
        ) as mock_archive:
            mock_archive.return_value = tmp_path / "test-backup.tar.zst"
            (tmp_path / "test-backup.tar.zst").write_bytes(b"fake archive")

            with patch(
                "ami.dataops.backup.create.service.copy_to_secondary_backup"
            ) as mock_secondary:
                mock_secondary.return_value = None

                with patch(
                    "ami.dataops.backup.create.service.cleanup_local_zip"
                ) as mock_cleanup:
                    mock_cleanup.return_value = None

                    result = await service.run_backup(options)

        assert result == "file_id_123"


class TestBackupCLIServiceIntegration:
    """Test that CLI properly calls service with correct arguments."""

    @pytest.mark.asyncio
    async def test_cli_run_calls_service_with_options_object(self, tmp_path):
        """Test that CLI.run() calls service.run_backup() with BackupOptions object.

        This test catches the bug where CLI was passing keyword arguments
        instead of a BackupOptions object.
        """

        # Create a strict mock that only accepts BackupOptions
        class StrictBackupService:
            async def run_backup(self, options: BackupOptions) -> str:
                # Verify we got a BackupOptions object, not kwargs
                if not isinstance(options, BackupOptions):
                    got = type(options).__name__
                    msg = f"run_backup expects BackupOptions, got {got}"
                    raise TypeError(msg)
                return "file_id_123"

        cli = BackupCLI(StrictBackupService())

        # Parse arguments
        args = cli.parse_arguments(
            [
                "--config-path",
                str(tmp_path),
                "--name",
                "my-backup",
                "--keep-local",
                str(tmp_path),
            ]
        )

        # This will fail if CLI passes kwargs instead of BackupOptions
        result = await cli.run(args)
        assert result == 0
