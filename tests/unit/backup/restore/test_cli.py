"""Unit tests for the modular backup restore system."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.dataops.backup.restore.cli import RestoreCLI
from ami.dataops.backup.restore.service import BackupRestoreService

# Test constants
EXPECTED_REVISION = 2
EXPECTED_PATH_COUNT = 2


class TestRestoreCLI:
    """Unit tests for the RestoreCLI functions."""

    def test_cli_argument_parsing(self):
        """Test that the CLI can parse arguments correctly."""
        mock_service = MagicMock()
        cli = RestoreCLI(mock_service)

        # Test parsing with file ID and paths
        args = cli.parse_arguments(["--file-id", "test123", "path/to/file.txt"])
        assert args.file_id == "test123"
        assert len(args.paths) == 1
        assert str(args.paths[0]) == "path/to/file.txt"

        # Test parsing with revision and multiple paths
        args = cli.parse_arguments(["--revision", "2", "path1.txt", "path2.txt"])
        assert args.revision == EXPECTED_REVISION
        assert len(args.paths) == EXPECTED_PATH_COUNT
        assert str(args.paths[0]) == "path1.txt"
        assert str(args.paths[1]) == "path2.txt"

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.service.extract_specific_paths")
    @patch("ami.dataops.backup.restore.service.DriveRestoreClient")
    @patch("ami.dataops.backup.restore.service.AuthenticationManager")
    async def test_selective_restore_from_drive_by_file_id(
        self, mock_auth, mock_drive_client, mock_extract
    ):
        """Test selective restore from drive by file ID."""
        service = BackupRestoreService(mock_drive_client, mock_auth)

        # Mock the expected behavior
        mock_drive_client.download_file = AsyncMock(return_value=True)
        mock_extract.return_value = True

        # Test selective restore with specific paths
        result = await service.selective_restore_from_drive_by_file_id(
            "test_file_id", [Path("file1.txt")], Path("/tmp/restore"), MagicMock()
        )

        assert result is True
        mock_extract.assert_called_once()

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.service.extract_specific_paths")
    @patch("ami.dataops.backup.restore.service.DriveRestoreClient")
    @patch("ami.dataops.backup.restore.service.AuthenticationManager")
    async def test_selective_restore_from_drive_by_revision(
        self, mock_auth, mock_drive_client, mock_extract
    ):
        """Test selective restore from drive by revision."""
        service = BackupRestoreService(mock_drive_client, mock_auth)

        # Mock the expected behavior
        mock_drive_client.list_backup_files = AsyncMock(
            return_value=[
                {
                    "id": "file1_id",
                    "name": "backup1.tar.zst",
                    "modifiedTime": "2025-01-01T00:00:00Z",
                }
            ]
        )
        mock_drive_client.download_file = AsyncMock(return_value=True)
        mock_extract.return_value = True

        # Test selective restore with revision and specific paths
        result = await service.selective_restore_from_drive_by_revision(
            0,  # First revision
            [Path("file1.txt")],
            Path("/tmp/restore"),
            MagicMock(),
        )

        assert result is True
        mock_extract.assert_called_once()

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.service.extract_specific_paths")
    @patch("ami.dataops.backup.restore.service.local_client.verify_backup_exists")
    @patch("ami.dataops.backup.restore.service.DriveRestoreClient")
    @patch("ami.dataops.backup.restore.service.AuthenticationManager")
    async def test_selective_restore_local_backup(
        self, mock_auth, mock_drive_client, mock_verify, mock_extract
    ):
        """Test selective restore from local backup."""
        service = BackupRestoreService(mock_drive_client, mock_auth)

        # Mock the expected behavior
        mock_verify.return_value = True
        mock_extract.return_value = True

        # Test selective restore from local backup
        result = await service.selective_restore_local_backup(
            Path("/tmp/backup.tar.zst"), [Path("file1.txt")], Path("/tmp/restore")
        )

        assert result is True
        mock_extract.assert_called_once()

    def test_cli_help_text_contains_path_argument(self):
        """Test that the CLI help text mentions path arguments."""
        mock_service = MagicMock()
        cli = RestoreCLI(mock_service)

        parser = cli.create_parser()
        help_text = parser.format_help()

        # Verify that the help mentions paths for selective restoration
        assert "paths" in help_text.lower()
        assert "specific file/directory paths" in help_text.lower()
