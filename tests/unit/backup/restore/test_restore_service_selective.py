"""Tests for restore service: listing, selective, validation."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.dataops.backup.backup_config import BackupConfig
from ami.dataops.backup.backup_exceptions import BackupError
from ami.dataops.backup.restore.service import BackupRestoreService

EXPECTED_BACKUP_COUNT = 2


class TestListAvailableDriveBackups:
    """Tests for BackupRestoreService.list_available_drive_backups method."""

    @pytest.mark.asyncio
    async def test_returns_drive_files(self) -> None:
        """Test returns list of drive files."""
        drive_client = MagicMock()
        drive_client.list_backup_files = AsyncMock(
            return_value=[
                {"id": "1", "name": "backup1.tar.zst"},
                {"id": "2", "name": "backup2.tar.zst"},
            ]
        )
        auth_manager = MagicMock()
        config = MagicMock(spec=BackupConfig)

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service.list_available_drive_backups(config)

        assert len(result) == EXPECTED_BACKUP_COUNT
        assert result[0]["name"] == "backup1.tar.zst"


class TestListAvailableLocalBackups:
    """Tests for BackupRestoreService.list_available_local_backups method."""

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.service.local_client.list_backups_in_directory")
    async def test_returns_local_backups(self, mock_list, tmp_path: Path) -> None:
        """Test returns list of local backup paths."""
        mock_list.return_value = [
            tmp_path / "backup1.tar.zst",
            tmp_path / "backup2.tar.zst",
        ]
        drive_client = MagicMock()
        auth_manager = MagicMock()

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service.list_available_local_backups(tmp_path)

        assert len(result) == EXPECTED_BACKUP_COUNT
        mock_list.assert_called_once_with(tmp_path)


class TestSelectiveRestoreFromDriveByFileId:
    """Tests for BackupRestoreService.selective_restore_from_drive_by_file_id method."""

    @pytest.mark.asyncio
    async def test_returns_false_on_download_failure(self, tmp_path: Path) -> None:
        """Test returns False when download fails."""
        drive_client = MagicMock()
        drive_client.download_file = AsyncMock(return_value=False)
        auth_manager = MagicMock()
        config = MagicMock(spec=BackupConfig)
        restore_path = tmp_path / "restore"
        paths = [Path("config/"), Path("data/")]

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service.selective_restore_from_drive_by_file_id(
            "file123", paths, restore_path, config
        )

        assert result is False

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.service.extract_specific_paths")
    async def test_returns_true_on_success(self, mock_extract, tmp_path: Path) -> None:
        """Test returns True on successful selective restore."""
        drive_client = MagicMock()
        drive_client.download_file = AsyncMock(return_value=True)
        auth_manager = MagicMock()
        config = MagicMock(spec=BackupConfig)
        restore_path = tmp_path / "restore"
        paths = [Path("config/")]

        mock_extract.return_value = True

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service.selective_restore_from_drive_by_file_id(
            "file123", paths, restore_path, config
        )

        assert result is True
        mock_extract.assert_called_once()

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.service.extract_specific_paths")
    async def test_returns_false_on_exception(
        self, mock_extract, tmp_path: Path
    ) -> None:
        """Test returns False on exception."""
        drive_client = MagicMock()
        drive_client.download_file = AsyncMock(return_value=True)
        auth_manager = MagicMock()
        config = MagicMock(spec=BackupConfig)
        restore_path = tmp_path / "restore"

        mock_extract.side_effect = Exception("Extract error")

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service.selective_restore_from_drive_by_file_id(
            "file123", [Path("config/")], restore_path, config
        )

        assert result is False


class TestSelectiveRestoreFromDriveByRevision:
    """Tests for selective_restore_from_drive_by_revision."""

    @pytest.mark.asyncio
    async def test_returns_false_when_no_files(self) -> None:
        """Test returns False when no backup files found."""
        drive_client = MagicMock()
        drive_client.list_backup_files = AsyncMock(return_value=[])
        auth_manager = MagicMock()
        config = MagicMock(spec=BackupConfig)

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service.selective_restore_from_drive_by_revision(
            0, [Path("config/")], Path("/restore"), config
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_revision_exceeds_available(self) -> None:
        """Test returns False when revision too high."""
        drive_client = MagicMock()
        drive_client.list_backup_files = AsyncMock(
            return_value=[{"id": "1", "name": "backup1"}]
        )
        auth_manager = MagicMock()
        config = MagicMock(spec=BackupConfig)

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service.selective_restore_from_drive_by_revision(
            5, [Path("config/")], Path("/restore"), config
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_calls_selective_restore_by_file_id(self, tmp_path: Path) -> None:
        """Test delegates to selective restore by file ID."""
        drive_client = MagicMock()
        drive_client.list_backup_files = AsyncMock(
            return_value=[{"id": "file1", "name": "backup1", "modifiedTime": "now"}]
        )
        auth_manager = MagicMock()
        config = MagicMock(spec=BackupConfig)
        restore_path = tmp_path / "restore"
        paths = [Path("config/")]

        service = BackupRestoreService(drive_client, auth_manager)

        with patch.object(
            service, "selective_restore_from_drive_by_file_id", new_callable=AsyncMock
        ) as mock_restore:
            mock_restore.return_value = True

            result = await service.selective_restore_from_drive_by_revision(
                0, paths, restore_path, config
            )

            assert result is True
            mock_restore.assert_called_once_with("file1", paths, restore_path, config)


class TestSelectiveRestoreLocalBackup:
    """Tests for BackupRestoreService.selective_restore_local_backup method."""

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.service.local_client.verify_backup_exists")
    async def test_raises_error_when_backup_not_found(
        self, mock_verify, tmp_path: Path
    ) -> None:
        """Test raises BackupError when backup not found."""
        mock_verify.return_value = False
        drive_client = MagicMock()
        auth_manager = MagicMock()

        service = BackupRestoreService(drive_client, auth_manager)

        with pytest.raises(BackupError):
            await service.selective_restore_local_backup(
                tmp_path / "backup.tar.zst",
                [Path("config/")],
                tmp_path / "restore",
            )

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.service.extract_specific_paths")
    @patch("ami.dataops.backup.restore.service.local_client.verify_backup_exists")
    async def test_returns_true_on_success(
        self, mock_verify, mock_extract, tmp_path: Path
    ) -> None:
        """Test returns True on successful selective restore."""
        mock_verify.return_value = True
        mock_extract.return_value = True
        drive_client = MagicMock()
        auth_manager = MagicMock()

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service.selective_restore_local_backup(
            tmp_path / "backup.tar.zst",
            [Path("config/")],
            tmp_path / "restore",
        )

        assert result is True

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.service.extract_specific_paths")
    @patch("ami.dataops.backup.restore.service.local_client.verify_backup_exists")
    async def test_returns_false_on_exception(
        self, mock_verify, mock_extract, tmp_path: Path
    ) -> None:
        """Test returns False on exception."""
        mock_verify.return_value = True
        mock_extract.side_effect = Exception("Error")
        drive_client = MagicMock()
        auth_manager = MagicMock()

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service.selective_restore_local_backup(
            tmp_path / "backup.tar.zst",
            [Path("config/")],
            tmp_path / "restore",
        )

        assert result is False


class TestValidateRestorePath:
    """Tests for BackupRestoreService.validate_restore_path method."""

    @pytest.mark.asyncio
    async def test_returns_true_for_existing_directory(self, tmp_path: Path) -> None:
        """Test returns True for existing directory."""
        drive_client = MagicMock()
        auth_manager = MagicMock()

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service.validate_restore_path(tmp_path)

        assert result is True

    @pytest.mark.asyncio
    async def test_returns_true_for_nonexistent_with_valid_parent(
        self, tmp_path: Path
    ) -> None:
        """Test returns True for nonexistent path with valid parent."""
        drive_client = MagicMock()
        auth_manager = MagicMock()
        restore_path = tmp_path / "new_restore"

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service.validate_restore_path(restore_path)

        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_for_file_path(self, tmp_path: Path) -> None:
        """Test returns False when path is a file."""
        drive_client = MagicMock()
        auth_manager = MagicMock()
        file_path = tmp_path / "file.txt"
        file_path.write_text("content")

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service.validate_restore_path(file_path)

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_parent_not_exists(self) -> None:
        """Test returns False when parent directory doesn't exist."""
        drive_client = MagicMock()
        auth_manager = MagicMock()
        restore_path = Path("/nonexistent/path/restore")

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service.validate_restore_path(restore_path)

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_exception(self) -> None:
        """Test returns False on exception."""
        drive_client = MagicMock()
        auth_manager = MagicMock()

        service = BackupRestoreService(drive_client, auth_manager)

        with patch.object(Path, "exists", side_effect=Exception("Error")):
            result = await service.validate_restore_path(Path("/test"))

        assert result is False
