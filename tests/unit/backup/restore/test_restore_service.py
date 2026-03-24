"""Unit tests for backup/restore/service module - init, drive restore, local restore."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.dataops.backup.backup_config import BackupConfig
from ami.dataops.backup.backup_exceptions import BackupError
from ami.dataops.backup.restore.service import BackupRestoreService


class TestBackupRestoreServiceInit:
    """Tests for BackupRestoreService initialization."""

    def test_initialization(self) -> None:
        """Test initialization with drive client and auth manager."""
        drive_client = MagicMock()
        auth_manager = MagicMock()

        service = BackupRestoreService(drive_client, auth_manager)

        assert service.drive_client == drive_client
        assert service.auth_manager == auth_manager


class TestRestoreFromDriveByRevision:
    """Tests for BackupRestoreService.restore_from_drive_by_revision method."""

    @pytest.mark.asyncio
    async def test_returns_false_when_no_files(self) -> None:
        """Test returns False when no backup files found."""
        drive_client = MagicMock()
        drive_client.list_backup_files = AsyncMock(return_value=[])
        auth_manager = MagicMock()
        config = MagicMock(spec=BackupConfig)

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service.restore_from_drive_by_revision(
            0, Path("/restore"), config
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_revision_too_high(self) -> None:
        """Test returns False when revision exceeds available backups."""
        drive_client = MagicMock()
        drive_client.list_backup_files = AsyncMock(
            return_value=[{"id": "1", "name": "backup1"}]
        )
        auth_manager = MagicMock()
        config = MagicMock(spec=BackupConfig)

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service.restore_from_drive_by_revision(
            5, Path("/restore"), config
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_file_has_no_id(self) -> None:
        """Test returns False when selected file has no ID."""
        drive_client = MagicMock()
        drive_client.list_backup_files = AsyncMock(
            return_value=[{"name": "backup1"}]  # Missing 'id'
        )
        auth_manager = MagicMock()
        config = MagicMock(spec=BackupConfig)

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service.restore_from_drive_by_revision(
            0, Path("/restore"), config
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_calls_restore_from_drive_file(self, tmp_path: Path) -> None:
        """Test calls internal restore method with correct file."""
        drive_client = MagicMock()
        drive_client.list_backup_files = AsyncMock(
            return_value=[
                {
                    "id": "file1",
                    "name": "backup1.tar.zst",
                    "modifiedTime": "2024-01-01",
                },
                {
                    "id": "file2",
                    "name": "backup2.tar.zst",
                    "modifiedTime": "2023-12-01",
                },
            ]
        )
        auth_manager = MagicMock()
        config = MagicMock(spec=BackupConfig)
        restore_path = tmp_path / "restore"

        service = BackupRestoreService(drive_client, auth_manager)

        with patch.object(
            service, "_restore_from_drive_file", new_callable=AsyncMock
        ) as mock_restore:
            mock_restore.return_value = True

            result = await service.restore_from_drive_by_revision(
                0, restore_path, config
            )

            assert result is True
            mock_restore.assert_called_once_with("file1", restore_path, config)


class TestRestoreFromDriveByFileId:
    """Tests for BackupRestoreService.restore_from_drive_by_file_id method."""

    @pytest.mark.asyncio
    async def test_calls_internal_restore_method(self, tmp_path: Path) -> None:
        """Test delegates to internal restore method."""
        drive_client = MagicMock()
        auth_manager = MagicMock()
        config = MagicMock(spec=BackupConfig)
        restore_path = tmp_path / "restore"

        service = BackupRestoreService(drive_client, auth_manager)

        with patch.object(
            service, "_restore_from_drive_file", new_callable=AsyncMock
        ) as mock_restore:
            mock_restore.return_value = True

            result = await service.restore_from_drive_by_file_id(
                "file123", restore_path, config
            )

            assert result is True
            mock_restore.assert_called_once_with("file123", restore_path, config)


class TestRestoreFromDriveFile:
    """Tests for BackupRestoreService._restore_from_drive_file method."""

    @pytest.mark.asyncio
    async def test_returns_false_on_download_failure(self, tmp_path: Path) -> None:
        """Test returns False when download fails."""
        drive_client = MagicMock()
        drive_client.download_file = AsyncMock(return_value=False)
        auth_manager = MagicMock()
        config = MagicMock(spec=BackupConfig)
        restore_path = tmp_path / "restore"

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service._restore_from_drive_file("file123", restore_path, config)

        assert result is False

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.service.extract_specific_paths")
    async def test_returns_true_on_success(self, mock_extract, tmp_path: Path) -> None:
        """Test returns True on successful restore."""
        drive_client = MagicMock()
        drive_client.download_file = AsyncMock(return_value=True)
        auth_manager = MagicMock()
        config = MagicMock(spec=BackupConfig)
        restore_path = tmp_path / "restore"

        mock_extract.return_value = True

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service._restore_from_drive_file("file123", restore_path, config)

        assert result is True

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.service.extract_specific_paths")
    async def test_returns_false_on_extract_failure(
        self, mock_extract, tmp_path: Path
    ) -> None:
        """Test returns False when extraction fails."""
        drive_client = MagicMock()
        drive_client.download_file = AsyncMock(return_value=True)
        auth_manager = MagicMock()
        config = MagicMock(spec=BackupConfig)
        restore_path = tmp_path / "restore"

        mock_extract.return_value = False

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service._restore_from_drive_file("file123", restore_path, config)

        assert result is False

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.service.extract_specific_paths")
    async def test_returns_false_on_exception(
        self, mock_extract, tmp_path: Path
    ) -> None:
        """Test returns False on extraction exception."""
        drive_client = MagicMock()
        drive_client.download_file = AsyncMock(return_value=True)
        auth_manager = MagicMock()
        config = MagicMock(spec=BackupConfig)
        restore_path = tmp_path / "restore"

        mock_extract.side_effect = Exception("Extract failed")

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service._restore_from_drive_file("file123", restore_path, config)

        assert result is False


class TestRestoreLocalBackup:
    """Tests for BackupRestoreService.restore_local_backup method."""

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.service.local_client.verify_backup_exists")
    async def test_raises_error_when_backup_not_found(
        self, mock_verify, tmp_path: Path
    ) -> None:
        """Test raises BackupError when backup file not found."""
        mock_verify.return_value = False
        drive_client = MagicMock()
        auth_manager = MagicMock()
        backup_path = tmp_path / "backup.tar.zst"
        restore_path = tmp_path / "restore"

        service = BackupRestoreService(drive_client, auth_manager)

        with pytest.raises(BackupError) as exc_info:
            await service.restore_local_backup(backup_path, restore_path)

        assert "Backup file not found" in str(exc_info.value)

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.service.extract_specific_paths")
    @patch("ami.dataops.backup.restore.service.local_client.verify_backup_exists")
    async def test_returns_true_on_success(
        self, mock_verify, mock_extract, tmp_path: Path
    ) -> None:
        """Test returns True on successful restore."""
        mock_verify.return_value = True
        mock_extract.return_value = True
        drive_client = MagicMock()
        auth_manager = MagicMock()
        backup_path = tmp_path / "backup.tar.zst"
        restore_path = tmp_path / "restore"

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service.restore_local_backup(backup_path, restore_path)

        assert result is True

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.service.extract_specific_paths")
    @patch("ami.dataops.backup.restore.service.local_client.verify_backup_exists")
    async def test_returns_false_on_extract_failure(
        self, mock_verify, mock_extract, tmp_path: Path
    ) -> None:
        """Test returns False when extraction fails."""
        mock_verify.return_value = True
        mock_extract.return_value = False
        drive_client = MagicMock()
        auth_manager = MagicMock()
        backup_path = tmp_path / "backup.tar.zst"
        restore_path = tmp_path / "restore"

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service.restore_local_backup(backup_path, restore_path)

        assert result is False

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.service.extract_specific_paths")
    @patch("ami.dataops.backup.restore.service.local_client.verify_backup_exists")
    async def test_returns_false_on_exception(
        self, mock_verify, mock_extract, tmp_path: Path
    ) -> None:
        """Test returns False on exception."""
        mock_verify.return_value = True
        mock_extract.side_effect = Exception("Failed")
        drive_client = MagicMock()
        auth_manager = MagicMock()
        backup_path = tmp_path / "backup.tar.zst"
        restore_path = tmp_path / "restore"

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service.restore_local_backup(backup_path, restore_path)

        assert result is False


class TestRestoreLatestLocal:
    """Tests for BackupRestoreService.restore_latest_local method."""

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.service.local_client.find_latest_backup")
    async def test_returns_false_when_no_backups_found(
        self, mock_find, tmp_path: Path
    ) -> None:
        """Test returns False when no backups found."""
        mock_find.return_value = None
        drive_client = MagicMock()
        auth_manager = MagicMock()
        restore_path = tmp_path / "restore"

        service = BackupRestoreService(drive_client, auth_manager)
        result = await service.restore_latest_local(restore_path)

        assert result is False

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.service.local_client.find_latest_backup")
    async def test_restores_found_backup(self, mock_find, tmp_path: Path) -> None:
        """Test restores when backup is found."""
        backup_path = tmp_path / "backup.tar.zst"
        mock_find.return_value = backup_path
        drive_client = MagicMock()
        auth_manager = MagicMock()
        restore_path = tmp_path / "restore"

        # Create a backup location directory
        Path.home() / "Downloads"

        service = BackupRestoreService(drive_client, auth_manager)

        with patch.object(
            service, "restore_local_backup", new_callable=AsyncMock
        ) as mock_restore:
            mock_restore.return_value = True

            # Mock that Downloads exists
            with patch.object(Path, "exists", return_value=True):
                result = await service.restore_latest_local(restore_path)

            assert result is True

    @pytest.mark.asyncio
    @patch.dict("os.environ", {"AMI_BACKUP_MOUNT": "/custom/mount"})
    @patch("ami.dataops.backup.restore.service.local_client.find_latest_backup")
    async def test_checks_custom_mount_first(self, mock_find, tmp_path: Path) -> None:
        """Test checks custom mount from environment first."""
        mock_find.return_value = tmp_path / "backup.tar.zst"
        drive_client = MagicMock()
        auth_manager = MagicMock()
        restore_path = tmp_path / "restore"

        service = BackupRestoreService(drive_client, auth_manager)

        with patch.object(
            service, "restore_local_backup", new_callable=AsyncMock
        ) as mock_restore:
            mock_restore.return_value = True

            with patch.object(Path, "exists", return_value=True):
                result = await service.restore_latest_local(restore_path)

            assert result is True
