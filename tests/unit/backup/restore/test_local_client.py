"""Unit tests for backup/restore/local_client module."""

import time
from pathlib import Path

import pytest

from ami.dataops.backup.restore.local_client import (
    find_backup_by_name,
    find_latest_backup,
    get_backup_size,
    list_backups_in_directory,
    validate_backup_path,
    verify_backup_exists,
)

EXPECTED_BACKUP_SIZE_BYTES = 100
EXPECTED_BACKUP_FILE_COUNT = 3


class TestFindLatestBackup:
    """Tests for find_latest_backup function."""

    @pytest.mark.asyncio
    async def test_returns_none_for_nonexistent_directory(self, tmp_path: Path) -> None:
        """Test returns None for nonexistent directory."""
        nonexistent = tmp_path / "nonexistent"
        result = await find_latest_backup(nonexistent)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_for_empty_directory(self, tmp_path: Path) -> None:
        """Test returns None for directory without backups."""
        result = await find_latest_backup(tmp_path)
        assert result is None

    @pytest.mark.asyncio
    async def test_finds_single_backup(self, tmp_path: Path) -> None:
        """Test finding a single backup file."""
        backup = tmp_path / "backup.tar.zst"
        backup.touch()

        result = await find_latest_backup(tmp_path)

        assert result == backup

    @pytest.mark.asyncio
    async def test_finds_latest_of_multiple_backups(self, tmp_path: Path) -> None:
        """Test finding the latest backup among multiple."""

        old_backup = tmp_path / "old.tar.zst"
        old_backup.touch()
        time.sleep(0.1)  # Ensure different mtime

        new_backup = tmp_path / "new.tar.zst"
        new_backup.touch()

        result = await find_latest_backup(tmp_path)

        assert result == new_backup


class TestVerifyBackupExists:
    """Tests for verify_backup_exists function."""

    @pytest.mark.asyncio
    async def test_returns_true_for_existing_file(self, tmp_path: Path) -> None:
        """Test returns True for existing file."""
        backup = tmp_path / "backup.tar.zst"
        backup.touch()

        result = await verify_backup_exists(backup)

        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_for_nonexistent_file(self, tmp_path: Path) -> None:
        """Test returns False for nonexistent file."""
        backup = tmp_path / "nonexistent.tar.zst"

        result = await verify_backup_exists(backup)

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_for_directory(self, tmp_path: Path) -> None:
        """Test returns False for directory."""
        result = await verify_backup_exists(tmp_path)

        assert result is False


class TestGetBackupSize:
    """Tests for get_backup_size function."""

    @pytest.mark.asyncio
    async def test_returns_size_for_existing_file(self, tmp_path: Path) -> None:
        """Test returns file size for existing file."""
        backup = tmp_path / "backup.tar.zst"
        backup.write_bytes(b"x" * 100)

        result = await get_backup_size(backup)

        assert result == EXPECTED_BACKUP_SIZE_BYTES

    @pytest.mark.asyncio
    async def test_returns_none_for_nonexistent_file(self, tmp_path: Path) -> None:
        """Test returns None for nonexistent file."""
        backup = tmp_path / "nonexistent.tar.zst"

        result = await get_backup_size(backup)

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_zero_for_empty_file(self, tmp_path: Path) -> None:
        """Test returns 0 for empty file."""
        backup = tmp_path / "empty.tar.zst"
        backup.touch()

        result = await get_backup_size(backup)

        assert result == 0


class TestFindBackupByName:
    """Tests for find_backup_by_name function."""

    @pytest.mark.asyncio
    async def test_returns_none_for_nonexistent_directory(self, tmp_path: Path) -> None:
        """Test returns None for nonexistent directory."""
        nonexistent = tmp_path / "nonexistent"
        result = await find_backup_by_name(nonexistent, "pattern")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_for_no_match(self, tmp_path: Path) -> None:
        """Test returns None when no files match."""
        backup = tmp_path / "other.tar.zst"
        backup.touch()

        result = await find_backup_by_name(tmp_path, "nomatch")

        assert result is None

    @pytest.mark.asyncio
    async def test_finds_matching_backup(self, tmp_path: Path) -> None:
        """Test finds backup matching pattern."""
        backup = tmp_path / "backup-2024-01-01.tar.zst"
        backup.touch()

        result = await find_backup_by_name(tmp_path, "2024-01")

        assert result == backup

    @pytest.mark.asyncio
    async def test_finds_latest_match(self, tmp_path: Path) -> None:
        """Test finds latest matching backup."""

        old = tmp_path / "backup-2024-01-01.tar.zst"
        old.touch()
        time.sleep(0.1)

        new = tmp_path / "backup-2024-01-15.tar.zst"
        new.touch()

        result = await find_backup_by_name(tmp_path, "2024-01")

        assert result == new


class TestListBackupsInDirectory:
    """Tests for list_backups_in_directory function."""

    @pytest.mark.asyncio
    async def test_returns_empty_for_nonexistent_directory(
        self, tmp_path: Path
    ) -> None:
        """Test returns empty list for nonexistent directory."""
        nonexistent = tmp_path / "nonexistent"
        result = await list_backups_in_directory(nonexistent)
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_for_empty_directory(self, tmp_path: Path) -> None:
        """Test returns empty list for directory without backups."""
        result = await list_backups_in_directory(tmp_path)
        assert result == []

    @pytest.mark.asyncio
    async def test_lists_all_backups(self, tmp_path: Path) -> None:
        """Test lists all backup files."""
        (tmp_path / "a.tar.zst").touch()
        (tmp_path / "b.tar.zst").touch()
        (tmp_path / "c.tar.zst").touch()

        result = await list_backups_in_directory(tmp_path)

        assert len(result) == EXPECTED_BACKUP_FILE_COUNT

    @pytest.mark.asyncio
    async def test_excludes_non_backup_files(self, tmp_path: Path) -> None:
        """Test excludes non .tar.zst files."""
        (tmp_path / "backup.tar.zst").touch()
        (tmp_path / "other.txt").touch()
        (tmp_path / "readme.md").touch()

        result = await list_backups_in_directory(tmp_path)

        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_sorted_by_mtime_newest_first(self, tmp_path: Path) -> None:
        """Test backups are sorted newest first."""

        old = tmp_path / "old.tar.zst"
        old.touch()
        time.sleep(0.1)

        new = tmp_path / "new.tar.zst"
        new.touch()

        result = await list_backups_in_directory(tmp_path)

        assert result[0] == new
        assert result[1] == old


class TestValidateBackupPath:
    """Tests for validate_backup_path function."""

    @pytest.mark.asyncio
    async def test_returns_true_for_valid_backup(self, tmp_path: Path) -> None:
        """Test returns True for valid backup path."""
        backup = tmp_path / "backup.tar.zst"
        backup.touch()

        result = await validate_backup_path(backup)

        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_for_nonexistent(self, tmp_path: Path) -> None:
        """Test returns False for nonexistent path."""
        backup = tmp_path / "nonexistent.tar.zst"

        result = await validate_backup_path(backup)

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_for_directory(self, tmp_path: Path) -> None:
        """Test returns False for directory path."""
        result = await validate_backup_path(tmp_path)

        assert result is False
