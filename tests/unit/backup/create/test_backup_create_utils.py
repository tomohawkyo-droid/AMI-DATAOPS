"""Unit tests for the backup utils module (create/utils.py)."""

import asyncio
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

from ami.dataops.backup.create import utils

SECOND_SUBPROCESS_CALL = 2


class TestBackupUtils:
    """Unit tests for the backup utils functions."""

    @patch.object(Path, "exists")
    def test_cleanup_local_zip_already_cleaned_up(self, mock_exists):
        """Test cleanup when zip file already doesn't exist."""
        mock_exists.return_value = False  # File doesn't exist

        zip_path = Path("/tmp/test.zip")
        result = asyncio.run(utils.cleanup_local_zip(zip_path, keep_local=False))

        assert result is True  # Should return True (no cleanup needed)

    @patch.object(Path, "exists")
    @patch.object(Path, "unlink")
    def test_cleanup_local_zip_keep_local(self, mock_unlink, mock_exists):
        """Test cleanup when keep_local is True."""
        mock_exists.return_value = True  # File exists

        zip_path = Path("/tmp/test.zip")
        result = asyncio.run(utils.cleanup_local_zip(zip_path, keep_local=True))

        assert result is True  # Should return True (kept the file)
        mock_unlink.assert_not_called()  # Should not have deleted the file

    @patch.object(Path, "exists")
    @patch.object(Path, "unlink")
    def test_cleanup_local_zip_delete_success(self, mock_unlink, mock_exists):
        """Test successful deletion of local zip."""
        mock_exists.return_value = True  # File exists
        mock_unlink.return_value = None  # No error on delete

        zip_path = Path("/tmp/test.zip")
        result = asyncio.run(utils.cleanup_local_zip(zip_path, keep_local=False))

        assert result is True  # Should return True (deleted successfully)
        mock_unlink.assert_called_once()

    @patch.object(Path, "exists")
    @patch.object(Path, "unlink", side_effect=Exception("Permission denied"))
    def test_cleanup_local_zip_delete_failure(self, mock_unlink, mock_exists):
        """Test cleanup when deletion fails."""
        mock_exists.return_value = True  # File exists

        zip_path = Path("/tmp/test.zip")
        result = asyncio.run(utils.cleanup_local_zip(zip_path, keep_local=False))

        assert result is False  # Should return False (deletion failed)

    @patch.object(Path, "iterdir")
    def test_cleanup_old_backups(self, mock_iterdir):
        """Test cleaning up old backups keeping most recent ones."""
        # Create mock files with proper stat() mock
        mock_file1 = MagicMock(spec=Path)
        mock_file1.name = "backup_20230101.tar.zst"
        mock_file1.suffixes = [".tar", ".zst"]
        mock_file1.is_file.return_value = True  # Mock is_file for the Path object

        # Create mock stat objects with mtime attributes
        mock_stat1 = MagicMock()
        mock_stat1.st_mtime = 1  # Oldest

        # Mock the stat method to return the stat object
        mock_file1.stat.return_value = mock_stat1

        mock_file2 = MagicMock(spec=Path)
        mock_file2.name = "backup_20230102.tar.zst"
        mock_file2.suffixes = [".tar", ".zst"]
        mock_file2.is_file.return_value = True

        mock_stat2 = MagicMock()
        mock_stat2.st_mtime = 3  # Newest
        mock_file2.stat.return_value = mock_stat2

        mock_file3 = MagicMock(spec=Path)
        mock_file3.name = "backup_20230103.tar.zst"
        mock_file3.suffixes = [".tar", ".zst"]
        mock_file3.is_file.return_value = True

        mock_stat3 = MagicMock()
        mock_stat3.st_mtime = 2  # Middle
        mock_file3.stat.return_value = mock_stat3

        mock_iterdir.return_value = [mock_file1, mock_file2, mock_file3]

        # Mock the unlink method for the files to be deleted
        with (
            patch.object(mock_file1, "unlink", return_value=None),
            patch.object(mock_file2, "unlink", return_value=None),
            patch.object(mock_file3, "unlink", return_value=None),
        ):
            directory = Path("/tmp/backups")
            result = asyncio.run(utils.cleanup_old_backups(directory, keep_count=1))

            assert result is True
            # When keeping 1 file, the 2 oldest should be deleted
            mock_file1.unlink.assert_called_once()  # Oldest (mtime=1)
            mock_file3.unlink.assert_called_once()  # Middle (mtime=2)
            mock_file2.unlink.assert_not_called()  # Newest (mtime=3)

    @patch.object(Path, "exists")
    @patch("subprocess.run")
    def test_validate_backup_file_success(self, mock_subprocess_run, mock_exists):
        """Test backup file validation succeeds for valid file."""
        mock_exists.return_value = True  # File exists

        # Mock successful subprocess calls
        zstd_test_result = MagicMock()
        zstd_test_result.returncode = 0
        zstd_decomp_result = MagicMock()
        zstd_decomp_result.returncode = 0
        zstd_decomp_result.stdout = b"mock tar data"
        tar_test_result = MagicMock()
        tar_test_result.returncode = 0

        def subprocess_side_effect(*args, **kwargs):
            cmd = args[0] if args else []
            if isinstance(cmd, list) and "zstd" in cmd and "--test" in cmd:
                return zstd_test_result
            elif isinstance(cmd, list) and "zstd" in cmd and "-d" in cmd:
                return zstd_decomp_result
            elif isinstance(cmd, list) and "tar" in cmd and "-t" in cmd:
                return tar_test_result
            return MagicMock()  # Default return

        mock_subprocess_run.side_effect = subprocess_side_effect

        zip_path = Path("/tmp/backup.tar.zst")
        result = asyncio.run(utils.validate_backup_file(zip_path))

        assert result is True

    @patch.object(Path, "exists")
    def test_validate_backup_file_not_exists(self, mock_exists):
        """Test backup file validation fails for non-existent file."""
        mock_exists.return_value = False  # File doesn't exist

        zip_path = Path("/tmp/backup.tar.zst")
        result = asyncio.run(utils.validate_backup_file(zip_path))

        assert result is False

    @patch.object(Path, "exists")
    @patch("subprocess.run")
    def test_validate_backup_file_zstd_fails(self, mock_subprocess_run, mock_exists):
        """Test validation fails when zstd test fails."""
        mock_exists.return_value = True

        zstd_test_result = MagicMock()
        zstd_test_result.returncode = 1  # zstd test fails
        mock_subprocess_run.return_value = zstd_test_result

        zip_path = Path("/tmp/corrupt.tar.zst")
        result = asyncio.run(utils.validate_backup_file(zip_path))

        assert result is False

    @patch.object(Path, "exists")
    @patch("subprocess.run")
    def test_validate_backup_file_tar_fails(self, mock_subprocess_run, mock_exists):
        """Test validation fails when tar validation fails."""
        mock_exists.return_value = True

        zstd_test_result = MagicMock()
        zstd_test_result.returncode = 0
        zstd_decomp_result = MagicMock()
        zstd_decomp_result.returncode = 0
        zstd_decomp_result.stdout = b"mock tar data"
        tar_test_result = MagicMock()
        tar_test_result.returncode = 1  # tar test fails

        def subprocess_side_effect(*args, **kwargs):
            cmd = args[0] if args else []
            if isinstance(cmd, list) and "zstd" in cmd and "--test" in cmd:
                return zstd_test_result
            elif isinstance(cmd, list) and "zstd" in cmd and "-d" in cmd:
                return zstd_decomp_result
            elif isinstance(cmd, list) and "tar" in cmd and "-t" in cmd:
                return tar_test_result
            return MagicMock()

        mock_subprocess_run.side_effect = subprocess_side_effect

        zip_path = Path("/tmp/bad_tar.tar.zst")
        result = asyncio.run(utils.validate_backup_file(zip_path))

        assert result is False

    @patch.object(Path, "exists")
    @patch("subprocess.run")
    def test_validate_backup_file_decompression_fails(
        self, mock_subprocess_run, mock_exists
    ):
        """Test validation fails when decompression fails."""
        mock_exists.return_value = True

        zstd_test_result = MagicMock()
        zstd_test_result.returncode = 0  # Test passes
        zstd_decomp_result = MagicMock()
        zstd_decomp_result.returncode = 1  # Decompression fails

        def subprocess_side_effect(*args, **kwargs):
            cmd = args[0] if args else []
            if isinstance(cmd, list) and "zstd" in cmd and "--test" in cmd:
                return zstd_test_result
            elif isinstance(cmd, list) and "zstd" in cmd and "-d" in cmd:
                return zstd_decomp_result
            return MagicMock()

        mock_subprocess_run.side_effect = subprocess_side_effect

        zip_path = Path("/tmp/decomp_fail.tar.zst")
        result = asyncio.run(utils.validate_backup_file(zip_path))

        assert result is False

    @patch.object(Path, "exists")
    @patch("ami.dataops.backup.create.utils._validate_backup_file_sync")
    def test_validate_backup_file_timeout(self, mock_validate_sync, mock_exists):
        """Test validation handles timeout."""
        mock_exists.return_value = True
        mock_validate_sync.side_effect = subprocess.TimeoutExpired("zstd", 30)

        zip_path = Path("/tmp/slow.tar.zst")
        result = asyncio.run(utils.validate_backup_file(zip_path))

        assert result is False

    @patch.object(Path, "exists")
    @patch("ami.dataops.backup.create.utils._validate_backup_file_sync")
    def test_validate_backup_file_unexpected_exception(
        self, mock_validate_sync, mock_exists
    ):
        """Test validation handles unexpected exceptions."""
        mock_exists.return_value = True
        mock_validate_sync.side_effect = Exception("Unexpected error")

        zip_path = Path("/tmp/error.tar.zst")
        result = asyncio.run(utils.validate_backup_file(zip_path))

        assert result is False

    @patch.object(Path, "iterdir", side_effect=Exception("Permission denied"))
    def test_cleanup_old_backups_exception(self, mock_iterdir):
        """Test cleanup handles exceptions gracefully."""
        directory = Path("/tmp/backups")
        result = asyncio.run(utils.cleanup_old_backups(directory, keep_count=5))

        assert result is False

    @patch.object(Path, "iterdir")
    def test_cleanup_old_backups_no_files_to_delete(self, mock_iterdir):
        """Test cleanup when there are fewer files than keep_count."""
        mock_file = MagicMock(spec=Path)
        mock_file.name = "backup_20230101.tar.zst"
        mock_file.suffixes = [".tar", ".zst"]
        mock_file.is_file.return_value = True
        mock_stat = MagicMock()
        mock_stat.st_mtime = 1
        mock_file.stat.return_value = mock_stat

        mock_iterdir.return_value = [mock_file]

        directory = Path("/tmp/backups")
        result = asyncio.run(utils.cleanup_old_backups(directory, keep_count=5))

        assert result is True
        mock_file.unlink.assert_not_called()

    @patch.object(Path, "iterdir")
    def test_cleanup_old_backups_skips_non_tar_zst_files(self, mock_iterdir):
        """Test cleanup ignores non .tar.zst files."""
        mock_tar_file = MagicMock(spec=Path)
        mock_tar_file.name = "backup.tar.zst"
        mock_tar_file.suffixes = [".tar", ".zst"]
        mock_tar_file.is_file.return_value = True
        mock_stat = MagicMock()
        mock_stat.st_mtime = 1
        mock_tar_file.stat.return_value = mock_stat

        mock_other_file = MagicMock(spec=Path)
        mock_other_file.name = "notes.txt"
        mock_other_file.suffixes = [".txt"]
        mock_other_file.is_file.return_value = True

        mock_iterdir.return_value = [mock_tar_file, mock_other_file]

        directory = Path("/tmp/backups")
        result = asyncio.run(utils.cleanup_old_backups(directory, keep_count=1))

        assert result is True
        # Only tar.zst file should be considered, so nothing deleted
        mock_tar_file.unlink.assert_not_called()


class TestValidateBackupFileSync:
    """Tests for the synchronous validation helper."""

    @patch("subprocess.run")
    def test_validate_backup_file_sync_success(self, mock_run):
        """Test successful sync validation."""
        zstd_test = MagicMock()
        zstd_test.returncode = 0
        zstd_decomp = MagicMock()
        zstd_decomp.returncode = 0
        zstd_decomp.stdout = b"tar data"
        tar_test = MagicMock()
        tar_test.returncode = 0

        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return zstd_test
            elif call_count[0] == SECOND_SUBPROCESS_CALL:
                return zstd_decomp
            else:
                return tar_test

        mock_run.side_effect = side_effect

        result = utils._validate_backup_file_sync(Path("/tmp/good.tar.zst"))
        assert result is None

    @patch("subprocess.run")
    def test_validate_backup_file_sync_zstd_fails(self, mock_run):
        """Test sync validation when zstd test fails."""
        zstd_test = MagicMock()
        zstd_test.returncode = 1
        mock_run.return_value = zstd_test

        result = utils._validate_backup_file_sync(Path("/tmp/bad.tar.zst"))
        assert result is not None
        assert "zstd validation" in result
