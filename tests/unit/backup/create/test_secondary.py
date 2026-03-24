"""Unit tests for the secondary backup module (create/secondary.py)."""

import asyncio
from pathlib import Path
from unittest.mock import patch

from ami.dataops.backup.create import secondary

EXPECTED_AVAILABLE_LOCATION_COUNT = 2
EXPECTED_COPY_CALL_COUNT = 2


class TestSecondaryBackup:
    """Unit tests for the secondary backup functions."""

    @patch("ami.dataops.backup.create.secondary._get_secondary_locations")
    @patch("ami.dataops.backup.create.secondary._is_backup_location_available")
    @patch("shutil.copy2")
    @patch.object(Path, "exists")
    def test_copy_to_secondary_backup_success(
        self, mock_exists, mock_copy, mock_available, mock_locations
    ):
        """Test successful copy to secondary location."""
        mock_exists.return_value = True  # Source file exists

        # Mock locations and availability
        location = Path("/Volumes/AMI-BACKUP")
        mock_locations.return_value = [location]
        mock_available.return_value = True

        zip_path = Path("/tmp/backup.tar.zst")
        result = asyncio.run(secondary.copy_to_secondary_backup(zip_path))

        assert result is True
        mock_copy.assert_called_once_with(str(zip_path), str(location / zip_path.name))

    @patch("ami.dataops.backup.create.secondary._get_secondary_locations")
    @patch("ami.dataops.backup.create.secondary._is_backup_location_available")
    @patch.object(Path, "exists")
    def test_copy_to_secondary_backup_no_locations(
        self, mock_exists, mock_available, mock_locations
    ):
        """Test behavior when no secondary locations are available."""
        mock_exists.return_value = True
        mock_locations.return_value = [Path("/non/existent")]
        mock_available.return_value = False

        zip_path = Path("/tmp/backup.tar.zst")
        result = asyncio.run(secondary.copy_to_secondary_backup(zip_path))

        assert result is False

    @patch("os.getenv")
    @patch.object(Path, "exists")
    def test_get_secondary_locations(self, mock_exists, mock_getenv):
        """Test location discovery via env var and defaults."""
        mock_getenv.return_value = "/mnt/ext-backup"
        mock_exists.side_effect = lambda: True  # Pretend /media/backup exists

        # We need to mock Path("/media/backup").exists() specifically
        with patch.object(Path, "exists", return_value=True):
            locations = secondary._get_secondary_locations()

            assert Path("/mnt/ext-backup") in locations
            assert Path("/media/backup") in locations

    @patch.object(Path, "exists")
    @patch.object(Path, "is_dir")
    @patch.object(Path, "touch")
    @patch.object(Path, "unlink")
    def test_is_backup_location_available_success(
        self, mock_unlink, mock_touch, mock_is_dir, mock_exists
    ):
        """Test availability check for a valid location."""
        mock_exists.return_value = True
        mock_is_dir.return_value = True
        mock_touch.return_value = None
        mock_unlink.return_value = None

        location = Path("/valid/backup")
        result = asyncio.run(secondary._is_backup_location_available(location))

        assert result is True
        mock_touch.assert_called_once()

    @patch.object(Path, "exists")
    def test_copy_to_secondary_backup_source_not_exists(self, mock_exists):
        """Test behavior when source file doesn't exist."""
        mock_exists.return_value = False

        zip_path = Path("/tmp/nonexistent.tar.zst")
        result = asyncio.run(secondary.copy_to_secondary_backup(zip_path))

        assert result is False

    @patch("ami.dataops.backup.create.secondary._get_secondary_locations")
    @patch("ami.dataops.backup.create.secondary._is_backup_location_available")
    @patch("shutil.copy2", side_effect=Exception("Disk full"))
    @patch.object(Path, "exists")
    def test_copy_to_secondary_backup_copy_fails(
        self, mock_exists, mock_copy, mock_available, mock_locations
    ):
        """Test behavior when copy operation fails."""
        mock_exists.return_value = True
        mock_locations.return_value = [Path("/Volumes/BACKUP")]
        mock_available.return_value = True

        zip_path = Path("/tmp/backup.tar.zst")
        result = asyncio.run(secondary.copy_to_secondary_backup(zip_path))

        assert result is False

    @patch.object(Path, "exists")
    @patch.object(Path, "is_dir")
    def test_is_backup_location_available_not_a_directory(
        self, mock_is_dir, mock_exists
    ):
        """Test availability check when path is not a directory."""
        mock_exists.return_value = True
        mock_is_dir.return_value = False

        location = Path("/valid/file_not_dir")
        result = asyncio.run(secondary._is_backup_location_available(location))

        assert result is False

    @patch.object(Path, "exists")
    @patch.object(Path, "is_dir")
    @patch.object(Path, "touch", side_effect=PermissionError("Access denied"))
    def test_is_backup_location_available_no_write_permission(
        self, mock_touch, mock_is_dir, mock_exists
    ):
        """Test availability check when write permission is denied."""
        mock_exists.return_value = True
        mock_is_dir.return_value = True

        location = Path("/readonly/backup")
        result = asyncio.run(secondary._is_backup_location_available(location))

        assert result is False

    @patch.object(Path, "exists")
    def test_is_backup_location_available_not_exists(self, mock_exists):
        """Test availability check when location doesn't exist."""
        mock_exists.return_value = False

        location = Path("/nonexistent/backup")
        result = asyncio.run(secondary._is_backup_location_available(location))

        assert result is False

    @patch.object(Path, "exists", side_effect=Exception("IO Error"))
    def test_is_backup_location_available_exception(self, mock_exists):
        """Test availability check handles unexpected exceptions."""
        location = Path("/error/backup")
        result = asyncio.run(secondary._is_backup_location_available(location))

        assert result is False

    @patch("ami.dataops.backup.create.secondary._get_secondary_locations")
    @patch("ami.dataops.backup.create.secondary._is_backup_location_available")
    def test_get_available_backup_locations_filters_unavailable(
        self, mock_available, mock_locations
    ):
        """Test get_available_backup_locations filters unavailable locations."""
        loc1 = Path("/Volumes/BACKUP1")
        loc2 = Path("/Volumes/BACKUP2")
        loc3 = Path("/Volumes/BACKUP3")
        mock_locations.return_value = [loc1, loc2, loc3]

        # Only loc1 and loc3 are available
        async def availability_check(loc):
            return loc in [loc1, loc3]

        mock_available.side_effect = availability_check

        result = asyncio.run(secondary.get_available_backup_locations())

        assert loc1 in result
        assert loc2 not in result
        assert loc3 in result
        assert len(result) == EXPECTED_AVAILABLE_LOCATION_COUNT

    @patch("os.getenv")
    @patch.object(Path, "exists")
    def test_get_secondary_locations_no_env_no_default(self, mock_exists, mock_getenv):
        """Test location discovery when no env var and no default mount."""
        mock_getenv.return_value = None
        mock_exists.return_value = False

        locations = secondary._get_secondary_locations()

        assert len(locations) == 0

    @patch("ami.dataops.backup.create.secondary._get_secondary_locations")
    @patch("ami.dataops.backup.create.secondary._is_backup_location_available")
    @patch("shutil.copy2")
    @patch.object(Path, "exists")
    def test_copy_to_secondary_backup_multiple_locations(
        self, mock_exists, mock_copy, mock_available, mock_locations
    ):
        """Test copying to multiple secondary locations."""
        mock_exists.return_value = True

        loc1 = Path("/Volumes/BACKUP1")
        loc2 = Path("/Volumes/BACKUP2")
        mock_locations.return_value = [loc1, loc2]
        mock_available.return_value = True

        zip_path = Path("/tmp/backup.tar.zst")
        result = asyncio.run(secondary.copy_to_secondary_backup(zip_path))

        assert result is True
        assert mock_copy.call_count == EXPECTED_COPY_CALL_COUNT
