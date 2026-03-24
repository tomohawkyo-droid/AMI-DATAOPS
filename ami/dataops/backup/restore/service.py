"""
Backup restore service.

Main business logic for backup restore operations.
"""

import os
import tempfile
from pathlib import Path

from loguru import logger

from ami.dataops.backup.backup_config import BackupConfig
from ami.dataops.backup.backup_exceptions import BackupError
from ami.dataops.backup.common.auth import AuthenticationManager
from ami.dataops.backup.common.constants import DEFAULT_BACKUP_MOUNT
from ami.dataops.backup.restore import local_client
from ami.dataops.backup.restore.drive_client import (
    DriveFileMetadata,
    DriveRestoreClient,
)
from ami.dataops.backup.restore.extractor import (
    extract_specific_paths,
)


class BackupRestoreService:
    """Main service for backup restore operations."""

    def __init__(
        self, drive_client: DriveRestoreClient, auth_manager: AuthenticationManager
    ):
        self.drive_client = drive_client
        self.auth_manager = auth_manager

    async def restore_from_drive_by_revision(
        self, revision: int, restore_path: Path, config: BackupConfig
    ) -> bool:
        """
        Restore from Google Drive by going back specified number of revisions.

        Args:
            revision: Number of revisions to go back (0 = latest, 1 = previous, etc.)
            restore_path: Path to restore the backup to
            config: Backup configuration

        Returns:
            True if restore was successful, False otherwise
        """
        logger.info(f"Fetching Drive backups to go back {revision} revision(s)...")

        backup_files = await self.drive_client.list_backup_files(config)

        if not backup_files:
            logger.error("No backup files found")
            return False

        if revision >= len(backup_files):
            count = len(backup_files)
            logger.error(f"Revision {revision} is beyond available backups ({count})")
            return False

        selected_file = backup_files[revision]  # 0 = latest, 1 = previous, etc.
        file_id = selected_file.get("id", "")
        file_name = selected_file.get("name", "Unknown")
        modified_time = selected_file.get("modifiedTime", "Unknown")

        if not file_id:
            logger.error("Selected backup file has no ID")
            return False

        logger.info(
            f"Selected backup: {file_name} (ID: {file_id}, Modified: {modified_time})"
        )
        logger.info(f"Restoring to: {restore_path.absolute()}")

        # Restore from the selected Google Drive backup
        return await self._restore_from_drive_file(file_id, restore_path, config)

    async def restore_from_drive_by_file_id(
        self, file_id: str, restore_path: Path, config: BackupConfig
    ) -> bool:
        """
        Restore from Google Drive using a specific file ID.

        Args:
            file_id: Google Drive file ID of the backup
            restore_path: Path to restore the backup to
            config: Backup configuration

        Returns:
            True if restore was successful, False otherwise
        """
        logger.info(f"Restoring from Google Drive backup: {file_id}")
        logger.info(f"Restoring to: {restore_path.absolute()}")

        return await self._restore_from_drive_file(file_id, restore_path, config)

    async def _restore_from_drive_file(
        self, file_id: str, restore_path: Path, config: BackupConfig
    ) -> bool:
        """
        Internal method to restore from a specific Google Drive file.

        Args:
            file_id: Google Drive file ID
            restore_path: Path to restore to
            config: Backup configuration

        Returns:
            True if successful, False otherwise
        """
        # Pre-flight: validate credentials before downloading
        logger.info("Validating credentials...")
        try:
            self.auth_manager.get_credentials()
        except Exception as e:
            msg = f"Credential check failed: {e}"
            raise BackupError(msg) from e

        # Create restore directory
        restore_path.mkdir(parents=True, exist_ok=True)

        # Download the archive to a temporary location
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            archive_path = temp_path / f"backup_{file_id}.tar.zst"

            # Download the file
            success = await self.drive_client.download_file(
                file_id, archive_path, config
            )
            if not success:
                logger.error(f"Failed to download backup file {file_id}")
                return False

            # Extract all contents
            try:
                logger.info(f"Extracting tar.zst backup to: {restore_path}")

                # Extract the contents (None means extract all)
                extract_success = await extract_specific_paths(
                    archive_path, None, restore_path
                )

                if extract_success:
                    logger.info("Drive restore completed successfully")
                    return True
                else:
                    logger.error("Failed to extract downloaded backup")
                    return False

            except Exception as e:
                logger.error(f"Restore from Google Drive failed: {e}")
                return False

    async def restore_local_backup(self, backup_path: Path, restore_path: Path) -> bool:
        """
        Restore from a local backup archive.

        Args:
            backup_path: Path to the local backup tar.zst file
            restore_path: Path to restore the backup to

        Returns:
            True if restore was successful, False otherwise
        """
        if not await local_client.verify_backup_exists(backup_path):
            msg = f"Backup file not found: {backup_path}"
            raise BackupError(msg)

        logger.info(f"Restoring from local backup: {backup_path}")
        logger.info(f"Restoring to: {restore_path}")

        try:
            # Create restore directory
            restore_path.mkdir(parents=True, exist_ok=True)

            # Extract all contents
            logger.info(f"Extracting tar.zst backup to: {restore_path}")

            # Extract the contents (None means extract all)
            extract_success = await extract_specific_paths(
                backup_path, None, restore_path
            )
        except Exception as e:
            logger.error(f"Local restore failed: {e}")
            return False

        if extract_success:
            logger.info("Local restore completed successfully")
            return True
        else:
            logger.error("Failed to extract local backup")
            return False

    async def restore_latest_local(self, restore_path: Path) -> bool:
        """
        Restore the latest local backup.

        Args:
            restore_path: Path to restore the backup to

        Returns:
            True if restore was successful, False otherwise
        """
        # Look for backups in common locations
        backup_locations = [
            Path.home() / "Downloads",  # Common download location
            DEFAULT_BACKUP_MOUNT,  # System default backup location
            Path.cwd() / ".backup",  # Project backup location
            Path.cwd() / "backup",  # Alternative backup location
        ]

        # Check env var for custom mount
        env_mount = os.getenv("AMI_BACKUP_MOUNT")
        if env_mount:
            backup_locations.insert(0, Path(env_mount))

        for backup_dir in backup_locations:
            if backup_dir.exists():
                latest_backup = await local_client.find_latest_backup(backup_dir)
                if latest_backup:
                    logger.info(f"Found latest backup: {latest_backup}")
                    return await self.restore_local_backup(latest_backup, restore_path)

        logger.error("No local backup files found")
        return False

    async def list_available_drive_backups(
        self, config: BackupConfig
    ) -> list[DriveFileMetadata]:
        """
        List available backups in Google Drive.

        Args:
            config: Backup configuration

        Returns:
            List of backup file metadata
        """
        return await self.drive_client.list_backup_files(config)

    async def list_available_local_backups(self, directory: Path) -> list[Path]:
        """
        List available local backups in a directory.

        Args:
            directory: Directory to search for backups

        Returns:
            List of backup file paths
        """
        return await local_client.list_backups_in_directory(directory)

    async def selective_restore_from_drive_by_file_id(
        self, file_id: str, paths: list[Path], restore_path: Path, config: BackupConfig
    ) -> bool:
        """
        Restore specific paths from a Google Drive backup.

        Args:
            file_id: Google Drive file ID of the backup
            paths: List of paths to restore from the archive
            restore_path: Path to restore the backup to
            config: Backup configuration

        Returns:
            True if restore was successful, False otherwise
        """
        logger.info(f"Restoring specific paths from Google Drive backup: {file_id}")
        logger.info(f"Target paths: {[str(p) for p in paths]}")
        logger.info(f"Restoring to: {restore_path.absolute()}")

        # Pre-flight: validate credentials before downloading
        logger.info("Validating credentials...")
        try:
            self.auth_manager.get_credentials()
        except Exception as e:
            msg = f"Credential check failed: {e}"
            raise BackupError(msg) from e

        # Create restore directory
        restore_path.mkdir(parents=True, exist_ok=True)

        # Download the archive to a temporary location
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            archive_path = temp_path / f"backup_{file_id}.tar.zst"

            # Download the file
            success = await self.drive_client.download_file(
                file_id, archive_path, config
            )
            if not success:
                logger.error(f"Failed to download backup file {file_id}")
                return False

            # Extract specific paths
            try:
                logger.info(
                    f"Extracting specific paths from tar.zst backup to: {restore_path}"
                )

                # Extract the specific paths
                extract_success = await extract_specific_paths(
                    archive_path, paths, restore_path
                )

                if extract_success:
                    logger.info("Selective drive restore completed successfully")
                    return True
                else:
                    logger.error(
                        "Failed to extract specified paths from downloaded backup"
                    )
                    return False

            except Exception as e:
                logger.error(f"Selective restore from Google Drive failed: {e}")
                return False

    async def selective_restore_from_drive_by_revision(
        self, revision: int, paths: list[Path], restore_path: Path, config: BackupConfig
    ) -> bool:
        """
        Restore specific paths from Drive backup by going back specified revisions.

        Args:
            revision: Number of revisions to go back (0 = latest, 1 = previous, etc.)
            paths: List of paths to restore from the archive
            restore_path: Path to restore the backup to
            config: Backup configuration

        Returns:
            True if restore was successful, False otherwise
        """
        logger.info(
            f"Fetching Drive backups to go back {revision} revision(s) for restore..."
        )

        backup_files = await self.drive_client.list_backup_files(config)

        if not backup_files:
            logger.error("No backup files found")
            return False

        if revision >= len(backup_files):
            count = len(backup_files)
            logger.error(f"Revision {revision} is beyond available backups ({count})")
            return False

        selected_file = backup_files[revision]  # 0 = latest, 1 = previous, etc.
        file_id = selected_file.get("id", "")
        file_name = selected_file.get("name", "Unknown")
        modified_time = selected_file.get("modifiedTime", "Unknown")

        if not file_id:
            logger.error("Selected backup file has no ID")
            return False

        logger.info(
            f"Selected backup: {file_name} (ID: {file_id}, Modified: {modified_time})"
        )

        # Restore the selected Google Drive backup with specific paths
        return await self.selective_restore_from_drive_by_file_id(
            file_id, paths, restore_path, config
        )

    async def selective_restore_local_backup(
        self, backup_path: Path, paths: list[Path], restore_path: Path
    ) -> bool:
        """
        Restore specific paths from a local backup archive.

        Args:
            backup_path: Path to the local backup tar.zst file
            paths: List of paths to restore from the archive
            restore_path: Path to restore the backup to

        Returns:
            True if restore was successful, False otherwise
        """
        if not await local_client.verify_backup_exists(backup_path):
            msg = f"Backup file not found: {backup_path}"
            raise BackupError(msg)

        logger.info(f"Restoring specific paths from local backup: {backup_path}")
        logger.info(f"Target paths: {[str(p) for p in paths]}")
        logger.info(f"Restoring to: {restore_path}")

        try:
            # Create restore directory
            restore_path.mkdir(parents=True, exist_ok=True)

            # Extract specific paths
            logger.info(
                f"Extracting specific paths from tar.zst backup to: {restore_path}"
            )

            # Extract the specific paths
            extract_success = await extract_specific_paths(
                backup_path, paths, restore_path
            )
        except Exception as e:
            logger.error(f"Selective local restore failed: {e}")
            return False

        if extract_success:
            logger.info("Selective local restore completed successfully")
            return True
        else:
            logger.error("Failed to extract specified paths from local backup")
            return False

    async def validate_restore_path(self, restore_path: Path) -> bool:
        """
        Validate that a restore path is safe to use.

        Args:
            restore_path: Path to validate

        Returns:
            True if path is valid and safe, False otherwise
        """
        try:
            # Check if the path exists and is a directory, or if its parent exists
            if not restore_path.exists():
                # Try to create the path if parent exists
                parent = restore_path.parent
                if not parent.exists():
                    logger.error(
                        f"Restore path parent directory does not exist: {parent}"
                    )
                    return False
            elif not restore_path.is_dir():
                logger.error(
                    f"Restore path exists but is not a directory: {restore_path}"
                )
                return False

            # Additional safety checks could be added here
            pass
        except Exception as e:
            logger.error(f"Error validating restore path: {e}")
            return False
        else:
            return True
