"""
Secondary backup service.

Handles copying backup files to secondary locations.
"""

import asyncio
import os
import shutil
from pathlib import Path

from loguru import logger

from ami.dataops.backup.common.constants import DEFAULT_BACKUP_MOUNT


def _get_secondary_locations() -> list[Path]:
    """Get list of configured secondary backup locations."""
    locations = []

    # Check for AMI_BACKUP_MOUNT env var
    env_mount = os.getenv("AMI_BACKUP_MOUNT")
    if env_mount:
        locations.append(Path(env_mount))

    # Optional: Add common Linux mount point if it exists
    if DEFAULT_BACKUP_MOUNT.exists():
        locations.append(DEFAULT_BACKUP_MOUNT)

    return locations


async def copy_to_secondary_backup(zip_path: Path) -> bool:
    """
    Copy backup to secondary backup location if available.

    This function checks for common backup mount points and copies
    the backup file to all available secondary locations.

    Args:
        zip_path: Path to the backup file to copy

    Returns:
        True if copy was successful to at least one location, False otherwise
    """
    if not zip_path.exists():
        logger.error(f"Backup file does not exist for secondary copy: {zip_path}")
        return False

    success_count = 0
    secondary_locations = _get_secondary_locations()

    for backup_location in secondary_locations:
        if await _is_backup_location_available(backup_location):
            try:
                # Create destination path
                dest_path = backup_location / zip_path.name

                # Copy the file
                shutil.copy2(str(zip_path), str(dest_path))
                logger.info(f"✓ Backup copied to secondary location: {dest_path}")
                success_count += 1
            except Exception as e:
                logger.error(f"Failed to copy backup to {backup_location}: {e}")
        else:
            logger.debug(f"Secondary backup location not available: {backup_location}")

    if success_count > 0:
        logger.info(f"Backup copied to {success_count} secondary location(s)")
        return True
    else:
        logger.warning("No secondary backup locations were available")
        return False


async def _is_backup_location_available(location: Path) -> bool:
    """
    Check if a backup location is available (exists and is writable).

    Args:
        location: Path to check

    Returns:
        True if location is available, False otherwise
    """
    try:
        # Check if path exists
        if not location.exists():
            return False

        # Check if it's a directory
        if not location.is_dir():
            return False

        # Try to create a temporary file to check write permissions
        test_file = location / ".backup_write_test"
        try:
            test_file.touch()
            test_file.unlink()  # Remove the test file
        except (PermissionError, OSError):
            return False
        else:
            return True

    except Exception:
        return False


async def get_available_backup_locations() -> list[Path]:
    """
    Get list of all currently available backup locations.

    Returns:
        List of available backup location paths
    """
    secondary_locations = _get_secondary_locations()

    # Check all locations in parallel
    results = await asyncio.gather(
        *[_is_backup_location_available(loc) for loc in secondary_locations]
    )

    return [
        loc
        for loc, available in zip(secondary_locations, results, strict=False)
        if available
    ]
