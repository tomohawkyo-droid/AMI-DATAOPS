"""
Backup utilities module.

Contains utility functions for backup operations.
"""

import subprocess
import tempfile
from pathlib import Path

from loguru import logger


async def cleanup_local_zip(zip_path: Path, keep_local: bool) -> bool:
    """
    Cleanup local zip file after upload based on keep_local setting.

    Args:
        zip_path: Path to the local zip file
        keep_local: Whether to keep the local zip file

    Returns:
        True if cleanup was successful or not needed, False if failed
    """
    if not zip_path.exists():
        logger.info("Local zip file already cleaned up")
        return True

    if keep_local:
        logger.info(f"Keeping local zip as requested: {zip_path}")
        return True

    try:
        zip_path.unlink()
        logger.info(f"Local zip file cleaned up: {zip_path}")
    except Exception as e:
        logger.error(f"Failed to cleanup local zip file {zip_path}: {e}")
        return False
    else:
        return True


async def cleanup_old_backups(directory: Path, keep_count: int = 5) -> bool:
    """
    Cleanup old backup files, keeping only the most recent ones.

    Args:
        directory: Directory containing backup files
        keep_count: Number of most recent backup files to keep

    Returns:
        True if cleanup was successful, False if failed
    """
    try:
        # Find all backup files (tar.zst files) using list comprehension
        backup_files = [
            (file_path.stat().st_mtime, file_path)
            for file_path in directory.iterdir()
            if file_path.is_file() and file_path.suffixes == [".tar", ".zst"]
        ]

        # Sort by modification time (most recent first)
        backup_files.sort(key=lambda x: x[0], reverse=True)

        # Remove old backups beyond the keep count
        files_to_delete = backup_files[keep_count:]
        for _mtime, file_path in files_to_delete:
            logger.info(f"Deleting old backup: {file_path}")
            file_path.unlink()

        if files_to_delete:
            logger.info(
                f"Deleted {len(files_to_delete)} old backup files, kept {keep_count}"
            )
        else:
            logger.info(f"No old backups to delete, keeping up to {keep_count} files")
    except Exception as e:
        logger.error(f"Failed to cleanup old backups in {directory}: {e}")
        return False
    else:
        return True


async def validate_backup_file(zip_path: Path) -> bool:
    """
    Validate that a backup file is properly formatted.

    Args:
        zip_path: Path to the backup file to validate

    Returns:
        True if backup file is valid, False otherwise
    """
    if not zip_path.exists():
        logger.error(f"Backup file does not exist for validation: {zip_path}")
        return False

    try:
        error = _validate_backup_file_sync(zip_path)
        if error:
            logger.error(error)
            return False
        logger.info(f"Backup file validated successfully: {zip_path}")
    except subprocess.TimeoutExpired:
        logger.error(f"Backup validation timed out for: {zip_path}")
        return False
    except Exception as e:
        logger.error(f"Backup validation failed for {zip_path}: {e}")
        return False
    else:
        return True


def _validate_backup_file_sync(zip_path: Path) -> str | None:
    """Synchronous validation of backup file. Returns error message or None."""
    # Check if it's a valid zstd compressed file
    result = subprocess.run(
        ["zstd", "--test", str(zip_path)],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        return f"Backup file failed zstd validation: {zip_path}"

    # Check if it's also a valid tar archive
    with tempfile.TemporaryDirectory():
        decomp_result = subprocess.run(
            ["zstd", "-d", "-c", str(zip_path)],
            capture_output=True,
            timeout=60,
            check=False,
        )

        if decomp_result.returncode != 0:
            return f"Backup file failed decompression test: {zip_path}"

        tar_result = subprocess.run(
            ["tar", "-t"],
            input=decomp_result.stdout,
            capture_output=True,
            timeout=30,
            check=False,
        )

        if tar_result.returncode != 0:
            return f"Backup file failed tar validation: {zip_path}"

    return None
