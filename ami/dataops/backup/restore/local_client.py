"""
Local file client for restore operations.

Handles operations related to local backup files.
"""

import asyncio
from pathlib import Path


async def find_latest_backup(directory: Path) -> Path | None:
    """
    Find the latest backup file in a directory.

    Args:
        directory: Directory to search for backups

    Returns:
        Path to the latest backup file or None if no backups found
    """
    if not directory.exists():
        return None

    # Look for tar.zst files in the directory (the actual backup format)
    loop = asyncio.get_event_loop()
    backup_files = await loop.run_in_executor(
        None, lambda: list(directory.glob("*.tar.zst"))
    )

    if not backup_files:
        return None

    # Sort by modification time (most recent first)
    latest_backup = await loop.run_in_executor(
        None, lambda: max(backup_files, key=lambda f: f.stat().st_mtime)
    )
    return latest_backup


async def verify_backup_exists(backup_path: Path) -> bool:
    """
    Verify that a backup file exists and is accessible.

    Args:
        backup_path: Path to the backup file

    Returns:
        True if file exists and is accessible, False otherwise
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, lambda: backup_path.exists() and backup_path.is_file()
    )


async def get_backup_size(backup_path: Path) -> int | None:
    """
    Get the size of a backup file.

    Args:
        backup_path: Path to the backup file

    Returns:
        Size in bytes or None if file doesn't exist
    """
    if not await verify_backup_exists(backup_path):
        return None

    loop = asyncio.get_event_loop()
    try:
        size = await loop.run_in_executor(None, lambda: backup_path.stat().st_size)
    except OSError:
        return None
    else:
        return size


async def find_backup_by_name(directory: Path, name_pattern: str) -> Path | None:
    """
    Find a backup file by name pattern in a directory.

    Args:
        directory: Directory to search in
        name_pattern: Pattern to match in file names

    Returns:
        Path to matching backup file or None if not found
    """
    if not directory.exists():
        return None

    loop = asyncio.get_event_loop()
    backup_files = await loop.run_in_executor(
        None, lambda: list(directory.glob(f"*{name_pattern}*.tar.zst"))
    )

    if not backup_files:
        return None

    # Return the most recent match
    latest_backup = await loop.run_in_executor(
        None, lambda: max(backup_files, key=lambda f: f.stat().st_mtime)
    )
    return latest_backup


async def list_backups_in_directory(directory: Path) -> list[Path]:
    """
    List all backup files in a directory.

    Args:
        directory: Directory to search for backups

    Returns:
        List of backup file paths
    """
    if not directory.exists():
        return []

    loop = asyncio.get_event_loop()
    backup_files = await loop.run_in_executor(
        None, lambda: list(directory.glob("*.tar.zst"))
    )

    # Sort by modification time (newest first)
    sorted_files = await loop.run_in_executor(
        None,
        lambda: sorted(backup_files, key=lambda f: f.stat().st_mtime, reverse=True),
    )

    return sorted_files


async def validate_backup_path(backup_path: Path) -> bool:
    """
    Validate that a backup path is safe and accessible.

    Args:
        backup_path: Path to validate

    Returns:
        True if path is valid and safe, False otherwise
    """
    # Check if path exists and is a file
    if not await verify_backup_exists(backup_path):
        return False

    # Resolve the path to catch any path traversal issues
    try:
        backup_path.resolve(strict=True)
    except (OSError, ValueError):
        return False

    return True
