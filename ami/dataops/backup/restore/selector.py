"""Interactive backup selector for restore operations."""

from ami.dataops.backup.restore.drive_client import DriveFileMetadata


def _parse_selection(choice: str, max_idx: int) -> int | None:
    """Parse user selection. Returns index or None on invalid input."""
    try:
        idx = int(choice)
        if 0 <= idx <= max_idx:
            return idx
    except ValueError:
        pass
    return None


def select_backup_interactive(backup_files: list[DriveFileMetadata]) -> str | None:
    """Interactively select a backup file from the list.

    Args:
        backup_files: List of backup file metadata dicts with 'id', 'name',
            'modifiedTime', 'size'

    Returns:
        The file ID of the selected backup, or None if cancelled
    """
    if not backup_files:
        return None

    print("\nAvailable backups:")
    print("-" * 80)

    for i, file_info in enumerate(backup_files):
        name = file_info.get("name", "Unknown")
        modified = file_info.get("modifiedTime", "Unknown")[:19]
        size = file_info.get("size", "?")
        print(f"  [{i}] {name}")
        print(f"      Modified: {modified}  Size: {size}")

    print("-" * 80)
    print("Enter number to select, or 'q' to cancel:")

    while True:
        try:
            choice = input("> ").strip()
        except (KeyboardInterrupt, EOFError):
            return None
        if choice.lower() == "q":
            return None
        selected = _parse_selection(choice, len(backup_files) - 1)
        if selected is not None:
            return backup_files[selected].get("id")
        if choice.lstrip("-").isdigit():
            print(f"Invalid selection. Enter 0-{len(backup_files) - 1} or 'q'")
        else:
            print("Invalid input. Enter a number or 'q'")
