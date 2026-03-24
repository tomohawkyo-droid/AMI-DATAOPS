"""
Common constants for backup/restore operations.
"""

from pathlib import Path

# Default patterns to exclude from backup
DEFAULT_EXCLUSION_PATTERNS = [
    # Version Control
    ".git/",
    # .gitignore and .gitmodules are now INCLUDED for full repo state restoration
    # Python-related (excluding root .venv which is handled by logic)
    "__pycache__/",
    "*.pyc",
    "*.pyo",
    "*.pyd",
    ".pytest_cache/",
    ".coverage",
    "htmlcov/",
    # .env and .envrc are now INCLUDED to preserve configuration secrets
    ".python-version",
    # System files
    ".DS_Store",
    "Thumbs.db",
    ".Spotlight-V100",
    ".Trashes",
    ".fseventsd",
    # Local configuration that might contain secrets
    ".vscode/",
    ".idea/",
    ".project",
    ".settings/",
    # Log files
    "*.log",
    # Large data files
    "*.zip",
    "*.tar",
    "*.gz",
    "*.bz2",
    "*.xz",
    "*.zst",  # Don't include existing zst files
    # Database files
    "*.db",
    "*.sqlite",
    "*.sqlite3",
    # Docker-managed service data (transient, root-owned)
    # Use wildcard to match postfix/spool anywhere in the path
    "*/postfix/spool/*",
]

# Secondary backup locations (Linux default)
DEFAULT_BACKUP_MOUNT = Path("/media/backup")

# Default backup file name pattern
DEFAULT_BACKUP_NAME = "ami-agents-backup"
DEFAULT_BACKUP_PATTERN = f"{DEFAULT_BACKUP_NAME}.tar.zst"
