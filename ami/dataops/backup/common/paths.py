"""
Path utilities for backup/restore operations.
Centralizes logic for finding the workspace root and tools.
"""

import os
import shutil
import sys
from pathlib import Path

__all__ = ["get_workspace_root"]


class _WorkspaceRootCache:
    """Cache for workspace root to avoid repeated filesystem lookups."""

    _value: Path | None = None

    @classmethod
    def get(cls) -> Path | None:
        return cls._value

    @classmethod
    def set(cls, path: Path) -> None:
        cls._value = path


def get_workspace_root() -> Path:
    """Get the AMI-AGENTS workspace root directory.

    Finds root by looking for the .boot-linux directory marker
    (unique to AMI-AGENTS, not present in sub-projects).
    Falls back to AMI_PROJECT_ROOT environment variable.
    """
    cached = _WorkspaceRootCache.get()
    if cached is not None:
        return cached

    env_root = os.environ.get("AMI_PROJECT_ROOT")
    if env_root:
        result = Path(env_root)
        _WorkspaceRootCache.set(result)
        return result

    # Walk up from this file looking for .boot-linux (AMI-AGENTS marker)
    current = Path(__file__).resolve()
    while current != current.parent:
        if (current / ".boot-linux").is_dir():
            _WorkspaceRootCache.set(current)
            return current
        current = current.parent

    msg = (
        "AMI-AGENTS workspace root not found (no .boot-linux directory in parent chain)"
    )
    raise RuntimeError(msg)


# Alias used by callers that reference the old name
get_project_root = get_workspace_root


def setup_sys_path() -> None:
    """Add workspace root to sys.path if not present."""
    root = get_workspace_root()
    root_str = str(root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)


def find_gcloud() -> str | None:
    """Find gcloud CLI binary (bootstrap symlink, local SDK, or system)."""
    root = get_workspace_root()

    # Check bootstrap symlink first
    boot_gcloud = root / ".boot-linux" / "bin" / "ami-gcloud"
    if boot_gcloud.exists():
        return str(boot_gcloud)

    # Check for local SDK installation
    local_gcloud = root / ".gcloud" / "google-cloud-sdk" / "bin" / "gcloud"
    if local_gcloud.exists():
        return str(local_gcloud)

    # Check system PATH
    system_gcloud = shutil.which("gcloud")
    if system_gcloud:
        return system_gcloud

    return None
