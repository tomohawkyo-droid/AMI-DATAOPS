"""
Archive service for backup creation operations.

Handles creation of compressed backup archives.
"""

import re
from pathlib import Path

from ami.dataops.backup.backup_exceptions import ArchiveError as BackupArchiveError
from ami.dataops.backup.common.constants import DEFAULT_EXCLUSION_PATTERNS
from ami.dataops.backup.utils.archive_utils import ArchiveError as UtilsArchiveError
from ami.dataops.backup.utils.archive_utils import (
    _should_exclude_path as utils_should_exclude_path,
)
from ami.dataops.backup.utils.archive_utils import (
    create_archive as utils_create_archive,
)


# Wrapper function using project-specific default patterns
def _should_exclude_path(
    path_str: str, root_dir_str: str, ignore_exclusions: bool = False
) -> bool:
    """Wrapper for utility function using default patterns."""
    return utils_should_exclude_path(
        path_str, root_dir_str, DEFAULT_EXCLUSION_PATTERNS, ignore_exclusions
    )


# Keep _is_illegal_filename if used elsewhere; it wasn't in
# the new utility since it wasn't used in find-based implementation
def _is_illegal_filename(name: str) -> bool:
    return any(c in name for c in "\n\r\t") or bool(re.search(r"[\x00-\x1f\x7f]", name))


# _get_files_to_backup_robust was internal; expose only if needed


async def create_zip_archive(
    root_dir: Path,
    output_filename: str | None = None,
    ignore_exclusions: bool = False,
    output_dir: Path | None = None,
) -> Path:
    """
    Create a timestamped tar.zst archive.

    Delegates to archive_utils.create_archive using default exclusion patterns.
    """
    try:
        return await utils_create_archive(
            root_dir=root_dir,
            output_filename=output_filename,
            exclusion_patterns=DEFAULT_EXCLUSION_PATTERNS,
            ignore_exclusions=ignore_exclusions,
            output_dir=output_dir,
        )
    except UtilsArchiveError as e:
        raise BackupArchiveError(str(e)) from e
