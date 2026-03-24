"""Archive utilities for creating compressed backups."""

from __future__ import annotations

import asyncio
import fnmatch
import os
from datetime import datetime
from pathlib import Path

import zstandard as zstd
from tqdm import tqdm

# tar exit codes
TAR_FATAL_ERROR = 2  # Fatal error exit code

CHUNK_SIZE = 65536  # 64 KB


class ArchiveError(Exception):
    """Error during archive creation."""

    pass


def _should_exclude_path(
    path_str: str,
    root_dir_str: str,
    exclusion_patterns: list[str],
    ignore_exclusions: bool = False,
) -> bool:
    """Check if a path should be excluded from the archive.

    Args:
        path_str: Path to check
        root_dir_str: Root directory of the archive
        exclusion_patterns: List of glob patterns to exclude
        ignore_exclusions: If True, don't exclude anything

    Returns:
        True if the path should be excluded
    """
    if ignore_exclusions:
        return False

    path = Path(path_str)

    # Handle paths outside root gracefully
    try:
        rel_path = path.relative_to(root_dir_str) if path.is_absolute() else path
    except ValueError:
        # Path is outside root - don't exclude it here, let caller handle
        return False

    for pattern in exclusion_patterns:
        # Strip trailing slashes from patterns for fnmatch pattern matching
        # (tar uses trailing slash for directories, fnmatch doesn't)
        clean_pattern = pattern.rstrip("/")
        rel_str = str(rel_path)

        if (
            fnmatch.fnmatch(rel_str, clean_pattern)
            or fnmatch.fnmatch(rel_str, pattern)
            or fnmatch.fnmatch(rel_path.name, clean_pattern)
            or any(fnmatch.fnmatch(part, clean_pattern) for part in rel_path.parts)
        ):
            return True

    return False


async def _stream_to_zstd(proc: asyncio.subprocess.Process, archive_path: Path) -> None:
    """Stream subprocess stdout to zstd compressed file."""
    if proc.stdout is None:
        msg = "Process has no stdout"
        raise ArchiveError(msg)

    cctx = zstd.ZstdCompressor(level=3, threads=os.cpu_count() or 1)
    with (
        open(archive_path, "wb") as fout,
        cctx.stream_writer(fout) as compressor,
        tqdm(unit="B", unit_scale=True, desc="Compressing") as pbar,
    ):
        while True:
            chunk = await proc.stdout.read(CHUNK_SIZE)
            if not chunk:
                break
            compressor.write(chunk)
            pbar.update(len(chunk))


async def create_archive(
    root_dir: Path,
    output_filename: str | None = None,
    exclusion_patterns: list[str] | None = None,
    ignore_exclusions: bool = False,
    output_dir: Path | None = None,
) -> Path:
    """Create a compressed tar.zst archive.

    Args:
        root_dir: Directory to archive
        output_filename: Custom output filename (without extension)
        exclusion_patterns: List of glob patterns to exclude
        ignore_exclusions: If True, include all files
        output_dir: Directory for output file (default: root_dir parent)

    Returns:
        Path to the created archive

    Raises:
        ArchiveError: If archive creation fails
    """
    if exclusion_patterns is None:
        exclusion_patterns = []

    root_dir = root_dir.resolve()
    if not root_dir.exists():
        msg = f"Source directory does not exist: {root_dir}"
        raise ArchiveError(msg)

    if output_dir is None:
        output_dir = root_dir.parent

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    base_name = output_filename or f"{root_dir.name}-{timestamp}"
    archive_path = output_dir / f"{base_name}.tar.zst"

    exclude_args = []
    for pattern in exclusion_patterns:
        exclude_args.extend(["--exclude", pattern])

    cmd = [
        "tar",
        "-cf",
        "-",
        "--ignore-failed-read",
        "--warning=no-file-changed",
        *exclude_args,
        "-C",
        str(root_dir.parent),
        root_dir.name,
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError:
        msg = "tar command not found"
        raise ArchiveError(msg) from None

    # Stream tar output directly to zstd file - never load full tar into RAM
    try:
        await _stream_to_zstd(proc, archive_path)
    except Exception as e:
        msg = f"Archive creation failed: {e}"
        raise ArchiveError(msg) from e

    stderr_data = await proc.stderr.read() if proc.stderr else b""
    await proc.wait()

    # tar exit codes: 0=success, 1=some files differ (warnings), 2=fatal error
    # With --ignore-failed-read, permission errors become warnings (exit 1)
    if proc.returncode == TAR_FATAL_ERROR:
        msg = f"tar failed: {stderr_data.decode()}"
        raise ArchiveError(msg)

    return archive_path
