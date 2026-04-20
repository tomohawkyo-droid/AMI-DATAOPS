"""Pure helpers for the ami-report wizard: scope discovery, window counts,
extension + window-key normalization, archive summary formatting.

Kept separate from `wizard.py` so the wizard module stays focused on the
interactive orchestration and stays under the 512-line cap.
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from ami.dataops.report.scanner import CandidateFile, TreeEntry

BYTES_PER_KIB = 1024.0
KIB_PER_MIB = 1024.0
ARCHIVE_PREVIEW_FILE_LIMIT = 20

SCOPE_SKIP_DIRS: frozenset[str] = frozenset(
    {
        ".git",
        ".venv",
        ".venvs",
        ".tox",
        ".boot-linux",
        ".gcloud",
        ".runtime",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        "node_modules",
        "__pycache__",
        "build",
        "dist",
        "tmp",
        "projects",
    }
)
DEFAULT_SCOPE_ALLOWED_SUFFIXES: tuple[str, ...] = (".log",)

WINDOW_OPTIONS: list[tuple[str, str, timedelta | None]] = [
    ("all", "All time", None),
    ("1m", "Last 1 minute", timedelta(minutes=1)),
    ("5m", "Last 5 minutes", timedelta(minutes=5)),
    ("15m", "Last 15 minutes", timedelta(minutes=15)),
    ("1h", "Last 1 hour", timedelta(hours=1)),
    ("8h", "Last 8 hours", timedelta(hours=8)),
    ("1d", "Last 1 day", timedelta(days=1)),
]
VALID_WINDOW_KEYS: frozenset[str] = frozenset(key for key, _, _ in WINDOW_OPTIONS)


def format_size(size_bytes: int) -> str:
    kib = size_bytes / BYTES_PER_KIB
    if kib < KIB_PER_MIB:
        return f"{kib:.1f} KiB"
    return f"{kib / KIB_PER_MIB:.1f} MiB"


def normalize_extensions(raw: str) -> frozenset[str]:
    """Parse a CSV list of extensions into a normalized frozenset."""
    out: set[str] = set()
    for item in raw.split(","):
        trimmed = item.strip().lower()
        if not trimmed:
            continue
        out.add(trimmed if trimmed.startswith(".") else f".{trimmed}")
    return frozenset(out)


def normalize_window_key(raw: str | None) -> str | None:
    if raw is None:
        return None
    normalized = raw.strip().lower()
    if not normalized:
        return None
    if normalized not in VALID_WINDOW_KEYS:
        valid = ", ".join(sorted(VALID_WINDOW_KEYS))
        msg = f"unknown --since value {raw!r}; expected one of: {valid}"
        raise ValueError(msg)
    return normalized


def window_cutoff(key: str | None, now_epoch: float) -> float | None:
    """Return the epoch cutoff for `key`, or None for `all` / unset."""
    if key is None or key == "all":
        return None
    for option_key, _, delta in WINDOW_OPTIONS:
        if option_key == key and delta is not None:
            return now_epoch - delta.total_seconds()
    return None


def count_per_window(entries: list[TreeEntry], now_epoch: float) -> dict[str, int]:
    """Return `{window_key: qualifying_file_count}` for every WINDOW_OPTIONS entry."""
    files = [e for e in entries if isinstance(e, CandidateFile) and e.toggleable]
    counts: dict[str, int] = {}
    for key, _, delta in WINDOW_OPTIONS:
        if delta is None:
            counts[key] = len(files)
            continue
        cutoff = now_epoch - delta.total_seconds()
        counts[key] = sum(1 for f in files if f.mtime_epoch >= cutoff)
    return counts


def find_scope_candidates(
    root: Path, *, allowed_suffixes: tuple[str, ...] | None = None
) -> list[tuple[Path, int]]:
    """Walk `root`, return `[(abs_dir_path, direct_match_count), ...]`.

    Starts with `root` + its total recursive count, then every descendant
    directory that directly contains at least one matching file. Junk
    dirs are pruned via `SCOPE_SKIP_DIRS`.
    """
    suffixes = allowed_suffixes or DEFAULT_SCOPE_ALLOWED_SUFFIXES
    counts: dict[Path, int] = {}
    total = 0
    abs_root = root.resolve()
    for dirpath, dirnames, filenames in os.walk(abs_root, topdown=True):
        dirnames[:] = [d for d in dirnames if d not in SCOPE_SKIP_DIRS]
        hits = sum(1 for f in filenames if f.lower().endswith(suffixes))
        if hits == 0:
            continue
        counts[Path(dirpath).resolve()] = hits
        total += hits
    result: list[tuple[Path, int]] = []
    if total > 0:
        result.append((abs_root, total))
    result.extend((path, counts[path]) for path in sorted(counts) if path != abs_root)
    return result


class ArchiveSummary(BaseModel):
    """Inputs to the archive-preview screen: compressed tar + per-file info."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    compressed_bytes: int
    uncompressed_bytes: int
    files: list[CandidateFile]


def render_archive_summary(summary: ArchiveSummary) -> str:
    """Format the archive-preview screen body. Pure function so tests can verify it."""
    head = (
        f"Archive:  {format_size(summary.compressed_bytes)} compressed  /  "
        f"{format_size(summary.uncompressed_bytes)} uncompressed\n"
        f"Files:    {len(summary.files)}\n\n"
    )
    shown = summary.files[:ARCHIVE_PREVIEW_FILE_LIMIT]
    lines = [
        f"  {candidate.relative_path:<60}{format_size(candidate.size_bytes)}"
        for candidate in shown
    ]
    extras = len(summary.files) - len(shown)
    if extras > 0:
        lines.append(f"  (+{extras} more)")
    return head + "\n".join(lines) + "\n\nReview complete?"
