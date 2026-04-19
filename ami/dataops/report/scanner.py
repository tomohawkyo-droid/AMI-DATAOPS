"""Walk candidate log roots + run pre-flight content checks.

The scanner builds the list of files the TUI multi-selects from. Each
candidate gets a pre-flight result: allowed (toggleable), rejected (shown
greyed out with a reason), or missing. Re-uses the intake validation
primitives so sender pre-flight and receiver post-flight enforce the
same content policy.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict

from ami.dataops.intake import validation

PreflightStatus = Literal["ok", "ext_not_allowed", "not_text", "file_too_large"]


class CandidateFile(BaseModel):
    """One file the TUI may offer for selection."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    absolute_path: Path
    relative_path: str
    size_bytes: int
    preflight: PreflightStatus
    reject_detail: str | None = None

    @property
    def toggleable(self) -> bool:
        return self.preflight == "ok"


def _preflight_one(
    path: Path, *, max_file_bytes: int
) -> tuple[PreflightStatus, str | None]:
    try:
        validation.validate_extension(path.name)
    except validation.ValidationRejected as exc:
        return "ext_not_allowed", exc.detail
    try:
        validation.validate_file_size(path.stat().st_size, max_file_bytes)
    except validation.ValidationRejected as exc:
        return "file_too_large", exc.detail
    try:
        validation.probe_text_content(path)
    except validation.ValidationRejected as exc:
        return "not_text", exc.detail
    return "ok", None


def scan_roots(
    roots: list[Path],
    *,
    max_file_bytes: int = validation.DEFAULT_MAX_FILE_BYTES,
) -> list[CandidateFile]:
    """Walk every root recursively and return CandidateFile entries.

    `roots` may contain directories or individual files. Missing roots are
    skipped with no error; the caller is responsible for reporting them.
    Files under every root are sorted for deterministic UI rendering.
    """
    candidates: list[CandidateFile] = []
    seen: set[Path] = set()
    for raw_root in roots:
        root = raw_root.expanduser().absolute()
        if not root.exists():
            continue
        paths = [root] if root.is_file() else sorted(root.rglob("*"))
        for path in paths:
            if not path.is_file():
                continue
            if path in seen:
                continue
            seen.add(path)
            preflight, detail = _preflight_one(path, max_file_bytes=max_file_bytes)
            try:
                rel = path.relative_to(root).as_posix()
            except ValueError:
                rel = path.name
            candidates.append(
                CandidateFile(
                    absolute_path=path,
                    relative_path=rel,
                    size_bytes=path.stat().st_size,
                    preflight=preflight,
                    reject_detail=detail,
                )
            )
    return candidates


def group_by_directory(
    candidates: list[CandidateFile],
) -> dict[str, list[CandidateFile]]:
    """Return `{parent_dir: [candidates in that dir]}`, in sorted key order.

    Consumed by the TUI to render group headers — the parent dir becomes
    the header, the files become the toggleable children under it.
    """
    buckets: dict[str, list[CandidateFile]] = {}
    for candidate in candidates:
        parent = str(Path(candidate.relative_path).parent)
        buckets.setdefault(parent, []).append(candidate)
    return dict(sorted(buckets.items()))
