"""Interactive TUI helpers for ami-report.

Every tree entry — folder or file — is rendered as a regular toggleable
row in SelectionDialog, indented by its depth so the hierarchy reads
naturally. Folders carry a "(N files)" annotation and include the whole
subtree at send time (expansion happens in scanner.expand_selection, not
here). Files render with their size; preflight rejects render disabled.
"""

from __future__ import annotations

from typing import cast

from pydantic import BaseModel, ConfigDict

from ami.cli_components import dialogs
from ami.cli_components.selection_dialog import (
    DialogItem,
    SelectionDialog,
    SelectionDialogConfig,
)
from ami.dataops.report.config import PeerEntry, ReportConfig
from ami.dataops.report.scanner import (
    CandidateFile,
    FolderEntry,
    TreeEntry,
    expand_selection,
)

BYTES_PER_KIB = 1024.0
KIB_PER_MIB = 1024.0
INDENT_STEP = "  "


class TUIResult(BaseModel):
    """Outcome of the interactive flow: selected entries + destination peer."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    selected: list[TreeEntry]
    peer: PeerEntry


def _size_label(size_bytes: int) -> str:
    kib = size_bytes / BYTES_PER_KIB
    if kib < KIB_PER_MIB:
        return f"{kib:.1f} KiB"
    return f"{kib / KIB_PER_MIB:.1f} MiB"


def _folder_label(entry: FolderEntry) -> tuple[str, str]:
    indent = INDENT_STEP * entry.depth
    label = f"{indent}[dir] {entry.relative_path}/"
    detail = f"{entry.descendant_file_count} files"
    if entry.descendant_file_count != entry.toggleable_descendant_count:
        skipped = entry.descendant_file_count - entry.toggleable_descendant_count
        detail += f" ({skipped} rejected by pre-flight)"
    return label, detail


def _file_label(entry: CandidateFile) -> tuple[str, str]:
    indent = INDENT_STEP * entry.depth
    label = f"{indent}{entry.relative_path.rsplit('/', 1)[-1]}"
    detail = _size_label(entry.size_bytes)
    if entry.preflight != "ok":
        detail = f"{detail} -- {entry.preflight}"
    return label, detail


def _entry_id(entry: TreeEntry) -> str:
    prefix = "folder:" if isinstance(entry, FolderEntry) else "file:"
    return f"{prefix}{entry.absolute_path.as_posix()}"


def _build_items(entries: list[TreeEntry]) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for entry in entries:
        if isinstance(entry, FolderEntry):
            label, detail = _folder_label(entry)
        else:
            label, detail = _file_label(entry)
        items.append(
            {
                "id": _entry_id(entry),
                "label": label,
                "description": detail,
                "value": entry,
                "is_header": False,
                "disabled": not entry.toggleable,
            }
        )
    return items


def _extract_selection(raw: object, entries: list[TreeEntry]) -> list[TreeEntry]:
    if not isinstance(raw, list):
        return []
    by_id: dict[str, TreeEntry] = {
        _entry_id(entry): entry for entry in entries if entry.toggleable
    }
    chosen: list[TreeEntry] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        identifier = item.get("id")
        if isinstance(identifier, str) and identifier in by_id:
            chosen.append(by_id[identifier])
    return chosen


def pick_tree(entries: list[TreeEntry]) -> list[TreeEntry]:
    """Render the mixed folder+file tree and return the operator's checks."""
    if not entries:
        return []
    items = _build_items(entries)
    dialog = SelectionDialog(
        items=cast("list[DialogItem]", items),
        config=SelectionDialogConfig(
            title="Select files + folders for the report",
            multi=True,
            width=100,
            max_height=20,
        ),
    )
    return _extract_selection(dialog.run(), entries)


def pick_peer(peers: list[PeerEntry]) -> PeerEntry | None:
    """Render the single-select peer list and return the chosen entry."""
    if not peers:
        return None
    if len(peers) == 1:
        return peers[0]
    labels = [f"{p.name} -- {p.endpoint}" for p in peers]
    selection = dialogs.select(labels, title="Choose destination peer")
    if selection is None:
        return None
    for candidate in peers:
        label = f"{candidate.name} -- {candidate.endpoint}"
        if label == selection:
            return candidate
    return None


def confirm_send(
    selected_files: list[CandidateFile], peer: PeerEntry, bundle_id: str
) -> bool:
    """Show a summary screen; return True when the operator confirms."""
    total_bytes = sum(c.size_bytes for c in selected_files)
    message = (
        f"Destination: {peer.name} ({peer.endpoint})\n"
        f"Bundle id:   {bundle_id}\n"
        f"Files:       {len(selected_files)}\n"
        f"Total size:  {_size_label(total_bytes)}\n"
    )
    return bool(dialogs.confirm(message, title="Send report?"))


def run_interactive(
    config: ReportConfig,
    entries: list[TreeEntry],
    bundle_id: str,
) -> TUIResult | None:
    """Drive the three-screen flow; return None on cancel / empty selection."""
    selected = pick_tree(entries)
    if not selected:
        return None
    peer = pick_peer(config.peers)
    if peer is None:
        return None
    expanded = expand_selection(selected, entries)
    if not confirm_send(expanded, peer, bundle_id):
        return None
    return TUIResult(selected=selected, peer=peer)


def resolve_selection_from_defaults(
    defaults: dict[str, object],
    entries: list[TreeEntry],
) -> list[TreeEntry]:
    """Non-interactive path: `--ci --defaults FILE` lists relative paths."""
    raw = defaults.get("files", [])
    if not isinstance(raw, list):
        return []
    wanted = {str(entry) for entry in raw if isinstance(entry, str)}
    matches: list[TreeEntry] = []
    for candidate in entries:
        if not candidate.toggleable:
            continue
        if candidate.relative_path in wanted:
            matches.append(candidate)
            continue
        short = candidate.relative_path.rsplit("/", 1)[-1]
        if short in wanted:
            matches.append(candidate)
    return matches
