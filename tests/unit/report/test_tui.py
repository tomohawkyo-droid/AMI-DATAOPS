"""Unit tests for ami.dataops.report.tui helpers (no keyboard simulation)."""

from __future__ import annotations

from pathlib import Path

from ami.dataops.report.scanner import CandidateFile, FolderEntry
from ami.dataops.report.tui import (
    _build_items,
    _entry_id,
    _extract_selection,
    _file_label,
    _folder_label,
    _size_label,
    resolve_selection_from_defaults,
)


def _file(rel: str, *, ok: bool = True, depth: int = 0) -> CandidateFile:
    return CandidateFile(
        absolute_path=Path(f"/tmp/{rel}"),
        relative_path=rel,
        size_bytes=1024,
        preflight="ok" if ok else "ext_not_allowed",
        reject_detail=None if ok else "bad ext",
        depth=depth,
    )


def _folder(rel: str, *, total: int, ok: int, depth: int = 0) -> FolderEntry:
    return FolderEntry(
        absolute_path=Path(f"/tmp/{rel}"),
        relative_path=rel,
        descendant_file_count=total,
        toggleable_descendant_count=ok,
        depth=depth,
    )


class TestSizeLabel:
    def test_kib_for_small(self) -> None:
        assert _size_label(512) == "0.5 KiB"

    def test_mib_for_large(self) -> None:
        assert _size_label(5 * 1024 * 1024) == "5.0 MiB"


class TestLabels:
    def test_folder_label_shows_count(self) -> None:
        label, detail = _folder_label(_folder("nested", total=3, ok=3, depth=1))
        assert label.endswith("[dir] nested/")
        assert label.startswith("  ")
        assert detail == "3 files"

    def test_folder_label_shows_rejected_count(self) -> None:
        _, detail = _folder_label(_folder("nested", total=5, ok=3))
        assert "2 rejected" in detail

    def test_file_label_indents_by_depth(self) -> None:
        label, _ = _file_label(_file("nested/a.log", depth=2))
        assert label.startswith("    ")
        assert label.endswith("a.log")


class TestBuildItems:
    def test_folders_are_not_headers(self) -> None:
        items = _build_items(
            [
                _folder("nested", total=1, ok=1),
                _file("nested/a.log", depth=1),
            ]
        )
        folder_item = items[0]
        assert folder_item["is_header"] is False
        assert folder_item["disabled"] is False

    def test_rejected_file_disabled(self) -> None:
        items = _build_items([_file("bad.exe", ok=False)])
        assert items[0]["disabled"] is True

    def test_folder_with_zero_toggleable_disabled(self) -> None:
        items = _build_items([_folder("empty", total=0, ok=0)])
        assert items[0]["disabled"] is True


class TestExtractSelection:
    def test_filters_by_toggleable_and_preserves_order(self) -> None:
        entries = [
            _folder("nested", total=2, ok=2),
            _file("nested/a.log", depth=1),
            _file("nested/b.log", depth=1),
        ]
        raw = [
            {"id": _entry_id(entries[2])},
            {"id": _entry_id(entries[0])},
        ]
        result = _extract_selection(raw, entries)
        ids = [_entry_id(r) for r in result]
        assert ids == [_entry_id(entries[2]), _entry_id(entries[0])]

    def test_non_list_returns_empty(self) -> None:
        assert _extract_selection(None, [_file("a.log")]) == []


class TestResolveFromDefaults:
    def test_matches_by_relative_path(self) -> None:
        entries = [_file("a.log"), _file("nested/b.log", depth=1)]
        result = resolve_selection_from_defaults(
            {"files": ["a.log", "nested/b.log"]}, entries
        )
        assert {c.relative_path for c in result} == {"a.log", "nested/b.log"}

    def test_matches_by_filename_only(self) -> None:
        entries = [_file("nested/trace.ndjson", depth=1)]
        result = resolve_selection_from_defaults({"files": ["trace.ndjson"]}, entries)
        assert len(result) == 1

    def test_ignores_disabled_entries(self) -> None:
        entries = [_file("bad.exe", ok=False)]
        assert resolve_selection_from_defaults({"files": ["bad.exe"]}, entries) == []
