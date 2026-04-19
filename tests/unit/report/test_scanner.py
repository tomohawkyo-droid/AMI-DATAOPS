"""Unit tests for ami.dataops.report.scanner."""

from __future__ import annotations

from pathlib import Path

from ami.dataops.report.scanner import (
    CandidateFile,
    group_by_directory,
    scan_roots,
)

_EXPECTED_OK_COUNT = 3
_EXPECTED_NESTED_GROUP_SIZE = 2


class TestScanRoots:
    def test_scans_directory_recursively(self, tmp_path: Path) -> None:
        (tmp_path / "a.log").write_text("one\n")
        (tmp_path / "nested").mkdir()
        (tmp_path / "nested" / "b.log").write_text("two\n")
        (tmp_path / "nested" / "c.ndjson").write_text('{"x":1}\n')
        candidates = scan_roots([tmp_path])
        ok = [c for c in candidates if c.toggleable]
        assert len(ok) == _EXPECTED_OK_COUNT

    def test_missing_roots_skipped(self, tmp_path: Path) -> None:
        candidates = scan_roots([tmp_path / "nope"])
        assert candidates == []

    def test_disallowed_extension_marked_reject(self, tmp_path: Path) -> None:
        (tmp_path / "ok.log").write_text("ok\n")
        (tmp_path / "bad.exe").write_bytes(b"MZ")
        candidates = scan_roots([tmp_path])
        by_name = {c.relative_path: c for c in candidates}
        assert by_name["ok.log"].preflight == "ok"
        assert by_name["bad.exe"].preflight == "ext_not_allowed"
        assert not by_name["bad.exe"].toggleable

    def test_binary_content_marked_not_text(self, tmp_path: Path) -> None:
        (tmp_path / "a.log").write_bytes(b"text\x00binary")
        candidates = scan_roots([tmp_path])
        assert len(candidates) == 1
        assert candidates[0].preflight == "not_text"

    def test_oversize_marked_file_too_large(self, tmp_path: Path) -> None:
        (tmp_path / "big.log").write_bytes(b"x" * 2048)
        candidates = scan_roots([tmp_path], max_file_bytes=1024)
        assert len(candidates) == 1
        assert candidates[0].preflight == "file_too_large"

    def test_direct_file_path_scanned(self, tmp_path: Path) -> None:
        file_path = tmp_path / "single.log"
        file_path.write_text("solo\n")
        candidates = scan_roots([file_path])
        assert len(candidates) == 1
        assert candidates[0].preflight == "ok"

    def test_duplicate_across_roots_deduped(self, tmp_path: Path) -> None:
        (tmp_path / "a.log").write_text("ok\n")
        candidates = scan_roots([tmp_path, tmp_path])
        assert len(candidates) == 1


class TestGroupByDirectory:
    def test_groups_files_by_parent(self, tmp_path: Path) -> None:
        (tmp_path / "nested").mkdir()
        (tmp_path / "a.log").write_text("1\n")
        (tmp_path / "nested" / "b.log").write_text("2\n")
        (tmp_path / "nested" / "c.log").write_text("3\n")
        groups = group_by_directory(scan_roots([tmp_path]))
        assert list(groups.keys()) == [".", "nested"]
        assert len(groups["nested"]) == _EXPECTED_NESTED_GROUP_SIZE


class TestCandidateFile:
    def test_toggleable_only_when_ok(self) -> None:
        ok = CandidateFile(
            absolute_path=Path("/tmp/a.log"),
            relative_path="a.log",
            size_bytes=5,
            preflight="ok",
        )
        rejected = CandidateFile(
            absolute_path=Path("/tmp/b.exe"),
            relative_path="b.exe",
            size_bytes=5,
            preflight="ext_not_allowed",
            reject_detail="extension",
        )
        assert ok.toggleable
        assert not rejected.toggleable
