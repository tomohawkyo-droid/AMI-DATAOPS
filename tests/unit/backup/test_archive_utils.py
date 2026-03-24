"""Unit tests for archive_utils."""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.dataops.backup.utils.archive_utils import (
    ArchiveError,
    _should_exclude_path,
    create_archive,
)


class TestShouldExcludePath:
    """Tests for _should_exclude_path function."""

    def test_returns_false_when_ignore_exclusions_true(self):
        """Test that ignore_exclusions=True includes everything."""
        result = _should_exclude_path(
            "/project/.git",
            "/project",
            [".git", "*.pyc"],
            ignore_exclusions=True,
        )
        assert result is False

    def test_excludes_matching_pattern(self):
        """Test that matching patterns are excluded."""
        result = _should_exclude_path(
            "/project/file.pyc",
            "/project",
            ["*.pyc"],
            ignore_exclusions=False,
        )
        assert result is True

    def test_excludes_directory_pattern(self):
        """Test that directory patterns are excluded."""
        result = _should_exclude_path(
            "/project/.git/objects",
            "/project",
            [".git"],
            ignore_exclusions=False,
        )
        assert result is True

    def test_includes_non_matching_path(self):
        """Test that non-matching paths are included."""
        result = _should_exclude_path(
            "/project/src/main.py",
            "/project",
            [".git", "*.pyc"],
            ignore_exclusions=False,
        )
        assert result is False

    def test_handles_path_outside_root(self):
        """Test that paths outside root are not excluded."""
        result = _should_exclude_path(
            "/other/project/file.txt",
            "/project",
            ["*.txt"],
            ignore_exclusions=False,
        )
        # Path outside root - function returns False (doesn't exclude)
        assert result is False

    def test_pattern_with_trailing_slash(self):
        """Test that patterns with trailing slashes work."""
        result = _should_exclude_path(
            "/project/node_modules/pkg",
            "/project",
            ["node_modules/"],
            ignore_exclusions=False,
        )
        assert result is True

    def test_matches_any_part_of_path(self):
        """Test that patterns match any part of the path."""
        result = _should_exclude_path(
            "/project/sub/deep/__pycache__/file.pyc",
            "/project",
            ["__pycache__"],
            ignore_exclusions=False,
        )
        assert result is True

    def test_empty_exclusion_patterns(self):
        """Test with empty exclusion patterns list."""
        result = _should_exclude_path(
            "/project/any/file.txt",
            "/project",
            [],
            ignore_exclusions=False,
        )
        assert result is False

    def test_relative_path_handling(self):
        """Test relative path handling."""
        result = _should_exclude_path(
            "src/test.pyc",
            "/project",
            ["*.pyc"],
            ignore_exclusions=False,
        )
        assert result is True


class TestArchiveError:
    """Tests for ArchiveError exception."""

    def test_archive_error_message(self):
        """Test ArchiveError stores message correctly."""
        error = ArchiveError("Test error message")
        assert str(error) == "Test error message"

    def test_archive_error_inheritance(self):
        """Test ArchiveError inherits from Exception."""
        error = ArchiveError("Test")
        assert isinstance(error, Exception)


class TestCreateArchive:
    """Tests for create_archive function."""

    @pytest.mark.asyncio
    async def test_create_archive_success(self):
        """Test successful archive creation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir) / "source"
            source_dir.mkdir()
            (source_dir / "file.txt").write_text("content")

            result = await create_archive(source_dir)

            assert result.exists()
            assert result.suffix == ".zst"
            assert ".tar" in result.name
            result.unlink()

    @pytest.mark.asyncio
    async def test_create_archive_nonexistent_source(self):
        """Test archive creation fails for nonexistent source."""
        with pytest.raises(ArchiveError) as exc_info:
            await create_archive(Path("/nonexistent/path"))

        assert "does not exist" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_create_archive_custom_filename(self):
        """Test archive creation with custom filename."""
        with tempfile.TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir) / "source"
            source_dir.mkdir()
            (source_dir / "file.txt").write_text("content")

            result = await create_archive(source_dir, output_filename="custom-backup")

            assert "custom-backup" in result.name
            assert result.exists()
            result.unlink()

    @pytest.mark.asyncio
    async def test_create_archive_custom_output_dir(self):
        """Test archive creation with custom output directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir) / "source"
            source_dir.mkdir()
            output_dir = Path(tmpdir) / "output"
            output_dir.mkdir()
            (source_dir / "file.txt").write_text("content")

            result = await create_archive(source_dir, output_dir=output_dir)

            assert result.parent == output_dir
            assert result.exists()
            result.unlink()

    @pytest.mark.asyncio
    async def test_create_archive_with_exclusions(self):
        """Test archive creation with exclusion patterns."""
        with tempfile.TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir) / "source"
            source_dir.mkdir()
            (source_dir / "keep.txt").write_text("keep")
            (source_dir / "exclude.pyc").write_text("exclude")

            result = await create_archive(source_dir, exclusion_patterns=["*.pyc"])

            assert result.exists()
            result.unlink()

    @pytest.mark.asyncio
    async def test_create_archive_tar_not_found(self):
        """Test archive creation fails when tar command not found."""
        with tempfile.TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir) / "source"
            source_dir.mkdir()
            (source_dir / "file.txt").write_text("content")

            with patch("asyncio.create_subprocess_exec", side_effect=FileNotFoundError):
                with pytest.raises(ArchiveError) as exc_info:
                    await create_archive(source_dir)

                assert "tar command not found" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_create_archive_tar_fails(self):
        """Test archive creation fails when tar returns fatal error (exit code 2)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir) / "source"
            source_dir.mkdir()
            (source_dir / "file.txt").write_text("content")

            mock_proc = MagicMock()
            mock_proc.returncode = 2  # TAR_FATAL_ERROR
            mock_proc.stdout = MagicMock()
            mock_proc.stdout.read = AsyncMock(side_effect=[b"", b""])
            mock_proc.stderr = MagicMock()
            mock_proc.stderr.read = AsyncMock(return_value=b"tar error")
            mock_proc.wait = AsyncMock()

            with patch(
                "asyncio.create_subprocess_exec",
                AsyncMock(return_value=mock_proc),
            ):
                with pytest.raises(ArchiveError) as exc_info:
                    await create_archive(source_dir)

                assert "tar failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_create_archive_compression_fails(self):
        """Test archive creation fails when compression fails."""
        with tempfile.TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir) / "source"
            source_dir.mkdir()
            (source_dir / "file.txt").write_text("content")

            mock_proc = MagicMock()
            mock_proc.returncode = 0
            mock_proc.stdout = MagicMock()
            mock_proc.stdout.read = AsyncMock(side_effect=[b"tar data", b""])
            mock_proc.stderr = MagicMock()
            mock_proc.stderr.read = AsyncMock(return_value=b"")
            mock_proc.wait = AsyncMock()

            with (
                patch(
                    "asyncio.create_subprocess_exec", AsyncMock(return_value=mock_proc)
                ),
                patch(
                    "zstandard.ZstdCompressor.stream_writer",
                    side_effect=Exception("Compression error"),
                ),
                pytest.raises(ArchiveError) as exc_info,
            ):
                await create_archive(source_dir)

            assert "Archive creation failed" in str(exc_info.value)
