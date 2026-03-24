"""Unit tests for the backup archiver module (create/archiver.py)."""

import tempfile
from pathlib import Path

import pytest

from ami.dataops.backup.create import archiver


class TestArchiver:
    """Unit tests for the archiver functions."""

    def test_should_exclude_path_git_directory(self):
        """Test that git directories are excluded."""
        root_dir = "/project"
        git_path = "/project/.git"
        assert archiver._should_exclude_path(git_path, root_dir) is True

    def test_should_exclude_path_subdir_venv(self):
        """Test that .venv in subdirectories are INCLUDED (no longer excluded)."""
        root_dir = "/project"
        sub_venv_path = "/project/subdir/.venv"
        assert archiver._should_exclude_path(sub_venv_path, root_dir) is False

    def test_should_include_root_venv(self):
        """Test that .venv at root is NOT excluded (only subdirectories)."""
        root_dir = "/project"
        root_venv_path = "/project/.venv"
        assert archiver._should_exclude_path(root_venv_path, root_dir) is False

    def test_should_exclude_path_node_modules(self):
        """Test that node_modules directories are INCLUDED (no longer excluded)."""
        root_dir = "/project"
        node_modules_path = "/project/node_modules"
        assert archiver._should_exclude_path(node_modules_path, root_dir) is False

    def test_should_exclude_path_pycache(self):
        """Test that __pycache__ directories are excluded."""
        root_dir = "/project"
        pycache_path = "/project/__pycache__"
        assert archiver._should_exclude_path(pycache_path, root_dir) is True

    def test_should_exclude_path_pyc_files(self):
        """Test that .pyc files are excluded."""
        root_dir = "/project"
        pyc_path = "/project/file.pyc"
        assert archiver._should_exclude_path(pyc_path, root_dir) is True

    def test_should_exclude_path_outside_root(self):
        """Test that paths outside root are NOT excluded by this function.

        Note: The _should_exclude_path function only checks exclusion patterns,
        not whether a path is within root. Path containment is handled elsewhere.
        """
        root_dir = "/project"
        # This path doesn't match any exclusion patterns, so it's not excluded
        other_path = "/other/file.txt"
        # The function doesn't check if path is within root, only pattern matching
        assert archiver._should_exclude_path(other_path, root_dir) is False

    def test_should_include_normal_file(self):
        """Test that normal files within root are included."""
        root_dir = "/project"
        normal_path = "/project/normal_file.txt"
        assert archiver._should_exclude_path(normal_path, root_dir) is False

    def test_illegal_filename_filtering(self):
        """Test that filenames with control characters are identified as illegal."""
        assert archiver._is_illegal_filename("normal.txt") is False
        assert archiver._is_illegal_filename("file\033.txt") is True
        assert archiver._is_illegal_filename("file\nname.txt") is True
        assert archiver._is_illegal_filename("file\r.txt") is True

    @pytest.mark.asyncio
    async def test_create_zip_archive_success(self):
        """Test successful archive creation."""
        # Use a real source directory from temp
        with tempfile.TemporaryDirectory() as source_dir:
            root = Path(source_dir)
            (root / "file1.txt").write_text("test content")

            # Actually create the archive
            result = await archiver.create_zip_archive(root)

            assert result.exists()
            assert ".tar.zst" in str(result)

            # Clean up
            result.unlink()

    def test_complete_exclusion_logic(self):
        """Test the complete exclusion logic with various path types."""
        root_dir = "/project"

        # Test cases: (path, should_be_excluded)
        # Note: The function only checks exclusion patterns, not path containment
        test_cases = [
            # Should be excluded (matches exclusion patterns)
            ("/project/.git", True),
            ("/project/sub/.git", True),
            ("/project/__pycache__", True),
            ("/project/file.pyc", True),
            # Should NOT be excluded
            ("/project/.venv", False),  # Root .venv should NOT be excluded
            ("/project/sub/.venv", False),  # Subdir .venv should now be INCLUDED
            ("/project/node_modules", False),  # node_modules should now be INCLUDED
            ("/project/normal_file.txt", False),
            ("/project/subdir/normal_file.txt", False),
            ("/project/subdir/.venv/file.txt", False),  # .venv subdirs INCLUDED
        ]

        for test_path, expected_exclusion in test_cases:
            result = archiver._should_exclude_path(test_path, root_dir)
            assert result == expected_exclusion, (
                f"Path {test_path} exclusion check failed."
                f" Expected {expected_exclusion}, got {result}"
            )
