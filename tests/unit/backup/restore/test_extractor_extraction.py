"""Unit tests for archive extraction functions (extract, pipeline, list)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ami.dataops.backup.backup_exceptions import ArchiveError
from ami.dataops.backup.restore import extractor

EXPECTED_UNPRUNED_PATH_COUNT = 3
EXPECTED_DEDUPLICATED_PATH_COUNT = 2


class TestArchiveExtractorAsync:
    """Tests for async extraction and listing functions."""

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.extractor.asyncio.get_event_loop")
    async def test_extract_specific_paths_executor_call(self, mock_loop):
        """Test that extract_specific_paths calls run_in_executor."""
        loop_instance = MagicMock()
        mock_loop.return_value = loop_instance

        # Make run_in_executor return a coroutine
        async def mock_run_in_executor(*args):
            return True

        loop_instance.run_in_executor.return_value = mock_run_in_executor()

        archive_path = Path("/tmp/backup.tar.zst")
        paths = [Path("test.txt")]
        dest = Path("/tmp/restore")

        await extractor.extract_specific_paths(archive_path, paths, dest)

        loop_instance.run_in_executor.assert_called_once()

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.extractor.asyncio.get_event_loop")
    async def test_list_archive_contents_calls_executor(self, mock_loop):
        """Test that list_archive_contents calls run_in_executor."""
        loop_instance = MagicMock()
        mock_loop.return_value = loop_instance

        async def mock_run_in_executor(*args):
            return ["file1.txt", "dir/file2.txt"]

        loop_instance.run_in_executor.return_value = mock_run_in_executor()

        archive_path = Path("/tmp/backup.tar.zst")
        result = await extractor.list_archive_contents(archive_path)

        assert "file1.txt" in result
        loop_instance.run_in_executor.assert_called_once()


class TestExtractSpecificPathsSync:
    """Tests for synchronous extraction logic."""

    @patch("ami.dataops.backup.restore.extractor._run_extraction_pipeline")
    @patch("ami.dataops.backup.restore.extractor._get_zstd_binary")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.mkdir")
    def test_extract_specific_paths_sync_logic(
        self,
        mock_mkdir,
        mock_exists,
        mock_get_zstd,
        mock_pipeline,
    ):
        """Test the synchronous extraction uses correct zstd | tar pipeline."""
        mock_exists.return_value = True
        mock_get_zstd.return_value = Path("/usr/bin/zstd")

        archive_path = Path("/tmp/backup.tar.zst")
        paths = [Path("test.txt")]
        dest = Path("/tmp/restore")

        result = extractor._extract_specific_paths_sync(archive_path, paths, dest)

        assert result is True
        mock_pipeline.assert_called_once()
        # Verify the pipeline was called with correct args
        call_args = mock_pipeline.call_args[0]
        called_archive_path, called_zstd_bin, tar_cmd = call_args
        assert called_archive_path == archive_path
        assert "/usr/bin/zstd" in called_zstd_bin
        assert "tar" in tar_cmd[0]
        assert str(dest) in tar_cmd

    @patch("pathlib.Path.exists")
    def test_extract_specific_paths_sync_archive_not_exists(self, mock_exists):
        """Test extraction fails when archive doesn't exist."""

        mock_exists.return_value = False

        with pytest.raises(ArchiveError) as exc_info:
            extractor._extract_specific_paths_sync(
                Path("/tmp/missing.tar.zst"),
                [Path("test.txt")],
                Path("/tmp/dest"),
            )

        assert "does not exist" in str(exc_info.value)

    @patch("ami.dataops.backup.restore.extractor._run_extraction_pipeline")
    @patch("ami.dataops.backup.restore.extractor._get_zstd_binary")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.mkdir")
    def test_extract_specific_paths_sync_all_files(
        self, mock_mkdir, mock_exists, mock_get_zstd, mock_pipeline
    ):
        """Test extraction of all files when paths is None."""
        mock_exists.return_value = True
        mock_get_zstd.return_value = Path("/usr/bin/zstd")

        result = extractor._extract_specific_paths_sync(
            Path("/tmp/backup.tar.zst"),
            None,  # Extract all
            Path("/tmp/dest"),
        )

        assert result is True
        call_args = mock_pipeline.call_args[0]
        _archive_path, _zstd_bin, tar_cmd = call_args
        # When paths is None, tar_cmd should NOT have specific paths appended
        assert tar_cmd == ["tar", "-xf", "-", "-C", "/tmp/dest"]

    @patch("ami.dataops.backup.restore.extractor._run_extraction_pipeline")
    @patch("ami.dataops.backup.restore.extractor._get_zstd_binary")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.mkdir")
    def test_extract_specific_paths_sync_exception(
        self, mock_mkdir, mock_exists, mock_get_zstd, mock_pipeline
    ):
        """Test extraction wraps exceptions in ArchiveError."""

        mock_exists.return_value = True
        mock_get_zstd.return_value = Path("/usr/bin/zstd")
        mock_pipeline.side_effect = Exception("Unexpected error")

        with pytest.raises(ArchiveError) as exc_info:
            extractor._extract_specific_paths_sync(
                Path("/tmp/backup.tar.zst"),
                [Path("test.txt")],
                Path("/tmp/dest"),
            )

        assert "Failed to extract" in str(exc_info.value)


class TestExtractionPipeline:
    """Tests for _run_extraction_pipeline."""

    @patch("ami.dataops.backup.restore.extractor.subprocess.Popen")
    def test_run_extraction_pipeline_success(self, mock_popen, tmp_path):
        """Test successful extraction pipeline."""
        # Create a fake archive file
        archive = tmp_path / "archive.tar.zst"
        archive.write_bytes(b"fake archive data")

        # Setup zstd process mock
        mock_zstd_proc = MagicMock()
        mock_zstd_proc.stdout = MagicMock()
        mock_zstd_proc.stdin = MagicMock()
        mock_zstd_proc.communicate.return_value = (b"", b"")
        mock_zstd_proc.returncode = 0

        # Setup tar process mock
        mock_tar_proc = MagicMock()
        mock_tar_proc.communicate.return_value = (b"", b"")
        mock_tar_proc.returncode = 0

        mock_popen.side_effect = [mock_zstd_proc, mock_tar_proc]

        tar_cmd = ["tar", "-xf", "-", "-C", "/tmp/dest"]

        # Should not raise
        extractor._run_extraction_pipeline(archive, "/usr/bin/zstd", tar_cmd)

        # Verify zstd was called with multi-core flag
        zstd_call_args = mock_popen.call_args_list[0][0][0]
        assert "-T0" in zstd_call_args

    @patch("ami.dataops.backup.restore.extractor.subprocess.Popen")
    def test_run_extraction_pipeline_zstd_fails(self, mock_popen, tmp_path):
        """Test extraction pipeline when zstd fails."""
        archive = tmp_path / "archive.tar.zst"
        archive.write_bytes(b"fake archive data")

        mock_zstd_proc = MagicMock()
        mock_zstd_proc.stdout = MagicMock()
        mock_zstd_proc.stdin = MagicMock()
        mock_zstd_proc.communicate.return_value = (b"", b"Decompression failed")
        mock_zstd_proc.returncode = 1

        mock_tar_proc = MagicMock()
        mock_tar_proc.communicate.return_value = (b"", b"")
        mock_tar_proc.returncode = 0

        mock_popen.side_effect = [mock_zstd_proc, mock_tar_proc]

        tar_cmd = ["tar", "-xf", "-", "-C", "/tmp/dest"]

        with pytest.raises(ArchiveError) as exc_info:
            extractor._run_extraction_pipeline(archive, "/usr/bin/zstd", tar_cmd)

        assert "Decompression failed" in str(exc_info.value)

    @patch("ami.dataops.backup.restore.extractor.subprocess.Popen")
    def test_run_extraction_pipeline_tar_fails_with_error(self, mock_popen, tmp_path):
        """Test extraction pipeline when tar fails with error."""
        archive = tmp_path / "archive.tar.zst"
        archive.write_bytes(b"fake archive data")

        mock_zstd_proc = MagicMock()
        mock_zstd_proc.stdout = MagicMock()
        mock_zstd_proc.stdin = MagicMock()
        mock_zstd_proc.communicate.return_value = (b"", b"")
        mock_zstd_proc.returncode = 0

        mock_tar_proc = MagicMock()
        mock_tar_proc.communicate.return_value = (b"", b"error: extraction failed")
        mock_tar_proc.returncode = 1

        mock_popen.side_effect = [mock_zstd_proc, mock_tar_proc]

        tar_cmd = ["tar", "-xf", "-", "-C", "/tmp/dest"]

        with pytest.raises(ArchiveError) as exc_info:
            extractor._run_extraction_pipeline(archive, "/usr/bin/zstd", tar_cmd)

        assert "Extraction failed" in str(exc_info.value)

    @patch("ami.dataops.backup.restore.extractor.subprocess.Popen")
    def test_run_extraction_pipeline_tar_warns(self, mock_popen, tmp_path):
        """Test extraction pipeline when tar has warnings but succeeds."""
        archive = tmp_path / "archive.tar.zst"
        archive.write_bytes(b"fake archive data")

        mock_zstd_proc = MagicMock()
        mock_zstd_proc.stdout = MagicMock()
        mock_zstd_proc.stdin = MagicMock()
        mock_zstd_proc.communicate.return_value = (b"", b"")
        mock_zstd_proc.returncode = 0

        mock_tar_proc = MagicMock()
        mock_tar_proc.communicate.return_value = (
            b"",
            b"tar: Removing leading / from paths",
        )
        mock_tar_proc.returncode = 1  # Non-zero but just warnings

        mock_popen.side_effect = [mock_zstd_proc, mock_tar_proc]

        tar_cmd = ["tar", "-xf", "-", "-C", "/tmp/dest"]

        # Should not raise for warnings only
        extractor._run_extraction_pipeline(archive, "/usr/bin/zstd", tar_cmd)

    def test_raise_decompression_error(self):
        """Test _raise_decompression_error raises ArchiveError."""

        with pytest.raises(ArchiveError) as exc_info:
            extractor._raise_decompression_error("bad data")

        assert "Decompression failed" in str(exc_info.value)
        assert "bad data" in str(exc_info.value)

    def test_raise_extraction_error(self):
        """Test _raise_extraction_error raises ArchiveError."""

        with pytest.raises(ArchiveError) as exc_info:
            extractor._raise_extraction_error("cannot write")

        assert "Extraction failed" in str(exc_info.value)
        assert "cannot write" in str(exc_info.value)


class TestZstdBinary:
    """Tests for _get_zstd_binary."""

    def test_get_zstd_binary_returns_path(self):
        """Test that _get_zstd_binary returns a Path object."""
        result = extractor._get_zstd_binary()
        assert isinstance(result, Path)


class TestPruneChildPaths:
    """Tests for _prune_child_paths."""

    def test_prune_child_paths(self):
        """Test that child paths are removed when parent is present."""
        paths = [
            Path("parent"),
            Path("parent/child"),
            Path("parent/child/grandchild"),
            Path("other"),
        ]

        result = extractor._prune_child_paths(paths)

        assert Path("parent") in result
        assert Path("other") in result
        assert Path("parent/child") not in result
        assert Path("parent/child/grandchild") not in result

    def test_prune_child_paths_no_children(self):
        """Test prune with no overlapping paths."""
        paths = [Path("a"), Path("b"), Path("c")]
        result = extractor._prune_child_paths(paths)

        assert len(result) == EXPECTED_UNPRUNED_PATH_COUNT
        assert Path("a") in result
        assert Path("b") in result
        assert Path("c") in result

    def test_prune_child_paths_duplicates(self):
        """Test prune removes duplicates."""
        paths = [Path("a"), Path("a"), Path("b")]
        result = extractor._prune_child_paths(paths)

        assert len(result) == EXPECTED_DEDUPLICATED_PATH_COUNT
        assert result.count(Path("a")) == 1
