"""Tests for restore CLI: logging, dispatch, execute, run."""

from argparse import Namespace
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ami.dataops.backup.restore.cli import RestoreCLI

MINIMUM_SUCCESS_LOG_CALL_COUNT = 3


class TestRestoreCLISetupLogging:
    """Tests for RestoreCLI._setup_logging method."""

    @patch("ami.dataops.backup.restore.cli.logger")
    def test_configures_logging(self, mock_logger) -> None:
        """Test configures logging."""
        cli = RestoreCLI()
        cli._setup_logging(verbose=False)

        mock_logger.remove.assert_called_once()
        mock_logger.add.assert_called_once()

    @patch("ami.dataops.backup.restore.cli.logger")
    def test_verbose_mode(self, mock_logger) -> None:
        """Test verbose mode sets DEBUG level."""
        cli = RestoreCLI()
        cli._setup_logging(verbose=True)

        # Check that add was called with DEBUG level
        call_args = mock_logger.add.call_args
        assert call_args[1]["level"] == "DEBUG"


class TestRestoreCLIRestoreFromFileId:
    """Tests for RestoreCLI._restore_from_file_id method."""

    @pytest.mark.asyncio
    async def test_selective_restore_with_paths(self, tmp_path: Path) -> None:
        """Test selective restore when paths provided."""
        service = MagicMock()
        service.selective_restore_from_drive_by_file_id = AsyncMock(return_value=True)
        config = MagicMock()

        args = Namespace(file_id="file123", paths=[Path("config/")])

        cli = RestoreCLI(service=service)
        result = await cli._restore_from_file_id(args, tmp_path, config)

        assert result is True
        service.selective_restore_from_drive_by_file_id.assert_called_once()

    @pytest.mark.asyncio
    async def test_full_restore_without_paths(self, tmp_path: Path) -> None:
        """Test full restore when no paths provided."""
        service = MagicMock()
        service.restore_from_drive_by_file_id = AsyncMock(return_value=True)
        config = MagicMock()

        args = Namespace(file_id="file123", paths=[])

        cli = RestoreCLI(service=service)
        result = await cli._restore_from_file_id(args, tmp_path, config)

        assert result is True
        service.restore_from_drive_by_file_id.assert_called_once()


class TestRestoreCLIRestoreFromLocalPath:
    """Tests for RestoreCLI._restore_from_local_path method."""

    @pytest.mark.asyncio
    async def test_selective_restore_with_paths(self, tmp_path: Path) -> None:
        """Test selective restore when paths provided."""
        service = MagicMock()
        service.selective_restore_local_backup = AsyncMock(return_value=True)

        args = Namespace(
            local_path=tmp_path / "backup.tar.zst", paths=[Path("config/")]
        )

        cli = RestoreCLI(service=service)
        result = await cli._restore_from_local_path(args, tmp_path)

        assert result is True
        service.selective_restore_local_backup.assert_called_once()

    @pytest.mark.asyncio
    async def test_full_restore_without_paths(self, tmp_path: Path) -> None:
        """Test full restore when no paths provided."""
        service = MagicMock()
        service.restore_local_backup = AsyncMock(return_value=True)

        args = Namespace(local_path=tmp_path / "backup.tar.zst", paths=[])

        cli = RestoreCLI(service=service)
        result = await cli._restore_from_local_path(args, tmp_path)

        assert result is True
        service.restore_local_backup.assert_called_once()


class TestRestoreCLIRestoreFromRevision:
    """Tests for RestoreCLI._restore_from_revision method."""

    @pytest.mark.asyncio
    async def test_selective_restore_with_paths(self, tmp_path: Path) -> None:
        """Test selective restore when paths provided."""
        service = MagicMock()
        service.selective_restore_from_drive_by_revision = AsyncMock(return_value=True)
        config = MagicMock()

        args = Namespace(revision=1, paths=[Path("config/")])

        cli = RestoreCLI(service=service)
        result = await cli._restore_from_revision(args, tmp_path, config)

        assert result is True
        service.selective_restore_from_drive_by_revision.assert_called_once()


class TestRestoreCLIExecuteRestore:
    """Tests for RestoreCLI._execute_restore method."""

    @pytest.mark.asyncio
    async def test_handles_latest_local(self, tmp_path: Path) -> None:
        """Test handles latest-local mode."""
        service = MagicMock()
        service.restore_latest_local = AsyncMock(return_value=True)
        config = MagicMock()

        args = Namespace(
            latest_local=True,
            interactive=False,
            file_id=None,
            local_path=None,
            revision=None,
            list_revisions=False,
            paths=[],
        )

        cli = RestoreCLI(service=service)
        result, handled = await cli._execute_restore(args, tmp_path, config)

        assert result is True
        assert handled is True

    @pytest.mark.asyncio
    async def test_warns_about_paths_with_latest_local(self, tmp_path: Path) -> None:
        """Test warns when paths provided with latest-local."""
        service = MagicMock()
        service.restore_latest_local = AsyncMock(return_value=True)
        config = MagicMock()

        args = Namespace(
            latest_local=True,
            interactive=False,
            file_id=None,
            local_path=None,
            revision=None,
            list_revisions=False,
            paths=[Path("config/")],
        )

        cli = RestoreCLI(service=service)
        _result, handled = await cli._execute_restore(args, tmp_path, config)

        assert handled is True

    @pytest.mark.asyncio
    async def test_returns_not_handled_when_no_source(self, tmp_path: Path) -> None:
        """Test returns not handled when no source specified."""
        service = MagicMock()
        config = MagicMock()

        args = Namespace(
            latest_local=False,
            interactive=False,
            file_id=None,
            local_path=None,
            revision=None,
            list_revisions=False,
            paths=[],
        )

        cli = RestoreCLI(service=service)
        result, handled = await cli._execute_restore(args, tmp_path, config)

        assert result is False
        assert handled is False


class TestRestoreCLILogSuccess:
    """Tests for RestoreCLI._log_success method."""

    @patch("ami.dataops.backup.restore.cli.logger")
    def test_logs_success(self, mock_logger, tmp_path: Path) -> None:
        """Test logs success message."""
        cli = RestoreCLI()
        cli._log_success(tmp_path, None)

        assert mock_logger.info.call_count >= MINIMUM_SUCCESS_LOG_CALL_COUNT

    @patch("ami.dataops.backup.restore.cli.logger")
    def test_logs_paths_when_provided(self, mock_logger, tmp_path: Path) -> None:
        """Test logs paths when provided."""
        cli = RestoreCLI()
        cli._log_success(tmp_path, [Path("config/")])

        # Check that paths were logged
        calls = [str(c) for c in mock_logger.info.call_args_list]
        assert any("config" in str(c) for c in calls)


class TestRestoreCLIRun:
    """Tests for RestoreCLI.run method."""

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.cli.BackupRestoreConfig.load")
    async def test_returns_error_on_invalid_path(
        self, mock_config, tmp_path: Path
    ) -> None:
        """Test returns 1 on invalid restore path."""
        service = MagicMock()
        service.validate_restore_path = AsyncMock(return_value=False)
        mock_config.return_value = MagicMock(restore_path=tmp_path)

        args = Namespace(
            verbose=False,
            config_path=tmp_path,
            restore_path=None,
            latest_local=False,
            interactive=False,
            file_id=None,
            local_path=None,
            revision=None,
            list_revisions=False,
            paths=[],
        )

        cli = RestoreCLI(service=service)
        result = await cli.run(args)

        assert result == 1

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.cli.BackupRestoreConfig.load")
    async def test_returns_success_on_restore(
        self, mock_config, tmp_path: Path
    ) -> None:
        """Test returns 0 on successful restore."""
        service = MagicMock()
        service.validate_restore_path = AsyncMock(return_value=True)
        service.restore_latest_local = AsyncMock(return_value=True)
        mock_config.return_value = MagicMock(restore_path=tmp_path)

        args = Namespace(
            verbose=False,
            config_path=tmp_path,
            restore_path=None,
            latest_local=True,
            interactive=False,
            file_id=None,
            local_path=None,
            revision=None,
            list_revisions=False,
            paths=[],
        )

        cli = RestoreCLI(service=service)
        result = await cli.run(args)

        assert result == 0

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.cli.BackupRestoreConfig.load")
    async def test_handles_keyboard_interrupt(
        self, mock_config, tmp_path: Path
    ) -> None:
        """Test handles KeyboardInterrupt."""
        service = MagicMock()
        service.validate_restore_path = AsyncMock(return_value=True)
        service.restore_latest_local = AsyncMock(side_effect=KeyboardInterrupt)
        mock_config.return_value = MagicMock(restore_path=tmp_path)

        args = Namespace(
            verbose=False,
            config_path=tmp_path,
            restore_path=None,
            latest_local=True,
            interactive=False,
            file_id=None,
            local_path=None,
            revision=None,
            list_revisions=False,
            paths=[],
        )

        cli = RestoreCLI(service=service)
        result = await cli.run(args)

        assert result == 1

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.restore.cli.BackupRestoreConfig.load")
    async def test_handles_exception(self, mock_config, tmp_path: Path) -> None:
        """Test handles general exception."""
        service = MagicMock()
        service.validate_restore_path = AsyncMock(return_value=True)
        service.restore_latest_local = AsyncMock(side_effect=Exception("Error"))
        mock_config.return_value = MagicMock(restore_path=tmp_path)

        args = Namespace(
            verbose=False,
            config_path=tmp_path,
            restore_path=None,
            latest_local=True,
            interactive=False,
            file_id=None,
            local_path=None,
            revision=None,
            list_revisions=False,
            paths=[],
        )

        cli = RestoreCLI(service=service)
        result = await cli.run(args)

        assert result == 1
