"""Unit tests for the backup service orchestration (create/service.py)."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.auth.exceptions import RefreshError
from googleapiclient.errors import HttpError

from ami.dataops.backup.backup_exceptions import BackupError, UploadError
from ami.dataops.backup.create.service import BackupOptions, BackupService


def _make_auth_upload_error() -> UploadError:
    """Create an UploadError with a RefreshError cause (simulates auth failure)."""
    cause = RefreshError("reauthentication needed")
    err = UploadError("Authentication required: reauthentication needed")
    err.__cause__ = cause
    return err


class TestBackupService:
    """Unit tests for the BackupService class."""

    def test_initialization(self):
        """Test that BackupService initializes with required services."""
        mock_uploader = MagicMock()
        mock_auth_manager = MagicMock()

        service = BackupService(uploader=mock_uploader, auth_manager=mock_auth_manager)

        assert service.uploader == mock_uploader
        assert service.auth_manager == mock_auth_manager

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.service.BackupConfig")
    @patch("ami.dataops.backup.create.service.create_zip_archive")
    @patch("ami.dataops.backup.create.service.copy_to_secondary_backup")
    @patch("ami.dataops.backup.create.service.cleanup_local_zip")
    @patch("pathlib.Path.cwd")
    async def test_run_backup_success(
        self,
        mock_cwd,
        mock_cleanup,
        mock_secondary,
        mock_archiver,
        mock_config_class,
    ):
        """Test successful backup run."""
        # Setup mocks
        mock_uploader = AsyncMock()
        mock_auth_manager = MagicMock()

        # Mock the config loading
        mock_config = MagicMock()
        mock_config_class.load.return_value = mock_config

        mock_cwd.return_value = Path("/tmp/test")

        # Mock archiver to return a path-like mock with stat()
        mock_zip = MagicMock()
        mock_zip.name = "backup.tar.zst"
        mock_zip.stat.return_value.st_size = 1024
        mock_archiver.return_value = mock_zip

        # Mock uploader to return a file ID
        mock_uploader.upload_to_gdrive.return_value = "test_file_id_123"

        # Mock secondary service and cleanup (they are awaited)
        mock_secondary.return_value = None
        mock_cleanup.return_value = None

        # Create service
        service = BackupService(uploader=mock_uploader, auth_manager=mock_auth_manager)

        # Create options
        options = BackupOptions(keep_local=False, retry_auth=True)

        # Run backup
        result = await service.run_backup(options)

        # Verify result
        assert result == "test_file_id_123"

        # Verify all steps were called
        mock_archiver.assert_called_once()
        mock_uploader.upload_to_gdrive.assert_called_once()
        mock_secondary.assert_called_once()
        mock_cleanup.assert_called_once()

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.service.BackupConfig")
    @patch("ami.dataops.backup.create.service.create_zip_archive")
    @patch("ami.dataops.backup.create.service.copy_to_secondary_backup")
    @patch("ami.dataops.backup.create.service.cleanup_local_zip")
    @patch("pathlib.Path.cwd")
    async def test_run_backup_auth_retry_success(
        self,
        mock_cwd,
        mock_cleanup,
        mock_secondary,
        mock_archiver,
        mock_config_class,
    ):
        """Test backup with auth retry that succeeds."""
        # Setup mocks
        mock_uploader = AsyncMock()
        mock_auth_manager = MagicMock()

        mock_zip = MagicMock()
        mock_zip.name = "backup.tar.zst"
        mock_zip.stat.return_value.st_size = 1024
        mock_archiver.return_value = mock_zip
        mock_secondary.return_value = None
        mock_cleanup.return_value = None

        # Mock the config loading
        mock_config = MagicMock()
        mock_config_class.load.return_value = mock_config

        mock_cwd.return_value = Path("/tmp/test")

        # First call raises UploadError with auth cause, second succeeds
        mock_uploader.upload_to_gdrive.side_effect = [
            _make_auth_upload_error(),
            "test_file_id_456",
        ]

        # Create service
        service = BackupService(uploader=mock_uploader, auth_manager=mock_auth_manager)

        # Mock _refresh_adc_credentials to return True
        service._refresh_adc_credentials = AsyncMock(return_value=True)

        # Create options
        options = BackupOptions(keep_local=False, retry_auth=True)

        result = await service.run_backup(options)
        # Should succeed after retry
        assert result == "test_file_id_456"
        assert service._refresh_adc_credentials.called

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.service.BackupConfig")
    @patch("ami.dataops.backup.create.service.create_zip_archive")
    @patch("ami.dataops.backup.create.service.copy_to_secondary_backup")
    @patch("ami.dataops.backup.create.service.cleanup_local_zip")
    @patch("pathlib.Path.cwd")
    async def test_run_backup_upload_error_no_retry(
        self,
        mock_cwd,
        mock_cleanup,
        mock_secondary,
        mock_archiver,
        mock_config_class,
    ):
        """Test backup with upload error when retry is disabled."""
        # Setup mocks
        mock_uploader = AsyncMock()
        mock_auth_manager = MagicMock()

        mock_zip = MagicMock()
        mock_zip.name = "backup.tar.zst"
        mock_zip.stat.return_value.st_size = 1024
        mock_archiver.return_value = mock_zip
        mock_secondary.return_value = None
        mock_cleanup.return_value = None

        mock_config = MagicMock()
        mock_config_class.load.return_value = mock_config
        mock_cwd.return_value = Path("/tmp/test")

        # Mock uploader to raise UploadError
        mock_uploader.upload_to_gdrive.side_effect = UploadError("Upload failed")

        service = BackupService(uploader=mock_uploader, auth_manager=mock_auth_manager)

        # Create options with retry_auth=False
        options = BackupOptions(keep_local=False, retry_auth=False)

        # Should raise UploadError when retry_auth=False
        with pytest.raises(UploadError):
            await service.run_backup(options)

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.service.BackupConfig")
    @patch("ami.dataops.backup.create.service.create_zip_archive")
    @patch("pathlib.Path.cwd")
    async def test_run_backup_fails_fast_on_bad_credentials(
        self,
        mock_cwd,
        mock_archiver,
        mock_config_class,
    ):
        """Test backup fails fast on bad credentials."""
        mock_uploader = AsyncMock()
        mock_auth_manager = MagicMock()
        mock_auth_manager.get_credentials.side_effect = RefreshError("token expired")

        mock_config = MagicMock()
        mock_config_class.load.return_value = mock_config
        mock_cwd.return_value = Path("/tmp/test")

        service = BackupService(uploader=mock_uploader, auth_manager=mock_auth_manager)
        options = BackupOptions(keep_local=False, retry_auth=True)

        with pytest.raises(BackupError, match="Credential check failed"):
            await service.run_backup(options)

        # Archive should never have been created
        mock_archiver.assert_not_called()

    def test_is_auth_error_refresh_error(self):
        """Test _is_auth_error detects RefreshError in cause chain."""
        service = BackupService(MagicMock(), MagicMock())
        error = UploadError("Upload failed")
        error.__cause__ = RefreshError("reauthentication required")

        assert service._is_auth_error(error) is True

    def test_is_auth_error_http_401(self):
        """Test _is_auth_error detects HTTP 401 errors."""

        service = BackupService(MagicMock(), MagicMock())
        http_err = HttpError(MagicMock(status=401), b"Unauthorized")
        error = UploadError("Upload failed")
        error.__cause__ = http_err

        assert service._is_auth_error(error) is True

    def test_is_auth_error_http_403(self):
        """Test _is_auth_error detects HTTP 403 errors."""

        service = BackupService(MagicMock(), MagicMock())
        http_err = HttpError(MagicMock(status=403), b"Forbidden")
        error = UploadError("Upload failed")
        error.__cause__ = http_err

        assert service._is_auth_error(error) is True

    def test_is_auth_error_nested_cause(self):
        """Test _is_auth_error walks nested __cause__ chain."""
        service = BackupService(MagicMock(), MagicMock())
        inner = RefreshError("invalid_grant")
        middle = RuntimeError("wrapped")
        middle.__cause__ = inner
        error = UploadError("Upload failed")
        error.__cause__ = middle

        assert service._is_auth_error(error) is True

    def test_is_auth_error_other_error(self):
        """Test _is_auth_error returns False for non-auth errors."""
        service = BackupService(MagicMock(), MagicMock())
        error = UploadError("Network timeout")

        assert service._is_auth_error(error) is False

    def test_is_auth_error_http_500_not_auth(self):
        """Test _is_auth_error returns False for HTTP 500."""

        service = BackupService(MagicMock(), MagicMock())
        http_err = HttpError(MagicMock(status=500), b"Server Error")
        error = UploadError("Upload failed")
        error.__cause__ = http_err

        assert service._is_auth_error(error) is False

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.service.find_gcloud")
    async def test_setup_auth_no_gcloud(self, mock_find_gcloud):
        """Test setup_auth returns 1 when gcloud not found."""
        mock_find_gcloud.return_value = None

        service = BackupService(MagicMock(), MagicMock())
        result = await service.setup_auth()

        assert result == 1

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.service.asyncio.create_subprocess_exec")
    @patch("ami.dataops.backup.create.service.find_gcloud")
    async def test_setup_auth_success(self, mock_find_gcloud, mock_subprocess):
        """Test setup_auth succeeds when gcloud auth succeeds."""
        mock_find_gcloud.return_value = "/usr/bin/gcloud"

        mock_process = AsyncMock()
        mock_process.returncode = 0
        mock_process.communicate.return_value = (b"", b"")
        mock_subprocess.return_value = mock_process

        service = BackupService(MagicMock(), MagicMock())
        result = await service.setup_auth()

        assert result == 0

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.service.asyncio.create_subprocess_exec")
    @patch("ami.dataops.backup.create.service.find_gcloud")
    async def test_setup_auth_failure(self, mock_find_gcloud, mock_subprocess):
        """Test setup_auth returns error code when gcloud auth fails."""
        mock_find_gcloud.return_value = "/usr/bin/gcloud"

        mock_process = AsyncMock()
        mock_process.returncode = 1
        mock_process.communicate.return_value = (b"", b"Auth failed")
        mock_subprocess.return_value = mock_process

        service = BackupService(MagicMock(), MagicMock())
        result = await service.setup_auth()

        assert result == 1

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.service.asyncio.create_subprocess_exec")
    @patch("ami.dataops.backup.create.service.find_gcloud")
    async def test_setup_auth_exception(self, mock_find_gcloud, mock_subprocess):
        """Test setup_auth returns 1 on exception."""
        mock_find_gcloud.return_value = "/usr/bin/gcloud"
        mock_subprocess.side_effect = Exception("Unexpected error")

        service = BackupService(MagicMock(), MagicMock())
        result = await service.setup_auth()

        assert result == 1

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.service.find_gcloud")
    async def test_refresh_adc_credentials_no_gcloud(self, mock_find_gcloud):
        """Test _refresh_adc_credentials returns False when no gcloud."""
        mock_find_gcloud.return_value = None

        service = BackupService(MagicMock(), MagicMock())
        result = await service._refresh_adc_credentials()

        assert result is False

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.service.Path")
    @patch("ami.dataops.backup.create.service.find_gcloud")
    async def test_refresh_adc_credentials_no_adc_file(
        self, mock_find_gcloud, mock_path
    ):
        """Test _refresh_adc_credentials returns False when no ADC file."""
        mock_find_gcloud.return_value = "/usr/bin/gcloud"
        mock_path.home.return_value.__truediv__.return_value.exists.return_value = False

        service = BackupService(MagicMock(), MagicMock())
        result = await service._refresh_adc_credentials()

        assert result is False

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.service.Path")
    @patch("ami.dataops.backup.create.service.find_gcloud")
    async def test_refresh_adc_credentials_timeout(self, mock_find_gcloud, mock_path):
        """Test _refresh_adc_credentials returns False on timeout."""
        mock_find_gcloud.return_value = "/usr/bin/gcloud"
        mock_path.home.return_value.__truediv__.return_value.exists.return_value = True

        service = BackupService(MagicMock(), MagicMock())
        service._check_and_refresh_token = AsyncMock(side_effect=TimeoutError())

        result = await service._refresh_adc_credentials()

        assert result is False

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.service.Path")
    @patch("ami.dataops.backup.create.service.find_gcloud")
    async def test_refresh_adc_credentials_exception(self, mock_find_gcloud, mock_path):
        """Test _refresh_adc_credentials returns False on exception."""
        mock_find_gcloud.return_value = "/usr/bin/gcloud"
        mock_path.home.return_value.__truediv__.return_value.exists.return_value = True

        service = BackupService(MagicMock(), MagicMock())
        service._check_and_refresh_token = AsyncMock(side_effect=Exception("Error"))

        result = await service._refresh_adc_credentials()

        assert result is False

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.service.asyncio.create_subprocess_exec")
    async def test_check_and_refresh_token_valid(self, mock_subprocess):
        """Test _check_and_refresh_token returns True when token valid."""
        mock_process = AsyncMock()
        mock_process.returncode = 0
        mock_process.communicate = AsyncMock(return_value=(b"token", b""))
        mock_subprocess.return_value = mock_process

        service = BackupService(MagicMock(), MagicMock())
        result = await service._check_and_refresh_token("/usr/bin/gcloud")

        assert result is True

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.service.asyncio.wait_for")
    @patch("ami.dataops.backup.create.service.asyncio.create_subprocess_exec")
    async def test_check_and_refresh_token_timeout(
        self, mock_subprocess, mock_wait_for
    ):
        """Test _check_and_refresh_token handles timeout."""
        mock_process = AsyncMock()
        mock_process.kill = MagicMock()
        mock_subprocess.return_value = mock_process
        mock_wait_for.side_effect = TimeoutError()

        service = BackupService(MagicMock(), MagicMock())

        with pytest.raises(TimeoutError):
            await service._check_and_refresh_token("/usr/bin/gcloud")

        mock_process.kill.assert_called_once()

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.service.asyncio.wait_for")
    @patch("ami.dataops.backup.create.service.asyncio.create_subprocess_exec")
    async def test_check_and_refresh_token_invalid_then_refresh(
        self, mock_subprocess, mock_wait_for
    ):
        """Test _check_and_refresh_token refreshes invalid token."""
        mock_process = AsyncMock()
        mock_process.returncode = 1
        mock_wait_for.return_value = (b"", b"Token expired")
        mock_subprocess.return_value = mock_process

        service = BackupService(MagicMock(), MagicMock())
        service._run_gcloud_login = AsyncMock(return_value=True)

        result = await service._check_and_refresh_token("/usr/bin/gcloud")

        assert result is True
        service._run_gcloud_login.assert_called_once_with("/usr/bin/gcloud")

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.service.asyncio.wait_for")
    @patch("ami.dataops.backup.create.service.asyncio.create_subprocess_exec")
    async def test_run_gcloud_login_success(self, mock_subprocess, mock_wait_for):
        """Test _run_gcloud_login returns True on success."""
        mock_process = AsyncMock()
        mock_process.returncode = 0
        mock_wait_for.return_value = (b"", b"")
        mock_subprocess.return_value = mock_process

        service = BackupService(MagicMock(), MagicMock())
        result = await service._run_gcloud_login("/usr/bin/gcloud")

        assert result is True

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.service.asyncio.wait_for")
    @patch("ami.dataops.backup.create.service.asyncio.create_subprocess_exec")
    async def test_run_gcloud_login_failure(self, mock_subprocess, mock_wait_for):
        """Test _run_gcloud_login returns False on failure."""
        mock_process = AsyncMock()
        mock_process.returncode = 1
        mock_wait_for.return_value = (b"", b"Login failed")
        mock_subprocess.return_value = mock_process

        service = BackupService(MagicMock(), MagicMock())
        result = await service._run_gcloud_login("/usr/bin/gcloud")

        assert result is False

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.service.asyncio.wait_for")
    @patch("ami.dataops.backup.create.service.asyncio.create_subprocess_exec")
    async def test_run_gcloud_login_timeout(self, mock_subprocess, mock_wait_for):
        """Test _run_gcloud_login returns False on timeout."""
        mock_process = AsyncMock()
        mock_process.kill = MagicMock()
        mock_wait_for.side_effect = TimeoutError()
        mock_subprocess.return_value = mock_process

        service = BackupService(MagicMock(), MagicMock())
        result = await service._run_gcloud_login("/usr/bin/gcloud")

        assert result is False
        mock_process.kill.assert_called_once()


class TestBackupOptions:
    """Tests for BackupOptions model."""

    def test_default_values(self):
        """Test BackupOptions has correct defaults."""
        options = BackupOptions()

        assert options.keep_local is False
        assert options.retry_auth is True
        assert options.source_dir is None
        assert options.output_filename is None
        assert options.ignore_exclusions is False
        assert options.config_path is None

    def test_custom_values(self):
        """Test BackupOptions accepts custom values."""
        options = BackupOptions(
            keep_local=True,
            retry_auth=False,
            source_dir=Path("/custom/source"),
            output_filename="custom-backup",
            ignore_exclusions=True,
            config_path=Path("/custom/config"),
        )
        assert options.keep_local is True
        assert options.retry_auth is False
        assert options.source_dir == Path("/custom/source")
        assert options.output_filename == "custom-backup"
        assert options.ignore_exclusions is True
        assert options.config_path == Path("/custom/config")
