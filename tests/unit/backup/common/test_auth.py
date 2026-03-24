"""Unit tests for backup/common/auth module."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ami.dataops.backup.backup_config import BackupConfig
from ami.dataops.backup.backup_exceptions import BackupConfigError, BackupError
from ami.dataops.backup.common.auth import (
    AuthenticationManager,
    ImpersonationCredentialsProvider,
    OAuthCredentialsProvider,
    ServiceAccountCredentialsProvider,
)


class TestImpersonationCredentialsProvider:
    """Tests for ImpersonationCredentialsProvider class."""

    def test_raises_error_when_no_service_account(self) -> None:
        """Test raises error when service account email not set."""
        config = MagicMock(spec=BackupConfig)
        config.service_account_email = None

        provider = ImpersonationCredentialsProvider(config)

        with pytest.raises(BackupConfigError) as exc_info:
            provider.get_credentials()

        assert "GDRIVE_SERVICE_ACCOUNT_EMAIL is not set" in str(exc_info.value)

    @patch("ami.dataops.backup.common.auth.Request")
    @patch("ami.dataops.backup.common.auth.impersonated_credentials.Credentials")
    @patch("ami.dataops.backup.common.auth.google.auth.default")
    def test_gets_impersonated_credentials(
        self, mock_default, mock_impersonated, mock_request
    ) -> None:
        """Test gets impersonated credentials successfully."""
        config = MagicMock(spec=BackupConfig)
        config.service_account_email = "test@project.iam.gserviceaccount.com"

        mock_source_creds = MagicMock()
        mock_default.return_value = (mock_source_creds, "project")

        mock_creds = MagicMock()
        mock_impersonated.return_value = mock_creds

        provider = ImpersonationCredentialsProvider(config)
        result = provider.get_credentials()

        assert result == mock_creds
        mock_impersonated.assert_called_once()
        mock_creds.refresh.assert_called_once()

    @patch("ami.dataops.backup.common.auth.google.auth.default")
    def test_raises_backup_error_on_failure(self, mock_default) -> None:
        """Test raises BackupError on impersonation failure."""
        config = MagicMock(spec=BackupConfig)
        config.service_account_email = "test@project.iam.gserviceaccount.com"

        mock_default.side_effect = Exception("Auth failed")

        provider = ImpersonationCredentialsProvider(config)

        with pytest.raises(BackupError) as exc_info:
            provider.get_credentials()

        assert "Impersonation failed" in str(exc_info.value)


class TestServiceAccountCredentialsProvider:
    """Tests for ServiceAccountCredentialsProvider class."""

    def test_raises_error_when_no_credentials_file(self) -> None:
        """Test raises error when credentials file not set."""
        config = MagicMock(spec=BackupConfig)
        config.credentials_file = None

        provider = ServiceAccountCredentialsProvider(config)

        with pytest.raises(BackupConfigError) as exc_info:
            provider.get_credentials()

        assert "GDRIVE_CREDENTIALS_FILE is not set" in str(exc_info.value)

    def test_raises_error_when_file_not_exists(self, tmp_path: Path) -> None:
        """Test raises error when credentials file doesn't exist."""
        config = MagicMock(spec=BackupConfig)
        config.credentials_file = str(tmp_path / "nonexistent.json")

        provider = ServiceAccountCredentialsProvider(config)

        with pytest.raises(BackupError) as exc_info:
            provider.get_credentials()

        assert "Service account key file not found" in str(exc_info.value)

    @patch(
        "ami.dataops.backup.common.auth.ServiceAccountCredentials.from_service_account_file"
    )
    def test_loads_credentials_from_file(self, mock_from_file, tmp_path: Path) -> None:
        """Test loads credentials from service account file."""
        creds_file = tmp_path / "credentials.json"
        creds_file.write_text("{}")

        config = MagicMock(spec=BackupConfig)
        config.credentials_file = str(creds_file)

        mock_creds = MagicMock()
        mock_from_file.return_value = mock_creds

        provider = ServiceAccountCredentialsProvider(config)
        result = provider.get_credentials()

        assert result == mock_creds
        mock_from_file.assert_called_once()


class TestOAuthCredentialsProvider:
    """Tests for OAuthCredentialsProvider class."""

    @patch("ami.dataops.backup.common.auth.pickle.load")
    def test_loads_existing_valid_token(self, mock_pickle_load, tmp_path: Path) -> None:
        """Test loads existing valid token from pickle file."""
        config = MagicMock(spec=BackupConfig)
        config.root_dir = tmp_path

        # Create mock credentials
        mock_creds = MagicMock()
        mock_creds.valid = True
        mock_creds.expired = False
        mock_pickle_load.return_value = mock_creds

        # Create empty token file
        token_path = tmp_path / "token.pickle"
        token_path.write_bytes(b"fake")

        provider = OAuthCredentialsProvider(config)
        result = provider.get_credentials()

        assert result.valid is True

    @patch("ami.dataops.backup.common.auth.pickle.dump")
    @patch("ami.dataops.backup.common.auth.pickle.load")
    @patch("ami.dataops.backup.common.auth.Request")
    def test_refreshes_expired_token(
        self, mock_request, mock_pickle_load, mock_pickle_dump, tmp_path: Path
    ) -> None:
        """Test refreshes expired token."""
        config = MagicMock(spec=BackupConfig)
        config.root_dir = tmp_path

        # Create mock expired credentials
        mock_creds = MagicMock()
        mock_creds.valid = False
        mock_creds.expired = True
        mock_creds.refresh_token = "refresh_token"
        mock_pickle_load.return_value = mock_creds

        # Create empty token file
        token_path = tmp_path / "token.pickle"
        token_path.write_bytes(b"fake")

        provider = OAuthCredentialsProvider(config)
        provider.get_credentials()

        mock_creds.refresh.assert_called_once()

    @patch(
        "ami.dataops.backup.common.auth.get_project_root",
        side_effect=RuntimeError,
    )
    def test_raises_error_when_no_credentials_json(
        self, mock_root, tmp_path: Path
    ) -> None:
        """Test raises error when credentials.json not found."""
        config = MagicMock(spec=BackupConfig)
        config.root_dir = tmp_path

        provider = OAuthCredentialsProvider(config)

        with pytest.raises(BackupError) as exc_info:
            provider.get_credentials()

        assert "OAuth client secrets file not found" in str(exc_info.value)

    @patch(
        "ami.dataops.backup.common.auth.get_project_root",
        side_effect=RuntimeError,
    )
    @patch("ami.dataops.backup.common.auth.pickle.dump")
    @patch("ami.dataops.backup.common.auth.InstalledAppFlow.from_client_secrets_file")
    def test_runs_oauth_flow_when_no_token(
        self, mock_flow, mock_pickle_dump, mock_root, tmp_path: Path
    ) -> None:
        """Test runs OAuth flow when no token exists."""
        config = MagicMock(spec=BackupConfig)
        config.root_dir = tmp_path

        # Create credentials.json
        creds_json = tmp_path / "credentials.json"
        creds_json.write_text('{"installed": {}}')

        mock_creds = MagicMock()
        mock_creds.valid = True
        mock_flow_instance = MagicMock()
        mock_flow_instance.run_local_server.return_value = mock_creds
        mock_flow.return_value = mock_flow_instance

        provider = OAuthCredentialsProvider(config)
        result = provider.get_credentials()

        mock_flow_instance.run_local_server.assert_called_once_with(port=0)
        assert result == mock_creds

    @patch("ami.dataops.backup.common.auth.pickle.load")
    @patch.dict("os.environ", {"GDRIVE_TOKEN_FILE": "custom_token.pickle"})
    def test_uses_custom_token_filename(self, mock_pickle_load, tmp_path: Path) -> None:
        """Test uses custom token filename from environment."""
        config = MagicMock(spec=BackupConfig)
        config.root_dir = tmp_path

        # Create mock credentials
        mock_creds = MagicMock()
        mock_creds.valid = True
        mock_pickle_load.return_value = mock_creds

        # Create custom token file
        token_path = tmp_path / "custom_token.pickle"
        token_path.write_bytes(b"fake")

        provider = OAuthCredentialsProvider(config)
        result = provider.get_credentials()

        assert result.valid is True


class TestAuthenticationManager:
    """Tests for AuthenticationManager class."""

    def test_creates_impersonation_provider(self) -> None:
        """Test creates impersonation provider for impersonation auth."""
        config = MagicMock(spec=BackupConfig)
        config.auth_method = "impersonation"

        manager = AuthenticationManager(config)

        assert isinstance(manager._provider, ImpersonationCredentialsProvider)

    def test_creates_key_provider(self) -> None:
        """Test creates service account provider for key auth."""
        config = MagicMock(spec=BackupConfig)
        config.auth_method = "key"

        manager = AuthenticationManager(config)

        assert isinstance(manager._provider, ServiceAccountCredentialsProvider)

    def test_creates_oauth_provider(self) -> None:
        """Test creates OAuth provider for oauth auth."""
        config = MagicMock(spec=BackupConfig)
        config.auth_method = "oauth"

        manager = AuthenticationManager(config)

        assert isinstance(manager._provider, OAuthCredentialsProvider)

    def test_raises_error_for_unknown_method(self) -> None:
        """Test raises error for unknown auth method."""
        config = MagicMock(spec=BackupConfig)
        config.auth_method = "unknown"

        with pytest.raises(BackupConfigError) as exc_info:
            AuthenticationManager(config)

        assert "Unknown auth method" in str(exc_info.value)

    def test_update_config_recreates_provider(self) -> None:
        """Test update_config recreates provider."""
        config1 = MagicMock(spec=BackupConfig)
        config1.auth_method = "oauth"

        config2 = MagicMock(spec=BackupConfig)
        config2.auth_method = "key"

        manager = AuthenticationManager(config1)
        assert isinstance(manager._provider, OAuthCredentialsProvider)

        manager.update_config(config2)
        assert isinstance(manager._provider, ServiceAccountCredentialsProvider)

    def test_get_credentials_delegates_to_provider(self) -> None:
        """Test get_credentials delegates to provider."""
        config = MagicMock(spec=BackupConfig)
        config.auth_method = "oauth"

        manager = AuthenticationManager(config)
        mock_provider = MagicMock()
        mock_creds = MagicMock()
        mock_provider.get_credentials.return_value = mock_creds
        manager._provider = mock_provider

        result = manager.get_credentials()

        assert result == mock_creds
        mock_provider.get_credentials.assert_called_once()
