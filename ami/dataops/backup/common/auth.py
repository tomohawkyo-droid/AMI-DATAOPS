"""
Authentication module for backup/restore operations.

This module provides a unified interface for different authentication methods
used in backup and restore operations, including Google Drive authentication.
"""

import os
import pickle
from pathlib import Path
from typing import Protocol, cast

import google.auth
import google.auth.exceptions
from google.auth import impersonated_credentials
from google.auth.credentials import Credentials as GoogleAuthCredentials
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow

from ami.dataops.backup.backup_config import BackupConfig
from ami.dataops.backup.backup_exceptions import BackupConfigError, BackupError
from ami.dataops.backup.common.paths import get_project_root


class CredentialsProvider(Protocol):
    """Protocol for different types of credentials providers."""

    def get_credentials(self) -> GoogleAuthCredentials:
        """Get credentials for authentication."""
        ...


class ImpersonationCredentialsProvider:
    """Provides credentials through service account impersonation."""

    def __init__(self, config: BackupConfig):
        self.config = config

    def get_credentials(self) -> GoogleAuthCredentials:
        """Get impersonated credentials."""
        if not self.config.service_account_email:
            msg = (
                "GDRIVE_SERVICE_ACCOUNT_EMAIL is not set.\n"
                "Add to your .env file:\n"
                "  GDRIVE_SERVICE_ACCOUNT_EMAIL="
                "my-sa@project.iam.gserviceaccount.com"
            )
            raise BackupConfigError(msg)

        try:
            # Get source credentials (user credentials from gcloud)
            auth_result = google.auth.default()
            source_creds = auth_result[0]

            # Impersonate the service account
            credentials = cast(
                GoogleAuthCredentials,
                impersonated_credentials.Credentials(
                    source_credentials=source_creds,
                    target_principal=self.config.service_account_email,
                    target_scopes=["https://www.googleapis.com/auth/drive"],
                    lifetime=3600,  # 1 hour
                ),
            )

            # Explicitly refresh to test if impersonation works
            request = Request()
            credentials.refresh(request)
        except google.auth.exceptions.DefaultCredentialsError as e:
            msg = (
                "Impersonation failed: no ADC found.\n"
                "Fix: Run 'ami-gcloud auth "
                "application-default login'\n"
                "Docs: https://cloud.google.com/"
                "docs/authentication/"
                "application-default-credentials"
            )
            raise BackupError(msg) from e
        except google.auth.exceptions.RefreshError as e:
            if "invalid_grant" in str(e).lower():
                msg = (
                    "Impersonation failed: "
                    "ADC token expired or revoked.\n"
                    "Fix: Run 'ami-gcloud auth "
                    "application-default login'"
                )
            else:
                sa = self.config.service_account_email
                msg = (
                    "Impersonation failed: "
                    "could not refresh credentials.\n"
                    f"Check that '{sa}' exists and "
                    "your account has the "
                    "'Service Account Token Creator'"
                    " role on it.\n"
                    f"Details: {e}"
                )
            raise BackupError(msg) from e
        except Exception as e:
            sa = self.config.service_account_email
            msg = (
                f"Impersonation failed: {e}\n"
                "Possible fixes:\n"
                "  1. Run 'ami-gcloud auth "
                "application-default login'\n"
                f"  2. Verify SA email: {sa}\n"
                "  3. Ensure your user has "
                "'Service Account Token Creator' "
                "role on the service account"
            )
            raise BackupError(msg) from e
        else:
            return credentials


class ServiceAccountCredentialsProvider:
    """Provides credentials from a service account key file."""

    def __init__(self, config: BackupConfig):
        self.config = config

    def get_credentials(self) -> GoogleAuthCredentials:
        """Get service account credentials from file."""
        if not self.config.credentials_file:
            msg = (
                "GDRIVE_CREDENTIALS_FILE is not set.\n"
                "Add to your .env file:\n"
                "  GDRIVE_CREDENTIALS_FILE="
                "/path/to/sa-key.json\n"
                "Create a key at: console.cloud"
                ".google.com/iam-admin/serviceaccounts"
            )
            raise BackupConfigError(msg)

        credentials_file_path = Path(self.config.credentials_file)
        if not credentials_file_path.exists():
            msg = (
                f"Service account key file not found at: {credentials_file_path}\n"
                "Check GDRIVE_CREDENTIALS_FILE in your .env."
            )
            raise BackupError(msg)

        return cast(
            GoogleAuthCredentials,
            ServiceAccountCredentials.from_service_account_file(
                credentials_file_path, scopes=["https://www.googleapis.com/auth/drive"]
            ),
        )


class OAuthCredentialsProvider:
    """Provides credentials through OAuth flow."""

    def __init__(self, config: BackupConfig):
        self.config = config

    def get_credentials(self) -> GoogleAuthCredentials:
        """Get OAuth credentials, either from existing token or by running auth flow."""
        scopes = ["https://www.googleapis.com/auth/drive"]
        try:
            project_root = get_project_root()
        except RuntimeError:
            project_root = self.config.root_dir

        creds = None
        # Allow custom token file path via environment variable, default to token.pickle
        token_filename = os.getenv("GDRIVE_TOKEN_FILE", "token.pickle")

        # Try current root_dir first, then fall back to project root
        token_path = self.config.root_dir / token_filename
        if not token_path.exists() and project_root != self.config.root_dir:
            token_path = project_root / token_filename

        # The file token.pickle stores the user's access and refresh tokens.
        if token_path.exists():
            with open(token_path, "rb") as token:
                creds = pickle.load(token)

        # If there are no valid credentials available, request authorization
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                # Refresh the token
                try:
                    creds.refresh(Request())
                except google.auth.exceptions.RefreshError as e:
                    msg = (
                        f"OAuth token refresh failed: {e}\n"
                        "Token may be expired or revoked.\n"
                        "Fix: Run 'ami-gcloud auth "
                        "application-default login'"
                    )
                    raise BackupError(msg) from e
            else:
                # Check if credentials.json exists for the OAuth flow
                credentials_json_path = self.config.root_dir / "credentials.json"
                if (
                    not credentials_json_path.exists()
                    and project_root != self.config.root_dir
                ):
                    credentials_json_path = project_root / "credentials.json"

                if not credentials_json_path.exists():
                    p = credentials_json_path
                    msg = (
                        "OAuth client secrets file "
                        f"not found at: {p}\n"
                        "This file contains your "
                        "Google Cloud OAuth client ID.\n"
                        "To obtain it:\n"
                        "  1. Go to console.cloud"
                        ".google.com/apis/credentials\n"
                        "  2. Create an OAuth 2.0 "
                        "Client ID (Desktop type)\n"
                        "  3. Download JSON, save as "
                        "'credentials.json' in project root"
                    )
                    raise BackupError(msg)

                # Use Local Server Flow (OOB is deprecated)
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(credentials_json_path), scopes
                )
                creds = flow.run_local_server(port=0)

            # Save the credentials for the next run
            # (save back to the path where it was found or root)
            with open(token_path, "wb") as token:
                pickle.dump(creds, token)

        return cast(GoogleAuthCredentials, creds)


class AuthenticationManager:
    """Manages different authentication methods for backup/restore operations."""

    def __init__(self, config: BackupConfig):
        self.config = config
        self._provider = self._create_provider()

    def _create_provider(self) -> CredentialsProvider:
        """Create the appropriate credentials provider based on auth method."""
        if self.config.auth_method == "impersonation":
            return ImpersonationCredentialsProvider(self.config)
        elif self.config.auth_method == "key":
            return ServiceAccountCredentialsProvider(self.config)
        elif self.config.auth_method == "oauth":
            return OAuthCredentialsProvider(self.config)
        else:
            msg = f"Unknown auth method: {self.config.auth_method}"
            raise BackupConfigError(msg)

    def update_config(self, config: BackupConfig) -> None:
        """Update the configuration and recreate the provider."""
        self.config = config
        self._provider = self._create_provider()

    def get_credentials(self) -> GoogleAuthCredentials:
        """Get credentials based on the configured method."""
        return self._provider.get_credentials()
