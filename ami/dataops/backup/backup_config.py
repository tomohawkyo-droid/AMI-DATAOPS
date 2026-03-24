"""Backup configuration module.

Handles loading and validation of backup configuration from .env file.
"""

import os
import subprocess
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

from ami.dataops.backup.backup_exceptions import BackupConfigError
from ami.dataops.backup.common.paths import find_gcloud, get_project_root

ADC_CREDENTIALS_PATH = (
    Path.home() / ".config/gcloud/application_default_credentials.json"
)


def check_adc_credentials_valid() -> bool:
    """Check if Application Default Credentials are valid and not expired."""
    gcloud_path = find_gcloud()
    if not gcloud_path or not ADC_CREDENTIALS_PATH.exists():
        return False

    try:
        result = subprocess.run(
            [gcloud_path, "auth", "application-default", "print-access-token"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (subprocess.TimeoutExpired, Exception):
        return False
    else:
        return result.returncode == 0


def refresh_adc_credentials() -> bool:
    """Attempt to refresh Application Default Credentials using gcloud."""
    gcloud_path = find_gcloud()
    if not gcloud_path:
        logger.error("gcloud CLI not found! Cannot refresh credentials.")
        return False

    if not ADC_CREDENTIALS_PATH.exists():
        logger.warning(
            "Application Default Credentials file not found, need to set up auth first."
        )
        return False

    try:
        return _check_and_refresh_adc_token(gcloud_path)
    except subprocess.TimeoutExpired:
        logger.error("Timeout while checking credentials with gcloud.")
        return False
    except Exception as e:
        logger.error(f"Error refreshing credentials: {e}")
        return False


def _check_and_refresh_adc_token(gcloud_path: str) -> bool:
    """Check token status and refresh if needed."""
    result = subprocess.run(
        [gcloud_path, "auth", "application-default", "print-access-token"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    if result.returncode == 0:
        logger.info("Access token is still valid.")
        return True

    logger.info("Current access token is invalid or expired, attempting refresh...")
    logger.debug(f"gcloud error output: {result.stderr}")

    refresh_result = subprocess.run(
        [gcloud_path, "auth", "application-default", "login"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    if refresh_result.returncode == 0:
        logger.info("Credentials successfully refreshed.")
        return True

    logger.error(f"Failed to refresh credentials: {refresh_result.stderr}")
    return False


class BackupConfig:
    """Backup configuration loaded from .env"""

    VALID_AUTH_METHODS = ("impersonation", "key", "oauth")

    def __init__(self, root_dir: Path):
        self.root_dir = root_dir
        self.auth_method: str = "oauth"
        self.service_account_email: str | None = None
        self.credentials_file: str | None = None
        self.folder_id: str | None = None
        self.gcloud_path: str | None = None

    @classmethod
    def load(cls, root_dir: Path) -> "BackupConfig":
        """Load configuration from .env file."""
        try:
            # Always prioritize project root discovery
            project_root = get_project_root()
            env_path = project_root / ".env"
        except RuntimeError:
            project_root = root_dir
            env_path = root_dir / ".env"

        if not env_path.exists():
            # Final alternative if root discovery didn't find a .env
            env_path = root_dir / ".env"
            if not env_path.exists():
                msg = f".env file not found at {env_path}"
                raise BackupConfigError(msg)
        else:
            # Use the directory where .env was actually found
            root_dir = env_path.parent

        load_dotenv(env_path)
        config = cls(root_dir)

        auth_method = os.getenv("GDRIVE_AUTH_METHOD", "oauth")
        if auth_method not in cls.VALID_AUTH_METHODS:
            msg = (
                f"Invalid GDRIVE_AUTH_METHOD='{auth_method}'. "
                f"Must be one of: {', '.join(cls.VALID_AUTH_METHODS)}"
            )
            raise BackupConfigError(msg)

        config.auth_method = auth_method
        config._configure_auth_method(root_dir)
        config.folder_id = os.getenv("GDRIVE_BACKUP_FOLDER_ID")
        return config

    def _configure_auth_method(self, root_dir: Path) -> None:
        """Configure the selected authentication method."""
        if self.auth_method == "impersonation":
            self._configure_impersonation_auth()
        elif self.auth_method == "key":
            self._configure_key_auth(root_dir)
        else:
            self._configure_oauth_auth()

    def _configure_impersonation_auth(self) -> None:
        """Configure service account impersonation authentication."""
        service_account_email = os.getenv("GDRIVE_SERVICE_ACCOUNT_EMAIL")
        if not service_account_email:
            msg = (
                "GDRIVE_SERVICE_ACCOUNT_EMAIL is not set.\n"
                "Add to your .env file:\n"
                "  GDRIVE_SERVICE_ACCOUNT_EMAIL=my-sa@project.iam.gserviceaccount.com"
            )
            raise BackupConfigError(msg)

        self.service_account_email = service_account_email
        self.gcloud_path = find_gcloud()

        logger.info("Using service account impersonation (secure)")
        logger.info(f"  Service Account: {service_account_email}")

        if not self.gcloud_path:
            logger.error("gcloud CLI not found on this system.")
            logger.error("Install options:")
            logger.error(
                "  Project-local: ./.boot-linux/bin/ami-gcloud (run bootstrap first)"
            )
            logger.error("  System-wide:   https://cloud.google.com/sdk/docs/install")
            msg = (
                "gcloud CLI is required for service account "
                "impersonation but was not found"
            )
            raise BackupConfigError(msg)

        self._log_gcloud_status()

    def _log_gcloud_status(self) -> None:
        """Log gcloud CLI status and credentials validity."""
        assert self.gcloud_path is not None  # Checked by caller
        gcloud_type = "local" if ".gcloud" in self.gcloud_path else "system"
        logger.info(f"  Using {gcloud_type} gcloud: {self.gcloud_path}")

        if check_adc_credentials_valid():
            logger.info("  ✓ Application Default Credentials are valid")
        else:
            logger.warning(
                "  ⚠️  Application Default Credentials are expired or invalid"
            )
            logger.warning("  To refresh, run one of:")
            logger.warning("    ami-gcloud auth application-default login")
            logger.warning("    gcloud auth application-default login")

    def _configure_key_auth(self, root_dir: Path) -> None:
        """Configure service account key file authentication."""
        credentials_file = os.getenv("GDRIVE_CREDENTIALS_FILE")
        if not credentials_file:
            msg = (
                "GDRIVE_CREDENTIALS_FILE is not set.\n"
                "Add to your .env file:\n"
                "  GDRIVE_CREDENTIALS_FILE=/path/to/sa-key.json\n"
                "To create a key: https://console.cloud.google.com/iam-admin/serviceaccounts"
            )
            raise BackupConfigError(msg)

        credentials_path = Path(credentials_file)
        if not credentials_path.is_absolute():
            credentials_path = root_dir / credentials_path

        if not credentials_path.exists():
            msg = (
                f"Service account key file not found at: "
                f"{credentials_path}\n"
                "Check GDRIVE_CREDENTIALS_FILE in your .env"
            )
            raise BackupConfigError(msg)

        self.credentials_file = str(credentials_path)

        logger.warning("⚠️  Using service account key file (security risk)")
        logger.warning("  Consider switching to service account impersonation")

    def _configure_oauth_auth(self) -> None:
        """Configure OAuth authentication."""
        logger.info(
            "Using regular user OAuth (requires initial browser authentication)"
        )
        logger.info("  First time setup will open a browser for authentication")
