"""
Configuration module for backup restore operations.

Extends the existing backup configuration with restore-specific settings.
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

from ami.dataops.backup.backup_config import BackupConfig
from ami.dataops.backup.backup_exceptions import BackupConfigError
from ami.dataops.backup.common.paths import find_gcloud, get_project_root


class BackupRestoreConfig(BackupConfig):
    """Backup restore configuration extending base backup configuration."""

    def __init__(self, root_dir: Path):
        super().__init__(root_dir)
        self.restore_path: Path = root_dir / "_restored"
        self.restore_timeout: int = 3600  # 1 hour default timeout
        self.preserve_permissions: bool = True
        self.preserve_timestamps: bool = True

    @classmethod
    def _load_impersonation_auth(cls, config: "BackupRestoreConfig") -> None:
        """Load impersonation authentication configuration."""
        service_account_email = os.getenv("GDRIVE_SERVICE_ACCOUNT_EMAIL")
        if not service_account_email:
            msg = "GDRIVE_SERVICE_ACCOUNT_EMAIL required"
            raise BackupConfigError(msg)

        config.service_account_email = service_account_email
        config.gcloud_path = find_gcloud()

        logger.info("Using service account impersonation (secure)")
        logger.info(f"  Service Account: {service_account_email}")

        if not config.gcloud_path:
            logger.error("  ❌ gcloud CLI not found!")
            msg = "gcloud CLI required"
            raise BackupConfigError(msg)

    @classmethod
    def _load_key_auth(cls, config: "BackupRestoreConfig", root_dir: Path) -> None:
        """Load key file authentication configuration."""
        credentials_file = os.getenv("GDRIVE_CREDENTIALS_FILE")
        if not credentials_file:
            msg = (
                "GDRIVE_CREDENTIALS_FILE must be set for key auth. "
                "Example: GDRIVE_CREDENTIALS_FILE=/path/to/service-account.json"
            )
            raise BackupConfigError(msg)

        credentials_path = Path(credentials_file)
        if not credentials_path.is_absolute():
            credentials_path = root_dir / credentials_path

        if not credentials_path.exists():
            msg = (
                f"Credentials file not found at {credentials_path}. "
                "Download the JSON key from Google Cloud Console."
            )
            raise BackupConfigError(msg)

        config.credentials_file = str(credentials_path)

        logger.warning("⚠️  Using service account key file (security risk)")
        logger.warning("  Consider switching to service account impersonation")

    @classmethod
    def _load_restore_config(
        cls, config: "BackupRestoreConfig", root_dir: Path
    ) -> None:
        """Load restore-specific configuration."""
        restore_path_str = os.getenv("RESTORE_PATH", str(root_dir / "_restored"))
        config.restore_path = Path(restore_path_str)
        config.restore_path.mkdir(parents=True, exist_ok=True)

        restore_timeout_str = os.getenv("RESTORE_TIMEOUT", "3600")
        try:
            config.restore_timeout = int(restore_timeout_str)
        except ValueError as e:
            msg = f"Invalid RESTORE_TIMEOUT: {restore_timeout_str}. Must be integer."
            raise BackupConfigError(msg) from e

        truthy_values = ["true", "1", "yes", "on"]
        preserve_permissions_str = os.getenv(
            "RESTORE_PRESERVE_PERMISSIONS", "true"
        ).lower()
        config.preserve_permissions = preserve_permissions_str in truthy_values

        preserve_timestamps_str = os.getenv(
            "RESTORE_PRESERVE_TIMESTAMPS", "true"
        ).lower()
        config.preserve_timestamps = preserve_timestamps_str in truthy_values

    @classmethod
    def load(cls, root_dir: Path) -> "BackupRestoreConfig":
        """Load restore configuration from .env file."""
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
        valid_methods = ["impersonation", "key", "oauth"]
        if auth_method not in valid_methods:
            msg = f"Invalid GDRIVE_AUTH_METHOD: {auth_method}. Use: {valid_methods}"
            raise BackupConfigError(msg)

        config.auth_method = auth_method

        if auth_method == "impersonation":
            cls._load_impersonation_auth(config)
        elif auth_method == "key":
            cls._load_key_auth(config, root_dir)
        elif auth_method == "oauth":
            logger.info(
                "Using regular user OAuth (requires initial browser authentication)"
            )
            logger.info("  First time setup will open a browser for authentication")

        config.folder_id = os.getenv("GDRIVE_BACKUP_FOLDER_ID")
        cls._load_restore_config(config, root_dir)

        return config
