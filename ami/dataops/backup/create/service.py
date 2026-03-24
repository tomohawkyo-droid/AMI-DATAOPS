"""
Backup service module.

Main business logic for backup operations.
"""

import asyncio
from pathlib import Path

from google.auth.exceptions import RefreshError, TransportError
from googleapiclient.errors import HttpError
from loguru import logger
from pydantic import BaseModel

from ami.dataops.backup.backup_config import BackupConfig
from ami.dataops.backup.backup_exceptions import BackupError, UploadError
from ami.dataops.backup.common.auth import AuthenticationManager
from ami.dataops.backup.common.constants import DEFAULT_BACKUP_NAME
from ami.dataops.backup.common.paths import find_gcloud
from ami.dataops.backup.create.archiver import create_zip_archive
from ami.dataops.backup.create.secondary import copy_to_secondary_backup
from ami.dataops.backup.create.uploader import BackupUploader
from ami.dataops.backup.create.utils import cleanup_local_zip

TOTAL_STEPS = 4
BYTES_PER_MB = 1024 * 1024


class BackupOptions(BaseModel):
    """Options for backup operations."""

    keep_local: bool = False
    retry_auth: bool = True
    source_dir: Path | None = None
    output_filename: str | None = None
    ignore_exclusions: bool = False
    config_path: Path | None = None


class BackupService:
    """Main service for backup operations."""

    def __init__(self, uploader: BackupUploader, auth_manager: AuthenticationManager):
        self.uploader = uploader
        self.auth_manager = auth_manager

    async def run_backup(self, options: BackupOptions) -> str:
        """
        Run the backup process.

        Args:
            options: BackupOptions containing all backup configuration.

        Returns:
            Google Drive file ID

        Raises:
            BackupError: If any step fails
        """
        # Determine paths
        source_dir = (
            Path.cwd() if options.source_dir is None else options.source_dir.resolve()
        )
        config_path = (
            Path.cwd() if options.config_path is None else options.config_path.resolve()
        )

        # Load configuration
        config = BackupConfig.load(config_path)

        # Update auth manager with new config
        self.auth_manager.update_config(config)

        # Pre-flight: validate credentials before spending time archiving
        logger.info("Validating credentials...")
        try:
            self.auth_manager.get_credentials()
        except Exception as e:
            msg = f"Credential check failed before archiving: {e}"
            raise BackupError(msg) from e
        logger.info("✓ Credentials valid")

        # Use CWD as output directory to avoid polluting source
        output_dir = Path.cwd()
        backup_name = options.output_filename or DEFAULT_BACKUP_NAME

        # Step 1: Create archive
        logger.info(f"[Step 1/{TOTAL_STEPS}] Creating archive...")
        zip_path = await create_zip_archive(
            source_dir,
            backup_name,
            options.ignore_exclusions,
            output_dir=output_dir,
        )
        archive_size_mb = zip_path.stat().st_size / BYTES_PER_MB
        logger.info(f"  Archive ready: {zip_path.name} ({archive_size_mb:.1f} MB)")

        # Step 2: Upload to Google Drive
        logger.info(f"[Step 2/{TOTAL_STEPS}] Uploading to Google Drive...")
        try:
            file_id = await self.uploader.upload_to_gdrive(zip_path, config)
        except UploadError as e:
            file_id = await self._handle_upload_error(
                e, zip_path, config, options.retry_auth
            )

        # Step 3: Copy to secondary backup location
        logger.info(f"[Step 3/{TOTAL_STEPS}] Copying to secondary backup...")
        await copy_to_secondary_backup(zip_path)

        # Step 4: Cleanup
        logger.info(f"[Step 4/{TOTAL_STEPS}] Cleaning up...")
        await cleanup_local_zip(zip_path, options.keep_local)

        return file_id

    def _is_auth_error(self, error: UploadError) -> bool:
        """Check if an upload error is authentication-related."""
        cause: BaseException | None = error.__cause__
        while cause is not None:
            if isinstance(cause, HttpError) and cause.resp.status in (401, 403):
                return True
            if isinstance(cause, (RefreshError, TransportError)):
                return True
            cause = getattr(cause, "__cause__", None)
        return False

    async def _handle_upload_error(
        self, error: UploadError, zip_path: Path, config: BackupConfig, retry_auth: bool
    ) -> str:
        """Handle upload error with potential credential refresh."""
        if not (retry_auth and self._is_auth_error(error)):
            raise error

        logger.warning(f"Authentication error detected: {error}")

        if config.auth_method == "oauth":
            logger.info("Attempting to re-authenticate via OAuth...")
            # Force re-creation of the auth provider to trigger a fresh OAuth flow
            self.auth_manager.update_config(config)
        else:
            logger.info("Attempting to refresh ADC credentials...")
            if not await self._refresh_adc_credentials():
                logger.error("Failed to refresh credentials.")
                raise error

        # Reset cached service so next call uses fresh credentials
        self.uploader._service = None
        logger.info("Retrying upload with refreshed credentials...")
        return await self.uploader.upload_to_gdrive(zip_path, config)

    async def setup_auth(self) -> int:
        """
        Set up Google Cloud authentication using local gcloud binary.

        Returns:
            Exit code from the gcloud auth command
        """
        logger.info("Setting up Google Cloud authentication...")

        gcloud_path = find_gcloud()
        if not gcloud_path:
            logger.error(
                "gcloud CLI not found! Please install with the appropriate script"
            )
            return 1

        logger.info(f"Using gcloud binary: {gcloud_path}")
        logger.info("Please follow browser instructions to complete authentication...")

        try:
            # Run the gcloud auth command
            process = await asyncio.create_subprocess_exec(
                str(gcloud_path),
                "auth",
                "application-default",
                "login",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _stdout, stderr = await process.communicate()

            if process.returncode == 0:
                logger.info("✓ Authentication setup completed successfully!")
                logger.info("You can now run the backup script.")
                return 0
            else:
                logger.error(f"Authentication setup failed: {process.returncode}")
                if stderr:
                    logger.error(f"Error output: {stderr.decode()}")
                return process.returncode or 1
        except Exception as e:
            logger.error(f"Unexpected error during authentication setup: {e}")
            return 1

    async def _refresh_adc_credentials(self) -> bool:
        """Attempt to refresh Application Default Credentials using gcloud."""
        gcloud_path = find_gcloud()
        if not gcloud_path:
            logger.error("gcloud CLI not found! Cannot refresh credentials.")
            return False

        adc_path = Path.home() / ".config/gcloud/application_default_credentials.json"
        if not adc_path.exists():
            logger.warning(
                "Application Default Credentials not found, set up auth first."
            )
            return False

        try:
            return await self._check_and_refresh_token(gcloud_path)
        except TimeoutError:
            logger.error("Timeout while checking credentials with gcloud.")
            return False
        except Exception as e:
            logger.error(f"Error refreshing credentials: {e}")
            return False

    async def _check_and_refresh_token(self, gcloud_path: str) -> bool:
        """Check token status and refresh if needed."""
        process = await asyncio.create_subprocess_exec(
            gcloud_path,
            "auth",
            "application-default",
            "print-access-token",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)
        except TimeoutError:
            process.kill()
            msg = "gcloud auth timed out"
            raise TimeoutError(msg) from None

        if process.returncode == 0:
            logger.info("Access token is still valid.")
            return True

        logger.info("Current access token is invalid or expired, attempting refresh...")
        logger.debug(f"gcloud error output: {stderr.decode()}")
        return await self._run_gcloud_login(gcloud_path)

    async def _run_gcloud_login(self, gcloud_path: str) -> bool:
        """Run gcloud login to refresh credentials."""
        refresh_process = await asyncio.create_subprocess_exec(
            gcloud_path,
            "auth",
            "application-default",
            "login",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _stdout_refresh, stderr_refresh = await asyncio.wait_for(
                refresh_process.communicate(), timeout=30
            )
        except TimeoutError:
            refresh_process.kill()
            logger.error("gcloud login timed out")
            return False

        if refresh_process.returncode == 0:
            logger.info("Credentials successfully refreshed.")
            return True

        logger.error(f"Failed to refresh credentials: {stderr_refresh.decode()}")
        return False
