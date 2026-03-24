"""
Command line interface for backup creation operations.

Handles command line argument parsing and execution for creating backups.
"""

import argparse
import os
import sys
from pathlib import Path

from loguru import logger

from ami.dataops.backup.backup_exceptions import (
    ArchiveError,
    BackupConfigError,
    BackupError,
    UploadError,
)
from ami.dataops.backup.create.service import BackupOptions, BackupService


class BackupCLI:
    """Command line interface for backup creation operations."""

    def __init__(self, service: BackupService | None = None):
        self.service = service

    def create_parser(self) -> argparse.ArgumentParser:
        """Create the argument parser with all available options."""
        parser = argparse.ArgumentParser(
            prog="backup_to_gdrive",
            description="Create and upload backups to Google Drive",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  # Upload and delete local zip (default)
  backup_to_gdrive

  # Backup specific directory
  backup_to_gdrive /path/to/data

  # Backup with specific name
  backup_to_gdrive --name my-backup

  # Upload and keep local zip
  backup_to_gdrive --keep-local

  # Set up authentication
  backup_to_gdrive --setup-auth

  # Disable auth retry on failure
  backup_to_gdrive --no-auth-retry

  # Use specific auth mode
  backup_to_gdrive --auth-mode impersonation
            """,
        )

        # Positional argument for source directory
        parser.add_argument(
            "source",
            nargs="?",
            type=Path,
            default=Path.cwd(),
            help="Directory to backup (default: current directory)",
        )

        # Configuration options
        parser.add_argument(
            "--config-path",
            type=Path,
            default=Path.cwd(),
            help="Path to directory containing .env file (default: current directory)",
        )

        parser.add_argument(
            "--name",
            help="Custom name for the backup file (default: auto-generated)",
        )

        parser.add_argument(
            "--include-all",
            action="store_true",
            help="Include all files (disable .git, node_modules exclusions)",
        )

        # Backup operation options
        parser.add_argument(
            "--keep-local",
            action="store_true",
            help="Keep local zip after upload (default: delete after upload)",
        )

        parser.add_argument(
            "--no-auth-retry",
            action="store_true",
            help="Disable authentication retry on failure",
        )

        parser.add_argument(
            "--auth-mode",
            choices=["oauth", "impersonation", "key"],
            help="Authentication mode to use (overrides GDRIVE_AUTH_METHOD env var)",
        )

        parser.add_argument(
            "--setup-auth",
            action="store_true",
            help="Set up Google Cloud authentication using local gcloud binary",
        )

        parser.add_argument(
            "--verbose", "-v", action="store_true", help="Enable verbose logging"
        )

        return parser

    def parse_arguments(self, argv: list[str]) -> argparse.Namespace:
        """Parse command line arguments."""
        parser = self.create_parser()
        return parser.parse_args(argv)

    def _setup_logging(self, verbose: bool) -> None:
        """Configure logging level based on verbose flag."""
        logger.remove()
        logger.add(sys.stderr, level="DEBUG" if verbose else "INFO")
        logger.info("=" * 60)
        logger.info("AMI Orchestrator Backup to Google Drive")
        logger.info("=" * 60)

    def _restore_auth_env(self, auth_mode: str | None, original: str | None) -> None:
        """Restore the original GDRIVE_AUTH_METHOD environment variable."""
        if not auth_mode:
            return
        if original is not None:
            os.environ["GDRIVE_AUTH_METHOD"] = original
        elif "GDRIVE_AUTH_METHOD" in os.environ:
            del os.environ["GDRIVE_AUTH_METHOD"]

    def _log_error_suggestions(self, e: Exception, auth_retry_enabled: bool) -> None:
        """Log helpful suggestions based on error type."""
        if isinstance(e, BackupConfigError):
            error_str = str(e).lower()
            if "credentials" in error_str or "authenticated" in error_str:
                logger.info(
                    "  To set up authentication, run: backup_to_gdrive --setup-auth"
                )
            elif "GDRIVE_AUTH_METHOD" in str(e):
                logger.info(
                    "  Set GDRIVE_AUTH_METHOD in .env (impersonation/key/oauth) "
                    "or use --auth-mode option."
                )
        elif isinstance(e, UploadError):
            error_str = str(e).lower()
            if "reauthentication" in error_str or "authenticated" in error_str:
                logger.info("  Auth may be needed. Run: backup_to_gdrive --setup-auth")
                if auth_retry_enabled:
                    logger.info("  (Auth retry was attempted but failed)")

    async def run(self, args: argparse.Namespace) -> int:
        """Main execution method."""
        self._setup_logging(args.verbose)

        if self.service is None:
            logger.error("Backup service not initialized")
            return 1

        if args.setup_auth:
            return await self.service.setup_auth()

        original_auth_method = None
        auth_retry_enabled = not args.no_auth_retry

        if args.auth_mode:
            original_auth_method = os.environ.get("GDRIVE_AUTH_METHOD")
            os.environ["GDRIVE_AUTH_METHOD"] = args.auth_mode
            logger.info(f"Using command-line auth mode override: {args.auth_mode}")

        try:
            options = BackupOptions(
                keep_local=args.keep_local,
                retry_auth=auth_retry_enabled,
                source_dir=args.source,
                output_filename=args.name,
                ignore_exclusions=args.include_all,
                config_path=args.config_path,
            )
            file_id = await self.service.run_backup(options)
        except (ArchiveError, BackupConfigError, UploadError, BackupError) as e:
            logger.error(f"{e.__class__.__name__}: {e}")
            self._log_error_suggestions(e, auth_retry_enabled)
            return 1
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return 1
        else:
            logger.info("=" * 60)
            logger.info("✓ Backup completed successfully")
            logger.info(f"  Google Drive File ID: {file_id}")
            logger.info("=" * 60)
            return 0
        finally:
            self._restore_auth_env(args.auth_mode, original_auth_method)
