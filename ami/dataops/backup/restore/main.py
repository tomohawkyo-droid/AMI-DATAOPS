"""Main entry point for backup restore operations.

Sets up dependency injection and runs the CLI application.
"""

import asyncio
import sys
from pathlib import Path

from loguru import logger

# Import path utility first to setup sys.path
try:
    from ami.dataops.backup.common.paths import setup_sys_path

    setup_sys_path()
except ImportError:
    # Fallback if we can't import paths yet
    script_path = Path(__file__) if "__file__" in globals() else Path(sys.argv[0])
    _repo_root = next(
        (p for p in script_path.resolve().parents if (p / "base").exists()), None
    )
    if _repo_root:
        sys.path.insert(0, str(_repo_root))

from ami.dataops.backup.common.auth import AuthenticationManager
from ami.dataops.backup.core.config import BackupRestoreConfig
from ami.dataops.backup.restore.cli import RestoreCLI
from ami.dataops.backup.restore.drive_client import DriveRestoreClient
from ami.dataops.backup.restore.revisions_client import RevisionsClient
from ami.dataops.backup.restore.service import BackupRestoreService


def main() -> int:
    """Main entry point for the backup restore application."""
    # Handle --help early before loading config
    if "-h" in sys.argv or "--help" in sys.argv:
        cli = RestoreCLI()
        cli.create_parser().print_help()
        return 0

    # Create all dependencies using dependency injection
    try:
        config = BackupRestoreConfig.load(Path.cwd())
        auth_manager = AuthenticationManager(config)
        drive_client = DriveRestoreClient(auth_manager)

        # Service uses functional modules for extraction, only needs clients and auth
        service = BackupRestoreService(drive_client, auth_manager)
        revisions_client = RevisionsClient(auth_manager)

        cli = RestoreCLI(service, revisions_client=revisions_client)
        args = cli.parse_arguments(sys.argv[1:])

        # Run the async main
        return asyncio.run(cli.run(args))
    except Exception as e:
        logger.error(f"Application failed to start: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
