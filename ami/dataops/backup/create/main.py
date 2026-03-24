"""Main entry point for backup creation operations.

Sets up dependency injection and runs the backup creation application.
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
    # Fallback if we can't import paths yet (e.g. running from odd location)
    script_path = Path(__file__) if "__file__" in globals() else Path(sys.argv[0])
    _repo_root = next(
        (p for p in script_path.resolve().parents if (p / "base").exists()), None
    )
    if _repo_root:
        sys.path.insert(0, str(_repo_root))

from ami.dataops.backup.backup_config import BackupConfig
from ami.dataops.backup.common.auth import AuthenticationManager
from ami.dataops.backup.create.cli import BackupCLI
from ami.dataops.backup.create.service import BackupService
from ami.dataops.backup.create.uploader import BackupUploader


def main() -> int:
    """Main entry point for the backup creation application."""
    # Handle --help early before loading config
    if "-h" in sys.argv or "--help" in sys.argv:
        cli = BackupCLI()
        cli.create_parser().print_help()
        return 0

    # Create all dependencies using dependency injection
    try:
        config = BackupConfig.load(Path.cwd())
        auth_manager = AuthenticationManager(config)
        uploader = BackupUploader(auth_manager)  # Uses auth manager

        # Service now uses functional modules internally, only needs uploader and auth
        service = BackupService(uploader, auth_manager)

        cli = BackupCLI(service)
        args = cli.parse_arguments(sys.argv[1:])

        # Run the async main
        return asyncio.run(cli.run(args))
    except Exception as e:
        logger.error(f"Application failed to start: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
