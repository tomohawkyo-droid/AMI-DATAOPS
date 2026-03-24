"""Integration test for the backup creation workflow.

Tests the archiver against the .git directory which provides a realistic
stress test without attempting to backup 280k+ files.

Note: Path setup is handled by tests/conftest.py.
"""

import asyncio
import tempfile
from pathlib import Path

import pytest

from ami.dataops.backup.create import archiver


@pytest.mark.asyncio
async def test_backup_workflow_on_git_directory():
    """
    Test the full backup creation process against the .git directory.
    This provides a realistic stress test with many small files and
    nested directories without the insane scale of the full repo.
    """
    git_dir = Path.cwd() / ".git"

    if not git_dir.exists():
        pytest.skip("No .git directory found")

    print(f"\n[Backup Test] Running backup against: {git_dir}")

    with tempfile.TemporaryDirectory() as temp_dir:
        output_dir = Path(temp_dir)

        print("[Backup Test] Starting create_zip_archive...")
        archive_path = await archiver.create_zip_archive(
            git_dir,
            output_dir=output_dir,
            ignore_exclusions=True,
        )

        print(f"[Backup Test] Archive created: {archive_path}")
        archive_size = archive_path.stat().st_size
        print(f"[Backup Test] Archive size: {archive_size / 1024 / 1024:.2f} MB")

        assert archive_path.exists()
        assert archive_size > 0


if __name__ == "__main__":
    asyncio.run(test_backup_workflow_on_git_directory())
