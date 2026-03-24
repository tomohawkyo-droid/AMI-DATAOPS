"""End-to-End integration test for backup creation.

MIRRORS USER WORKFLOW 1:1 AT SCALE.
Generates a large number of files to reproduce scale-based segfaults.

Note: Path setup is handled by tests/conftest.py.
"""

import asyncio
import contextlib
import os
import tempfile
from pathlib import Path

import pytest

from ami.dataops.backup.create import archiver


@pytest.mark.asyncio
async def test_backup_at_scale():
    """
    Reproduce the segfault by creating 300,000 files and running the archiver.
    Also includes broken symlinks and deep nesting.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        base_dir = Path(temp_dir)
        # We create the project root inside temp_dir to keep it clean
        project_root = base_dir / "large_project"
        project_root.mkdir()

        print(f"\n[Reproduction] Generating 300,000 files at {project_root}...")

        # 1. Create 300k files across many directories to stress the walker
        # Using a shallow but wide tree to avoid hitting standard recursion limits early
        for i in range(300):
            sub = project_root / f"dir_{i}"
            sub.mkdir()
            for j in range(1000):
                (sub / f"file_{j}.txt").write_text("shite")

        # 2. Add Hostile Files
        # Circular symlink
        os.symlink(project_root, project_root / "loop")

        # Broken symlink
        os.symlink(base_dir / "non-existent", project_root / "broken")

        # FIFO - Windows does not support mkfifo
        with contextlib.suppress(AttributeError):
            os.mkfifo(project_root / "pipe")

        print(f"[Reproduction] Starting archive of {project_root}...")

        try:
            # Mirror the exact call in ami-backup
            # Note: we use a real output dir in the temp space
            archive_path = await archiver.create_zip_archive(
                project_root, output_dir=base_dir
            )

            print(f"[Reproduction] Success! Archive created: {archive_path}")
            assert archive_path.exists()

        except Exception as e:
            print(f"[Reproduction] FAILED with error: {e}")
            raise


if __name__ == "__main__":
    asyncio.run(test_backup_at_scale())
