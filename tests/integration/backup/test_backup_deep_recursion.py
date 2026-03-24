"""Integration test to trigger 'object of type cell has no len()' error.

REPRODUCTION ATTEMPT 2: Injecting circular symlinks and special files.

Note: Path setup is handled by tests/conftest.py.
"""

import asyncio
import contextlib
import os
import socket
import tempfile
from pathlib import Path

import pytest

from ami.dataops.backup.create import archiver


@pytest.mark.asyncio
async def test_backup_stress_recursion_and_logging():
    """
    Stress test with circular symlinks and special files.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        base_dir = Path(temp_dir)
        project_root = base_dir / "stress_project"
        project_root.mkdir()

        # Create standard structure
        (project_root / "normal.txt").write_text("normal")

        # 1. Circular Symlink (The Loop of Death)
        # link 'loop' points to 'project_root' (itself)
        # os.walk(follow_links=False) handles this, but if True...
        loop_link = project_root / "loop"
        os.symlink(project_root, loop_link)

        # 2. FIFO (Named Pipe) - Windows does not support mkfifo
        fifo_path = project_root / "test_fifo"
        with contextlib.suppress(AttributeError):
            os.mkfifo(fifo_path)

        # 3. Socket - Windows or no perms
        sock_path = project_root / "test.sock"
        with contextlib.suppress(AttributeError, OSError):
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.bind(str(sock_path))

        print(f"\n[Test] Created hostile structure at {project_root}")

        # Run the archive creation
        print("[Test] Starting create_zip_archive...")
        try:
            # We use a real output dir
            output_dir = base_dir

            # This should trigger the walker
            archive_path = await archiver.create_zip_archive(
                project_root, output_dir=output_dir, ignore_exclusions=False
            )

            print(f"[Test] Archive created at: {archive_path}")
            assert archive_path.exists()

        except Exception as e:
            print(f"[Test] Failed with error: {e}")
            raise
        finally:
            if "s" in locals():
                s.close()


if __name__ == "__main__":
    asyncio.run(test_backup_stress_recursion_and_logging())
