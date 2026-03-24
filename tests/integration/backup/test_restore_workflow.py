"""
Integration test for the backup restore workflow.
Verifies that archives created by the system can be correctly restored.
"""

import asyncio
import os
import tempfile
from pathlib import Path

import pytest

from ami.dataops.backup.create import archiver
from ami.dataops.backup.restore import extractor

# Test constants
EXPECTED_BINARY_FILE_SIZE = 1024


@pytest.mark.asyncio
async def test_full_restore_workflow():
    """
    Test the full restore process:
    1. Create a dummy structure.
    2. Archive it using the actual archiver.
    3. Restore it using the actual extractor.
    4. Verify integrity.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        base_dir = Path(temp_dir)
        original_dir = base_dir / "original"
        restore_dir = base_dir / "restored"
        original_dir.mkdir()

        # Populate original directory
        (original_dir / "file1.txt").write_text("content 1")
        (original_dir / "subdir").mkdir()
        (original_dir / "subdir" / "file2.txt").write_text("content 2")

        # Create binary file
        with open(original_dir / "data.bin", "wb") as f:
            f.write(os.urandom(1024))  # 1KB binary

        print(f"\n[Test] Created original structure at {original_dir}")

        # 1. Create Archive
        print("[Test] Archiving...")
        archive_path = await archiver.create_zip_archive(
            original_dir, output_dir=base_dir
        )
        print(f"[Test] Archive created at: {archive_path}")

        assert archive_path.exists()

        # 2. List Contents
        print("[Test] Listing archive contents...")
        contents = await extractor.list_archive_contents(archive_path)
        print(f"[Test] Contents: {contents}")

        # Check expected files are in the list (names might be relative)
        # archiver usually stores paths relative to root_dir
        # so we expect "file1.txt", "subdir/file2.txt"
        assert any("file1.txt" in f for f in contents)
        assert any("subdir/file2.txt" in f for f in contents)

        # 3. Full Restore
        print(f"[Test] Restoring to {restore_dir}...")

        # We need to pass the list of paths to extract_specific_paths
        # In a full restore scenario, we'd list everything.
        paths_to_restore = [Path(p) for p in contents]

        success = await extractor.extract_specific_paths(
            archive_path, paths_to_restore, restore_dir
        )
        assert success is True

        # 4. Verification
        # Note: Archive preserves the root directory name ("original") in paths
        print("[Test] Verifying restored files...")
        assert (restore_dir / "original" / "file1.txt").exists()
        assert (restore_dir / "original" / "file1.txt").read_text() == "content 1"

        assert (restore_dir / "original" / "subdir" / "file2.txt").exists()
        assert (
            restore_dir / "original" / "subdir" / "file2.txt"
        ).read_text() == "content 2"

        assert (restore_dir / "original" / "data.bin").exists()
        assert (
            restore_dir / "original" / "data.bin"
        ).stat().st_size == EXPECTED_BINARY_FILE_SIZE

        print("[Test] Restore verification successful!")


if __name__ == "__main__":
    asyncio.run(test_full_restore_workflow())
