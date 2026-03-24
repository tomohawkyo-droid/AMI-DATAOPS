"""Tests for revision display formatting."""

from ami.dataops.backup.restore.revision_display import display_revision_list
from ami.dataops.backup.types import DriveRevisionInfo


class TestDisplayRevisionList:
    """Tests for display_revision_list function."""

    def test_displays_single_revision(self, capsys) -> None:
        """Test displays a single revision."""
        revisions: list[DriveRevisionInfo] = [
            DriveRevisionInfo(
                id="rev1",
                modifiedTime="2024-01-15T10:30:00",
                size="1048576",
                keepForever=False,
            )
        ]
        display_revision_list("backup.tar.zst", revisions)

        captured = capsys.readouterr()
        assert "backup.tar.zst" in captured.out
        assert "2024-01-15T10:30:00" in captured.out
        assert "Latest" in captured.out
        assert "1 revision(s) available" in captured.out

    def test_displays_multiple_revisions(self, capsys) -> None:
        """Test displays multiple revisions with correct labels."""
        revisions: list[DriveRevisionInfo] = [
            DriveRevisionInfo(
                id="rev2",
                modifiedTime="2024-02-01T00:00:00",
                size="2097152",
            ),
            DriveRevisionInfo(
                id="rev1",
                modifiedTime="2024-01-01T00:00:00",
                size="1048576",
            ),
        ]
        display_revision_list("backup.tar.zst", revisions)

        captured = capsys.readouterr()
        assert "Latest" in captured.out
        assert "Rev ~1" in captured.out
        assert "2 revision(s) available" in captured.out

    def test_displays_empty_revisions(self, capsys) -> None:
        """Test displays message when no revisions."""
        display_revision_list("backup.tar.zst", [])

        captured = capsys.readouterr()
        assert "No revisions found" in captured.out

    def test_displays_kept_forever_marker(self, capsys) -> None:
        """Test shows kept marker for pinned revisions."""
        revisions: list[DriveRevisionInfo] = [
            DriveRevisionInfo(
                id="rev1",
                modifiedTime="2024-01-01T00:00:00",
                size="1024",
                keepForever=True,
            )
        ]
        display_revision_list("backup.tar.zst", revisions)

        captured = capsys.readouterr()
        assert "[kept]" in captured.out

    def test_displays_revision_ids(self, capsys) -> None:
        """Test displays revision IDs."""
        revisions: list[DriveRevisionInfo] = [
            DriveRevisionInfo(
                id="abc123def456",
                modifiedTime="2024-01-01T00:00:00",
                size="0",
            )
        ]
        display_revision_list("test.tar.zst", revisions)

        captured = capsys.readouterr()
        assert "abc123def456" in captured.out
