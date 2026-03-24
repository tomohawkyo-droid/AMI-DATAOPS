"""Unit tests for backup/restore/selector module."""

from unittest.mock import patch

from ami.dataops.backup.restore.drive_client import DriveFileMetadata
from ami.dataops.backup.restore.selector import select_backup_interactive


class TestSelectBackupInteractive:
    """Tests for select_backup_interactive function."""

    def test_returns_none_for_empty_list(self) -> None:
        """Test returns None for empty backup list."""
        result = select_backup_interactive([])
        assert result is None

    @patch("builtins.input", return_value="q")
    def test_returns_none_on_cancel(self, mock_input, capsys) -> None:
        """Test returns None when user cancels with 'q'."""
        backup_files: list[DriveFileMetadata] = [
            {
                "id": "file1",
                "name": "backup1.tar.zst",
                "modifiedTime": "2024-01-01T00:00:00",
                "size": "1024",
            }
        ]

        result = select_backup_interactive(backup_files)

        assert result is None

    @patch("builtins.input", return_value="0")
    def test_returns_file_id_for_valid_selection(self, mock_input, capsys) -> None:
        """Test returns file ID for valid selection."""
        backup_files: list[DriveFileMetadata] = [
            {
                "id": "file1",
                "name": "backup1.tar.zst",
                "modifiedTime": "2024-01-01T00:00:00",
                "size": "1024",
            },
            {
                "id": "file2",
                "name": "backup2.tar.zst",
                "modifiedTime": "2024-01-02T00:00:00",
                "size": "2048",
            },
        ]

        result = select_backup_interactive(backup_files)

        assert result == "file1"

    @patch("builtins.input", return_value="1")
    def test_selects_second_backup(self, mock_input, capsys) -> None:
        """Test selects second backup when user enters 1."""
        backup_files: list[DriveFileMetadata] = [
            {
                "id": "file1",
                "name": "backup1.tar.zst",
                "modifiedTime": "2024-01-01T00:00:00",
                "size": "1024",
            },
            {
                "id": "file2",
                "name": "backup2.tar.zst",
                "modifiedTime": "2024-01-02T00:00:00",
                "size": "2048",
            },
        ]

        result = select_backup_interactive(backup_files)

        assert result == "file2"

    @patch("builtins.input", side_effect=["invalid", "0"])
    def test_reprompts_on_invalid_input(self, mock_input, capsys) -> None:
        """Test reprompts when invalid input given."""
        backup_files: list[DriveFileMetadata] = [
            {
                "id": "file1",
                "name": "backup1.tar.zst",
                "modifiedTime": "2024-01-01T00:00:00",
                "size": "1024",
            }
        ]

        result = select_backup_interactive(backup_files)

        assert result == "file1"
        captured = capsys.readouterr()
        assert "Invalid input" in captured.out

    @patch("builtins.input", side_effect=["99", "0"])
    def test_reprompts_on_out_of_range(self, mock_input, capsys) -> None:
        """Test reprompts when selection out of range."""
        backup_files: list[DriveFileMetadata] = [
            {
                "id": "file1",
                "name": "backup1.tar.zst",
                "modifiedTime": "2024-01-01T00:00:00",
                "size": "1024",
            }
        ]

        result = select_backup_interactive(backup_files)

        assert result == "file1"
        captured = capsys.readouterr()
        assert "Invalid selection" in captured.out

    @patch("builtins.input", side_effect=KeyboardInterrupt)
    def test_returns_none_on_keyboard_interrupt(self, mock_input) -> None:
        """Test returns None on KeyboardInterrupt."""
        backup_files: list[DriveFileMetadata] = [
            {
                "id": "file1",
                "name": "backup1.tar.zst",
                "modifiedTime": "2024-01-01T00:00:00",
                "size": "1024",
            }
        ]

        result = select_backup_interactive(backup_files)

        assert result is None

    @patch("builtins.input", side_effect=EOFError)
    def test_returns_none_on_eof(self, mock_input) -> None:
        """Test returns None on EOFError."""
        backup_files: list[DriveFileMetadata] = [
            {
                "id": "file1",
                "name": "backup1.tar.zst",
                "modifiedTime": "2024-01-01T00:00:00",
                "size": "1024",
            }
        ]

        result = select_backup_interactive(backup_files)

        assert result is None

    @patch("builtins.input", return_value="0")
    def test_prints_backup_info(self, mock_input, capsys) -> None:
        """Test prints backup information."""
        backup_files: list[DriveFileMetadata] = [
            {
                "id": "file1",
                "name": "backup1.tar.zst",
                "modifiedTime": "2024-01-01T00:00:00.000Z",
                "size": "1024",
            }
        ]

        select_backup_interactive(backup_files)

        captured = capsys.readouterr()
        assert "backup1.tar.zst" in captured.out
        assert "2024-01-01" in captured.out
        assert "1024" in captured.out

    @patch("builtins.input", return_value="0")
    def test_handles_missing_fields(self, mock_input, capsys) -> None:
        """Test handles missing fields in backup metadata."""
        backup_files: list[DriveFileMetadata] = [
            {"id": "file1"}  # Missing name, modifiedTime, size
        ]

        result = select_backup_interactive(backup_files)

        assert result == "file1"
        captured = capsys.readouterr()
        assert "Unknown" in captured.out

    @patch("builtins.input", return_value="Q")
    def test_cancel_case_insensitive(self, mock_input) -> None:
        """Test cancel works with uppercase Q."""
        backup_files: list[DriveFileMetadata] = [
            {
                "id": "file1",
                "name": "backup1.tar.zst",
                "modifiedTime": "2024-01-01T00:00:00",
                "size": "1024",
            }
        ]

        result = select_backup_interactive(backup_files)

        assert result is None
