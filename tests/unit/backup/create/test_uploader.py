"""Unit tests for the backup uploader module (create/uploader.py)."""

from unittest.mock import MagicMock, patch

import pytest

from ami.dataops.backup.backup_exceptions import UploadError
from ami.dataops.backup.create.uploader import BackupUploader


class TestBackupUploader:
    """Unit tests for the BackupUploader class."""

    def test_initialization(self):
        """Test that BackupUploader initializes correctly with auth_manager."""
        mock_auth_manager = MagicMock()
        uploader = BackupUploader(mock_auth_manager)

        assert uploader.auth_manager == mock_auth_manager
        assert uploader._service is None

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.uploader.build")
    async def test_get_service_creates_google_service(self, mock_build):
        """Test that _get_service creates the Google Drive service."""
        mock_auth_manager = MagicMock()
        mock_credentials = MagicMock()
        mock_auth_manager.get_credentials.return_value = mock_credentials
        mock_service = MagicMock()
        mock_build.return_value = mock_service

        uploader = BackupUploader(mock_auth_manager)

        # Call the async method
        service = await uploader._get_service()

        assert service == mock_service
        mock_build.assert_called_once_with("drive", "v3", credentials=mock_credentials)
        # Verify it's cached
        assert uploader._service == mock_service

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.uploader.MediaFileUpload")
    @patch("ami.dataops.backup.create.uploader.build")
    async def test_upload_to_gdrive_new_file(
        self, mock_build, mock_media_upload, tmp_path
    ):
        """Test uploading a file that doesn't already exist."""
        # Setup mocks
        mock_auth_manager = MagicMock()
        mock_credentials = MagicMock()
        mock_auth_manager.get_credentials.return_value = mock_credentials

        # Setup the service mock chain
        mock_service = MagicMock()
        mock_drive_files = MagicMock()
        mock_service.files.return_value = mock_drive_files

        # Setup list (search) response - no existing files
        mock_list_request = MagicMock()
        mock_drive_files.list.return_value = mock_list_request
        mock_list_request.execute.return_value = {"files": []}

        # Setup create response - next_chunk returns (status, response)
        mock_create_request = MagicMock()
        mock_drive_files.create.return_value = mock_create_request
        mock_create_request.next_chunk.return_value = (
            None,
            {
                "id": "test_file_id_123",
                "name": "test-archive.tar.zst",
                "webViewLink": "https://drive.google.com/file/d/test_file_id_123/view",
            },
        )

        mock_build.return_value = mock_service

        # Create uploader and config
        uploader = BackupUploader(mock_auth_manager)
        config = MagicMock()
        config.folder_id = "test_folder_id"
        zip_path = tmp_path / "test-archive.tar.zst"
        zip_path.write_bytes(b"fake archive data")

        # Perform upload
        file_id = await uploader.upload_to_gdrive(zip_path, config)

        # Verify the results
        assert file_id == "test_file_id_123"
        mock_drive_files.create.assert_called_once()

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.uploader.MediaFileUpload")
    @patch("ami.dataops.backup.create.uploader.build")
    async def test_upload_to_gdrive_existing_file_update(
        self, mock_build, mock_media_upload, tmp_path
    ):
        """Test uploading when a file with same name exists.

        Verifies that the existing file gets updated.
        """
        # Setup mocks
        mock_auth_manager = MagicMock()
        mock_credentials = MagicMock()
        mock_auth_manager.get_credentials.return_value = mock_credentials

        mock_service = MagicMock()
        mock_drive_files = MagicMock()
        mock_service.files.return_value = mock_drive_files

        # Setup list to return an existing file
        mock_list_request = MagicMock()
        mock_drive_files.list.return_value = mock_list_request
        mock_list_request.execute.return_value = {
            "files": [{"id": "existing_file_id_456", "name": "test-archive.tar.zst"}]
        }

        # Setup update response - next_chunk returns (status, response)
        mock_update_request = MagicMock()
        mock_drive_files.update.return_value = mock_update_request
        mock_update_request.next_chunk.return_value = (
            None,
            {
                "id": "existing_file_id_456",
                "name": "test-archive.tar.zst",
                "webViewLink": "https://drive.google.com/file/d/existing_file_id_456/view",
            },
        )

        mock_build.return_value = mock_service

        # Create uploader and config
        uploader = BackupUploader(mock_auth_manager)
        config = MagicMock()
        config.folder_id = "test_folder_id"
        zip_path = tmp_path / "test-archive.tar.zst"
        zip_path.write_bytes(b"fake archive data")

        # Perform upload
        file_id = await uploader.upload_to_gdrive(zip_path, config)

        # Verify the results - should have updated existing file
        assert file_id == "existing_file_id_456"
        mock_drive_files.update.assert_called_once()
        mock_drive_files.create.assert_not_called()

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.uploader.MediaFileUpload")
    @patch("ami.dataops.backup.create.uploader.build")
    async def test_upload_to_gdrive_error_handling(
        self, mock_build, mock_media_upload, tmp_path
    ):
        """Test that upload handles errors properly."""
        # Setup mocks
        mock_auth_manager = MagicMock()
        mock_credentials = MagicMock()
        mock_auth_manager.get_credentials.return_value = mock_credentials

        mock_service = MagicMock()
        mock_drive_files = MagicMock()
        mock_service.files.return_value = mock_drive_files

        # Setup list to return no existing files
        mock_list_request = MagicMock()
        mock_drive_files.list.return_value = mock_list_request
        mock_list_request.execute.return_value = {"files": []}

        # Make create operation raise an exception via next_chunk
        mock_create_request = MagicMock()
        mock_drive_files.create.return_value = mock_create_request
        mock_create_request.next_chunk.side_effect = Exception("API Error")

        mock_build.return_value = mock_service

        # Create uploader and config
        uploader = BackupUploader(mock_auth_manager)
        config = MagicMock()
        config.folder_id = "test_folder_id"
        zip_path = tmp_path / "test-archive.tar.zst"
        zip_path.write_bytes(b"fake archive data")

        # Should raise UploadError
        with pytest.raises(UploadError, match="Upload failed:"):
            await uploader.upload_to_gdrive(zip_path, config)

    @pytest.mark.asyncio
    @patch("ami.dataops.backup.create.uploader.MediaFileUpload")
    @patch("ami.dataops.backup.create.uploader.build")
    async def test_upload_to_gdrive_no_file_id_returned(
        self, mock_build, mock_media_upload, tmp_path
    ):
        """Test that upload raises error when no file ID is returned."""
        # Setup mocks
        mock_auth_manager = MagicMock()
        mock_credentials = MagicMock()
        mock_auth_manager.get_credentials.return_value = mock_credentials

        mock_service = MagicMock()
        mock_drive_files = MagicMock()
        mock_service.files.return_value = mock_drive_files

        # Setup list to return no existing files
        mock_list_request = MagicMock()
        mock_drive_files.list.return_value = mock_list_request
        mock_list_request.execute.return_value = {"files": []}

        # Setup create response with no ID - next_chunk returns (status, response)
        mock_create_request = MagicMock()
        mock_drive_files.create.return_value = mock_create_request
        mock_create_request.next_chunk.return_value = (
            None,
            {"name": "test-archive.tar.zst"},  # No ID field
        )

        mock_build.return_value = mock_service

        # Create uploader and config
        uploader = BackupUploader(mock_auth_manager)
        config = MagicMock()
        config.folder_id = "test_folder_id"
        zip_path = tmp_path / "test-archive.tar.zst"
        zip_path.write_bytes(b"fake archive data")

        # Should raise UploadError when no file ID is returned
        with pytest.raises(
            UploadError, match="Upload succeeded but no file ID returned"
        ):
            await uploader.upload_to_gdrive(zip_path, config)
