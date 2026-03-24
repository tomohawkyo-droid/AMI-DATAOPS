"""Tests for the Google Drive Revisions API client."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ami.dataops.backup.restore.revisions_client import RevisionsClient

SAMPLE_REVISION_ID = "r1abc"
SAMPLE_FILE_ID = "file123"
DOWNLOAD_COMPLETE_PCT = 100
EXPECTED_TWO_REVISIONS = 2


class TestRevisionsClientListRevisions:
    """Tests for RevisionsClient.list_revisions method."""

    @pytest.mark.asyncio
    async def test_returns_revisions_newest_first(self) -> None:
        """Test returns revisions in reverse order (newest first)."""
        auth = MagicMock()
        client = RevisionsClient(auth)

        mock_service = MagicMock()
        mock_revisions = MagicMock()
        mock_revisions.list.return_value.execute.return_value = {
            "revisions": [
                {"id": "r1", "modifiedTime": "2024-01-01T00:00:00"},
                {"id": "r2", "modifiedTime": "2024-02-01T00:00:00"},
            ]
        }
        mock_service.revisions.return_value = mock_revisions
        client._service = mock_service

        revisions = await client.list_revisions(SAMPLE_FILE_ID)

        assert len(revisions) == EXPECTED_TWO_REVISIONS
        assert revisions[0]["id"] == "r2"
        assert revisions[1]["id"] == "r1"

    @pytest.mark.asyncio
    async def test_returns_empty_on_no_revisions(self) -> None:
        """Test returns empty list when no revisions found."""
        auth = MagicMock()
        client = RevisionsClient(auth)

        mock_service = MagicMock()
        mock_revisions = MagicMock()
        mock_revisions.list.return_value.execute.return_value = {"revisions": []}
        mock_service.revisions.return_value = mock_revisions
        client._service = mock_service

        revisions = await client.list_revisions(SAMPLE_FILE_ID)

        assert revisions == []

    @pytest.mark.asyncio
    async def test_returns_empty_on_api_error(self) -> None:
        """Test returns empty list on API error."""
        auth = MagicMock()
        client = RevisionsClient(auth)

        mock_service = MagicMock()
        mock_revisions = MagicMock()
        mock_revisions.list.return_value.execute.side_effect = RuntimeError("API error")
        mock_service.revisions.return_value = mock_revisions
        client._service = mock_service

        revisions = await client.list_revisions(SAMPLE_FILE_ID)

        assert revisions == []

    @pytest.mark.asyncio
    async def test_returns_empty_on_null_revisions(self) -> None:
        """Test returns empty list when revisions key is None."""
        auth = MagicMock()
        client = RevisionsClient(auth)

        mock_service = MagicMock()
        mock_revisions = MagicMock()
        mock_revisions.list.return_value.execute.return_value = {"revisions": None}
        mock_service.revisions.return_value = mock_revisions
        client._service = mock_service

        revisions = await client.list_revisions(SAMPLE_FILE_ID)

        assert revisions == []

    @pytest.mark.asyncio
    async def test_populates_all_fields(self) -> None:
        """Test all revision fields are populated."""
        auth = MagicMock()
        client = RevisionsClient(auth)

        mock_service = MagicMock()
        mock_revisions = MagicMock()
        mock_revisions.list.return_value.execute.return_value = {
            "revisions": [
                {
                    "id": SAMPLE_REVISION_ID,
                    "modifiedTime": "2024-03-15T10:00:00",
                    "size": "1048576",
                    "originalFilename": "backup.tar.zst",
                    "keepForever": True,
                }
            ]
        }
        mock_service.revisions.return_value = mock_revisions
        client._service = mock_service

        revisions = await client.list_revisions(SAMPLE_FILE_ID)

        assert len(revisions) == 1
        rev = revisions[0]
        assert rev["id"] == SAMPLE_REVISION_ID
        assert rev["size"] == "1048576"
        assert rev["originalFilename"] == "backup.tar.zst"
        assert rev["keepForever"] is True


class TestRevisionsClientDownloadRevision:
    """Tests for RevisionsClient.download_revision method."""

    @pytest.mark.asyncio
    async def test_download_success(self, tmp_path: Path) -> None:
        """Test successful revision download."""
        auth = MagicMock()
        client = RevisionsClient(auth)

        mock_service = MagicMock()
        client._service = mock_service

        dest = tmp_path / "downloaded.tar.zst"

        with patch(
            "ami.dataops.backup.restore.revisions_client.MediaIoBaseDownload"
        ) as mock_dl_cls:
            mock_dl = MagicMock()
            mock_dl.next_chunk.return_value = (None, True)
            mock_dl_cls.return_value = mock_dl

            result = await client.download_revision(
                SAMPLE_FILE_ID, SAMPLE_REVISION_ID, dest
            )

        assert result is True

    @pytest.mark.asyncio
    async def test_download_failure(self, tmp_path: Path) -> None:
        """Test download returns False on error."""
        auth = MagicMock()
        client = RevisionsClient(auth)

        mock_service = MagicMock()
        mock_service.revisions.return_value.get_media.side_effect = RuntimeError(
            "Download failed"
        )
        client._service = mock_service

        dest = tmp_path / "fail.tar.zst"
        result = await client.download_revision(
            SAMPLE_FILE_ID, SAMPLE_REVISION_ID, dest
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        """Test creates parent directories for destination."""
        auth = MagicMock()
        client = RevisionsClient(auth)

        mock_service = MagicMock()
        client._service = mock_service

        dest = tmp_path / "nested" / "dir" / "file.tar.zst"

        with patch(
            "ami.dataops.backup.restore.revisions_client.MediaIoBaseDownload"
        ) as mock_dl_cls:
            mock_dl = MagicMock()
            mock_dl.next_chunk.return_value = (None, True)
            mock_dl_cls.return_value = mock_dl

            await client.download_revision(SAMPLE_FILE_ID, SAMPLE_REVISION_ID, dest)

        assert dest.parent.exists()


class TestRevisionsClientGetService:
    """Tests for RevisionsClient._get_service method."""

    @pytest.mark.asyncio
    async def test_builds_service_on_first_call(self) -> None:
        """Test service is built on first access."""
        auth = MagicMock()
        auth.get_credentials.return_value = MagicMock()
        client = RevisionsClient(auth)

        with patch("ami.dataops.backup.restore.revisions_client.build") as mock_build:
            mock_build.return_value = MagicMock()
            service = await client._get_service()

        assert service is not None
        mock_build.assert_called_once_with(
            "drive", "v3", credentials=auth.get_credentials()
        )

    @pytest.mark.asyncio
    async def test_reuses_cached_service(self) -> None:
        """Test service is cached after first build."""
        auth = MagicMock()
        client = RevisionsClient(auth)
        mock_service = MagicMock()
        client._service = mock_service

        service = await client._get_service()

        assert service is mock_service
