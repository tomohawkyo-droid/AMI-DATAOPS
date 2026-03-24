"""
Backup uploader module.

Handles uploading archives to Google Drive using configured authentication.
"""

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple, Protocol, cast

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from loguru import logger
from tqdm import tqdm

from ami.dataops.backup.backup_config import BackupConfig
from ami.dataops.backup.backup_exceptions import UploadError
from ami.dataops.backup.types import DriveFileResponse

if TYPE_CHECKING:
    from ami.dataops.backup.common.auth import AuthenticationManager


class DriveFilesResource(Protocol):
    """Protocol for Google Drive files() resource."""

    def list(self, **kwargs: object) -> "DriveRequest":
        """List files."""
        ...

    def create(self, **kwargs: object) -> "DriveRequest":
        """Create file."""
        ...

    def update(self, **kwargs: object) -> "DriveRequest":
        """Update file."""
        ...


class UploadProgress(Protocol):
    """Protocol for upload progress status."""

    def progress(self) -> float:
        """Return upload progress as a float between 0.0 and 1.0."""
        ...


class ChunkResult(NamedTuple):
    """Result from a resumable upload chunk."""

    status: UploadProgress | None
    response: DriveFileResponse | None


class DriveRequest(Protocol):
    """Protocol for Google Drive request objects."""

    def execute(self) -> DriveFileResponse:
        """Execute the request."""
        ...

    def next_chunk(self) -> ChunkResult:
        """Execute the next chunk of a resumable upload."""
        ...


class DriveService(Protocol):
    """Protocol for Google Drive service."""

    def files(self) -> DriveFilesResource:
        """Get files resource."""
        ...


class BackupUploader:
    """Uploads backup archives to Google Drive."""

    def __init__(self, auth_manager: "AuthenticationManager") -> None:
        self.auth_manager = auth_manager
        self._service: DriveService | None = None

    async def _get_service(self) -> DriveService:
        """Get or create the Google Drive service client."""
        if self._service is None:
            credentials = self.auth_manager.get_credentials()
            self._service = cast(
                DriveService, build("drive", "v3", credentials=credentials)
            )
        return self._service

    def _chunked_upload(
        self, request: DriveRequest, total_size: int
    ) -> DriveFileResponse:
        """Execute resumable upload with tqdm progress bar."""
        response = None
        with tqdm(
            total=total_size, unit="B", unit_scale=True, desc="Uploading"
        ) as pbar:
            while response is None:
                status, response = request.next_chunk()
                if status:
                    pbar.update(int(status.progress() * total_size) - pbar.n)
            pbar.update(pbar.total - pbar.n)  # ensure 100%
        return response

    async def _search_existing_file(
        self, service: DriveService, search_query: str
    ) -> str | None:
        """Search for an existing file and return its ID if found."""
        try:
            results = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: (
                    service.files()
                    .list(
                        q=search_query,
                        spaces="drive",
                        fields="files(id, name)",
                        includeItemsFromAllDrives=True,
                        supportsAllDrives=True,
                    )
                    .execute()
                ),
            )

            files = results.get("files")
            if isinstance(files, list) and files and isinstance(files[0], dict):
                file_id_val = files[0].get("id")
                if isinstance(file_id_val, str):
                    return file_id_val
        except Exception as e:
            logger.warning(f"File search failed, proceeding with upload: {e}")
        return None

    async def upload_to_gdrive(self, zip_path: Path, config: BackupConfig) -> str:
        """
        Upload archive file to Google Drive using configured authentication.

        Args:
            zip_path: Path to archive file to upload
            config: Backup configuration with auth method

        Returns:
            Google Drive file ID

        Raises:
            UploadError: If upload fails
        """
        try:
            service = await self._get_service()

            # Build search query for existing files
            search_query = f"name = '{zip_path.name}' and trashed = false"
            if config.folder_id:
                search_query += f" and '{config.folder_id}' in parents"

            # Build file metadata - include optional parents if configured
            if config.folder_id:
                file_metadata = {"name": zip_path.name, "parents": [config.folder_id]}
            else:
                file_metadata = {"name": zip_path.name}

            existing_file_id = await self._search_existing_file(service, search_query)

            # Upload with resumable flag for large files
            media = MediaFileUpload(
                str(zip_path),
                mimetype="application/zstd",
                resumable=True,
            )
            total_size = zip_path.stat().st_size

            if existing_file_id:
                file = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self._chunked_upload(
                        service.files().update(
                            fileId=existing_file_id,
                            media_body=media,
                            fields="id,name,webViewLink",
                            supportsAllDrives=True,
                        ),
                        total_size,
                    ),
                )
            else:
                file = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self._chunked_upload(
                        service.files().create(
                            body=file_metadata,
                            media_body=media,
                            fields="id,name,webViewLink",
                            supportsAllDrives=True,
                        ),
                        total_size,
                    ),
                )

        except Exception as e:
            msg = f"Upload failed: {e}"
            raise UploadError(msg) from e

        raw_file_id = file.get("id")
        if not raw_file_id or not isinstance(raw_file_id, str):
            msg = "Upload succeeded but no file ID returned"
            raise UploadError(msg)
        file_id: str = raw_file_id

        # Log success
        logger.info("✓ Upload complete")
        logger.info(f"  File ID: {file_id}")
        if file.get("name"):
            logger.info(f"  Name: {file.get('name')}")
        if file.get("webViewLink"):
            logger.info(f"  Link: {file.get('webViewLink')}")

        return file_id
