"""
Google Drive client for restore operations.

Handles listing and downloading backup files from Google Drive.
"""

import asyncio
from pathlib import Path
from typing import Protocol, TypedDict, cast

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from loguru import logger
from tqdm import tqdm

from ami.dataops.backup.backup_config import BackupConfig
from ami.dataops.backup.common.auth import AuthenticationManager
from ami.dataops.backup.common.constants import DEFAULT_BACKUP_PATTERN
from ami.dataops.backup.types import DriveListResponse


class DriveFileMetadata(TypedDict, total=False):
    """Metadata for a file in Google Drive."""

    id: str
    name: str
    modifiedTime: str
    size: str


class DriveFilesResource(Protocol):
    """Protocol for Google Drive files() resource."""

    def list(self, **kwargs: object) -> "DriveRequest":
        """List files."""
        ...

    def get(self, **kwargs: object) -> "DriveRequest":
        """Get file metadata."""
        ...

    def get_media(self, **kwargs: object) -> "DriveRequest":
        """Get file media content."""
        ...


class DriveRequest(Protocol):
    """Protocol for Google Drive request objects."""

    def execute(self) -> DriveListResponse:
        """Execute the request."""
        ...


class DriveService(Protocol):
    """Protocol for Google Drive service."""

    def files(self) -> DriveFilesResource:
        """Get files resource."""
        ...


class DriveRestoreClient:
    """Client for interacting with Google Drive for restore operations."""

    def __init__(self, auth_manager: AuthenticationManager) -> None:
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

    async def list_backup_files(self, config: BackupConfig) -> list[DriveFileMetadata]:
        """
        List all backup files from Google Drive.

        Args:
            config: Backup configuration

        Returns:
            List of backup file metadata dicts with 'id', 'name',
            'modifiedTime', and 'size' keys
        """
        try:
            service = await self._get_service()

            # Build search query to find backup files
            search_query = (
                f"name contains '{DEFAULT_BACKUP_PATTERN}' and trashed = false"
            )
            if config.folder_id:
                search_query += f" and '{config.folder_id}' in parents"

            # Search for backup files, ordered by modification time (newest first)
            results = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: (
                    service.files()
                    .list(
                        q=search_query,
                        spaces="drive",
                        fields="files(id, name, modifiedTime, size)",
                        orderBy="modifiedTime desc",
                        includeItemsFromAllDrives=True,
                        supportsAllDrives=True,
                    )
                    .execute()
                ),
            )

            raw_files = results.get("files")
            if not raw_files or not isinstance(raw_files, list):
                logger.warning("No backup files found in Google Drive")
                return []
            # Convert raw API result to typed list
            files: list[DriveFileMetadata] = [
                DriveFileMetadata(
                    id=str(item.get("id", "")),
                    name=str(item.get("name", "")),
                    modifiedTime=str(item.get("modifiedTime", "")),
                    size=str(item.get("size", "")),
                )
                for item in raw_files
                if isinstance(item, dict)
            ]
        except Exception as e:
            logger.error(f"Error fetching backup files from Google Drive: {e}")
            return []
        else:
            return files

    async def download_file(
        self, file_id: str, destination: Path, config: BackupConfig
    ) -> bool:
        """
        Download a file from Google Drive.

        Args:
            file_id: Google Drive file ID
            destination: Local path to save the file
            config: Backup configuration

        Returns:
            True if download was successful, False otherwise
        """
        try:
            service = await self._get_service()

            # Get file metadata first to verify it's the backup file
            file_metadata = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: (
                    service.files()
                    .get(fileId=file_id, supportsAllDrives=True)
                    .execute()
                ),
            )

            name = file_metadata.get("name", "Unknown")
            size = file_metadata.get("size", "Unknown")
            total = int(str(size)) if size != "Unknown" else None

            # Create destination directory if it doesn't exist
            destination.parent.mkdir(parents=True, exist_ok=True)

            # Download the file
            request = service.files().get_media(fileId=file_id, supportsAllDrives=True)

            with open(destination, "wb") as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                with tqdm(
                    total=total, unit="B", unit_scale=True, desc=f"Downloading {name}"
                ) as pbar:
                    while done is False:
                        status, done = downloader.next_chunk()
                        if status and total:
                            pbar.update(int(status.progress() * total) - pbar.n)
                    if total:
                        pbar.update(pbar.total - pbar.n)
        except Exception as e:
            logger.error(f"Download from Google Drive failed: {e}")
            return False
        else:
            return True

    async def get_file_metadata(self, file_id: str) -> DriveFileMetadata | None:
        """
        Get metadata for a specific file in Google Drive.

        Args:
            file_id: Google Drive file ID

        Returns:
            File metadata dictionary or None if not found/error
        """
        try:
            service = await self._get_service()

            raw_metadata = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: (
                    service.files()
                    .get(fileId=file_id, supportsAllDrives=True)
                    .execute()
                ),
            )
            if not isinstance(raw_metadata, dict):
                return None
            return DriveFileMetadata(
                id=str(raw_metadata.get("id", "")),
                name=str(raw_metadata.get("name", "")),
                modifiedTime=str(raw_metadata.get("modifiedTime", "")),
                size=str(raw_metadata.get("size", "")),
            )
        except Exception as e:
            logger.error(f"Failed to get file metadata: {e}")
            return None

    async def verify_backup_exists(self, file_id: str) -> bool:
        """
        Verify that a specific backup file exists in Google Drive.

        Args:
            file_id: Google Drive file ID to check

        Returns:
            True if file exists, False otherwise
        """
        try:
            metadata = await self.get_file_metadata(file_id)
        except Exception:
            return False
        else:
            return metadata is not None
