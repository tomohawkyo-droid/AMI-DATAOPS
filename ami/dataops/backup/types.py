"""Type definitions for backup/restore operations.

Google Drive API response types used by the GDrive backup mode.
"""

from typing_extensions import TypedDict


class DriveFileResponse(TypedDict, total=False):
    """Response from Google Drive file operations."""

    id: str
    name: str
    webViewLink: str
    mimeType: str
    size: str
    createdTime: str
    modifiedTime: str


class DriveListResponse(TypedDict, total=False):
    """Response from Google Drive list operations."""

    files: list[DriveFileResponse]
    nextPageToken: str


class DriveRevisionInfo(TypedDict, total=False):
    """Metadata for a Google Drive file revision."""

    id: str
    modifiedTime: str
    size: str
    originalFilename: str
    keepForever: bool


class DriveRevisionListResponse(TypedDict, total=False):
    """Response from Google Drive revisions.list()."""

    revisions: list[DriveRevisionInfo]
