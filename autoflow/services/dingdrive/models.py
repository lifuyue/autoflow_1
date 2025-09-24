"""Domain models and exceptions for DingTalk Drive integration."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal


class DriveError(RuntimeError):
    """Base error raised for DingDrive failures."""

    def __init__(self, message: str, *, status_code: int | None = None, payload: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload or {}


class DriveAuthError(DriveError):
    """Raised when authentication with DingTalk Drive fails."""


class DriveNotFound(DriveError):
    """Raised when the requested resource cannot be located in DingTalk Drive."""


class DriveRetryableError(DriveError):
    """Raised for retryable I/O issues (network/server errors)."""


class DriveRequestError(DriveError):
    """Raised for non-retryable HTTP or protocol errors from DingTalk."""


@dataclass(slots=True)
class DriveItem:
    """Common representation for DingTalk Drive entities."""

    id: str
    name: str
    item_type: Literal["file", "folder"]
    parent_id: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    size: int | None = None
    mime_type: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class FileItem(DriveItem):
    """Represents a DingTalk Drive file."""

    item_type: Literal["file"] = "file"


@dataclass(slots=True)
class FolderItem(DriveItem):
    """Represents a DingTalk Drive folder."""

    item_type: Literal["folder"] = "folder"


__all__ = [
    "DriveError",
    "DriveAuthError",
    "DriveNotFound",
    "DriveRetryableError",
    "DriveRequestError",
    "DriveItem",
    "FileItem",
    "FolderItem",
]
