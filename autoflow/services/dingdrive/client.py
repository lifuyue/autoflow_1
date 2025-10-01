"""Primary client implementation for DingTalk Drive."""

from __future__ import annotations

import logging
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any, Protocol

from autoflow.core.logger import get_logger

from .auth import AuthClient
from .config import DingDriveConfig, load_parent_id
from .directory import DirectoryClient
from .http import HttpClient
from .models import DriveItem, DriveRequestError, FileItem, FolderItem
from .paths import normalize_item_name, normalize_parent_id
from .uploader import DingDriveUploader, ProgressCallback
from .utils import ensure_directory, parse_datetime
from .verifier import Verifier

LOGGER = get_logger()


class StorageProvider(Protocol):
    """Abstract storage provider contract used by upload pipelines."""

    def list_children(self, parent_id: str) -> list[dict[str, Any]]:
        """List child items under the given folder identifier."""

    def create_folder(self, parent_id: str, name: str) -> str:
        """Create a folder and return its identifier."""

    def upload_file(
        self,
        parent_id: str,
        local_path: str,
        *,
        name: str | None = None,
        progress_cb: ProgressCallback | None = None,
    ) -> str:
        """Upload a file and return the created item identifier."""

    def download_file(self, file_id: str, dest_path: str) -> str:
        """Download the specified file to ``dest_path`` and return the written path."""

    def delete(self, item_id: str) -> None:
        """Remove the file or folder from the provider."""

    def rename(self, item_id: str, new_name: str) -> None:
        """Rename the referenced item."""

    def move(self, item_id: str, new_parent_id: str) -> None:
        """Move the item under a different parent folder."""


class DingDriveClient(StorageProvider):
    """High level CRUD client for DingTalk Drive."""

    def __init__(
        self,
        config: DingDriveConfig,
        *,
        http_client: HttpClient | None = None,
        auth: AuthClient | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._logger = logger or LOGGER
        if http_client is None:
            self._http = HttpClient(config, auth_client=auth, logger=self._logger)
        else:
            self._http = http_client
        self._directory = DirectoryClient(config, self._http, logger=self._logger)
        self._uploader = DingDriveUploader(config, self._http, logger=self._logger)
        self._verifier = Verifier(config, self._http, logger=self._logger)

    @classmethod
    def from_profile(cls, profile_name: str) -> "DingDriveClient":
        """Instantiate a client from ``profiles.yaml`` configuration."""

        config = DingDriveConfig.from_profile(profile_name)
        return cls(config)

    def list_children(self, parent_id: str) -> list[dict[str, Any]]:
        parent = normalize_parent_id(parent_id)
        raw_items = self._directory.list_children(parent)
        items: list[dict[str, Any]] = []
        for raw in raw_items:
            parsed = self._parse_item(raw)
            if parsed:
                items.append(self._item_to_dict(parsed))
        return items

    def create_folder(self, parent_id: str, name: str) -> str:
        parent = normalize_parent_id(parent_id)
        folder_name = normalize_item_name(name)
        folder_id = self._directory.create_folder(parent, folder_name)
        return folder_id

    def ensure_folder(self, path: str) -> str:
        """Ensure a folder path exists relative to root and return its id."""

        resolved = self._directory.ensure_folder(path)
        return resolved

    def upload_file(
        self,
        parent_id: str,
        local_path: str,
        *,
        name: str | None = None,
        progress_cb: ProgressCallback | None = None,
    ) -> str:
        parent = normalize_parent_id(parent_id)
        upload_path = Path(local_path)
        if not upload_path.exists():
            raise FileNotFoundError(local_path)
        file_name = normalize_item_name(name or upload_path.name)
        file_id = self._uploader.upload(parent, upload_path, display_name=file_name, progress_cb=progress_cb)
        self._logger.info(
            "dingdrive.client upload_finished parent=%s name=%s file_id=%s",
            parent,
            file_name,
            file_id,
        )
        return file_id

    def download_file(self, file_id: str, dest_path: str) -> str:
        info = self._verifier.get_download_info(file_id)
        destination = Path(dest_path).expanduser()
        remote_name = info.name or f"{file_id}.bin"
        if destination.exists() and destination.is_dir():
            destination = destination / remote_name
        elif str(dest_path).endswith((os.sep, "/")):
            destination = destination / remote_name
        ensure_directory(destination)
        with self._http.request_oss(
            "GET",
            info.url,
            expected_status=(200,),
            stream=True,
            timeout=self._config.timeout_sec,
            allow_retry=True,
        ) as stream:
            with destination.open("wb") as handle:
                for chunk in stream.iter_content(chunk_size=1024 * 128):
                    if chunk:
                        handle.write(chunk)
        if info.size is not None and destination.stat().st_size != info.size:
            raise DriveRequestError(
                "Downloaded size mismatch",
                payload={"expected": info.size, "actual": destination.stat().st_size},
            )
        return str(destination)

    def delete(self, item_id: str) -> None:
        self._http.request_openapi(
            "DELETE",
            f"/drive/spaces/{self._config.space_id}/files/{item_id}",
            expected_status=(200, 204),
        )

    def rename(self, item_id: str, new_name: str) -> None:
        rename_to = normalize_item_name(new_name)
        self._http.request_openapi(
            "PATCH",
            f"/drive/spaces/{self._config.space_id}/files/{item_id}",
            json_body={"name": rename_to},
            expected_status=(200, 204),
        )

    def move(self, item_id: str, new_parent_id: str) -> None:
        payload = {
            "fileIds": [item_id],
            "targetParentId": normalize_parent_id(new_parent_id),
            "moveAction": "move",
        }
        self._http.request_openapi(
            "POST",
            f"/drive/spaces/{self._config.space_id}/files/move",
            json_body=payload,
            expected_status=(200, 204),
        )

    def get_preview_url(self, file_id: str) -> str | None:
        try:
            response = self._http.request_openapi(
                "POST",
                f"/drive/spaces/{self._config.space_id}/files/preview",
                json_body={"fileId": file_id},
            )
        except DriveRequestError:
            return None
        data = response.json()
        return data.get("previewUrl") or data.get("url")

    def resolve_default_parent(self) -> str:
        """Return the configured default parent id if available."""

        return load_parent_id(self._config) or "root"

    def close(self) -> None:
        """Release the underlying HTTP session."""

        self._http.session.close()

    # Internal helpers -------------------------------------------------

    def _parse_item(self, raw: dict[str, Any]) -> DriveItem | None:
        item_type = raw.get("type") or raw.get("fileType") or raw.get("nodeType")
        if item_type == "folder":
            return FolderItem(
                id=str(raw.get("id") or raw.get("folderId")),
                name=str(raw.get("name", "")),
                parent_id=raw.get("parentId"),
                created_at=parse_datetime(raw.get("createdAt") or raw.get("gmtCreate")),
                updated_at=parse_datetime(raw.get("updatedAt") or raw.get("gmtModified")),
                extra=raw,
            )
        if item_type == "file":
            return FileItem(
                id=str(raw.get("id") or raw.get("fileId")),
                name=str(raw.get("name", "")),
                parent_id=raw.get("parentId"),
                created_at=parse_datetime(raw.get("createdAt") or raw.get("gmtCreate")),
                updated_at=parse_datetime(raw.get("updatedAt") or raw.get("gmtModified")),
                size=int(raw.get("size") or raw.get("fileSize") or 0) or None,
                mime_type=raw.get("mimeType") or raw.get("contentType"),
                extra=raw,
            )
        return None

    def _item_to_dict(self, item: DriveItem) -> dict[str, Any]:
        payload = asdict(item)
        if item.created_at:
            payload["created_at"] = item.created_at.isoformat()
        if item.updated_at:
            payload["updated_at"] = item.updated_at.isoformat()
        return payload


__all__ = ["DingDriveClient", "StorageProvider"]
