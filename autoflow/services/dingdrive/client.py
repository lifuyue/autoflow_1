"""Primary client implementation for DingTalk Drive."""

from __future__ import annotations

import logging
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any, Iterable, Protocol

from requests import Response

from autoflow.core.logger import get_logger

from .config import DingDriveConfig
from .http import AUTHORIZATION_HEADER, DingTalkAuth, HttpClient
from .models import (
    DriveAuthError,
    DriveItem,
    DriveRequestError,
    FileItem,
    FolderItem,
)
from .paths import normalize_item_name, normalize_parent_id
from .utils import chunk_count, detect_mime_type, ensure_directory, iter_file_chunks, parse_datetime

LOGGER = get_logger()


class StorageProvider(Protocol):
    """Abstract storage provider contract used by upload pipelines."""

    def list_children(self, parent_id: str) -> list[dict[str, Any]]:
        """List child items under the given folder identifier."""

    def create_folder(self, parent_id: str, name: str) -> str:
        """Create a folder and return its identifier."""

    def upload_file(self, parent_id: str, local_path: str, *, name: str | None = None) -> str:
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
        auth: DingTalkAuth | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._http = http_client or HttpClient(config, logger=logger)
        self._auth = auth or DingTalkAuth(config, session=self._http.session, logger=logger)
        self._logger = logger or LOGGER

    @classmethod
    def from_profile(cls, profile_name: str) -> "DingDriveClient":
        """Instantiate a client from ``profiles.yaml`` configuration."""

        config = DingDriveConfig.from_profile(profile_name)
        return cls(config)

    def list_children(self, parent_id: str) -> list[dict[str, Any]]:
        parent = normalize_parent_id(parent_id)
        response = self._authorized_request(
            "GET",
            f"/drive/spaces/{self._config.space_id}/files",
            params={"parentId": parent},
        )
        data = response.json() if response.content else {}
        items: list[dict[str, Any]] = []
        for raw in data.get("items", data.get("files", [])):
            parsed = self._parse_item(raw)
            if parsed:
                items.append(self._item_to_dict(parsed))
        return items

    def create_folder(self, parent_id: str, name: str) -> str:
        parent = normalize_parent_id(parent_id)
        folder_name = normalize_item_name(name)
        response = self._authorized_request(
            "POST",
            f"/drive/spaces/{self._config.space_id}/folders",
            json_body={"parentId": parent, "name": folder_name},
            expected_status=(200, 201),
        )
        payload = response.json()
        folder_id = payload.get("id") or payload.get("folderId")
        if not folder_id:
            raise DriveRequestError("Folder creation response missing id", payload=payload)
        return folder_id

    def upload_file(self, parent_id: str, local_path: str, *, name: str | None = None) -> str:
        parent = normalize_parent_id(parent_id)
        upload_path = Path(local_path)
        if not upload_path.exists():
            raise FileNotFoundError(local_path)
        file_name = normalize_item_name(name or upload_path.name)
        file_size = upload_path.stat().st_size
        mime_type = detect_mime_type(upload_path)

        upload_info = self._request_upload_info(
            parent_id=parent,
            file_name=file_name,
            file_size=file_size,
            mime_type=mime_type,
        )
        self._perform_upload(upload_info, upload_path)
        file_id = self._confirm_upload(upload_info)
        return file_id

    def download_file(self, file_id: str, dest_path: str) -> str:
        response = self._authorized_request(
            "POST",
            f"/drive/spaces/{self._config.space_id}/files/download",
            json_body={"fileId": file_id},
        )
        payload = response.json()
        download_url = payload.get("downloadUrl") or payload.get("url")
        if not download_url:
            raise DriveRequestError("Download URL missing", payload=payload)

        destination = Path(dest_path).expanduser()
        remote_name = payload.get("name") or payload.get("fileName") or f"{file_id}.bin"
        if destination.exists() and destination.is_dir():
            destination = destination / remote_name
        elif str(dest_path).endswith((os.sep, "/")):
            destination = destination / remote_name
        ensure_directory(destination)
        with self._http.session.get(download_url, stream=True, timeout=self._config.timeout_sec) as stream:
            if stream.status_code >= 400:
                raise DriveRequestError(
                    f"Failed to download file: HTTP {stream.status_code}",
                    status_code=stream.status_code,
                )
            with destination.open("wb") as handle:
                for chunk in stream.iter_content(chunk_size=1024 * 128):
                    if chunk:
                        handle.write(chunk)
        return str(destination)

    def delete(self, item_id: str) -> None:
        self._authorized_request(
            "DELETE",
            f"/drive/spaces/{self._config.space_id}/files/{item_id}",
            expected_status=(200, 204),
        )

    def rename(self, item_id: str, new_name: str) -> None:
        rename_to = normalize_item_name(new_name)
        self._authorized_request(
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
        self._authorized_request(
            "POST",
            f"/drive/spaces/{self._config.space_id}/files/move",
            json_body=payload,
            expected_status=(200, 204),
        )

    def get_preview_url(self, file_id: str) -> str | None:
        try:
            response = self._authorized_request(
                "POST",
                f"/drive/spaces/{self._config.space_id}/files/preview",
                json_body={"fileId": file_id},
            )
        except DriveRequestError:
            return None
        data = response.json()
        return data.get("previewUrl") or data.get("url")

    def close(self) -> None:
        """Release the underlying HTTP session."""

        self._http.session.close()

    # Internal helpers -------------------------------------------------

    def _authorized_request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        expected_status: Iterable[int] = (200,),
    ) -> Response:
        headers: dict[str, str] = {}
        for attempt in (0, 1):
            token = self._auth.get_access_token(force_refresh=attempt == 1)
            headers[AUTHORIZATION_HEADER] = token
            try:
                return self._http.request(
                    method,
                    path,
                    headers=headers,
                    params=params,
                    json_body=json_body,
                    expected_status=expected_status,
                )
            except DriveAuthError as exc:
                if attempt == 0:
                    self._logger.info("Access token rejected, refreshing: %s", exc)
                    self._auth.invalidate()
                    continue
                raise

    def _request_upload_info(
        self,
        *,
        parent_id: str,
        file_name: str,
        file_size: int,
        mime_type: str,
    ) -> dict[str, Any]:
        payload = {
            "parentId": parent_id,
            "name": file_name,
            "size": file_size,
            "mimeType": mime_type,
        }
        response = self._authorized_request(
            "POST",
            f"/drive/spaces/{self._config.space_id}/files/upload",
            json_body=payload,
        )
        return response.json()

    def _perform_upload(self, upload_info: dict[str, Any], path: Path) -> None:
        if upload_info.get("uploadUrl"):
            self._upload_single_part(upload_info, path)
            return
        if upload_info.get("parts"):
            self._upload_multipart_parts(upload_info, path)
            return
        multipart = upload_info.get("multipart") or upload_info.get("multiUploadInfo")
        if multipart:
            self._upload_generic_multipart(upload_info, path, multipart)
            return
        raise DriveRequestError("Unsupported upload instructions", payload=upload_info)

    def _upload_single_part(self, upload_info: dict[str, Any], path: Path) -> None:
        method = upload_info.get("httpMethod", "PUT")
        upload_url = upload_info["uploadUrl"]
        headers = upload_info.get("headers") or upload_info.get("uploadHeaders") or {}
        with path.open("rb") as handle:
            self._http.request(
                method,
                upload_url,
                headers=headers,
                data=handle,
                expected_status=(200, 201, 204),
                use_base_url=False,
            )

    def _upload_multipart_parts(self, upload_info: dict[str, Any], path: Path) -> None:
        parts = upload_info.get("parts", [])
        chunk_size = int(upload_info.get("chunkSize") or self._config.upload_chunk_size)
        total_chunks = chunk_count(path.stat().st_size, chunk_size)
        if len(parts) != total_chunks:
            self._logger.debug(
                "Mismatch between provided part metadata (%d) and calculated chunks (%d)",
                len(parts),
                total_chunks,
            )
        for part_number, chunk, meta in zip(range(1, total_chunks + 1), iter_file_chunks(path, chunk_size=chunk_size), parts):
            upload_url = meta.get("uploadUrl")
            method = meta.get("httpMethod", "PUT")
            headers = meta.get("headers") or {}
            self._http.request(
                method,
                upload_url,
                headers=headers,
                data=chunk,
                expected_status=(200, 201, 204),
                use_base_url=False,
            )

    def _upload_generic_multipart(self, upload_info: dict[str, Any], path: Path, metadata: dict[str, Any]) -> None:
        urls = metadata.get("uploadUrls") or metadata.get("parts")
        if not urls:
            raise DriveRequestError("Multipart upload metadata missing URLs", payload=metadata)
        chunk_size = int(metadata.get("partSize") or metadata.get("chunkSize") or self._config.upload_chunk_size)
        for url, chunk in zip(urls, iter_file_chunks(path, chunk_size=chunk_size)):
            if isinstance(url, dict):
                upload_url = url.get("uploadUrl")
                method = url.get("httpMethod", "PUT")
                headers = url.get("headers") or {}
            else:
                upload_url = url
                method = metadata.get("httpMethod", "PUT")
                headers = metadata.get("headers") or {}
            if not upload_url:
                raise DriveRequestError("Upload URL missing for multipart chunk", payload=metadata)
            self._http.request(
                method,
                upload_url,
                headers=headers,
                data=chunk,
                expected_status=(200, 201, 204),
                use_base_url=False,
            )

    def _confirm_upload(self, upload_info: dict[str, Any]) -> str:
        payload = {
            "uploadKey": upload_info.get("uploadKey"),
            "spaceId": self._config.space_id,
        }
        if upload_info.get("parentId"):
            payload["parentId"] = upload_info["parentId"]
        if upload_info.get("name"):
            payload["fileName"] = upload_info["name"]
        if not payload["uploadKey"]:
            raise DriveRequestError("Upload key missing in upload info", payload=upload_info)
        response = self._authorized_request(
            "POST",
            f"/drive/spaces/{self._config.space_id}/files/complete",
            json_body=payload,
        )
        data = response.json()
        file_id = (
            data.get("fileId")
            or data.get("resourceId")
            or upload_info.get("resourceId")
            or upload_info.get("fileId")
        )
        if not file_id:
            raise DriveRequestError("Unable to determine uploaded file id", payload=data)
        return str(file_id)

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
