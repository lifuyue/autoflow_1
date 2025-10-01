"""Directory management helpers for DingTalk Drive."""

from __future__ import annotations

import logging
from typing import Any

from autoflow.core.logger import get_logger

from .config import DingDriveConfig
from .http import HttpClient
from .models import DriveRequestError
from .paths import normalize_item_name, normalize_parent_id

LOGGER = get_logger()


class DirectoryClient:
    """Provide folder discovery and creation operations."""

    def __init__(
        self,
        config: DingDriveConfig,
        http_client: HttpClient,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._http = http_client
        self._logger = logger or LOGGER

    def ensure_folder(self, path: str) -> str:
        """Ensure the folder path exists and return the final folder id."""

        segments = [segment.strip() for segment in path.split("/") if segment.strip()]
        current_id = "root"
        if not segments:
            return current_id

        first = segments[0]
        if first.startswith("id:") and len(first) > 3:
            current_id = first[3:]
            segments = segments[1:]
        elif normalize_parent_id(first) == "root":
            segments = segments[1:]

        for name in segments:
            folder_name = normalize_item_name(name)
            existing = self._find_folder_by_name(current_id, folder_name)
            if existing:
                current_id = existing
                continue
            current_id = self.create_folder(current_id, folder_name)
        return current_id

    def list_children(self, parent_id: str) -> list[dict[str, Any]]:
        """Return raw child metadata under a parent folder."""

        parent = normalize_parent_id(parent_id)
        response = self._http.request_openapi(
            "GET",
            f"/drive/spaces/{self._config.space_id}/files",
            params={"parentId": parent},
        )
        data = response.json() if response.content else {}
        items = data.get("items") or data.get("files") or []
        result: list[dict[str, Any]] = []
        for item in items:
            if isinstance(item, dict):
                result.append(item)
        return result

    def get_info(self, item_id: str) -> dict[str, Any]:
        """Fetch metadata for a file or folder."""

        response = self._http.request_openapi(
            "GET",
            f"/drive/spaces/{self._config.space_id}/files/{item_id}",
        )
        payload = response.json()
        if not isinstance(payload, dict):
            raise DriveRequestError("Invalid metadata response", payload={"body": payload})
        return payload

    def create_folder(self, parent_id: str, name: str) -> str:
        """Create a folder under the given parent and return its id."""

        folder_name = normalize_item_name(name)
        parent = normalize_parent_id(parent_id)
        response = self._http.request_openapi(
            "POST",
            f"/drive/spaces/{self._config.space_id}/folders",
            json_body={"parentId": parent, "name": folder_name},
            expected_status=(200, 201),
        )
        payload = response.json()
        folder_id = payload.get("id") or payload.get("folderId")
        if not folder_id:
            raise DriveRequestError("Folder creation response missing id", payload=payload)
        self._logger.info(
            "dingdrive.directory created_folder parent=%s name=%s folder_id=%s",
            parent,
            folder_name,
            folder_id,
        )
        return str(folder_id)

    def _find_folder_by_name(self, parent_id: str, name: str) -> str | None:
        for item in self.list_children(parent_id):
            if item.get("type") == "folder" or item.get("fileType") == "folder" or item.get("nodeType") == "folder":
                if item.get("name") == name:
                    folder_id = item.get("id") or item.get("folderId")
                    if folder_id:
                        return str(folder_id)
        return None


__all__ = ["DirectoryClient"]
