"""File upload helpers for DingTalk Drive."""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Mapping

from autoflow.core.logger import get_logger

from .config import (
    DingDriveConfig,
    load_concurrency,
    load_multipart_threshold,
    load_part_size,
    load_retry_config,
)
from .http import HttpClient
from .models import DriveRequestError, DriveRetryableError
from .utils import detect_mime_type

LOGGER = get_logger()

RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


@dataclass(slots=True)
class UploadProgress:
    """Represents the current upload progress state."""

    filename: str
    total_bytes: int
    uploaded_bytes: int
    total_parts: int
    completed_parts: int
    state: str
    message: str | None = None


ProgressCallback = Callable[[UploadProgress], None]


@dataclass(slots=True)
class PartDescriptor:
    part_number: int
    offset: int
    size: int
    upload_url: str
    method: str
    headers: Mapping[str, str]


class DingDriveUploader:
    """Handle DingTalk Drive small and multipart uploads."""

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
        self._part_size = load_part_size(config)
        self._threshold = load_multipart_threshold(config)
        self._concurrency = max(1, load_concurrency(config))
        self._retry = load_retry_config(config)

    def upload(
        self,
        parent_id: str,
        file_path: Path,
        *,
        display_name: str,
        progress_cb: ProgressCallback | None = None,
    ) -> str:
        """Upload file using small or multipart strategy."""

        file_size = file_path.stat().st_size
        if file_size <= self._threshold:
            return self.upload_small(parent_id, file_path, display_name=display_name, progress_cb=progress_cb)
        return self.upload_multipart(parent_id, file_path, display_name=display_name, progress_cb=progress_cb)

    def upload_small(
        self,
        parent_id: str,
        file_path: Path,
        *,
        display_name: str,
        progress_cb: ProgressCallback | None = None,
    ) -> str:
        """Upload small files with a single PUT request."""

        metadata = self._init_upload(parent_id, display_name, file_path)
        upload_url = metadata.get("uploadUrl") or metadata.get("url")
        if not upload_url:
            raise DriveRequestError("Small file upload metadata missing uploadUrl", payload=metadata)
        method = metadata.get("httpMethod", "PUT")
        headers = metadata.get("headers") or metadata.get("uploadHeaders") or {}

        self._emit_progress(
            progress_cb,
            UploadProgress(
                filename=display_name,
                total_bytes=file_path.stat().st_size,
                uploaded_bytes=0,
                total_parts=1,
                completed_parts=0,
                state="initializing",
            ),
        )

        with file_path.open("rb") as handle:
            self._http.request_oss(
                method,
                upload_url,
                headers=headers,
                data=handle,
                expected_status=(200, 201, 204),
                allow_retry=True,
            )

        self._emit_progress(
            progress_cb,
            UploadProgress(
                filename=display_name,
                total_bytes=file_path.stat().st_size,
                uploaded_bytes=file_path.stat().st_size,
                total_parts=1,
                completed_parts=1,
                state="committing",
            ),
        )

        file_id = self._confirm_upload(metadata)
        self._emit_progress(
            progress_cb,
            UploadProgress(
                filename=display_name,
                total_bytes=file_path.stat().st_size,
                uploaded_bytes=file_path.stat().st_size,
                total_parts=1,
                completed_parts=1,
                state="completed",
            ),
        )
        return file_id

    def upload_multipart(
        self,
        parent_id: str,
        file_path: Path,
        *,
        display_name: str,
        progress_cb: ProgressCallback | None = None,
    ) -> str:
        """Upload large files using multipart strategy with retries."""

        file_size = file_path.stat().st_size
        metadata = self._init_upload(parent_id, display_name, file_path, multipart=True)
        parts = self._build_part_plan(metadata, file_size)
        if not parts:
            raise DriveRequestError("Multipart upload instructions missing parts", payload=metadata)

        progress = UploadProgress(
            filename=display_name,
            total_bytes=file_size,
            uploaded_bytes=0,
            total_parts=len(parts),
            completed_parts=0,
            state="uploading",
        )
        self._emit_progress(progress_cb, progress)

        lock = threading.Lock()
        part_retry_config = self._retry

        def worker(part: PartDescriptor) -> None:
            attempts = max(1, part_retry_config.max_attempts)
            delay = max(0.05, part_retry_config.backoff_ms / 1000.0)
            max_delay = max(delay, part_retry_config.max_backoff_ms / 1000.0)
            last_exc: Exception | None = None
            for attempt in range(1, attempts + 1):
                try:
                    expected_size = min(part.size, max(0, file_size - part.offset))
                    with file_path.open("rb") as handle:
                        handle.seek(part.offset)
                        data = handle.read(expected_size)
                    self._http.request_oss(
                        part.method,
                        part.upload_url,
                        headers=part.headers,
                        data=data,
                        expected_status=(200, 201, 204),
                        allow_retry=False,
                    )
                    break
                except DriveRetryableError as exc:
                    last_exc = exc
                except DriveRequestError as exc:
                    if exc.status_code in RETRYABLE_STATUSES:
                        last_exc = exc
                    else:
                        raise
                if attempt < attempts:
                    sleep_for = min(max_delay, delay * (2 ** (attempt - 1)))
                    time.sleep(sleep_for)
            else:
                if last_exc is None:  # pragma: no cover - defensive
                    raise DriveRetryableError("Multipart upload failed", payload={"part": part.part_number})
                raise last_exc

            with lock:
                progress.uploaded_bytes += expected_size
                progress.completed_parts += 1
                self._emit_progress(progress_cb, replace(progress))

        with ThreadPoolExecutor(max_workers=self._concurrency) as executor:
            futures = [executor.submit(worker, part) for part in parts]
            for future in as_completed(futures):
                future.result()

        progress.uploaded_bytes = file_size
        progress.completed_parts = len(parts)
        progress.state = "committing"
        self._emit_progress(progress_cb, replace(progress))
        file_id = self._confirm_upload(metadata)
        progress.state = "completed"
        self._emit_progress(progress_cb, replace(progress))
        return file_id

    # Internal helpers -------------------------------------------------

    def _init_upload(
        self,
        parent_id: str,
        display_name: str,
        file_path: Path,
        *,
        multipart: bool = False,
    ) -> dict[str, object]:
        mime_type = detect_mime_type(file_path)
        payload: dict[str, object] = {
            "parentId": parent_id,
            "name": display_name,
            "size": file_path.stat().st_size,
            "mimeType": mime_type,
        }
        if multipart:
            payload["chunkSize"] = self._part_size
        response = self._http.request_openapi(
            "POST",
            f"/drive/spaces/{self._config.space_id}/files/upload",
            json_body=payload,
        )
        data = response.json()
        if not isinstance(data, dict):
            raise DriveRequestError("Upload metadata response invalid", payload={"body": data})
        return data

    def _confirm_upload(self, metadata: Mapping[str, object]) -> str:
        payload: dict[str, object] = {
            "uploadKey": metadata.get("uploadKey"),
            "spaceId": self._config.space_id,
        }
        if not payload["uploadKey"]:
            raise DriveRequestError("Missing uploadKey in metadata", payload=dict(metadata))
        if metadata.get("parentId"):
            payload["parentId"] = metadata["parentId"]
        if metadata.get("name"):
            payload["fileName"] = metadata["name"]
        response = self._http.request_openapi(
            "POST",
            f"/drive/spaces/{self._config.space_id}/files/complete",
            json_body=payload,
        )
        result = response.json()
        if not isinstance(result, dict):
            raise DriveRequestError("Upload completion response invalid", payload={"body": result})
        file_id = (
            result.get("fileId")
            or result.get("resourceId")
            or metadata.get("resourceId")
            or metadata.get("fileId")
        )
        if not file_id:
            raise DriveRequestError("Unable to resolve uploaded file id", payload=result)
        self._logger.info(
            "dingdrive.uploader upload_completed file_id=%s size=%s",
            file_id,
            metadata.get("size"),
        )
        return str(file_id)

    def _build_part_plan(self, metadata: Mapping[str, object], file_size: int) -> list[PartDescriptor]:
        parts: list[PartDescriptor] = []
        provided = metadata.get("parts")
        multipart = metadata.get("multipart") or metadata.get("multiUploadInfo")
        headers_fallback = metadata.get("headers") or {}
        if isinstance(provided, list) and provided:
            for entry in provided:
                if not isinstance(entry, Mapping):
                    continue
                upload_url = entry.get("uploadUrl")
                if not upload_url:
                    continue
                part_number = int(entry.get("partNumber") or entry.get("partId") or len(parts) + 1)
                size = int(entry.get("size") or metadata.get("chunkSize") or self._part_size)
                headers = entry.get("headers") or headers_fallback
                method = entry.get("httpMethod", "PUT")
                offset = (part_number - 1) * size
                parts.append(
                    PartDescriptor(
                        part_number=part_number,
                        offset=offset,
                        size=size,
                        upload_url=str(upload_url),
                        method=str(method),
                        headers=dict(headers),
                    )
                )
        elif isinstance(multipart, Mapping):
            urls = multipart.get("parts") or multipart.get("uploadUrls") or []
            method = multipart.get("httpMethod", "PUT")
            headers = multipart.get("headers") or headers_fallback
            size = int(multipart.get("partSize") or multipart.get("chunkSize") or self._part_size)
            for idx, url_info in enumerate(urls, start=1):
                if isinstance(url_info, Mapping):
                    upload_url = url_info.get("uploadUrl") or url_info.get("url")
                    part_headers = url_info.get("headers") or headers
                    part_method = url_info.get("httpMethod", method)
                else:
                    upload_url = url_info
                    part_headers = headers
                    part_method = method
                if not upload_url:
                    continue
                parts.append(
                    PartDescriptor(
                        part_number=idx,
                        offset=(idx - 1) * size,
                        size=size,
                        upload_url=str(upload_url),
                        method=str(part_method),
                        headers=dict(part_headers or {}),
                    )
                )
        if not parts:
            # Compute deterministic plan as last resort
            total_parts = max(1, (file_size + self._part_size - 1) // self._part_size)
            upload_url = metadata.get("uploadUrl")
            if not upload_url:
                return []
            for idx in range(1, total_parts + 1):
                size = min(self._part_size, file_size - (idx - 1) * self._part_size)
                parts.append(
                    PartDescriptor(
                        part_number=idx,
                        offset=(idx - 1) * self._part_size,
                        size=size,
                        upload_url=str(upload_url),
                        method="PUT",
                        headers=dict(headers_fallback),
                    )
                )
        return parts

    def _emit_progress(self, callback: ProgressCallback | None, progress: UploadProgress) -> None:
        if callback:
            callback(progress)


__all__ = [
    "DingDriveUploader",
    "UploadProgress",
    "ProgressCallback",
]
