"""Download verification helpers for DingTalk Drive."""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Mapping

from autoflow.core.logger import get_logger

from .config import DingDriveConfig, load_timeout
from .http import HttpClient
from .models import DriveRequestError

LOGGER = get_logger()


@dataclass(slots=True)
class DownloadInfo:
    """Metadata returned for a downloadable file."""

    file_id: str
    name: str | None
    url: str
    size: int | None
    hash_type: str | None
    hash_value: str | None
    sample_hash: str | None = None


class Verifier:
    """Inspect download metadata and perform optional range sampling."""

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
        self._timeout = load_timeout(config)

    def get_download_info(self, file_id: str, *, sample_bytes: int | None = None) -> DownloadInfo:
        """Fetch metadata and optionally download a ranged sample."""

        response = self._http.request_openapi(
            "POST",
            f"/drive/spaces/{self._config.space_id}/files/download",
            json_body={"fileId": file_id},
        )
        payload = response.json()
        if not isinstance(payload, Mapping):
            raise DriveRequestError("Invalid download metadata", payload={"body": payload})

        url = payload.get("downloadUrl") or payload.get("url")
        if not isinstance(url, str) or not url:
            raise DriveRequestError("Download metadata missing url", payload=dict(payload))
        size = self._parse_int(payload.get("size") or payload.get("fileSize"))
        hash_value = payload.get("contentHash") or payload.get("hashValue") or payload.get("md5")
        hash_type = payload.get("contentHashName") or payload.get("hashType")
        name = payload.get("name") or payload.get("fileName")

        sample_hash: str | None = None
        if sample_bytes:
            sample_hash = self._fetch_sample(url, sample_bytes)
        return DownloadInfo(
            file_id=file_id,
            name=name if isinstance(name, str) else None,
            url=url,
            size=size,
            hash_type=hash_type if isinstance(hash_type, str) else None,
            hash_value=hash_value if isinstance(hash_value, str) else None,
            sample_hash=sample_hash,
        )

    # Internal helpers -------------------------------------------------

    def _fetch_sample(self, url: str, sample_bytes: int) -> str:
        headers = {
            "Range": f"bytes=0-{max(0, sample_bytes - 1)}",
        }
        digest = hashlib.sha256()
        total = 0
        with self._http.request_oss(
            "GET",
            url,
            headers=headers,
            expected_status=(200, 206),
            stream=True,
            timeout=self._timeout,
            allow_retry=True,
        ) as response:
            for chunk in response.iter_content(chunk_size=8192):
                if not chunk:
                    break
                digest.update(chunk)
                total += len(chunk)
                if total >= sample_bytes:
                    break
        hex_digest = digest.hexdigest()
        self._logger.debug(
            "dingdrive.verifier sampled range url=%s range=%s hash=%s bytes=%d",
            self._redact_url(url),
            headers["Range"],
            hex_digest,
            total,
        )
        return hex_digest

    def _redact_url(self, url: str) -> str:
        if "?" in url:
            return url.split("?")[0]
        return url

    @staticmethod
    def _parse_int(value: object) -> int | None:
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None


__all__ = ["Verifier", "DownloadInfo"]
