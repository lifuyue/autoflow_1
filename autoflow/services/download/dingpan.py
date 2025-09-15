from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable
import uuid

import requests

from autoflow.core.logger import get_logger
from autoflow.core.errors import DownloadError
from autoflow.services.browser.runner import BrowserRunner
from .base import ICloudProvider


class DingPanProvider(ICloudProvider):
    """DingTalk Drive provider.

    Supports direct_url/API placeholders; falls back to BrowserRunner if necessary.
    """

    def __init__(self, cfg: dict[str, Any]):
        self.cfg = cfg
        self.logger = get_logger()

    def download(
        self,
        profile: Any,
        dest_dir: Path,
        credentials_provider: Callable[[bool], dict[str, str] | None] | None = None,
    ) -> list[str]:
        dest_dir.mkdir(parents=True, exist_ok=True)
        direct_url = (self.cfg or {}).get("direct_url")
        api = (self.cfg or {}).get("api")
        link_url = (self.cfg or {}).get("link_url")

        if direct_url:
            self.logger.info("通过直链下载: %s", direct_url)
            name = self.cfg.get("filename", f"dingpan_{uuid.uuid4().hex[:8]}.xlsx")
            out = dest_dir / name
            try:
                headers = {}
                token = os.getenv("DINGPAN_TOKEN")
                if token:
                    headers["Authorization"] = f"Bearer {token}"
                with requests.get(direct_url, headers=headers, stream=True, timeout=60) as r:  # type: ignore
                    r.raise_for_status()
                    with open(out, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
            except Exception as e:  # noqa: BLE001
                raise DownloadError(f"直链下载失败: {e}") from e
            return [str(out)]

        if api:
            # TODO: Implement API client here if available
            raise DownloadError("钉盘 API 下载暂未实现，请配置 direct_url 或 link_url")

        if link_url:
            # Use browser automation fallback
            br = BrowserRunner(headless=False)
            try:
                br.open(link_url)
                br.login_if_needed(config=self.cfg.get("login", {}), credentials_provider=credentials_provider)
                # TODO: Implement actual download automation.
                # For MVP we skip real file download and raise for clear guidance.
                raise DownloadError("浏览器自动化下载占位：请在 config/selectors 中补充下载选择器")
            finally:
                br.close()

        raise DownloadError("未提供直链/direct_url、API 或 link_url")

