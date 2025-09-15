from __future__ import annotations

from pathlib import Path
from typing import Any, Callable
import uuid
import requests

from autoflow.core.logger import get_logger
from autoflow.core.errors import DownloadError
from autoflow.services.browser.runner import BrowserRunner
from .base import ICloudProvider


class KDocsDriveProvider(ICloudProvider):
    """KDocs/Kingsoft Drive provider with direct_url/Browser fallback."""

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
        link_url = (self.cfg or {}).get("link_url")

        if direct_url:
            self.logger.info("通过直链下载(金山云盘): %s", direct_url)
            name = self.cfg.get("filename", f"kdocs_{uuid.uuid4().hex[:8]}.xlsx")
            out = dest_dir / name
            try:
                with requests.get(direct_url, stream=True, timeout=60) as r:  # type: ignore
                    r.raise_for_status()
                    with open(out, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
            except Exception as e:  # noqa: BLE001
                raise DownloadError(f"直链下载失败(金山): {e}") from e
            return [str(out)]

        if link_url:
            br = BrowserRunner(headless=False)
            try:
                br.open(link_url)
                br.login_if_needed(config=self.cfg.get("login", {}), credentials_provider=credentials_provider)
                # TODO: Implement actual download steps using selectors
                raise DownloadError("浏览器自动化下载占位：请在 config/selectors 中补充下载选择器")
            finally:
                br.close()

        raise DownloadError("未提供直链/direct_url 或 link_url")

