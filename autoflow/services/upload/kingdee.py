from __future__ import annotations

from pathlib import Path
from typing import Any, Callable
import time

import requests

from autoflow.core.logger import get_logger
from autoflow.services.browser.runner import BrowserRunner
from .base import IUploader


class KingdeeUploader(IUploader):
    """Kingdee uploader with API-first then browser fallback."""

    def __init__(self, cfg: dict[str, Any]):
        self.cfg = cfg
        self.logger = get_logger()

    def upload(
        self,
        profile: Any,
        file_path: Path,
        shots_dir: Path,
        credentials_provider: Callable[[bool], dict[str, str] | None] | None = None,
    ) -> dict[str, Any]:
        api = (self.cfg or {}).get("api")
        if api and api.get("url"):
            url = api["url"]
            self.logger.info("通过 API 上传到金蝶: %s", url)
            headers = api.get("headers", {})
            try:
                with open(file_path, "rb") as f:
                    resp = requests.post(url, headers=headers, files={"file": (file_path.name, f)})
                ok = resp.status_code < 300
                return {"status": "ok" if ok else "fail", "code": resp.status_code, "text": resp.text}
            except Exception as e:  # noqa: BLE001
                # Fall through to browser
                self.logger.warning("API 调用失败，切换浏览器自动化: %s", e)

        upload_url = (self.cfg or {}).get("upload_url")
        selectors_file = (self.cfg or {}).get("selectors_file", "autoflow/config/selectors/kingdee.yaml")
        br = BrowserRunner(headless=False, shots_dir=shots_dir)
        try:
            br.open(upload_url)
            br.login_if_needed(config=(self.cfg or {}).get("login", {}), credentials_provider=credentials_provider)
            br.do_upload(selectors_file, file_path)
            time.sleep(1)
            shot_path = br.screenshot(name_prefix="kingdee_uploaded")
            return {"status": "ok", "screenshot": str(shot_path)}
        finally:
            br.close()

