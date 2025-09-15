from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from autoflow.core.logger import get_logger
from autoflow.services.browser.runner import BrowserRunner
from .base import IUploader


class TaxEhallUploader(IUploader):
    """Electronic tax hall uploader via browser automation."""

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
        upload_url = (self.cfg or {}).get("upload_url")
        selectors_file = (self.cfg or {}).get("selectors_file", "autoflow/config/selectors/tax_ehall.yaml")
        br = BrowserRunner(headless=False, shots_dir=shots_dir)
        try:
            br.open(upload_url)
            br.login_if_needed(config=(self.cfg or {}).get("login", {}), credentials_provider=credentials_provider)
            br.do_upload(selectors_file, file_path)
            shot_path = br.screenshot(name_prefix="tax_uploaded")
            return {"status": "ok", "screenshot": str(shot_path)}
        finally:
            br.close()

