from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable

from autoflow.core.errors import ConfigError


class IUploader(ABC):
    """Interface for target system uploaders."""

    @abstractmethod
    def upload(
        self,
        profile: Any,
        file_path: Path,
        shots_dir: Path,
        credentials_provider: Callable[[bool], dict[str, str] | None] | None = None,
    ) -> dict[str, Any]:
        """Upload file_path to target and return a result dict."""


def uploader_from_config(cfg: dict[str, Any]) -> IUploader:
    utype = (cfg or {}).get("type", "kingdee").lower()
    if utype in {"kingdee", "kd"}:
        from .kingdee import KingdeeUploader

        return KingdeeUploader(cfg)
    if utype in {"tax_ehall", "tax", "electax"}:
        from .tax_ehall import TaxEhallUploader

        return TaxEhallUploader(cfg)
    raise ConfigError(f"未知上传目标: {utype}")

