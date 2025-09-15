from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable

from autoflow.core.errors import ConfigError


class ICloudProvider(ABC):
    """Interface for cloud download providers."""

    @abstractmethod
    def download(
        self,
        profile: Any,
        dest_dir: Path,
        credentials_provider: Callable[[bool], dict[str, str] | None] | None = None,
    ) -> list[str]:
        """Download required source files to dest_dir and return list of file paths."""


def provider_from_config(cfg: dict[str, Any]) -> ICloudProvider:
    ptype = (cfg or {}).get("type", "dingpan").lower()
    if ptype in {"dingpan", "dingtalk", "dd"}:
        from .dingpan import DingPanProvider

        return DingPanProvider(cfg)
    if ptype in {"kdocs", "kdocs_drive", "jinshan", "ksyun"}:
        from .kdocs_drive import KDocsDriveProvider

        return KDocsDriveProvider(cfg)
    raise ConfigError(f"未知下载提供方: {ptype}")

