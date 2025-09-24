"""Configuration loader for DingTalk Drive client."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml

from autoflow.core.logger import get_logger
from autoflow.core.profiles import resolve_config_path
from .models import DriveError

LOGGER = get_logger()

DEFAULT_BASE_URL = "https://api.dingtalk.com/v1.0"
DEFAULT_AUTH_URL = "https://oapi.dingtalk.com/gettoken"
DEFAULT_TIMEOUT = 10.0
DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024


@dataclass(slots=True)
class RetryConfig:
    """Retry parameters for DingTalk HTTP requests."""

    max_attempts: int = 3
    backoff_ms: int = 200
    max_backoff_ms: int = 2000

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any] | None) -> "RetryConfig":
        if not data:
            return cls()
        return cls(
            max_attempts=int(data.get("max_attempts", cls.max_attempts)),
            backoff_ms=int(data.get("backoff_ms", cls.backoff_ms)),
            max_backoff_ms=int(data.get("max_backoff_ms", cls.max_backoff_ms)),
        )


@dataclass(slots=True)
class DingDriveConfig:
    """Resolved configuration for DingTalk Drive operations."""

    app_key: str
    app_secret: str
    space_id: str
    timeout_sec: float = DEFAULT_TIMEOUT
    retries: RetryConfig = field(default_factory=RetryConfig)
    verify_tls: bool = True
    trust_env: bool = False
    base_url: str = DEFAULT_BASE_URL
    auth_url: str = DEFAULT_AUTH_URL
    upload_chunk_size: int = DEFAULT_CHUNK_SIZE
    proxies: Mapping[str, str] | None = None

    @classmethod
    def from_profile(cls, profile_name: str, *, config_path: str | Path | None = None) -> "DingDriveConfig":
        """Create a configuration instance from profiles.yaml.

        Args:
            profile_name: Logical profile name under the ``dingdrive`` section.
            config_path: Optional override for the config file path.

        Returns:
            Parsed ``DingDriveConfig`` instance.

        Raises:
            DriveError: If the configuration cannot be loaded or is invalid.
        """

        raw = _load_profiles_file(path=config_path).get(profile_name)
        if raw is None:
            raise DriveError(f"dingdrive profile '{profile_name}' not found in profiles.yaml")
        return cls.from_mapping(raw)

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "DingDriveConfig":
        """Create a configuration instance from a mapping."""

        def _require(key: str) -> str:
            value = data.get(key)
            if value is None or (isinstance(value, str) and not value.strip()):
                raise DriveError(f"Missing required DingDrive config value: {key}")
            return _expand_env(value)

        timeout_val = data.get("timeout_sec", DEFAULT_TIMEOUT)
        chunk_size = data.get("upload_chunk_size", DEFAULT_CHUNK_SIZE)
        proxies_raw = data.get("proxies")
        proxies: Mapping[str, str] | None = None
        if isinstance(proxies_raw, Mapping):
            proxies = {k: _expand_env(v) for k, v in proxies_raw.items()}

        return cls(
            app_key=_require("app_key"),
            app_secret=_require("app_secret"),
            space_id=_require("space_id"),
            timeout_sec=float(timeout_val),
            retries=RetryConfig.from_mapping(_ensure_mapping(data.get("retries"))),
            verify_tls=bool(data.get("verify_tls", True)),
            trust_env=bool(data.get("trust_env", False)),
            base_url=_expand_env(data.get("base_url", DEFAULT_BASE_URL)),
            auth_url=_expand_env(data.get("auth_url", DEFAULT_AUTH_URL)),
            upload_chunk_size=int(chunk_size),
            proxies=proxies,
        )


def _ensure_mapping(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return value
    return None


def _expand_env(value: Any) -> Any:
    if isinstance(value, str):
        expanded = os.path.expandvars(value)
        if "${" in value and "}" in value and expanded == value:
            raise DriveError(f"Environment variable not set for value: {value}")
        return expanded
    return value


def _load_profiles_file(*, path: str | Path | None) -> dict[str, Mapping[str, Any]]:
    cfg_path = resolve_config_path(path or "profiles.yaml")
    if not cfg_path.exists():
        raise DriveError(f"profiles.yaml not found at {cfg_path}")
    with cfg_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    section = data.get("dingdrive")
    if not isinstance(section, Mapping):
        raise DriveError("profiles.yaml missing 'dingdrive' section")
    profiles: dict[str, Mapping[str, Any]] = {}
    for key, value in section.items():
        if not isinstance(value, Mapping):
            LOGGER.warning("Ignoring dingdrive profile %s with invalid type", key)
            continue
        profiles[str(key)] = value
    if not profiles:
        raise DriveError("No dingdrive profiles defined in profiles.yaml")
    return profiles


__all__ = ["DingDriveConfig", "RetryConfig"]
