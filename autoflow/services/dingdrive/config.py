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
DEFAULT_MULTIPART_THRESHOLD = 32 * 1024 * 1024
DEFAULT_UPLOAD_CONCURRENCY = 4

CLIENT_ID_ENV = "DINGDRIVE_CLIENT_ID"
CLIENT_SECRET_ENV = "DINGDRIVE_CLIENT_SECRET"
SPACE_ID_ENV = "DINGDRIVE_SPACE_ID"
PARENT_ID_ENV = "DINGDRIVE_PARENT_ID"
MULTIPART_THRESHOLD_ENV = "DINGDRIVE_MULTIPART_THRESHOLD"
PART_SIZE_ENV = "DINGDRIVE_PART_SIZE"
UPLOAD_CONCURRENCY_ENV = "DINGDRIVE_UPLOAD_CONCURRENCY"
TIMEOUT_ENV = "DINGDRIVE_TIMEOUT_SEC"
RETRY_ATTEMPTS_ENV = "DINGDRIVE_RETRY_ATTEMPTS"
RETRY_BACKOFF_MS_ENV = "DINGDRIVE_RETRY_BACKOFF_MS"
RETRY_MAX_BACKOFF_MS_ENV = "DINGDRIVE_RETRY_MAX_BACKOFF_MS"


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
    multipart_threshold: int = DEFAULT_MULTIPART_THRESHOLD
    upload_concurrency: int = DEFAULT_UPLOAD_CONCURRENCY
    default_parent_id: str | None = None
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
        chunk_size = data.get("upload_chunk_size", data.get("part_size", DEFAULT_CHUNK_SIZE))
        threshold_val = data.get("multipart_threshold", DEFAULT_MULTIPART_THRESHOLD)
        concurrency_val = data.get("upload_concurrency", DEFAULT_UPLOAD_CONCURRENCY)
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
            multipart_threshold=int(threshold_val),
            upload_concurrency=int(concurrency_val),
            default_parent_id=_expand_env(data.get("parent_id")),
            proxies=proxies,
        )


def _read_env(key: str) -> str | None:
    value = os.getenv(key)
    if value is None:
        return None
    return value.strip()


def _read_env_int(key: str) -> int | None:
    value = _read_env(key)
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError as exc:  # noqa: BLE001 - configuration validation
        raise DriveError(f"Environment variable {key} must be an integer") from exc


def _read_env_float(key: str) -> float | None:
    value = _read_env(key)
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError as exc:  # noqa: BLE001 - configuration validation
        raise DriveError(f"Environment variable {key} must be a number") from exc


def load_client_id(config: DingDriveConfig | None = None) -> str:
    """Return the DingTalk Drive client identifier from env or configuration."""

    value = _read_env(CLIENT_ID_ENV) or (config.app_key if config else None)
    if not value:
        raise DriveError("DingDrive client id not configured")
    return value


def load_client_secret(config: DingDriveConfig | None = None) -> str:
    """Return the DingTalk Drive client secret from env or configuration."""

    value = _read_env(CLIENT_SECRET_ENV) or (config.app_secret if config else None)
    if not value:
        raise DriveError("DingDrive client secret not configured")
    return value


def load_space_id(config: DingDriveConfig | None = None) -> str:
    """Return the DingTalk Drive space identifier."""

    value = _read_env(SPACE_ID_ENV) or (config.space_id if config else None)
    if not value:
        raise DriveError("DingDrive space id not configured")
    return value


def load_parent_id(config: DingDriveConfig | None = None) -> str | None:
    """Return the default parent identifier (optional)."""

    return _read_env(PARENT_ID_ENV) or (config.default_parent_id if config else None)


def load_multipart_threshold(config: DingDriveConfig | None = None) -> int:
    """Return the multipart upload threshold in bytes."""

    value = _read_env_int(MULTIPART_THRESHOLD_ENV)
    if value is not None:
        return value
    if config:
        return int(config.multipart_threshold)
    return DEFAULT_MULTIPART_THRESHOLD


def load_part_size(config: DingDriveConfig | None = None) -> int:
    """Return the preferred chunk size for multipart uploads."""

    value = _read_env_int(PART_SIZE_ENV)
    if value is not None:
        return value
    if config:
        return int(config.upload_chunk_size)
    return DEFAULT_CHUNK_SIZE


def load_concurrency(config: DingDriveConfig | None = None) -> int:
    """Return the maximum upload concurrency."""

    value = _read_env_int(UPLOAD_CONCURRENCY_ENV)
    if value is not None:
        return max(1, value)
    if config:
        return max(1, int(config.upload_concurrency))
    return DEFAULT_UPLOAD_CONCURRENCY


def load_timeout(config: DingDriveConfig | None = None) -> float:
    """Return the request timeout in seconds."""

    value = _read_env_float(TIMEOUT_ENV)
    if value is not None:
        return value
    if config:
        return float(config.timeout_sec)
    return DEFAULT_TIMEOUT


def load_retry_config(config: DingDriveConfig | None = None) -> RetryConfig:
    """Return retry configuration applying environment overrides."""

    attempts = _read_env_int(RETRY_ATTEMPTS_ENV)
    backoff = _read_env_int(RETRY_BACKOFF_MS_ENV)
    max_backoff = _read_env_int(RETRY_MAX_BACKOFF_MS_ENV)
    if config is None:
        base = RetryConfig()
    else:
        base = RetryConfig(
            max_attempts=config.retries.max_attempts,
            backoff_ms=config.retries.backoff_ms,
            max_backoff_ms=config.retries.max_backoff_ms,
        )
    return RetryConfig(
        max_attempts=attempts or base.max_attempts,
        backoff_ms=backoff or base.backoff_ms,
        max_backoff_ms=max_backoff or base.max_backoff_ms,
    )


def resolve_config(profile: str | None = None) -> DingDriveConfig:
    """Resolve configuration from a profile or environment variables with overrides."""

    if profile:
        base = DingDriveConfig.from_profile(profile)
    else:
        base = DingDriveConfig(
            app_key=load_client_id(None),
            app_secret=load_client_secret(None),
            space_id=load_space_id(None),
        )
    return DingDriveConfig(
        app_key=load_client_id(base),
        app_secret=load_client_secret(base),
        space_id=load_space_id(base),
        timeout_sec=load_timeout(base),
        retries=load_retry_config(base),
        verify_tls=base.verify_tls,
        trust_env=base.trust_env,
        base_url=base.base_url,
        auth_url=base.auth_url,
        upload_chunk_size=load_part_size(base),
        multipart_threshold=load_multipart_threshold(base),
        upload_concurrency=load_concurrency(base),
        default_parent_id=load_parent_id(base),
        proxies=base.proxies,
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


__all__ = [
    "DingDriveConfig",
    "RetryConfig",
    "CLIENT_ID_ENV",
    "CLIENT_SECRET_ENV",
    "SPACE_ID_ENV",
    "PARENT_ID_ENV",
    "MULTIPART_THRESHOLD_ENV",
    "PART_SIZE_ENV",
    "UPLOAD_CONCURRENCY_ENV",
    "TIMEOUT_ENV",
    "RETRY_ATTEMPTS_ENV",
    "RETRY_BACKOFF_MS_ENV",
    "RETRY_MAX_BACKOFF_MS_ENV",
    "load_client_id",
    "load_client_secret",
    "load_space_id",
    "load_parent_id",
    "load_multipart_threshold",
    "load_part_size",
    "load_concurrency",
    "load_timeout",
    "load_retry_config",
    "resolve_config",
]
