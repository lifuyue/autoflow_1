from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # noqa: BLE001
    def load_dotenv(*args, **kwargs):  # type: ignore
        return False
import sys

from .errors import ConfigError


load_dotenv(override=False)


@dataclass
class Profile:
    """A single profile/head with its configuration.

    Attributes:
        name: Profile key.
        display_name: Human readable name.
        company_name: Display company name used in template.
        download: Download configuration dict.
        transform: Transform configuration dict.
        upload: Upload configuration dict.
        meta: Arbitrary metadata.
    """

    name: str
    display_name: str
    company_name: str
    download: Dict[str, Any]
    transform: Dict[str, Any]
    upload: Dict[str, Any]
    meta: Dict[str, Any] | None = None

    def get(self, dotted: str, default: Any | None = None) -> Any:
        target: Any = self
        for part in dotted.split('.'):
            if isinstance(target, Profile):
                target = getattr(target, part, default)
            elif isinstance(target, dict):
                target = target.get(part, default)
            else:
                return default
        return target


def _is_frozen() -> bool:
    return getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS")


def _project_root() -> Path:
    env = os.getenv("AUTOFLOW_ROOT")
    if env:
        return Path(env)
    # When frozen (PyInstaller onefile), resources are under sys._MEIPASS
    if _is_frozen():
        return Path(getattr(sys, "_MEIPASS"))  # type: ignore[arg-type]
    # In source layout, this file is under <root>/autoflow/core
    return Path(__file__).resolve().parents[2]


def _app_dir_writable_base() -> Path:
    """Writable base for runtime files (work/logs/out).

    - Frozen: alongside the executable
    - Source: repository root
    """
    if _is_frozen():
        try:
            return Path(sys.executable).resolve().parent
        except Exception:  # noqa: BLE001
            return Path.cwd()
    return Path(__file__).resolve().parents[2]


def _config_dir() -> Path:
    return _project_root() / "autoflow" / "config"


def _work_dir() -> Path:
    # Always use a writable location outside of bundled resources
    return _app_dir_writable_base() / "autoflow" / "work"


def ensure_work_dirs() -> dict[str, Path]:
    base = _work_dir()
    inbox = base / "inbox"
    out = base / "out"
    tmp = base / "tmp"
    logs = base / "logs"
    shot = logs / "shot"
    for p in (inbox, out, tmp, logs, shot):
        p.mkdir(parents=True, exist_ok=True)
    return {"inbox": inbox, "out": out, "tmp": tmp, "logs": logs, "shot": shot}


def load_profiles(path: str | Path | None = None) -> dict[str, Profile]:
    """Load profiles from config/profiles.yaml.

    Returns a dict of profile-key -> Profile.
    """
    cfg_path = Path(path) if path else _config_dir() / "profiles.yaml"
    if not cfg_path.exists():
        raise ConfigError(f"profiles.yaml 未找到: {cfg_path}")
    with cfg_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    profiles_raw = data.get("profiles", {})
    if not profiles_raw:
        raise ConfigError("profiles.yaml 中未定义任何 profiles")
    profiles: dict[str, Profile] = {}
    for key, p in profiles_raw.items():
        try:
            prof = Profile(
                name=key,
                display_name=p.get("display_name", key),
                company_name=p.get("company_name", p.get("display_name", key)),
                download=p.get("download", {}),
                transform=p.get("transform", {}),
                upload=p.get("upload", {}),
                meta=p.get("meta", {}),
            )
        except Exception as e:  # noqa: BLE001
            raise ConfigError(f"配置错误: {key}: {e}") from e
        profiles[key] = prof
    return profiles


def resolve_config_path(path: str | Path) -> Path:
    p = Path(path)
    if p.is_absolute():
        return p
    # Support paths with or without leading 'autoflow/'
    parts = p.parts
    if parts and parts[0] == "autoflow":
        return _project_root() / p
    return _project_root() / "autoflow" / p


def encrypt(text: str) -> str:
    """Placeholder for local encryption.

    TODO: Replace with Windows DPAPI/Keyring based encryption if needed.
    Currently returns the original text to keep MVP simple.
    """
    return text


def decrypt(text: str) -> str:
    """Placeholder for decryption (see encrypt)."""
    return text

