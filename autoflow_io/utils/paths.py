"""Filesystem helpers for standard AutoFlow workspace structure."""

# Module responsibilities:
# - Define the default ~/AutoFlow directory layout and create folders on demand.
# - Offer small helpers to resolve output paths without overwriting templates accidentally.

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

DEFAULT_BASE = Path.home() / "AutoFlow"


def ensure_default_structure(base: Optional[Path] = None) -> Dict[str, Path]:
    """Ensure the default AutoFlow directory structure exists.

    Args:
        base: Optional override for the AutoFlow base directory.

    Returns:
        Mapping with keys ``base``, ``inbox``, ``out``, ``tmp``, ``logs``.
    """

    target_base = base or DEFAULT_BASE
    paths = {
        "base": target_base,
        "inbox": target_base / "inbox",
        "out": target_base / "out",
        "tmp": target_base / "tmp",
        "logs": target_base / "logs",
    }
    for key, path in paths.items():
        if key == "base":
            path.mkdir(parents=True, exist_ok=True)
        else:
            path.mkdir(parents=True, exist_ok=True)
    return paths


def prepare_output_path(filename: str, base: Optional[Path] = None) -> Path:
    """Prepare an output path inside the AutoFlow out directory.

    Args:
        filename: Desired file name.
        base: Optional override for the AutoFlow base directory.

    Returns:
        Final path under the ``out`` directory.
    """

    paths = ensure_default_structure(base)
    out_dir = paths["out"]
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / filename
