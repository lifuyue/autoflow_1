"""
RESPONSIBILITIES
- Resolve and create the ~/AutoFlow directory scaffold used for persistence.
- Provide helpers for locating store files and scratch directories.
PROCESS OVERVIEW
1. resolve_root() expands user input or falls back to ~/AutoFlow.
2. ensure_structure() materializes store/inbox/out/tmp/logs directories.
3. store_file_path() returns the canonical store location for a workbook.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

_DEFAULT_SUBDIRS: tuple[str, ...] = ("store", "inbox", "out", "tmp", "logs")


def resolve_root(root: str | os.PathLike[str] | None = None) -> Path:
    """Return the persistence root, defaulting to ~/AutoFlow."""

    if root is None:
        base = Path.home() / "AutoFlow"
    else:
        base = Path(root)
    return base.expanduser().resolve()


def ensure_structure(root: str | os.PathLike[str] | None = None, *, subdirs: Iterable[str] | None = None) -> dict[str, Path]:
    """Ensure persistence directories exist and return a mapping."""

    base = resolve_root(root)
    resolved: dict[str, Path] = {}
    requested = tuple(subdirs) if subdirs is not None else _DEFAULT_SUBDIRS
    base.mkdir(parents=True, exist_ok=True)
    for name in requested:
        target = base / name
        target.mkdir(parents=True, exist_ok=True)
        resolved[name] = target
    return resolved


def store_file_path(filename: str, root: str | os.PathLike[str] | None = None) -> Path:
    """Return the absolute path for a store workbook under \"store\"."""

    directories = ensure_structure(root)
    return directories["store"] / filename


def tmp_file_path(filename: str, root: str | os.PathLike[str] | None = None) -> Path:
    """Return a path located in the tmp directory."""

    directories = ensure_structure(root)
    return directories["tmp"] / filename
