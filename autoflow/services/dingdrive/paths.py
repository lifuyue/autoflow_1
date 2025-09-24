"""Helpers for working with DingTalk Drive identifiers and pseudo-paths."""

from __future__ import annotations

from pathlib import PurePosixPath

ROOT_ALIASES = {"root", "ROOT", "/", ""}


def normalize_parent_id(parent_id: str | None) -> str:
    """Translate user provided parent reference into DingTalk format."""

    if parent_id is None:
        return "root"
    parent = parent_id.strip()
    if parent in ROOT_ALIASES:
        return "root"
    return parent


def normalize_item_name(name: str) -> str:
    """Sanitize drive item names by trimming whitespace."""

    normalized = name.strip()
    if not normalized:
        raise ValueError("Drive item name must not be empty")
    return normalized


def join_drive_path(*parts: str) -> str:
    """Join fragments using POSIX separators for display/documentation."""

    return str(PurePosixPath(*[p.strip("/") for p in parts if p]))


__all__ = ["normalize_parent_id", "normalize_item_name", "join_drive_path"]
