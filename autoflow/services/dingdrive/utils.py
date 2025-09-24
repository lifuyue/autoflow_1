"""Utility helpers for DingTalk Drive operations."""

from __future__ import annotations

import mimetypes
import os
from datetime import datetime
from pathlib import Path
from typing import Generator, Iterable


def detect_mime_type(path: str | os.PathLike[str]) -> str:
    """Best-effort MIME type detection."""

    mime, _ = mimetypes.guess_type(str(path))
    return mime or "application/octet-stream"


def iter_file_chunks(
    path: str | os.PathLike[str], *, chunk_size: int
) -> Generator[bytes, None, None]:
    """Yield file content in deterministic chunk sizes."""

    with open(path, "rb") as handle:
        while True:
            block = handle.read(chunk_size)
            if not block:
                break
            yield block


def ensure_directory(dest_path: str | os.PathLike[str]) -> None:
    """Create parent directories for the destination path."""

    Path(dest_path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def parse_datetime(value: str | None) -> datetime | None:
    """Parse ISO-8601 timestamps returned by DingTalk APIs."""

    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def chunk_count(file_size: int, chunk_size: int) -> int:
    """Return the number of chunks needed for the given file size."""

    return max(1, (file_size + chunk_size - 1) // chunk_size)


__all__ = [
    "detect_mime_type",
    "iter_file_chunks",
    "ensure_directory",
    "parse_datetime",
    "chunk_count",
]
