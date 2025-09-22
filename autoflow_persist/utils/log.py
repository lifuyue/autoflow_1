"""
RESPONSIBILITIES
- Provide a persistence-local logger helper reusing the core logging setup.
- Ensure the ~/AutoFlow/logs directory exists before logger creation.
PROCESS OVERVIEW
1. Callers request get_logger(name, root).
2. ensure_structure() creates the log directory if necessary.
3. The core autoflow logger is reused and a child logger is returned.
"""

from __future__ import annotations

import logging
from pathlib import Path

from autoflow.core.logger import get_logger as core_get_logger

from .paths import ensure_structure


def get_logger(name: str, root: Path | None = None) -> logging.Logger:
    """Return a namespaced logger for persistence modules."""

    directories = ensure_structure(root)
    base_logger = core_get_logger(directories["logs"])
    return base_logger.getChild(name)
