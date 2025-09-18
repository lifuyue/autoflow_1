"""Logging helpers for the autoflow_io package."""

# Module responsibilities:
# - Centralize logging configuration with file + stream handlers.
# - Provide get_logger() that ensures directories exist and configuration occurs once.

from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path
from typing import Optional

DEFAULT_LOG_BASE = Path.home() / "AutoFlow" / "logs"
_LOG_CONFIGURED = False


def _resolve_log_dir(log_dir: Optional[Path] = None) -> Path:
    """Resolve the log directory, ensuring existence."""
    target = log_dir or DEFAULT_LOG_BASE
    target.mkdir(parents=True, exist_ok=True)
    return target


def _configure_logging(log_dir: Optional[Path] = None) -> None:
    """Configure root logging once with rotating file + console handlers."""
    global _LOG_CONFIGURED
    if _LOG_CONFIGURED:
        return

    directory = _resolve_log_dir(log_dir)
    log_path = directory / "autoflow_io.log"

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=2_000_000, backupCount=3
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    root_logger = logging.getLogger("autoflow_io")
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    root_logger.propagate = False

    _LOG_CONFIGURED = True


def get_logger(name: str, log_dir: Optional[Path] = None) -> logging.Logger:
    """Return a package-scoped logger.

    Args:
        name: Logger name suffix appended to the package root logger namespace.
        log_dir: Optional override for the logging directory.

    Returns:
        Configured logger scoped under ``autoflow_io``.
    """

    _configure_logging(log_dir)
    return logging.getLogger(f"autoflow_io.{name}")
