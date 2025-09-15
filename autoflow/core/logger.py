from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import sys
from .profiles import _work_dir


_LOGGER: logging.Logger | None = None


def get_logger(log_dir: Path | None = None) -> logging.Logger:
    """Return a configured application logger writing to ./autoflow/work/logs/app.log.

    Creates the directory if needed. Uses rotating file handler.
    """
    global _LOGGER
    if _LOGGER is not None:
        return _LOGGER

    if log_dir is None:
        base = _work_dir() / "logs"
    else:
        base = Path(log_dir)
    base.mkdir(parents=True, exist_ok=True)
    log_path = base / "app.log"

    logger = logging.getLogger("autoflow")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(log_path, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)

    _LOGGER = logger
    return logger

