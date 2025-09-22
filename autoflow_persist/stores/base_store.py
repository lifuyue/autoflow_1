"""
RESPONSIBILITIES
- Define shared interfaces and exceptions for XLSX-based stores.
- Outline the workflow for init/import/upsert/query/healthcheck used by concrete stores.
- Provide basic locking helpers to guarantee single-writer semantics per process.
PROCESS OVERVIEW
1. init_store -> resolve target path, ensure directories and workbook skeleton exist.
2. bulk_import -> load sheet, normalize payloads, delegate to upsert per record.
3. upsert -> merge a single record by primary key, keeping created/updated timestamps.
4. query -> filter the in-memory frame returned from the sheet read helper.
5. healthcheck -> verify dependencies, directory write access, and lock availability.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Mapping, MutableMapping, Protocol


class StoreError(RuntimeError):
    """Base exception type for persistence-layer failures."""


class StoreInitializationError(StoreError):
    """Raised when a store cannot be initialized due to missing prerequisites."""


class StoreValidationError(StoreError):
    """Raised when input data fails validation rules."""


class StoreLockedError(StoreError):
    """Raised when a target workbook is locked by another process."""


@dataclass(slots=True)
class PersistHealth:
    """Structured report produced by health checks."""

    dependencies: dict[str, bool]
    writable_paths: dict[str, bool]
    locked_paths: list[str]
    issues: list[str] = field(default_factory=list)

    def is_healthy(self) -> bool:
        """Return True when no issues are observed."""

        return not self.issues and all(self.dependencies.values()) and all(
            self.writable_paths.values()
        )


class SupportsToDict(Protocol):
    """Typed protocol for records that offer dict serialization."""

    def to_dict(self) -> MutableMapping[str, object]:
        """Return a dict representation ready for persistence."""


class BaseStore(ABC):
    """Abstract class shared by concrete XLSX-backed stores."""

    sheet_name: str
    columns: tuple[str, ...]

    def __init__(self, *, logger: logging.Logger | None = None) -> None:
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def init_store(self) -> Path:
        """Ensure backing workbook exists, returning absolute path."""

    @abstractmethod
    def upsert(self, record: Mapping[str, object], *, download_url: str | None = None) -> None:
        """Merge a single record into the store, updating timestamps as needed."""

    @abstractmethod
    def bulk_import(self, payload: Iterable[Mapping[str, object]], **kwargs: object) -> int:
        """Import multiple records, returning the count of inserted/updated rows."""

    @abstractmethod
    def query(self, params: Mapping[str, object]) -> object:
        """Run a query and return results (usually a pandas.DataFrame)."""

    @abstractmethod
    def healthcheck(self) -> PersistHealth:
        """Run diagnostics for the store and return a structured report."""
