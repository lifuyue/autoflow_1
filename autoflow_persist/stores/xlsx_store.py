"""
RESPONSIBILITIES
- Placeholder XLSX store for structured XLSX ingestion results.
- Mirrors rates store initialization while deferring full CRUD logic.
PROCESS OVERVIEW
1. init_xlsx_store() ensures xlsx_store.xlsx exists under ~/AutoFlow/store.
2. Future upsert/query implementations will populate the xlsx_index sheet.
3. healthcheck() currently validates directory and workbook presence.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd

from autoflow_persist.schemas.xlsxrec import XlsxIndexRecord
from autoflow_persist.stores.base_store import BaseStore, PersistHealth, StoreInitializationError
from autoflow_persist.utils.excel_io import ensure_workbook
from autoflow_persist.utils.log import get_logger
from autoflow_persist.utils.paths import ensure_structure, store_file_path

_XLSX_WORKBOOK = "xlsx_store.xlsx"
_XLSX_SHEET = "xlsx_index"
_XLSX_COLUMNS: tuple[str, ...] = (
    "source_sha256",
    "source_path",
    "sheet_name",
    "rows",
    "mapping_version",
    "download_url",
    "created_at",
)


class XLSXStore(BaseStore):
    """Placeholder implementation for XLSX ingestion persistence."""

    sheet_name = _XLSX_SHEET
    columns = _XLSX_COLUMNS

    def __init__(self, root: Path | str | None = None, *, logger: logging.Logger | None = None) -> None:
        resolved_root = Path(root).expanduser().resolve() if root else None
        super().__init__(logger=logger or get_logger("xlsx_store", resolved_root))
        self._root = resolved_root
        self.path = store_file_path(_XLSX_WORKBOOK, self._root)

    def init_store(self) -> Path:
        try:
            ensure_workbook(self.path, self.sheet_name, self.columns)
        except OSError as exc:
            raise StoreInitializationError(str(exc)) from exc
        return self.path

    def upsert(self, record: Mapping[str, object] | XlsxIndexRecord, *, download_url: str | None = None) -> None:
        raise NotImplementedError("TODO: implement XLSX store upsert")

    def bulk_import(self, payload: Iterable[Mapping[str, object] | XlsxIndexRecord], **kwargs: object) -> int:
        raise NotImplementedError("TODO: implement XLSX store bulk import")

    def query(self, params: Mapping[str, object]) -> pd.DataFrame:
        raise NotImplementedError("TODO: implement XLSX store query")

    def healthcheck(self) -> PersistHealth:
        issues: list[str] = []
        try:
            ensure_structure(self._root)
            ensure_workbook(self.path, self.sheet_name, self.columns)
        except OSError as exc:
            issues.append(str(exc))
        writable = {str(self.path.parent): self.path.parent.exists()}
        return PersistHealth(
            dependencies={"openpyxl": True},
            writable_paths=writable,
            locked_paths=[],
            issues=issues,
        )


def init_xlsx_store(root: Path | None = None) -> Path:
    store = XLSXStore(root)
    return store.init_store()
