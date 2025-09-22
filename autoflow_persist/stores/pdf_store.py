"""
RESPONSIBILITIES
- Placeholder XLSX store for future PDF comparison metadata.
- Only initializes workbook and surfaces TODO markers for core operations.
PROCESS OVERVIEW
1. init_pdf_store() ensures pdf_store.xlsx exists under ~/AutoFlow/store.
2. Future upsert/query implementations will mirror the rates store patterns.
3. healthcheck() currently validates path and workbook availability only.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd

from autoflow_persist.schemas.pdfrec import PDFIndexRecord
from autoflow_persist.stores.base_store import BaseStore, PersistHealth, StoreInitializationError
from autoflow_persist.utils.excel_io import ensure_workbook
from autoflow_persist.utils.log import get_logger
from autoflow_persist.utils.paths import ensure_structure, store_file_path

_PDF_WORKBOOK = "pdf_store.xlsx"
_PDF_SHEET = "pdf_index"
_PDF_COLUMNS: tuple[str, ...] = (
    "file_sha256",
    "file_name",
    "local_path",
    "parsed_ok",
    "invoice_no",
    "amount",
    "currency",
    "download_url",
    "created_at",
)


class PDFStore(BaseStore):
    """Placeholder implementation for PDF persistence."""

    sheet_name = _PDF_SHEET
    columns = _PDF_COLUMNS

    def __init__(self, root: Path | str | None = None, *, logger: logging.Logger | None = None) -> None:
        resolved_root = Path(root).expanduser().resolve() if root else None
        super().__init__(logger=logger or get_logger("pdf_store", resolved_root))
        self._root = resolved_root
        self.path = store_file_path(_PDF_WORKBOOK, self._root)

    def init_store(self) -> Path:
        try:
            ensure_workbook(self.path, self.sheet_name, self.columns)
        except OSError as exc:
            raise StoreInitializationError(str(exc)) from exc
        return self.path

    def upsert(self, record: Mapping[str, object] | PDFIndexRecord, *, download_url: str | None = None) -> None:
        raise NotImplementedError("TODO: implement PDF store upsert")

    def bulk_import(self, payload: Iterable[Mapping[str, object] | PDFIndexRecord], **kwargs: object) -> int:
        raise NotImplementedError("TODO: implement PDF store bulk import")

    def query(self, params: Mapping[str, object]) -> pd.DataFrame:
        raise NotImplementedError("TODO: implement PDF store query")

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


def init_pdf_store(root: Path | None = None) -> Path:
    store = PDFStore(root)
    return store.init_store()
