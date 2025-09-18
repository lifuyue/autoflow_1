"""Excel input helpers."""

# Module responsibilities:
# - Provide a thin wrapper around pandas.read_excel with strong validation.
# - Emit structured logs for traceability and future auditing.

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional, Union

import pandas as pd

from .utils.log import get_logger

logger = get_logger("excel_reader")

SheetType = Union[str, int, None]


def read_table(
    path: Path,
    sheet: SheetType = None,
    usecols: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """Load a DataFrame from an Excel workbook.

    Args:
        path: Path to the workbook.
        sheet: Sheet name or index; defaults to the first sheet.
        usecols: Optional iterable of columns to include.

    Returns:
        DataFrame containing the requested data.

    Raises:
        FileNotFoundError: When the Excel file does not exist.
        ValueError: When pandas fails to parse the sheet/columns requested.
    """

    if not path.exists():
        raise FileNotFoundError(f"Source workbook not found: {path}")

    logger.info("Reading Excel workbook", extra={"path": str(path), "sheet": sheet})

    try:
        df = pd.read_excel(path, sheet_name=sheet, usecols=list(usecols) if usecols else None)
    except ValueError as exc:
        logger.error("Failed to read Excel workbook", extra={"error": str(exc)})
        raise

    if isinstance(df, dict):
        # pandas returns a dict when sheet_name is a list; this API expects a single sheet.
        raise ValueError("read_table expects a single sheet; received multiple sheets")

    logger.info(
        "Excel workbook loaded",
        extra={"rows": len(df.index), "columns": df.columns.tolist()},
    )
    return df
