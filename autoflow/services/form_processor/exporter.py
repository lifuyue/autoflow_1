"""Excel exporter for processed forms."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
from openpyxl import Workbook

_EXPORT_COLUMNS: Iterable[str] = (
    "project",
    "date",
    "currency",
    "amount",
    "base_currency",
    "base_amount",
    "exchange_rate",
    "rate_date_used",
    "need_confirm",
    "source_file",
    "source_row",
)


def export_template(frame: pd.DataFrame, output_dir: Path) -> Path:
    """Write accepted rows into a simple Excel template."""

    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"processed_forms_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    wb = Workbook()
    ws = wb.active
    ws.title = "Invoice"
    ws["A1"] = "Processed Items"  # TODO: replace with real template glue during integration

    headers = list(_EXPORT_COLUMNS)
    ws.append(headers)

    for _, row in frame.iterrows():
        values = []
        for col in headers:
            value = row.get(col)
            if isinstance(value, list):
                value = "; ".join(str(v) for v in value)
            values.append(value)
        ws.append(values)

    wb.save(path)
    return path
