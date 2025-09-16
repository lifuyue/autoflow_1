"""Data models used by the form processor service."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import List, Optional

import pandas as pd


@dataclass(slots=True)
class ProcessedRow:
    """Normalized form row after mapping, cleaning, and computation."""

    project: Optional[str]
    amount: Optional[Decimal]
    currency: Optional[str]
    date: Optional[str]
    base_amount: Optional[Decimal]
    base_currency: str
    need_confirm: bool = False
    issues: List[str] = field(default_factory=list)
    source_file: Optional[str] = None
    source_row: Optional[int] = None
    rate_date_used: Optional[str] = None


@dataclass(slots=True)
class ProcessedFrame:
    """Wrapper around a pandas ``DataFrame`` keeping processed rows."""

    dataframe: pd.DataFrame

    def copy(self) -> "ProcessedFrame":
        """Return a deep copy of the underlying frame."""

        return ProcessedFrame(dataframe=self.dataframe.copy(deep=True))

    def to_dataframe(self) -> pd.DataFrame:
        """Expose the underlying DataFrame."""

        return self.dataframe


__all__ = ["ProcessedRow", "ProcessedFrame"]
