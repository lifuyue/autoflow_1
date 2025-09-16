"""Cleaning helpers for form ingestion."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import List

import pandas as pd


def _empty_issue_list(size: int) -> List[List[str]]:
    return [[] for _ in range(size)]


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False


def normalize_whitespace(frame: pd.DataFrame) -> pd.DataFrame:
    """Trim surrounding whitespace for string columns."""

    df = frame.copy()
    for col in df.select_dtypes(include=["object"]).columns:
        if col == "issues":
            continue
        df[col] = df[col].map(lambda x: x.strip() if isinstance(x, str) else x)
    return df


def clean_dataframe(frame: pd.DataFrame) -> pd.DataFrame:
    """Clean raw dataframe into normalized types."""

    df = normalize_whitespace(frame)
    if "issues" not in df.columns:
        df["issues"] = _empty_issue_list(len(df))

    if "amount" in df.columns:
        clean_amounts: List[Decimal | None] = []
        for idx, value in df["amount"].items():
            if _is_missing(value):
                clean_amounts.append(None)
                continue
            if isinstance(value, Decimal):
                clean_amounts.append(value)
                continue
            text = str(value).strip().replace(",", "")
            try:
                clean_amounts.append(Decimal(text))
            except (InvalidOperation, ValueError):
                issues = df.at[idx, "issues"]
                issues.append("invalid_amount")
                clean_amounts.append(None)
        df["amount"] = clean_amounts

    if "currency" in df.columns:
        normalized_currency: List[str | None] = []
        for idx, value in df["currency"].items():
            if _is_missing(value):
                normalized_currency.append(None)
                continue
            normalized = str(value).strip().upper()
            if len(normalized) not in (2, 3):
                df.at[idx, "issues"].append("invalid_currency")
                normalized_currency.append(normalized)
            else:
                normalized_currency.append(normalized)
        df["currency"] = normalized_currency

    if "date" in df.columns:
        normalized_dates: List[str | None] = []
        for idx, value in df["date"].items():
            if _is_missing(value):
                normalized_dates.append(None)
                continue
            parsed = pd.to_datetime(value, errors="coerce")
            if pd.isna(parsed):
                df.at[idx, "issues"].append("invalid_date")
                normalized_dates.append(None)
            else:
                normalized_dates.append(parsed.date().isoformat())
        df["date"] = normalized_dates

    return df
