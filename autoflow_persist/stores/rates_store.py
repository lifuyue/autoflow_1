"""
RESPONSIBILITIES
- Manage the XLSX-backed permanent store for FX mid rates.
- Handle initialization, CSV imports, idempotent upserts, and range queries.
PROCESS OVERVIEW
1. init_rates_store() ensures ~/AutoFlow/store/rates_store.xlsx is ready.
2. bulk_import_csv() canonicalizes historical CSV payloads and upserts each record.
3. upsert_rate() merges a single record keyed by base/quote/rate_date.
4. query_rates() returns a pandas.DataFrame filtered by date range and currency pair.
5. healthcheck() verifies dependencies, permissions, and lock availability.
"""

from __future__ import annotations

import csv
import logging
import os
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd

from autoflow_persist.schemas.rates import RatesQuery, RatesRecord
from autoflow_persist.stores.base_store import (
    BaseStore,
    PersistHealth,
    StoreInitializationError,
    StoreValidationError,
)
from autoflow_persist.utils.excel_io import ensure_workbook, read_sheet, write_sheet, workbook_lock
from autoflow_persist.utils.log import get_logger
from autoflow_persist.utils.paths import ensure_structure, store_file_path

RATES_SHEET_NAME = "rates"
RATES_WORKBOOK = "rates_store.xlsx"
RATES_COLUMNS: tuple[str, ...] = (
    "base_currency",
    "quote_currency",
    "rate_mid",
    "rate_date",
    "fetch_date",
    "source",
    "fallback_strategy",
    "year",
    "month",
    "download_url",
    "created_at",
    "updated_at",
)

_DECIMAL_QUANT = Decimal("0.0001")

_CSV_ALIASES: dict[str, set[str]] = {
    "rate_mid": {"mid_rate", "rate_mid", "中间价", "中间价(1美元)"},
    "rate_date": {"rate_date", "query_date", "查询日期", "目标日期"},
    "fetch_date": {"fetch_date", "请求日期", "source_date", "来源日期"},
    "year": {"year", "年份"},
    "month": {"month", "月份"},
    "fallback_strategy": {"fallback", "fallback_used", "回退策略"},
    "source": {"source", "rate_source", "数据源", "来源渠道"},
    "download_url": {"download_url", "下载地址"},
}


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_rate_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        raise StoreValidationError("rate_date is required")
    try:
        if "T" in text:
            return datetime.fromisoformat(text).date()
        return date.fromisoformat(text)
    except ValueError as exc:
        raise StoreValidationError(f"Invalid rate_date: {text}") from exc


def _parse_fetch_datetime(value: object, fallback: date) -> str:
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.replace(microsecond=0).isoformat()
    if isinstance(value, date):
        as_dt = datetime.combine(value, time.min, tzinfo=timezone.utc)
        return as_dt.isoformat()
    text = str(value).strip() if value is not None else ""
    if not text:
        as_dt = datetime.combine(fallback, time.min, tzinfo=timezone.utc)
        return as_dt.isoformat()
    try:
        if "T" in text:
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.replace(microsecond=0).isoformat()
        parsed_date = date.fromisoformat(text)
        as_dt = datetime.combine(parsed_date, time.min, tzinfo=timezone.utc)
        return as_dt.isoformat()
    except ValueError as exc:
        raise StoreValidationError(f"Invalid fetch_date: {text}") from exc


def _parse_decimal(value: object) -> Decimal:
    text = str(value).strip()
    if not text:
        raise StoreValidationError("rate_mid is required")
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise StoreValidationError(f"Invalid rate_mid: {text}") from exc


def _canonical_csv_key(raw_key: str | None) -> str | None:
    if raw_key is None:
        return None
    token = str(raw_key).strip()
    lowered = token.lower()
    for canonical, aliases in _CSV_ALIASES.items():
        if token in aliases or lowered in {alias.lower() for alias in aliases}:
            return canonical
    return token


class RatesStore(BaseStore):
    """Concrete XLSX store handling FX mid rates."""

    sheet_name = RATES_SHEET_NAME
    columns = RATES_COLUMNS

    def __init__(self, root: Path | str | None = None, *, logger: logging.Logger | None = None) -> None:
        resolved_root = Path(root).expanduser().resolve() if root else None
        super().__init__(logger=logger or get_logger("rates_store", resolved_root))
        self._root = resolved_root
        self.path = store_file_path(RATES_WORKBOOK, self._root)

    # BaseStore API -----------------------------------------------------------------

    def init_store(self) -> Path:
        self.logger.debug("Ensuring rates store workbook exists at %s", self.path)
        try:
            ensure_workbook(self.path, self.sheet_name, self.columns)
        except OSError as exc:
            raise StoreInitializationError(str(exc)) from exc
        return self.path

    def upsert(
        self,
        record: Mapping[str, object] | RatesRecord,
        *,
        download_url: str | None = None,
    ) -> None:
        payload = self._normalize_record(record, download_url=download_url)
        self.init_store()
        key = self._primary_key(payload)
        now_iso = _utcnow_iso()
        with workbook_lock(self.path):
            rows = read_sheet(self.path, self.sheet_name, self.columns, use_lock=False)
            existing = [self._normalize_loaded_row(row) for row in rows]
            updated: list[dict[str, object]] = []
            matched = False
            for row in existing:
                if self._primary_key(row) == key:
                    payload.setdefault("created_at", row.get("created_at", now_iso))
                    payload["updated_at"] = now_iso
                    matched = True
                    updated.append(payload)
                else:
                    updated.append(row)
            if not matched:
                payload.setdefault("created_at", now_iso)
                payload["updated_at"] = now_iso
                updated.append(payload)
            write_sheet(self.path, self.sheet_name, updated, self.columns, use_lock=False)

    def bulk_import(self, payload: Iterable[Mapping[str, object] | RatesRecord], **kwargs: object) -> int:
        count = 0
        download_url = kwargs.get("download_url")
        for record in payload:
            override = download_url if isinstance(download_url, str) else None
            self.upsert(record, download_url=override)
            count += 1
        return count

    def query(self, params: Mapping[str, object] | RatesQuery) -> pd.DataFrame:
        filters = self._normalize_query(params)
        self.init_store()
        rows = read_sheet(self.path, self.sheet_name, self.columns)
        if not rows:
            return pd.DataFrame(columns=self.columns)
        normalized = [self._normalize_loaded_row(row) for row in rows]
        frame = pd.DataFrame(normalized, columns=self.columns)
        if frame.empty:
            return frame

        frame["base_currency"] = frame["base_currency"].astype(str).str.upper()
        frame["quote_currency"] = frame["quote_currency"].astype(str).str.upper()
        frame["rate_mid"] = frame["rate_mid"].apply(
            lambda val: Decimal(str(val)) if str(val).strip() else None
        )
        frame["rate_date"] = frame["rate_date"].apply(
            lambda val: _parse_rate_date(val) if str(val).strip() else None
        )
        frame["fetch_date"] = frame["fetch_date"].astype(str)
        frame["year"] = frame["year"].apply(lambda val: int(val) if str(val).strip() else None)
        frame["month"] = frame["month"].apply(lambda val: int(val) if str(val).strip() else None)

        base = filters.get("base_currency")
        if base:
            frame = frame[frame["base_currency"] == base]
        quote = filters.get("quote_currency")
        if quote:
            frame = frame[frame["quote_currency"] == quote]
        start_date = filters.get("start_date")
        if start_date:
            frame = frame[frame["rate_date"].notnull()]
            frame = frame[frame["rate_date"] >= start_date]
        end_date = filters.get("end_date")
        if end_date:
            frame = frame[frame["rate_date"].notnull()]
            frame = frame[frame["rate_date"] <= end_date]

        frame = frame.sort_values("rate_date")
        return frame.reset_index(drop=True)

    def healthcheck(self) -> PersistHealth:
        issues: list[str] = []
        dependencies = {"pandas": True, "openpyxl": True}
        writable_paths: dict[str, bool] = {}
        locked: list[str] = []

        try:
            ensure_structure(self._root)
        except OSError as exc:
            issues.append(f"Failed to ensure root directories: {exc}")

        target_dir = self.path.parent
        target_dir.mkdir(parents=True, exist_ok=True)
        writable_paths[str(target_dir)] = os.access(target_dir, os.W_OK | os.X_OK)
        try:
            ensure_workbook(self.path, self.sheet_name, self.columns)
        except StoreInitializationError as exc:
            issues.append(str(exc))
        try:
            with workbook_lock(self.path):
                pass
        except Exception as exc:  # noqa: BLE001 - capture locking issues generically
            locked.append(str(self.path))
            issues.append(f"Lock acquisition failed: {exc}")

        return PersistHealth(
            dependencies=dependencies,
            writable_paths=writable_paths,
            locked_paths=locked,
            issues=issues,
        )

    # Helpers ----------------------------------------------------------------------

    def _normalize_record(
        self,
        record: Mapping[str, object] | RatesRecord,
        *,
        download_url: str | None,
    ) -> dict[str, object]:
        if isinstance(record, RatesRecord):
            payload = record.to_dict()
        else:
            payload = dict(record)
        if download_url is not None:
            payload["download_url"] = download_url

        for field in ("base_currency", "quote_currency", "rate_mid", "rate_date", "fetch_date", "source"):
            if not str(payload.get(field, "")).strip():
                raise StoreValidationError(f"Missing required field: {field}")

        rate_mid = _parse_decimal(payload["rate_mid"]).quantize(_DECIMAL_QUANT)
        rate_date = _parse_rate_date(payload["rate_date"])
        fetch_date_iso = _parse_fetch_datetime(payload.get("fetch_date"), rate_date)

        fallback = str(payload.get("fallback_strategy", "") or "").strip()
        download_val = str(payload.get("download_url", "") or "").strip()

        normalized: dict[str, object] = {
            "base_currency": str(payload.get("base_currency")).upper(),
            "quote_currency": str(payload.get("quote_currency")).upper(),
            "rate_mid": f"{rate_mid}",
            "rate_date": rate_date.isoformat(),
            "fetch_date": fetch_date_iso,
            "source": str(payload.get("source")),
            "fallback_strategy": fallback,
            "year": rate_date.year,
            "month": rate_date.month,
            "download_url": download_val,
        }
        if "created_at" in payload and str(payload["created_at"]).strip():
            normalized["created_at"] = str(payload["created_at"]).strip()
        if "updated_at" in payload and str(payload["updated_at"]).strip():
            normalized["updated_at"] = str(payload["updated_at"]).strip()
        return normalized

    def _normalize_loaded_row(self, row: Mapping[str, object]) -> dict[str, object]:
        normalized: dict[str, object] = {}
        for column in self.columns:
            value = row.get(column, "")
            if isinstance(value, datetime):
                normalized[column] = value.replace(microsecond=0).isoformat()
            elif isinstance(value, date):
                normalized[column] = value.isoformat()
            elif value is None:
                normalized[column] = ""
            else:
                normalized[column] = value
        if str(normalized.get("base_currency", "")).strip():
            normalized["base_currency"] = str(normalized["base_currency"]).upper()
        if str(normalized.get("quote_currency", "")).strip():
            normalized["quote_currency"] = str(normalized["quote_currency"]).upper()
        if str(normalized.get("rate_date", "")).strip():
            rate_dt = _parse_rate_date(normalized["rate_date"])
            normalized["rate_date"] = rate_dt.isoformat()
            normalized["year"] = rate_dt.year
            normalized["month"] = rate_dt.month
        return normalized

    def _normalize_query(self, params: Mapping[str, object] | RatesQuery) -> dict[str, object]:
        if isinstance(params, RatesQuery):
            raw = params.to_dict()
        else:
            raw = dict(params)
        result: dict[str, object] = {}
        base = raw.get("base_currency")
        if base:
            result["base_currency"] = str(base).upper()
        quote = raw.get("quote_currency")
        if quote:
            result["quote_currency"] = str(quote).upper()
        start = raw.get("start_date")
        if start:
            result["start_date"] = _parse_rate_date(start)
        end = raw.get("end_date")
        if end:
            result["end_date"] = _parse_rate_date(end)
        return result

    @staticmethod
    def _primary_key(payload: Mapping[str, object]) -> tuple[str, str, str]:
        return (
            str(payload["base_currency"]).upper(),
            str(payload["quote_currency"]).upper(),
            str(payload["rate_date"]),
        )

    # CSV helpers ------------------------------------------------------------------

    def import_csv(
        self,
        csv_path: Path,
        *,
        base: str,
        quote: str,
        source: str,
        fallback: str | None = None,
        download_url: str | None = None,
    ) -> int:
        rows = self._load_csv(csv_path, base, quote, source, fallback)
        return self.bulk_import(rows, download_url=download_url)

    def _load_csv(
        self,
        csv_path: Path,
        base: str,
        quote: str,
        source: str,
        fallback: str | None,
    ) -> list[dict[str, object]]:
        if not csv_path.exists():
            raise FileNotFoundError(csv_path)
        records: list[dict[str, object]] = []
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                canonical: dict[str, object] = {}
                for key, value in row.items():
                    canonical_key = _canonical_csv_key(key)
                    if canonical_key:
                        canonical[canonical_key] = value
                rate_mid_raw = canonical.get("rate_mid")
                rate_mid = _parse_decimal(rate_mid_raw).quantize(_DECIMAL_QUANT)
                rate_date = canonical.get("rate_date")
                if not rate_date:
                    year = canonical.get("year")
                    month = canonical.get("month")
                    if year and month:
                        rate_date = f"{int(year):04d}-{int(month):02d}-01"
                    else:
                        raise StoreValidationError("CSV row missing rate_date/year/month")
                parsed_date = _parse_rate_date(rate_date)
                fetch_date_value = canonical.get("fetch_date") or rate_date
                fetch_iso = _parse_fetch_datetime(fetch_date_value, parsed_date)
                record = {
                    "base_currency": base,
                    "quote_currency": quote,
                    "rate_mid": f"{rate_mid}",
                    "rate_date": parsed_date.isoformat(),
                    "fetch_date": fetch_iso,
                    "source": canonical.get("source") or source,
                    "fallback_strategy": canonical.get("fallback_strategy") or (fallback or ""),
                    "year": parsed_date.year,
                    "month": parsed_date.month,
                    "download_url": canonical.get("download_url", ""),
                }
                records.append(record)
        return records


# Convenience facade ---------------------------------------------------------------


def init_rates_store(root: Path | None = None) -> Path:
    store = RatesStore(root)
    return store.init_store()


def upsert_rate(
    rec: RatesRecord | Mapping[str, object],
    *,
    root: Path | None = None,
    download_url: str | None = None,
) -> None:
    store = RatesStore(root)
    store.upsert(rec, download_url=download_url)


def bulk_import_csv(
    csv_path: Path,
    *,
    base: str,
    quote: str,
    source: str,
    fallback: str | None = None,
    root: Path | None = None,
    download_url: str | None = None,
) -> int:
    store = RatesStore(root)
    return store.import_csv(
        csv_path,
        base=base,
        quote=quote,
        source=source,
        fallback=fallback,
        download_url=download_url,
    )


def query_rates(params: RatesQuery | Mapping[str, object], *, root: Path | None = None) -> pd.DataFrame:
    store = RatesStore(root)
    return store.query(params)


def rates_healthcheck(root: Path | None = None) -> PersistHealth:
    store = RatesStore(root)
    return store.healthcheck()
