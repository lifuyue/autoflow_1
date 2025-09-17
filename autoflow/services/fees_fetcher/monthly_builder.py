"""Utilities for constructing monthly USD/CNY central parity caches."""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Callable, Iterable, Mapping, Optional, Sequence

import yaml

from autoflow.services.form_processor.providers import RateLookupError

from . import provider_router
from .pbc_client import CertHostnameMismatch

LOGGER = logging.getLogger(__name__)

CANONICAL_FIELDS = (
    "year",
    "month",
    "mid_rate",
    "query_date",
    "source_date",
    "rate_source",
    "fallback_used",
)

FIELD_ALIASES: dict[str, set[str]] = {
    "year": {"year", "年份"},
    "month": {"month", "月份"},
    "mid_rate": {"mid_rate", "中间价", "中间价(1美元)"},
    "query_date": {"query_date", "查询日期", "目标日期", "首个工作日"},
    "source_date": {"source_date", "来源日期", "公告日期"},
    "rate_source": {"rate_source", "数据源", "来源渠道"},
    "fallback_used": {"fallback_used", "回退策略", "fallback"},
}

OUTPUT_HEADER = [
    "年份",
    "月份",
    "中间价",
    "查询日期",
    "来源日期",
    "数据源",
    "回退策略",
]


@dataclass(frozen=True)
class MonthlyRateResult:
    """Computed monthly USD/CNY rate with routing metadata."""

    year: int
    month: int
    query_date: str
    request_date: str
    mid_rate: Decimal
    source_date: str
    rate_source: str
    fallback_used: str

    def to_csv_row(self) -> dict[str, str]:
        """Return canonical CSV row mapping for persistence."""

        return {
            "year": str(self.year),
            "month": f"{self.month:02d}",
            "mid_rate": format_rate(self.mid_rate),
            "query_date": self.query_date,
            "source_date": self.source_date,
            "rate_source": self.rate_source,
            "fallback_used": self.fallback_used,
        }


def _default_lookup(target: str, prefer: str) -> tuple[Decimal, str, str, str]:
    return provider_router.fetch_with_fallback(target, prefer_source=prefer)


def _normalize_header_cell(cell: str) -> str:
    return cell.strip()


def _resolve_header_map(header: Sequence[str]) -> dict[str, int]:
    mapping: dict[str, int] = {}
    for idx, cell in enumerate(header):
        normalized = _normalize_header_cell(cell)
        for field, aliases in FIELD_ALIASES.items():
            if normalized in aliases and field not in mapping:
                mapping[field] = idx
                break
    return mapping


def _is_header_row(cells: Sequence[str], mapping: Mapping[str, int]) -> bool:
    if not mapping:
        return False
    # Treat as header when year/month columns are non-numeric labels.
    for field in ("year", "month"):
        idx = mapping.get(field)
        if idx is None or idx >= len(cells):
            continue
        token = _normalize_header_cell(cells[idx])
        if token.isdigit():
            return False
    return True


def _row_to_record(cells: Sequence[str], mapping: Mapping[str, int]) -> dict[str, str]:
    record: dict[str, str] = {}
    for field in CANONICAL_FIELDS:
        idx = mapping.get(field)
        if idx is None or idx >= len(cells):
            continue
        record[field] = _normalize_header_cell(cells[idx])
    return record


def _ensure_all_fields(
    record: Mapping[str, str],
    *,
    year: int,
    month: int,
) -> dict[str, str]:
    normalized = {field: "" for field in CANONICAL_FIELDS}
    for field in CANONICAL_FIELDS:
        value = record.get(field, "")
        if value is None:
            continue
        text = value.strip()
        if not text:
            continue
        normalized[field] = text

    normalized["year"] = str(year)
    normalized["month"] = f"{month:02d}"

    if not normalized["query_date"]:
        normalized["query_date"] = normalized.get("source_date", "")
    if not normalized["fallback_used"]:
        normalized["fallback_used"] = "none"

    return normalized


def _load_existing_records(csv_path: Path) -> dict[tuple[int, int], dict[str, str]]:
    records: dict[tuple[int, int], dict[str, str]] = {}
    if not csv_path.exists():
        return records

    with open(csv_path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        try:
            first_row = next(reader)
        except StopIteration:
            return records

        header = [_normalize_header_cell(cell) for cell in first_row]
        mapping = _resolve_header_map(header)

        if not _is_header_row(header, mapping):
            # Treat the first row as data; estimate default mapping for legacy files.
            if not mapping:
                mapping = {"year": 0, "month": 1, "mid_rate": 2, "source_date": 3}
            rows_iter = [header]
        else:
            rows_iter = []

        rows_iter.extend(reader)

    for raw_row in rows_iter:
        if not raw_row:
            continue
        cells = [_normalize_header_cell(cell) for cell in raw_row]
        record = _row_to_record(cells, mapping)
        year_raw = record.get("year")
        month_raw = record.get("month")
        try:
            year_val = int(year_raw) if year_raw is not None else None
            month_val = int(month_raw) if month_raw is not None else None
        except (TypeError, ValueError):
            LOGGER.warning("Skipping malformed CSV row during merge: %s", raw_row)
            continue
        if year_val is None or month_val is None:
            LOGGER.warning("Skipping CSV row missing year/month: %s", raw_row)
            continue
        records[(year_val, month_val)] = _ensure_all_fields(record, year=year_val, month=month_val)

    return records


def _normalize_row_input(row: Mapping[str, object]) -> dict[str, str]:
    normalized = {field: "" for field in CANONICAL_FIELDS}
    for field in CANONICAL_FIELDS:
        if field not in row:
            continue
        value = row[field]
        if value is None:
            continue
        if field == "year":
            try:
                normalized[field] = str(int(value))
            except (TypeError, ValueError) as exc:  # pragma: no cover - defensive guard
                raise ValueError(f"Invalid year value: {value!r}") from exc
            continue
        if field == "month":
            try:
                normalized[field] = f"{int(value):02d}"
            except (TypeError, ValueError) as exc:  # pragma: no cover - defensive guard
                raise ValueError(f"Invalid month value: {value!r}") from exc
            continue
        normalized[field] = str(value).strip()

    if not normalized["fallback_used"]:
        normalized["fallback_used"] = "none"

    return normalized


def _is_business_day(
    target: date,
    *,
    holidays: Optional[set[str]] = None,
    workdays: Optional[set[str]] = None,
) -> bool:
    iso = target.isoformat()
    if holidays and iso in holidays:
        return False
    if target.weekday() >= 5:
        return bool(workdays and iso in workdays)
    return True


def _iter_months(start: date, end: date) -> Iterable[date]:
    current = start.replace(day=1)
    terminal = end.replace(day=1)
    while current <= terminal:
        yield current
        year = current.year + (1 if current.month == 12 else 0)
        month = 1 if current.month == 12 else current.month + 1
        current = date(year, month, 1)


def first_business_day(
    year: int,
    month: int,
    *,
    holidays: set[str] | None = None,
    workdays: set[str] | None = None,
) -> str:
    """Return the first business day for the given month."""

    current = date(year, month, 1)
    limit = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)

    while current < limit:
        if _is_business_day(current, holidays=holidays, workdays=workdays):
            return current.isoformat()
        current += timedelta(days=1)

    raise ValueError(f"No business day found for {year}-{month:02d}")


def plan_missing_months(csv_path: Path, start: date, end: date) -> list[tuple[int, int]]:
    """Compute month pairs that require population."""

    existing_rows = _load_existing_records(csv_path)
    existing_keys = set(existing_rows.keys())
    missing: list[tuple[int, int]] = []
    for marker in _iter_months(start, end):
        key = (marker.year, marker.month)
        if key not in existing_keys:
            missing.append(key)
    return missing


def fetch_month_rate(
    year: int,
    month: int,
    *,
    holidays: set[str] | None = None,
    workdays: set[str] | None = None,
    prefer_source: str = "auto",
    lookup: Callable[[str, str], tuple[Decimal, str, str, str]] = _default_lookup,
) -> MonthlyRateResult:
    """Retrieve a monthly rate using the configured lookup strategy."""

    first_day_str = first_business_day(year, month, holidays=holidays, workdays=workdays)
    first_day = datetime.strptime(first_day_str, "%Y-%m-%d").date()
    attempts: list[date] = [first_day]

    forward_cursor = first_day
    forward_trials = 0
    while forward_trials < 3:
        forward_cursor += timedelta(days=1)
        if forward_cursor.month != month:
            break
        if not _is_business_day(forward_cursor, holidays=holidays, workdays=workdays):
            continue
        attempts.append(forward_cursor)
        forward_trials += 1

    if len(attempts) > 1:
        LOGGER.info(
            "Forward fallback candidates for %04d-%02d: %s",
            year,
            month,
            ", ".join(d.isoformat() for d in attempts[1:]),
        )

    backward_cursor = first_day - timedelta(days=1)
    while backward_cursor.month == month:
        attempts.append(backward_cursor)
        backward_cursor -= timedelta(days=1)
    if len(attempts) > 1:
        LOGGER.info(
            "Full candidate sequence for %04d-%02d: %s",
            year,
            month,
            ", ".join(d.isoformat() for d in attempts),
        )

    tried: set[str] = set()
    for candidate in attempts:
        iso = candidate.isoformat()
        if iso in tried:
            continue
        tried.add(iso)
        LOGGER.debug("Attempting rate lookup for %s", iso)
        try:
            rate, source_date, rate_source, fallback_used = lookup(iso, prefer_source)
        except CertHostnameMismatch:
            raise
        except RateLookupError as exc:
            LOGGER.debug("Rate unavailable for %s: %s", iso, exc)
            continue
        LOGGER.info(
            "Monthly fetch success: query_date=%s request_date=%s source_date=%s source=%s fallback=%s",
            first_day_str,
            iso,
            source_date,
            rate_source,
            fallback_used,
        )
        return MonthlyRateResult(
            year=year,
            month=month,
            query_date=first_day_str,
            request_date=iso,
            mid_rate=rate,
            source_date=source_date,
            rate_source=rate_source,
            fallback_used=fallback_used or "none",
        )

    raise RateLookupError(
        f"USD/CNY rate unavailable for {year}-{month:02d}",
        original_date=f"{year}-{month:02d}-01",
    )


def load_cn_calendar() -> tuple[set[str], set[str]]:
    """Load Chinese mainland working calendar adjustments."""

    config_path = Path(__file__).resolve().parents[2] / "config" / "cn_workdays.yaml"
    if not config_path.exists():
        return set(), set()

    try:
        with open(config_path, "r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
    except Exception as exc:  # pragma: no cover - configuration errors are rare
        LOGGER.warning("Failed to read CN calendar config: %s", exc)
        return set(), set()

    holidays = set(payload.get("holidays", []) or [])
    workdays = set(payload.get("workdays", []) or [])
    return holidays, workdays


def upsert_csv(csv_path: Path, rows: Sequence[Mapping[str, object]]) -> None:
    """Persist or update monthly rate rows."""

    existing = _load_existing_records(csv_path)

    for row in rows:
        normalized = _normalize_row_input(row)
        year_str = normalized.get("year")
        month_str = normalized.get("month")
        if not year_str or not month_str:
            raise ValueError("CSV rows must include year and month")
        try:
            year_val = int(year_str)
            month_val = int(month_str)
        except ValueError as exc:  # pragma: no cover - defensive guard
            raise ValueError(f"Invalid year/month in row: {row!r}") from exc

        existing[(year_val, month_val)] = _ensure_all_fields(
            normalized,
            year=year_val,
            month=month_val,
        )

    ordered_keys = sorted(existing.keys())
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = csv_path.with_suffix(csv_path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(OUTPUT_HEADER)
        for key in ordered_keys:
            record = existing[key]
            writer.writerow([record.get(field, "") for field in CANONICAL_FIELDS])
    tmp_path.replace(csv_path)


def format_rate(rate: Decimal) -> str:
    """Format a rate to four decimal places using half-up rounding."""

    return str(rate.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))
