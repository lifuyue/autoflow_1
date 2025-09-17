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

CANONICAL_TO_OUTPUT = {
    "year": "年份",
    "month": "月份",
    "mid_rate": "中间价",
    "query_date": "查询日期",
    "source_date": "来源日期",
    "rate_source": "数据源",
    "fallback_used": "回退策略",
}

FIELD_ALIASES: dict[str, set[str]] = {
    "year": {"year", "年份"},
    "month": {"month", "月份"},
    "mid_rate": {"mid_rate", "中间价", "中间价(1美元)"},
    "query_date": {"query_date", "查询日期", "目标日期", "首个工作日"},
    "source_date": {"source_date", "来源日期", "公告日期"},
    "rate_source": {"rate_source", "数据源", "来源渠道"},
    "fallback_used": {"fallback_used", "回退策略", "fallback"},
}

OUTPUT_HEADER = [CANONICAL_TO_OUTPUT[field] for field in CANONICAL_FIELDS]


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


_ALIAS_LOOKUP: dict[str, str] = {}
for canonical, aliases in FIELD_ALIASES.items():
    for alias in aliases:
        normalized_alias = alias.strip()
        _ALIAS_LOOKUP.setdefault(normalized_alias, canonical)
        _ALIAS_LOOKUP.setdefault(normalized_alias.lower(), canonical)


def _canonical_field_for(raw_key: str | None) -> str | None:
    if raw_key is None:
        return None
    token = _normalize_header_cell(str(raw_key))
    return _ALIAS_LOOKUP.get(token) or _ALIAS_LOOKUP.get(token.lower())


def _canonicalize_mapping(payload: Mapping[str, object]) -> dict[str, object]:
    canonical: dict[str, object] = {}
    for key, value in payload.items():
        canonical_key = _canonical_field_for(key)
        if not canonical_key:
            continue
        canonical[canonical_key] = value
    return canonical


def _ensure_all_fields(
    record: Mapping[str, object],
    *,
    year: int,
    month: int,
) -> dict[str, str]:
    canonical_record = _canonicalize_mapping(record)
    normalized = {field: "" for field in CANONICAL_FIELDS}

    for field in CANONICAL_FIELDS:
        value = canonical_record.get(field)
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        normalized[field] = text

    normalized["year"] = str(int(year))
    normalized["month"] = f"{int(month):02d}"

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
        rows = list(csv.reader(handle))

    if not rows:
        return records

    header_cells = [_normalize_header_cell(cell) for cell in rows[0]]
    header_mapping: dict[str, int] = {}
    for idx, cell in enumerate(header_cells):
        canonical = _canonical_field_for(cell)
        if canonical and canonical not in header_mapping:
            header_mapping[canonical] = idx

    def _has_numeric_year_month() -> bool:
        year_idx = header_mapping.get("year")
        month_idx = header_mapping.get("month")
        year_numeric = year_idx is not None and year_idx < len(header_cells) and header_cells[year_idx].isdigit()
        month_numeric = month_idx is not None and month_idx < len(header_cells) and header_cells[month_idx].isdigit()
        return year_numeric and month_numeric

    header_is_present = bool(header_mapping) and not _has_numeric_year_month()

    data_rows = rows[1:] if header_is_present else rows
    header_aliases = header_cells if header_is_present else None

    for raw_row in data_rows:
        if not raw_row or not any(cell.strip() for cell in raw_row):
            continue
        cells = [_normalize_header_cell(cell) for cell in raw_row]
        if header_aliases is not None:
            raw_mapping = {
                header_aliases[idx]: cells[idx]
                for idx in range(min(len(header_aliases), len(cells)))
            }
        else:
            raw_mapping = {
                CANONICAL_FIELDS[idx]: cells[idx]
                for idx in range(min(len(CANONICAL_FIELDS), len(cells)))
            }
        record = _canonicalize_mapping(raw_mapping)
        year_raw = record.get("year")
        month_raw = record.get("month")
        if year_raw is None or month_raw is None:
            LOGGER.warning("Skipping CSV row missing year/month: %s", raw_row)
            continue
        try:
            year_val = int(str(year_raw).strip())
            month_val = int(str(month_raw).strip())
        except (TypeError, ValueError):
            LOGGER.warning("Skipping malformed CSV row during merge: %s", raw_row)
            continue
        records[(year_val, month_val)] = _ensure_all_fields(record, year=year_val, month=month_val)

    return records


def _normalize_row_input(row: Mapping[str, object]) -> dict[str, str]:
    normalized = {field: "" for field in CANONICAL_FIELDS}
    canonical_row = _canonicalize_mapping(row)

    for field, value in canonical_row.items():
        if value is None:
            continue
        if field == "year":
            try:
                normalized[field] = str(int(str(value).strip()))
            except (TypeError, ValueError) as exc:  # pragma: no cover - defensive guard
                raise ValueError(f"Invalid year value: {value!r}") from exc
            continue
        if field == "month":
            try:
                month_int = int(str(value).strip())
            except (TypeError, ValueError) as exc:  # pragma: no cover - defensive guard
                raise ValueError(f"Invalid month value: {value!r}") from exc
            normalized[field] = f"{month_int:02d}"
            continue
        text = str(value).strip()
        if not text:
            continue
        normalized[field] = text

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
    consumed = 0
    changed = 0

    for row in rows:
        consumed += 1
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

        record = _ensure_all_fields(normalized, year=year_val, month=month_val)
        key = (year_val, month_val)
        if existing.get(key) != record:
            existing[key] = record
            changed += 1

    ordered_keys = sorted(existing.keys())
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = csv_path.with_suffix(csv_path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_HEADER)
        writer.writeheader()
        for key in ordered_keys:
            record = existing[key]
            writer.writerow({
                CANONICAL_TO_OUTPUT[field]: record.get(field, "")
                for field in CANONICAL_FIELDS
            })
    tmp_path.replace(csv_path)

    LOGGER.info(
        "CSV upsert done: path=%s consumed=%d changed=%d total_rows=%d",
        csv_path,
        consumed,
        changed,
        len(ordered_keys),
    )


def format_rate(rate: Decimal) -> str:
    """Format a rate to four decimal places using half-up rounding."""

    return str(rate.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))
