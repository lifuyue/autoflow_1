"""Utilities for constructing monthly USD/CNY central parity caches."""

from __future__ import annotations

import csv
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Iterable, Optional

import yaml

from autoflow.services.form_processor.providers import RateLookupError

from .pbc_client import CertHostnameMismatch
from .pbc_provider import PBOCRateProvider

LOGGER = logging.getLogger(__name__)

_HEADER = ("年份", "月份", "中间价", "来源日期")


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


def plan_missing_months(csv_path: Path, start: date, today: date) -> list[tuple[int, int]]:
    """Compute month pairs that require population."""

    existing: set[tuple[int, int]] = set()
    if csv_path.exists():
        with open(csv_path, "r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            for row in reader:
                if not row:
                    continue
                if row[:2] == list(_HEADER[:2]):
                    continue
                try:
                    year = int(row[0])
                    month = int(row[1])
                except (ValueError, IndexError):  # pragma: no cover - malformed rows
                    LOGGER.warning("Skipping malformed CSV row: %s", row)
                    continue
                existing.add((year, month))

    missing: list[tuple[int, int]] = []
    for marker in _iter_months(start, today):
        key = (marker.year, marker.month)
        if key not in existing:
            missing.append(key)
    return missing


def fetch_month_rate(
    year: int,
    month: int,
    provider: PBOCRateProvider,
    *,
    holidays: set[str] | None = None,
    workdays: set[str] | None = None,
) -> tuple[Decimal, str]:
    """Retrieve a monthly rate using the configured provider."""

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
            rate = provider.get_rate(iso, "USD", "CNY")
        except CertHostnameMismatch:
            raise
        except RateLookupError as exc:
            LOGGER.debug("Rate unavailable for %s: %s", iso, exc)
            continue
        return rate, iso

    raise RateLookupError(f"USD/CNY rate unavailable for {year}-{month:02d}", original_date=f"{year}-{month:02d}-01")


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


def upsert_csv(csv_path: Path, rows: list[tuple[int, int, str, str]]) -> None:
    """Persist or update monthly rate rows."""

    existing: dict[tuple[int, int], tuple[int, int, str, str]] = {}
    if csv_path.exists():
        with open(csv_path, "r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            for row in reader:
                if not row:
                    continue
                if row[:4] == list(_HEADER):
                    continue
                try:
                    year = int(row[0])
                    month = int(row[1])
                    rate_str = row[2]
                    source = row[3]
                except (ValueError, IndexError):  # pragma: no cover - malformed rows ignored
                    LOGGER.warning("Skipping malformed CSV row during merge: %s", row)
                    continue
                existing[(year, month)] = (year, month, rate_str, source)

    for year, month, rate_str, source_date in rows:
        existing[(year, month)] = (year, month, rate_str, source_date)

    ordered = sorted(existing.values(), key=lambda item: (item[0], item[1]))
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = csv_path.with_suffix(csv_path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(_HEADER)
        for year, month, rate_str, source in ordered:
            writer.writerow([year, f"{month:02d}", rate_str, source])
    tmp_path.replace(csv_path)


def format_rate(rate: Decimal) -> str:
    """Format a rate to four decimal places using half-up rounding."""

    return str(rate.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))
