"""SAFE backed USD/CNY midpoint retrieval."""

from __future__ import annotations

import logging
import os
import re
from datetime import date as date_cls, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Iterable

from bs4 import BeautifulSoup

from . import pbc_client

LOGGER = logging.getLogger(__name__)

PORTAL_URL = "https://www.safe.gov.cn/AppStructured/hlw/RMBQuery.do"

DATE_HEADER_KEYWORDS = ("日期",)
USD_HEADER_KEYWORDS = ("美元", "usd")
SNAPSHOT_SIZE_LIMIT = 200_000
SNAPSHOT_ENV_VAR = "SAFE_SNAPSHOT_DIR"
DEFAULT_SNAPSHOT_DIR = Path(__file__).resolve().parents[3] / "snap"


def get_usd_cny_midpoint_from_portal(
    sess, target_date: str
) -> tuple[Decimal, str, str]:  # noqa: D401 - signature defined by specification
    """Return USD/CNY midpoint published on SAFE portal."""

    del sess
    target_iso = _parse_iso_date(target_date)
    window_start, window_end = _build_query_window(target_iso)

    form_payload = {
        "startDate": window_start.isoformat(),
        "endDate": window_end.isoformat(),
    }

    html = _fetch_portal_html(form_payload, target_date)
    _persist_snapshot(html, window_start)

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"id": "InfoTable"})
    if not table:
        raise LookupError("SAFE portal missing InfoTable")

    header_cells = _extract_header_cells(table)
    if not header_cells:
        raise LookupError("SAFE portal table missing header")

    date_idx = _locate_column(header_cells, DATE_HEADER_KEYWORDS)
    usd_idx = _locate_column(header_cells, USD_HEADER_KEYWORDS)
    if date_idx is None:
        raise LookupError("SAFE portal header lacks date column")
    if usd_idx is None:
        raise LookupError("SAFE portal header lacks USD column")

    raw_rows = _collect_rows(table)
    if not raw_rows:
        raise LookupError("SAFE portal contained no data rows")

    parsed_rows = []
    for cells in raw_rows:
        if len(cells) <= max(date_idx, usd_idx):
            continue
        date_cell = cells[date_idx].strip()
        usd_cell = cells[usd_idx].strip()
        if not date_cell or not usd_cell:
            continue
        row_date = _parse_row_date(date_cell)
        if row_date is None:
            LOGGER.debug("Skipping SAFE row with unparseable date: %s", date_cell)
            continue
        try:
            rate_raw = _parse_decimal(usd_cell)
        except InvalidOperation:
            LOGGER.debug("Skipping SAFE row with invalid USD value: %s", usd_cell)
            continue
        parsed_rows.append((row_date, rate_raw, usd_cell))

    if not parsed_rows:
        raise LookupError("SAFE portal contained no USD rows")

    per_100 = _detect_per_100(header_cells[usd_idx], (row[1] for row in parsed_rows), soup)

    adjusted_rows: dict[date_cls, Decimal] = {}
    for row_date, raw_rate, _ in parsed_rows:
        adjusted = raw_rate / Decimal("100") if per_100 else raw_rate
        adjusted_rows[row_date] = adjusted.quantize(Decimal("0.0001"), ROUND_HALF_UP)

    chosen_date = _select_forward_date(adjusted_rows.keys(), window_start, window_end)
    if chosen_date is None:
        LOGGER.info(
            "SAFE forward probing: %s..%s -> miss",
            window_start.isoformat(),
            window_end.isoformat(),
        )
        raise LookupError(f"SAFE portal missing rate for {target_date}")

    rate = adjusted_rows[chosen_date]
    LOGGER.info(
        "SAFE forward probing: %s..%s -> hit %s rate=%s",
        window_start.isoformat(),
        window_end.isoformat(),
        chosen_date.isoformat(),
        rate,
    )

    return rate, chosen_date.isoformat(), "safe_portal"


def _fetch_portal_html(payload: dict[str, str], target_date: str) -> str:
    try:
        response = pbc_client._request(PORTAL_URL, method="POST", data=payload)
    except pbc_client.CertHostnameMismatch:
        raise
    except pbc_client.FetchTimeout:
        raise
    except pbc_client.PBOCClientError as exc:  # noqa: SLF001 - intentional internal reuse
        LOGGER.debug("SAFE POST failed, retrying with GET: %s", exc)
        try:
            response = pbc_client._request(PORTAL_URL, params=payload)
        except pbc_client.CertHostnameMismatch:
            raise
        except pbc_client.FetchTimeout:
            raise
        except pbc_client.PBOCClientError as exc2:  # noqa: SLF001 - intentional internal reuse
            raise LookupError(f"SAFE portal unavailable for {target_date}") from exc2

    html = response.text
    if not html.strip():
        raise LookupError(f"SAFE portal empty for {target_date}")
    return html


def _persist_snapshot(html: str, window_start: date_cls) -> None:
    try:
        snapshot_dir = Path(os.getenv(SNAPSHOT_ENV_VAR, DEFAULT_SNAPSHOT_DIR))
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        month_tag = window_start.strftime("%Y-%m")
        snapshot_path = snapshot_dir / f"safe_{month_tag}.html"
        encoded = html.encode("utf-8")
        payload = encoded[:SNAPSHOT_SIZE_LIMIT]
        with open(snapshot_path, "wb") as handle:
            handle.write(payload)
        LOGGER.debug("SAFE snapshot saved: %s (%d bytes)", snapshot_path, len(payload))
    except Exception as exc:  # pragma: no cover - diagnostics best effort
        LOGGER.debug("Failed to persist SAFE snapshot: %s", exc)


def _extract_header_cells(table) -> list[str]:
    header = table.find("tr")
    if not header:
        return []
    return [cell.get_text(strip=True) for cell in header.find_all(["th", "td"])]


def _collect_rows(table) -> list[list[str]]:
    rows: list[list[str]] = []
    for row in table.find_all("tr"):
        cells = [cell.get_text(strip=True) for cell in row.find_all(["td", "th"])]
        if cells:
            rows.append(cells)
    # Drop header row if present
    return rows[1:] if len(rows) > 1 else []


def _locate_column(header: Iterable[str], keywords: Iterable[str]) -> int | None:
    for idx, cell in enumerate(header):
        normalized = cell.strip().lower()
        for keyword in keywords:
            if keyword.lower() in normalized:
                return idx
    return None


def _detect_per_100(
    header_text: str,
    candidate_rates: Iterable[Decimal],
    soup: BeautifulSoup,
) -> bool:
    header_lower = header_text.lower()
    if "100" in header_lower or "每100" in header_text:
        return True
    for rate in candidate_rates:
        if rate >= Decimal("50"):
            return True
    doc_text = soup.get_text(" ", strip=True)
    return "每100" in doc_text or "100美元" in doc_text


def _select_forward_date(
    available_dates: Iterable[date_cls],
    window_start: date_cls,
    window_end: date_cls,
) -> date_cls | None:
    candidates = sorted(
        date for date in set(available_dates) if window_start <= date <= window_end
    )
    if not candidates:
        return None
    return candidates[0]


def _parse_decimal(raw: str) -> Decimal:
    cleaned = raw.replace(",", "").strip()
    if not cleaned:
        raise InvalidOperation
    return Decimal(cleaned)


def _parse_iso_date(value: str) -> date_cls:
    try:
        return date_cls.fromisoformat(value)
    except ValueError as exc:  # pragma: no cover - defensive guard
        raise LookupError(f"Invalid ISO date: {value}") from exc


def _parse_row_date(raw: str) -> date_cls | None:
    text = raw.strip()
    if not text:
        return None
    normalized = text.replace("/", "-")
    try:
        return date_cls.fromisoformat(normalized)
    except ValueError:
        match = re.search(r"(\d{4})\D+(\d{1,2})\D+(\d{1,2})", text)
        if not match:
            return None
        year, month, day = (int(part) for part in match.groups())
        try:
            return date_cls(year, month, day)
        except ValueError:
            return None


def _build_query_window(target: date_cls) -> tuple[date_cls, date_cls]:
    month_start = target.replace(day=1)
    first_business = _first_business_day(month_start)
    month_end = _month_end(month_start)
    forward_limit = first_business + timedelta(days=10)
    window_end = forward_limit if forward_limit <= month_end else month_end
    return first_business, window_end


def _first_business_day(month_start: date_cls) -> date_cls:
    cursor = month_start
    while cursor.month == month_start.month:
        if cursor.weekday() < 5:
            return cursor
        cursor += timedelta(days=1)
    raise ValueError(f"No business day found for {month_start:%Y-%m}")


def _month_end(month_start: date_cls) -> date_cls:
    if month_start.month == 12:
        next_month = month_start.replace(year=month_start.year + 1, month=1, day=1)
    else:
        next_month = month_start.replace(month=month_start.month + 1, day=1)
    return next_month - timedelta(days=1)
