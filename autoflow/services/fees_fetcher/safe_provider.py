"""SAFE backed USD/CNY midpoint retrieval."""

from __future__ import annotations

import logging
from datetime import date as date_cls, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from bs4 import BeautifulSoup

from . import pbc_client

LOGGER = logging.getLogger(__name__)

PORTAL_URL = "https://www.safe.gov.cn/AppStructured/hlw/RMBQuery.do"


def get_usd_cny_midpoint_from_portal(
    sess, target_date: str
) -> tuple[Decimal, str, str]:  # noqa: D401 - signature defined by specification
    """Return USD/CNY midpoint published on SAFE portal."""

    # Use the shared PBOC client machinery for consistent TLS handling.
    del sess
    try:
        response = pbc_client._request(PORTAL_URL)
    except pbc_client.CertHostnameMismatch:
        raise
    except pbc_client.FetchTimeout:
        raise
    except pbc_client.PBOCClientError as exc:  # noqa: SLF001 - intentional internal reuse
        raise LookupError(f"SAFE portal unavailable for {target_date}") from exc

    html = response.text
    if not html.strip():
        raise LookupError(f"SAFE portal empty for {target_date}")

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"id": "InfoTable"})
    if not table:
        raise LookupError("SAFE portal missing InfoTable")

    header = table.find("tr")
    if not header:
        raise LookupError("SAFE portal table missing header")

    header_cells = [cell.get_text(strip=True) for cell in header.find_all("th")]
    try:
        usd_idx = header_cells.index("美元")
    except ValueError as exc:
        raise LookupError("SAFE portal header lacks USD column") from exc

    rows = []
    for row in table.find_all("tr")[1:]:
        cells = [cell.get_text(strip=True) for cell in row.find_all("td")]
        if len(cells) <= usd_idx:
            continue
        row_date = cells[0]
        usd_raw = cells[usd_idx]
        if not row_date or not usd_raw:
            continue
        try:
            normalized = _parse_decimal(usd_raw)
        except InvalidOperation:
            LOGGER.debug("Skipping SAFE row with invalid USD value: %s", usd_raw)
            continue
        rows.append((row_date, normalized))

    if not rows:
        raise LookupError("SAFE portal contained no USD rows")

    doc_text = soup.get_text(" ", strip=True)
    per_100_hint = "每100" in doc_text or "100美元" in doc_text
    per_100 = per_100_hint or any(rate >= Decimal("100") for _, rate in rows)

    rate_map: dict[str, Decimal] = {}
    for row_date, raw in rows:
        adjusted = raw / Decimal("100") if per_100 else raw
        rate_map[row_date] = adjusted.quantize(Decimal("0.0001"), ROUND_HALF_UP)

    target_iso = _parse_iso_date(target_date)
    if target_iso.isoformat() in rate_map:
        return rate_map[target_iso.isoformat()], target_iso.isoformat(), "safe_portal"

    forward_rate = _find_forward_rate(rate_map, target_iso)
    if forward_rate is None:
        raise LookupError(f"SAFE portal missing rate for {target_date}")

    return forward_rate


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


def _find_forward_rate(
    rate_map: dict[str, Decimal], target: date_cls
) -> tuple[Decimal, str, str] | None:
    attempts = 0
    cursor = target
    while attempts < 3:
        cursor += timedelta(days=1)
        if cursor.weekday() >= 5:
            continue
        attempts += 1
        key = cursor.isoformat()
        if key in rate_map:
            return rate_map[key], key, "safe_portal"
    return None
