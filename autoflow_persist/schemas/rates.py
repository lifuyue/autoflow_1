"""
RESPONSIBILITIES
- Provide typed containers for rate records and query filters.
- Offer helpers that normalize decimals and dates prior to persistence.
PROCESS OVERVIEW
1. External callers instantiate RatesRecord/RatesQuery with rich types.
2. to_dict() prepares canonical, ISO formatted payloads for XLSX stores.
3. Stores rely on the normalized dict to enforce schema consistency.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Mapping, MutableMapping

_DECIMAL_QUANT = Decimal("0.0001")


def _iso_date(value: date | datetime | str) -> str:
    if isinstance(value, datetime):
        return value.replace(microsecond=0).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _as_decimal_text(value: Decimal, *, quantize: Decimal = _DECIMAL_QUANT) -> str:
    normalized = value.quantize(quantize, rounding=ROUND_HALF_UP)
    return format(normalized, "f")


@dataclass(slots=True)
class RatesRecord:
    base_currency: str
    quote_currency: str
    rate_mid: Decimal
    rate_date: date
    fetch_date: date | datetime
    source: str
    fallback_strategy: str | None = None
    download_url: str | None = None

    def to_dict(self) -> MutableMapping[str, object]:
        payload: MutableMapping[str, object] = {
            "base_currency": self.base_currency.upper(),
            "quote_currency": self.quote_currency.upper(),
            "rate_mid": _as_decimal_text(self.rate_mid),
            "rate_date": _iso_date(self.rate_date),
            "fetch_date": _iso_date(self.fetch_date),
            "source": self.source,
            "fallback_strategy": self.fallback_strategy or "",
            "download_url": self.download_url or "",
        }
        rate_dt = self.rate_date
        if isinstance(rate_dt, datetime):
            rate_dt = rate_dt.date()
        payload["year"] = rate_dt.year
        payload["month"] = rate_dt.month
        return payload


@dataclass(slots=True)
class RatesQuery:
    base_currency: str | None = None
    quote_currency: str | None = None
    start_date: date | None = None
    end_date: date | None = None

    def to_dict(self) -> Mapping[str, object]:
        return {
            "base_currency": self.base_currency.upper() if self.base_currency else None,
            "quote_currency": self.quote_currency.upper() if self.quote_currency else None,
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
        }
