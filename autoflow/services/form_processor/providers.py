"""Rate provider abstractions and helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, Mapping, Tuple


class RateLookupError(RuntimeError):
    """Raised when a rate cannot be obtained."""

    def __init__(self, message: str, *, original_date: str | None = None) -> None:
        super().__init__(message)
        self.original_date = original_date


class RateFallbackUsed(RateLookupError):
    """Raised when a fallback date is used but a rate is returned."""

    def __init__(self, rate: Decimal, used_date: str, *, original_date: str | None = None) -> None:
        super().__init__(
            f"rate for {original_date or used_date} unavailable, fallback to {used_date}",
            original_date=original_date,
        )
        self.rate = rate
        self.used_date = used_date


@dataclass(slots=True)
class MockRateProvider:
    """In-memory provider used in tests and CLI demos."""

    rates: Mapping[Tuple[str, str], Mapping[str, Decimal]]
    fallback_window_days: int = 7

    def get_rate(self, date: str, from_ccy: str, to_ccy: str) -> Decimal:
        from_ccy = from_ccy.upper()
        to_ccy = to_ccy.upper()
        if from_ccy == to_ccy:
            return Decimal("1")

        key = (from_ccy, to_ccy)
        pair_rates: Mapping[str, Decimal] | None = self.rates.get(key)
        if not pair_rates:
            raise RateLookupError(f"no rate configured for {from_ccy}->{to_ccy}", original_date=date)

        if date in pair_rates:
            return Decimal(pair_rates[date])

        try:
            cur = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError as exc:  # noqa: BLE001
            raise RateLookupError(f"invalid rate lookup date: {date}") from exc

        for _ in range(self.fallback_window_days):
            cur -= timedelta(days=1)
            lookup = cur.isoformat()
            if lookup in pair_rates:
                raise RateFallbackUsed(
                    rate=Decimal(pair_rates[lookup]),
                    used_date=lookup,
                    original_date=date,
                )

        raise RateLookupError(f"rate unavailable for {from_ccy}->{to_ccy} on {date}", original_date=date)


@dataclass(slots=True)
class StaticRateProvider:
    """Return constant rates for testing without external dependencies."""

    default_rate: Decimal = Decimal("1")
    overrides: Dict[Tuple[str, str], Decimal] = field(default_factory=dict)

    def get_rate(self, date: str, from_ccy: str, to_ccy: str) -> Decimal:
        from_ccy = from_ccy.upper()
        to_ccy = to_ccy.upper()
        if from_ccy == to_ccy:
            return Decimal("1")
        key = (from_ccy, to_ccy)
        if key in self.overrides:
            return Decimal(self.overrides[key])
        return self.default_rate


__all__ = [
    "MockRateProvider",
    "RateLookupError",
    "RateFallbackUsed",
    "StaticRateProvider",
]
