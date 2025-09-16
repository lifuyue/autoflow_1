"""Computation helpers for currency conversion and totals."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP

import pandas as pd

from .providers import RateFallbackUsed, RateLookupError


def compute_base_amounts(
    frame: pd.DataFrame,
    base_currency: str,
    round_digits: int,
    rate_provider,
) -> pd.DataFrame:
    """Append base currency amounts using the provided rate provider."""

    df = frame.copy()
    df["base_currency"] = base_currency
    if "exchange_rate" not in df:
        df["exchange_rate"] = None
    if "base_amount" not in df:
        df["base_amount"] = None
    if "rate_date_used" not in df:
        df["rate_date_used"] = None

    quant = Decimal("1").scaleb(-round_digits)

    for idx, row in df.iterrows():
        amount = row.get("amount")
        currency = row.get("currency")
        date = row.get("date")
        if amount is None or currency is None or date is None:
            continue
        if not isinstance(amount, Decimal):
            continue
        try:
            rate = rate_provider.get_rate(date, currency, base_currency)
            rate_date = date
        except RateFallbackUsed as exc:
            rate = exc.rate
            rate_date = exc.used_date
            df.at[idx, "issues"].append("rate_fallback")
        except RateLookupError:
            df.at[idx, "issues"].append("rate_unavailable")
            continue
        if rate is None:
            df.at[idx, "issues"].append("rate_unavailable")
            continue
        base_amount = (amount * rate).quantize(quant, rounding=ROUND_HALF_UP)
        df.at[idx, "exchange_rate"] = rate
        df.at[idx, "base_amount"] = base_amount
        df.at[idx, "rate_date_used"] = rate_date

    return df
