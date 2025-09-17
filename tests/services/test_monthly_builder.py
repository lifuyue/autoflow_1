"""Unit tests for monthly rate builder utilities."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from autoflow.services.fees_fetcher.monthly_builder import (
    fetch_month_rate,
    first_business_day,
    format_rate,
    plan_missing_months,
    upsert_csv,
)
from autoflow.services.form_processor.providers import RateLookupError


def test_first_business_day_basic() -> None:
    assert first_business_day(2023, 1) == "2023-01-02"


def test_first_business_day_with_overrides() -> None:
    holidays = {"2025-09-01"}
    workdays = {"2025-09-06"}
    assert first_business_day(2025, 9, holidays=holidays, workdays=workdays) == "2025-09-02"
    # Weekend promoted via workdays flag.
    assert first_business_day(2023, 7, workdays={"2023-07-01"}) == "2023-07-01"


def test_plan_missing_months(tmp_path: Path) -> None:
    csv_path = tmp_path / "monthly.csv"
    csv_path.write_text(
        "年份,月份,中间价,来源日期\n2023,01,6.9600,2023-01-03\n2023,03,6.8700,2023-03-02\n",
        encoding="utf-8",
    )
    missing = plan_missing_months(csv_path, date(2023, 1, 1), date(2023, 4, 30))
    assert missing == [(2023, 2), (2023, 4)]


def test_fetch_month_rate_forward_fallback() -> None:
    mapping: dict[str, tuple[Decimal, str, str, str]] = {
        "2023-01-03": (Decimal("6.8899"), "2023-01-03", "cfets_notice", "cfets")
    }

    def lookup(date: str, prefer: str) -> tuple[Decimal, str, str, str]:
        try:
            return mapping[date]
        except KeyError as exc:
            raise RateLookupError("missing") from exc

    rate, source_date, rate_source, fallback_used = fetch_month_rate(
        2023,
        1,
        lookup=lookup,
    )
    assert rate == Decimal("6.8899")
    assert source_date == "2023-01-03"
    assert rate_source == "cfets_notice"
    assert fallback_used == "cfets"


def test_fetch_month_rate_backward_fallback() -> None:
    mapping: dict[str, tuple[Decimal, str, str, str]] = {
        "2023-07-01": (Decimal("7.1000"), "2023-07-01", "safe_portal", "forward")
    }

    def lookup(date: str, prefer: str) -> tuple[Decimal, str, str, str]:
        if date == "2023-07-03":
            raise RateLookupError("missing forward")
        if date == "2023-07-04":
            raise RateLookupError("missing forward")
        try:
            return mapping[date]
        except KeyError as exc:
            raise RateLookupError("missing") from exc

    rate, source_date, rate_source, fallback_used = fetch_month_rate(
        2023,
        7,
        lookup=lookup,
    )
    assert rate == Decimal("7.1000")
    assert source_date == "2023-07-01"
    assert rate_source == "safe_portal"
    assert fallback_used == "forward"


def test_fetch_month_rate_failures() -> None:
    def lookup(date: str, prefer: str) -> tuple[Decimal, str, str, str]:
        raise RateLookupError("missing")

    with pytest.raises(RateLookupError):
        fetch_month_rate(2024, 5, lookup=lookup)


def test_upsert_csv_merges(tmp_path: Path) -> None:
    csv_path = tmp_path / "rates.csv"
    upsert_csv(
        csv_path,
        [
            (2023, 1, "6.9600", "2023-01-03"),
            (2023, 2, "6.7500", "2023-02-01"),
        ],
    )
    upsert_csv(
        csv_path,
        [
            (2023, 2, "6.7600", "2023-02-02"),
            (2023, 3, "6.8300", "2023-03-01"),
        ],
    )
    content = csv_path.read_text(encoding="utf-8").splitlines()
    assert content[0] == "年份,月份,中间价,来源日期"
    assert content[1] == "2023,01,6.9600,2023-01-03"
    assert content[2] == "2023,02,6.7600,2023-02-02"
    assert content[3] == "2023,03,6.8300,2023-03-01"


def test_format_rate() -> None:
    assert format_rate(Decimal("7.12345")) == "7.1235"
    assert format_rate(Decimal("7.12344")) == "7.1234"
