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


def test_first_business_day_september_2025() -> None:
    # 2025-09-01 is a Monday, thus the first business day.
    assert first_business_day(2025, 9) == "2025-09-01"


def test_first_business_day_with_overrides() -> None:
    holidays = {"2025-09-01"}
    workdays = {"2025-09-06"}
    assert first_business_day(2025, 9, holidays=holidays, workdays=workdays) == "2025-09-02"
    # Weekend promoted via workdays flag.
    assert first_business_day(2023, 7, workdays={"2023-07-01"}) == "2023-07-01"


def test_plan_missing_months(tmp_path: Path) -> None:
    csv_path = tmp_path / "monthly.csv"
    csv_path.write_text(
        "年份,月份,中间价,查询日期,来源日期,数据源,回退策略\n"
        "2023,01,6.9600,2023-01-02,2023-01-03,cfets_notice,cfets\n"
        "2023,03,6.8700,2023-03-01,2023-03-02,cfets_notice,cfets\n",
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

    result = fetch_month_rate(
        2023,
        1,
        lookup=lookup,
    )
    assert result.mid_rate == Decimal("6.8899")
    assert result.source_date == "2023-01-03"
    assert result.rate_source == "cfets_notice"
    assert result.fallback_used == "cfets"
    assert result.query_date == "2023-01-02"
    assert result.request_date == "2023-01-03"


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

    result = fetch_month_rate(
        2023,
        7,
        lookup=lookup,
    )
    assert result.mid_rate == Decimal("7.1000")
    assert result.source_date == "2023-07-01"
    assert result.rate_source == "safe_portal"
    assert result.fallback_used == "forward"
    assert result.query_date == "2023-07-03"
    assert result.request_date == "2023-07-01"


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
            {
                "year": 2023,
                "month": 1,
                "mid_rate": "6.9600",
                "query_date": "2023-01-02",
                "source_date": "2023-01-03",
                "rate_source": "cfets_notice",
                "fallback_used": "cfets",
            },
            {
                "year": 2023,
                "month": 2,
                "mid_rate": "6.7500",
                "query_date": "2023-02-01",
                "source_date": "2023-02-01",
                "rate_source": "safe_portal",
                "fallback_used": "none",
            },
        ],
    )
    upsert_csv(
        csv_path,
        [
            {
                "year": 2023,
                "month": 2,
                "mid_rate": "6.7600",
                "query_date": "2023-02-01",
                "source_date": "2023-02-02",
                "rate_source": "safe_portal",
                "fallback_used": "forward",
            },
            {
                "year": 2023,
                "month": 3,
                "mid_rate": "6.8300",
                "query_date": "2023-03-01",
                "source_date": "2023-03-01",
                "rate_source": "cfets_notice",
                "fallback_used": "cfets",
            },
        ],
    )
    content = csv_path.read_text(encoding="utf-8").splitlines()
    assert content[0] == "年份,月份,中间价,查询日期,来源日期,数据源,回退策略"
    assert content[1] == "2023,01,6.9600,2023-01-02,2023-01-03,cfets_notice,cfets"
    assert content[2] == "2023,02,6.7600,2023-02-01,2023-02-02,safe_portal,forward"
    assert content[3] == "2023,03,6.8300,2023-03-01,2023-03-01,cfets_notice,cfets"


def test_format_rate() -> None:
    assert format_rate(Decimal("7.12345")) == "7.1235"
    assert format_rate(Decimal("7.12344")) == "7.1234"
