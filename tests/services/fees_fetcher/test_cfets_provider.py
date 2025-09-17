"""Tests for CFETS notice parser."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from autoflow.services.fees_fetcher import cfets_provider
from autoflow.services.fees_fetcher import pbc_client


def test_cfets_notice_parses_rate(monkeypatch: pytest.MonkeyPatch) -> None:
    html = (
        "<html><body>中国外汇交易中心 2025年9月15日 公布人民币汇率中间价，"
        "1美元对人民币7.1056元。</body></html>"
    )

    monkeypatch.setattr(
        pbc_client,
        "_request",
        lambda url: SimpleNamespace(text=html),
    )

    rate, source_date, source = cfets_provider.get_usd_cny_midpoint_from_notice(
        None, "2025-09-15"
    )

    assert rate == Decimal("7.1056")
    assert source_date == "2025-09-15"
    assert source == "cfets_notice"


def test_cfets_notice_without_rate(monkeypatch: pytest.MonkeyPatch) -> None:
    html = "<html><body>2025年9月15日 未公布美元对人民币报价。</body></html>"

    monkeypatch.setattr(
        pbc_client,
        "_request",
        lambda url: SimpleNamespace(text=html),
    )

    with pytest.raises(LookupError):
        cfets_provider.get_usd_cny_midpoint_from_notice(None, "2025-09-15")

