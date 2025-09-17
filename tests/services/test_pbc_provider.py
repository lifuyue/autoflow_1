"""Tests for the PBOC rate provider integration."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from autoflow.services.fees_fetcher import PBOCRateProvider
from autoflow.services.fees_fetcher import pbc_client


@pytest.fixture()
def sample_article_html() -> str:
    return (
        "<html><body>中国人民银行授权中国外汇交易中心公布，2025年1月2日"
        "银行间外汇市场人民币汇率中间价为1美元对人民币7.1879元，"
        "1欧元对人民币...</body></html>"
    )


@pytest.fixture()
def sample_keychart_html() -> str:
    return (
        "<html><body><table><tr><th>日期</th><th>数值</th></tr>"
        "<tr><td>2025-01-02</td><td>1美元对人民币7.1879元</td></tr>"
        "</table></body></html>"
    )


def test_parse_article_extracts_rate(monkeypatch: pytest.MonkeyPatch, sample_article_html: str) -> None:
    def fake_request(url: str) -> SimpleNamespace:
        return SimpleNamespace(
            text=sample_article_html,
            apparent_encoding="utf-8",
            raise_for_status=lambda: None,
        )

    monkeypatch.setattr(pbc_client, "_request", fake_request)
    date_iso, rate = pbc_client.parse_article("http://example.com/article.html")

    assert date_iso == "2025-01-02"
    assert rate == Decimal("7.1879")


def test_parse_article_returns_rate_none_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    html = "<html><body>2025年1月2日未公布美元对人民币报价。</body></html>"

    def fake_request(url: str) -> SimpleNamespace:
        return SimpleNamespace(
            text=html,
            apparent_encoding="utf-8",
            raise_for_status=lambda: None,
        )

    monkeypatch.setattr(pbc_client, "_request", fake_request)
    date_iso, rate = pbc_client.parse_article("http://example.com/article.html")

    assert date_iso == "2025-01-02"
    assert rate is None


def test_probe_keychart_extracts_rate(monkeypatch: pytest.MonkeyPatch, sample_keychart_html: str) -> None:
    def fake_request(url: str) -> SimpleNamespace:
        return SimpleNamespace(
            text=sample_keychart_html,
            apparent_encoding="utf-8",
            raise_for_status=lambda: None,
        )

    monkeypatch.setattr(pbc_client, "_request", fake_request)

    rate = pbc_client.probe_keychart("2025-01-02")

    assert rate == Decimal("7.1879")


def test_provider_prefers_articles(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = PBOCRateProvider()

    monkeypatch.setattr(pbc_client, "iter_article_urls", lambda max_pages=15: iter(["a", "b"]))

    def fake_parse(url: str) -> tuple[str, Decimal | None]:
        if url == "a":
            return "2025-01-02", Decimal("7.1879")
        return "2025-01-01", Decimal("7.1000")

    monkeypatch.setattr(pbc_client, "parse_article", fake_parse)
    monkeypatch.setattr(pbc_client, "probe_keychart", lambda date: None)

    rate = provider.get_rate("2025-01-02", "USD", "CNY")
    assert rate == Decimal("7.1879")


def test_provider_falls_back_to_keychart(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = PBOCRateProvider()

    monkeypatch.setattr(pbc_client, "iter_article_urls", lambda max_pages=15: iter(["a", "b"]))
    monkeypatch.setattr(pbc_client, "parse_article", lambda url: ("2025-01-01", None))
    monkeypatch.setattr(pbc_client, "probe_keychart", lambda date: Decimal("7.1879"))

    rate = provider.get_rate("2025-01-02", "USD", "CNY")
    assert rate == Decimal("7.1879")


@pytest.mark.slow
@pytest.mark.online
def test_provider_live_rate(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = PBOCRateProvider(max_pages=2)

    try:
        rate = provider.get_rate("2025-01-02", "USD", "CNY")
    except Exception as exc:  # pragma: no cover - network dependent
        pytest.skip(f"Live rate lookup skipped: {exc}")
    else:
        assert rate == Decimal("7.1879")
