"""Tests for provider router fallbacks."""

from __future__ import annotations

from decimal import Decimal

import pytest

from autoflow.services.fees_fetcher import pbc_client, provider_router
from autoflow.services.form_processor.providers import RateLookupError


def test_router_falls_back_to_cfets(monkeypatch: pytest.MonkeyPatch) -> None:
    pbc_client.reset_metrics()

    def fail_pbc(target: str, *, manage_cycle: bool = False):  # type: ignore[override]
        raise pbc_client.CertHostnameMismatch("pbc.gov.cn", {"host": "pbc.gov.cn"})

    def ok_cfets(_sess, target: str):  # type: ignore[override]
        return Decimal("7.1056"), target, "cfets_notice"

    monkeypatch.setattr(provider_router, "fetch_pbc_midpoint", fail_pbc)
    monkeypatch.setattr(provider_router.cfets_provider, "get_usd_cny_midpoint_from_notice", ok_cfets)

    rate, source_date, rate_source, fallback_used = provider_router.fetch_with_fallback("2025-09-15")

    assert rate == Decimal("7.1056")
    assert source_date == "2025-09-15"
    assert rate_source == "cfets_notice"
    assert fallback_used == "cfets"

    metrics = pbc_client.get_metrics()
    assert metrics.rate_source == "cfets_notice"
    assert metrics.fallback_used == "cfets"


def test_router_falls_back_to_safe_forward(monkeypatch: pytest.MonkeyPatch) -> None:
    pbc_client.reset_metrics()

    def fail_pbc(target: str, *, manage_cycle: bool = False):  # type: ignore[override]
        raise RateLookupError("missing")

    def fail_cfets(_sess, target: str):  # type: ignore[override]
        raise LookupError("not found")

    def ok_safe(_sess, target: str):  # type: ignore[override]
        return Decimal("7.1027"), "2025-09-16", "safe_portal"

    monkeypatch.setattr(provider_router, "fetch_pbc_midpoint", fail_pbc)
    monkeypatch.setattr(provider_router.cfets_provider, "get_usd_cny_midpoint_from_notice", fail_cfets)
    monkeypatch.setattr(provider_router.safe_provider, "get_usd_cny_midpoint_from_portal", ok_safe)

    rate, source_date, rate_source, fallback_used = provider_router.fetch_with_fallback("2025-09-15")

    assert rate == Decimal("7.1027")
    assert source_date == "2025-09-16"
    assert rate_source == "safe_portal"
    assert fallback_used == "forward"


def test_router_all_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_pbc(target: str, *, manage_cycle: bool = False):  # type: ignore[override]
        raise RateLookupError("missing")

    def fail_cfets(_sess, target: str):  # type: ignore[override]
        raise LookupError("not found")

    def fail_safe(_sess, target: str):  # type: ignore[override]
        raise LookupError("not found")

    monkeypatch.setattr(provider_router, "fetch_pbc_midpoint", fail_pbc)
    monkeypatch.setattr(provider_router.cfets_provider, "get_usd_cny_midpoint_from_notice", fail_cfets)
    monkeypatch.setattr(provider_router.safe_provider, "get_usd_cny_midpoint_from_portal", fail_safe)

    with pytest.raises(RateLookupError):
        provider_router.fetch_with_fallback("2025-09-15")

