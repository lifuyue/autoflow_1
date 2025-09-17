"""Unit conversion behaviour for SAFE provider."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from autoflow.services.fees_fetcher import pbc_client, safe_provider


def test_safe_values_are_scaled_from_per_100(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    html = """
    <html>
      <body>
        <table id='InfoTable'>
          <tr><th>日期</th><th>美元(每100美元)</th></tr>
          <tr><td>2025-07-02</td><td>712.34</td></tr>
        </table>
      </body>
    </html>
    """

    def fake_request(
        url: str,
        *,
        method: str = "GET",
        data: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        **_kwargs,
    ) -> SimpleNamespace:
        return SimpleNamespace(text=html)

    monkeypatch.setattr(pbc_client, "_request", fake_request)
    monkeypatch.setenv("SAFE_SNAPSHOT_DIR", str(tmp_path))

    rate, source_date, source = safe_provider.get_usd_cny_midpoint_from_portal(
        None, "2025-07-01"
    )

    assert rate == Decimal("7.1234")
    assert source_date == "2025-07-02"
    assert source == "safe_portal"


def test_safe_values_detect_large_number(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    html = """
    <html>
      <body>
        <table id='InfoTable'>
          <tr><th>日期</th><th>美元</th></tr>
          <tr><td>2025-07-02</td><td>710.11</td></tr>
        </table>
        <p>每100美元折合人民币</p>
      </body>
    </html>
    """

    def fake_request(
        url: str,
        *,
        method: str = "GET",
        data: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        **_kwargs,
    ) -> SimpleNamespace:
        return SimpleNamespace(text=html)

    monkeypatch.setattr(pbc_client, "_request", fake_request)
    monkeypatch.setenv("SAFE_SNAPSHOT_DIR", str(tmp_path))

    rate, source_date, source = safe_provider.get_usd_cny_midpoint_from_portal(
        None, "2025-07-01"
    )

    assert rate == Decimal("7.1011")
    assert source_date == "2025-07-02"
    assert source == "safe_portal"
