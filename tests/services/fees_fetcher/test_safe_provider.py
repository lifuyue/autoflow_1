"""Tests for SAFE portal parser."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from autoflow.services.fees_fetcher import pbc_client, safe_provider


SAFE_HTML = """
<html>
  <body>
    <table id="InfoTable">
      <tr><th>日期</th><th>美元</th><th>欧元</th></tr>
      <tr><td>2025-09-16</td><td>710.27</td><td>835.71</td></tr>
      <tr><td>2025-09-15</td><td>710.56</td><td>833.27</td></tr>
    </table>
  </body>
</html>
"""


def test_safe_portal_parses_per_100(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        pbc_client,
        "_request",
        lambda url: SimpleNamespace(text=SAFE_HTML),
    )

    rate, source_date, source = safe_provider.get_usd_cny_midpoint_from_portal(
        None, "2025-09-15"
    )

    assert rate == Decimal("7.1056")
    assert source_date == "2025-09-15"
    assert source == "safe_portal"


def test_safe_portal_forward_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    html = """
    <html>
      <body>
        <table id="InfoTable">
          <tr><th>日期</th><th>美元</th></tr>
          <tr><td>2025-09-16</td><td>710.27</td></tr>
        </table>
      </body>
    </html>
    """

    monkeypatch.setattr(
        pbc_client,
        "_request",
        lambda url: SimpleNamespace(text=html),
    )

    rate, source_date, source = safe_provider.get_usd_cny_midpoint_from_portal(
        None, "2025-09-15"
    )

    assert rate == Decimal("7.1027")
    assert source_date == "2025-09-16"
    assert source == "safe_portal"


def test_safe_portal_handles_rounding(monkeypatch: pytest.MonkeyPatch) -> None:
    html = """
    <html>
      <body>
        <table id="InfoTable">
          <tr><th>日期</th><th>美元</th></tr>
          <tr><td>2025-09-04</td><td>710.52</td></tr>
        </table>
      </body>
    </html>
    """

    monkeypatch.setattr(
        pbc_client,
        "_request",
        lambda url: SimpleNamespace(text=html),
    )

    rate, source_date, source = safe_provider.get_usd_cny_midpoint_from_portal(
        None, "2025-09-01"
    )

    assert rate == Decimal("7.1052")
    assert source_date == "2025-09-04"
    assert source == "safe_portal"


def test_safe_portal_missing_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    empty_html = "<html><body><table id='InfoTable'><tr><th>日期</th><th>美元</th></tr></table></body></html>"

    monkeypatch.setattr(
        pbc_client,
        "_request",
        lambda url: SimpleNamespace(text=empty_html),
    )

    with pytest.raises(LookupError):
        safe_provider.get_usd_cny_midpoint_from_portal(None, "2025-09-15")
