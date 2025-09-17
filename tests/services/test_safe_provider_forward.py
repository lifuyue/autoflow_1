"""Forward fill behaviour for SAFE provider."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from autoflow.services.fees_fetcher import pbc_client, safe_provider


def _fake_response(html: str) -> SimpleNamespace:
    return SimpleNamespace(text=html)


def test_forward_fill_hits_within_window(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    html = """
    <html>
      <body>
        <table id='InfoTable'>
          <tr><th>日期</th><th>美元</th></tr>
          <tr><td>2025-09-04</td><td>710.52</td></tr>
        </table>
      </body>
    </html>
    """

    captured: list[dict[str, object]] = []

    def fake_request(
        url: str,
        *,
        method: str = "GET",
        data: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        **_kwargs,
    ) -> SimpleNamespace:
        captured.append({"method": method, "data": data, "params": params})
        return _fake_response(html)

    monkeypatch.setattr(pbc_client, "_request", fake_request)
    monkeypatch.setenv("SAFE_SNAPSHOT_DIR", str(tmp_path))

    rate, source_date, source = safe_provider.get_usd_cny_midpoint_from_portal(
        None, "2025-09-01"
    )

    assert rate == Decimal("7.1052")
    assert source_date == "2025-09-04"
    assert source == "safe_portal"

    assert captured, "expected SAFE provider to issue at least one request"
    first_call = captured[0]
    assert first_call["method"] == "POST"
    assert first_call["data"] == {"startDate": "2025-09-01", "endDate": "2025-09-11"}


def test_forward_fill_miss_raises(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    html = """
    <html>
      <body>
        <table id='InfoTable'>
          <tr><th>日期</th><th>美元</th></tr>
          <tr><td>2025-09-20</td><td>710.52</td></tr>
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
        return _fake_response(html)

    monkeypatch.setattr(pbc_client, "_request", fake_request)
    monkeypatch.setenv("SAFE_SNAPSHOT_DIR", str(tmp_path))

    with pytest.raises(LookupError):
        safe_provider.get_usd_cny_midpoint_from_portal(None, "2025-09-01")
