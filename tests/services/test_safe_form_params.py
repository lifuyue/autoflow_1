"""Form parameter behaviour for SAFE provider."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from autoflow.services.fees_fetcher import pbc_client, safe_provider


def test_safe_request_uses_expected_window(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    html = """
    <html>
      <body>
        <table id='InfoTable'>
          <tr><th>日期</th><th>美元</th></tr>
          <tr><td>2025-02-04</td><td>710.12</td></tr>
        </table>
      </body>
    </html>
    """

    calls: list[dict[str, object]] = []

    def fake_request(
        url: str,
        *,
        method: str = "GET",
        data: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        **_kwargs,
    ) -> SimpleNamespace:
        calls.append({"method": method, "data": data, "params": params})
        return SimpleNamespace(text=html)

    monkeypatch.setattr(pbc_client, "_request", fake_request)
    monkeypatch.setenv("SAFE_SNAPSHOT_DIR", str(tmp_path))

    rate, source_date, source = safe_provider.get_usd_cny_midpoint_from_portal(
        None, "2025-02-10"
    )

    assert rate == Decimal("7.1012")
    assert source_date == "2025-02-04"
    assert source == "safe_portal"

    assert calls
    first = calls[0]
    assert first["method"] == "POST"
    window = first["data"]
    assert window == {"startDate": "2025-02-03", "endDate": "2025-02-13"}


def test_safe_get_fallback(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    html = """
    <html>
      <body>
        <table id='InfoTable'>
          <tr><th>日期</th><th>美元</th></tr>
          <tr><td>2025-01-06</td><td>710.22</td></tr>
        </table>
      </body>
    </html>
    """

    calls: list[dict[str, object]] = []

    def fake_request(
        url: str,
        *,
        method: str = "GET",
        data: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        **_kwargs,
    ) -> SimpleNamespace:
        calls.append({"method": method, "data": data, "params": params})
        if method == "POST":
            raise pbc_client.PBOCClientError("POST not allowed")
        return SimpleNamespace(text=html)

    monkeypatch.setattr(pbc_client, "_request", fake_request)
    monkeypatch.setenv("SAFE_SNAPSHOT_DIR", str(tmp_path))

    rate, source_date, source = safe_provider.get_usd_cny_midpoint_from_portal(
        None, "2025-01-02"
    )

    assert rate == Decimal("7.1022")
    assert source_date == "2025-01-06"
    assert source == "safe_portal"

    assert len(calls) == 2
    assert calls[0]["method"] == "POST"
    assert calls[1]["method"] == "GET"
    assert calls[1]["params"] == {"startDate": "2025-01-01", "endDate": "2025-01-11"}
