from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from autoflow.cli import app
from autoflow.services.fees_fetcher.pbc_client import CertHostnameMismatch
from autoflow.services.fees_fetcher import pbc_client
from autoflow.services.fees_fetcher import provider_router


@pytest.fixture(autouse=True)
def reset_client_config():
    pbc_client.reset_request_config()
    pbc_client.reset_metrics()
    yield
    pbc_client.reset_request_config()
    pbc_client.reset_metrics()


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def tls_error() -> CertHostnameMismatch:
    return CertHostnameMismatch(
        "www.pbc.gov.cn",
        {
            "host": "www.pbc.gov.cn",
            "error_code": "CERT_HOSTNAME_MISMATCH",
            "san_contains_host": False,
            "server_cert_subject": "CN=default.example",
            "server_cert_issuer": "CN=Example CA",
        },
    )


def test_build_month_cli_pending(
    monkeypatch: pytest.MonkeyPatch,
    runner: CliRunner,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    tls_error: CertHostnameMismatch,
) -> None:
    def raise_tls(*_args, **_kwargs):
        raise tls_error

    monkeypatch.setattr(provider_router, "fetch_with_fallback", raise_tls)

    caplog.set_level("INFO")
    result = runner.invoke(
        app,
        [
            "build-monthly-rates",
            "--start",
            "2025-09",
            "--output",
            str(tmp_path / "rates.csv"),
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0


def test_get_rate_cli_exits_with_tls_diagnostics(monkeypatch: pytest.MonkeyPatch, runner: CliRunner, tls_error: CertHostnameMismatch) -> None:
    def raise_tls(*_args, **_kwargs):
        raise tls_error

    monkeypatch.setattr("autoflow.cli.fetch_with_fallback", raise_tls)

    result = runner.invoke(
        app,
        [
            "get-rate",
            "--date",
            "2025-09-01",
            "--from",
            "USD",
            "--to",
            "CNY",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 2
    assert "tls hostname mismatch" in result.output.lower()
