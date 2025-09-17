from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from requests.exceptions import SSLError

from autoflow.services.fees_fetcher import pbc_client


@pytest.fixture(autouse=True)
def reset_client_state():
    pbc_client.reset_request_config()
    pbc_client.reset_metrics()
    yield
    pbc_client.reset_request_config()
    pbc_client.reset_metrics()


def test_session_trust_env_disabled() -> None:
    assert hasattr(pbc_client._SESSION, "trust_env")
    assert pbc_client._SESSION.trust_env is False


def test_request_raises_cert_hostname_mismatch(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    host = "www.pbc.gov.cn"
    url = f"https://{host}/path"

    def fake_get(url, timeout=None, proxies=None):  # noqa: ANN001
        raise SSLError("hostname 'www.pbc.gov.cn' doesn't match 'default.example'")

    monkeypatch.setattr(pbc_client, "_SESSION", SimpleNamespace(get=fake_get))
    monkeypatch.setattr(pbc_client.tls_diag, "resolve_ips", lambda host, family: (["1.1.1.1"], ["2001:db8::1"]))
    monkeypatch.setattr(
        pbc_client.tls_diag,
        "probe_cert",
        lambda host, ip: {
            "host": host,
            "connected_ip": ip,
            "server_cert_subject": "CN=default.example",
            "server_cert_issuer": "CN=Example CA",
            "server_cert_san": ["DNS:default.example"],
            "san_contains_host": False,
            "openssl_version": "OpenSSL",
            "requests_version": "0",
            "proxy_env_detected": False,
        },
    )

    pbc_client.begin_request_cycle(10)
    with pytest.raises(pbc_client.CertHostnameMismatch) as excinfo:
        pbc_client._request(url)  # noqa: SLF001 - intentional internal call
    pbc_client.end_request_cycle()

    metrics = pbc_client.get_metrics()
    assert metrics.tls_hostname_mismatch == 1

    diag = excinfo.value.diagnostics
    assert diag["error_code"] == "CERT_HOSTNAME_MISMATCH"
    assert diag["host"] == host
