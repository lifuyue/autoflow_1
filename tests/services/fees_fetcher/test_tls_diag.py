from __future__ import annotations

import socket
from types import SimpleNamespace

import pytest

from autoflow.services.fees_fetcher import tls_diag


def test_resolve_ips_filters_family(monkeypatch: pytest.MonkeyPatch) -> None:
    entries = [
        (socket.AF_INET, None, None, None, ("1.2.3.4", 0)),
        (socket.AF_INET6, None, None, None, ("2001:db8::1", 0, 0, 0)),
    ]

    monkeypatch.setattr(tls_diag.socket, "getaddrinfo", lambda *args, **kwargs: entries)

    ipv4, ipv6 = tls_diag.resolve_ips("example.com", "auto")
    assert ipv4 == ["1.2.3.4"]
    assert ipv6 == ["2001:db8::1"]

    ipv4_only, _ = tls_diag.resolve_ips("example.com", "4")
    assert ipv4_only == ["1.2.3.4"]

    _, ipv6_only = tls_diag.resolve_ips("example.com", "6")
    assert ipv6_only == ["2001:db8::1"]

    entries_no_v4 = [(socket.AF_INET6, None, None, None, ("2001:db8::1", 0, 0, 0))]
    monkeypatch.setattr(tls_diag.socket, "getaddrinfo", lambda *args, **kwargs: entries_no_v4)
    with pytest.raises(ValueError):
        tls_diag.resolve_ips("example.com", "4")


def test_probe_cert_extracts_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_cert = {
        "subject": ((("commonName", "default.example"),),),
        "issuer": ((("commonName", "Example CA"),),),
        "subjectAltName": (("DNS", "default.example"), ("DNS", "www.example.com")),
    }

    class FakeSSLSocket:
        def __init__(self, sock, server_hostname):
            self.sock = sock

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

        def getpeercert(self):
            return fake_cert

    class FakeContext:
        def wrap_socket(self, sock, server_hostname):
            return FakeSSLSocket(sock, server_hostname)

    class FakeSocket:
        def __init__(self, family, sock_type):
            self.family = family

        def settimeout(self, timeout):
            self.timeout = timeout

        def connect(self, *_args, **_kwargs):
            return None

        def close(self):
            return None

    monkeypatch.setattr(tls_diag.ssl, "create_default_context", lambda: FakeContext())
    monkeypatch.setattr(tls_diag.socket, "socket", FakeSocket)

    info = tls_diag.probe_cert("www.example.com", "1.2.3.4", timeout=1.0)
    assert info["connected_ip"] == "1.2.3.4"
    assert info["server_cert_subject"].startswith("commonName=default.example")
    assert info["server_cert_issuer"].startswith("commonName=Example CA")
    assert info["san_contains_host"] is True


def test_ip_family_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int] = []

    def fake_allowed() -> int:
        calls.append(1)
        return socket.AF_UNSPEC

    monkeypatch.setattr(tls_diag.urllib3_connection, "allowed_gai_family", fake_allowed)

    with tls_diag.ip_family_guard("4"):
        assert tls_diag.urllib3_connection.allowed_gai_family() == socket.AF_INET
    assert tls_diag.urllib3_connection.allowed_gai_family is fake_allowed
