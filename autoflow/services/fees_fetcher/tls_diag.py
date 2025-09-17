"""TLS diagnostics utilities for PBOC scraping."""

from __future__ import annotations

import json
import os
import socket
import ssl
from contextlib import contextmanager
from typing import Any, Iterator, Tuple

import requests

try:
    import urllib3.util.connection as urllib3_connection
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("urllib3 is required for TLS diagnostics") from exc


_IP_FAMILY_MAP = {
    "auto": socket.AF_UNSPEC,
    "4": socket.AF_INET,
    "6": socket.AF_INET6,
}


def resolve_ips(host: str, family: str = "auto") -> tuple[list[str], list[str]]:
    """Resolve IPv4/IPv6 addresses for *host* per *family* preference."""

    family = family.lower()
    if family not in _IP_FAMILY_MAP:
        raise ValueError(f"invalid ip-family: {family}")

    target_family = _IP_FAMILY_MAP[family]
    ipv4: list[str] = []
    ipv6: list[str] = []

    infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    for info in infos:
        af, _, _, _, sockaddr = info
        addr = sockaddr[0]
        if af == socket.AF_INET:
            if addr not in ipv4:
                ipv4.append(addr)
        elif af == socket.AF_INET6:
            if addr not in ipv6:
                ipv6.append(addr)

    if family == "4" and not ipv4:
        raise ValueError(f"no IPv4 address resolved for {host}")
    if family == "6" and not ipv6:
        raise ValueError(f"no IPv6 address resolved for {host}")

    return ipv4, ipv6


def probe_cert(host: str, ip: str, timeout: float = 3.0) -> dict[str, Any]:
    """Open a TLS connection to *ip* (for *host*) and return certificate summary."""

    family = socket.AF_INET6 if ":" in ip else socket.AF_INET
    context = ssl.create_default_context()
    sock = socket.socket(family, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    san_contains_host = False
    cert_subject = ""
    cert_issuer = ""
    san_entries: list[str] = []
    try:
        sock.connect((ip, 443))
        with context.wrap_socket(sock, server_hostname=host) as ssock:
            cert_dict = ssock.getpeercert()
    finally:
        sock.close()

    def _flatten_name(name_field: tuple[tuple[tuple[str, str], ...], ...]) -> str:
        parts = []
        for rdn in name_field:
            for typ, val in rdn:
                parts.append(f"{typ}={val}")
        return ', '.join(parts)

    subject = cert_dict.get("subject", ())
    issuer = cert_dict.get("issuer", ())
    san = cert_dict.get("subjectAltName", ())

    cert_subject = _flatten_name(subject)
    cert_issuer = _flatten_name(issuer)
    for san_type, san_value in san:
        san_entries.append(f"{san_type}:{san_value}")
        if san_type.lower() == "dns" and san_value.lower() == host.lower():
            san_contains_host = True

    return {
        "host": host,
        "connected_ip": ip,
        "server_cert_subject": cert_subject,
        "server_cert_issuer": cert_issuer,
        "server_cert_san": san_entries,
        "san_contains_host": san_contains_host,
        "openssl_version": ssl.OPENSSL_VERSION,
        "requests_version": requests.__version__,
        "proxy_env_detected": _has_proxy_env(),
    }


def build_tls_diag_payload(host: str, ipv4: list[str], ipv6: list[str], ip_family: str, cert_info: dict[str, Any], error_code: str) -> dict[str, Any]:
    """Construct structured diagnostic payload for TLS failures."""

    payload = {
        "host": host,
        "resolved_ipv4": ipv4,
        "resolved_ipv6": ipv6,
        "ip_family_used": ip_family,
        "error_code": error_code,
    }
    payload.update(cert_info)
    return payload


def _has_proxy_env() -> bool:
    keys = {"http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"}
    return any(key in os.environ for key in keys)


@contextmanager
def ip_family_guard(family: str) -> Iterator[None]:
    """Context manager to control DNS family resolution in urllib3."""

    original = getattr(urllib3_connection, "allowed_gai_family", None)

    def _forced_family() -> int:
        if family == "4":
            return socket.AF_INET
        if family == "6":
            return socket.AF_INET6
        return original()

    if family in {"4", "6"} and original is not None:
        urllib3_connection.allowed_gai_family = _forced_family  # type: ignore[assignment]
    try:
        yield
    finally:
        if original is not None:
            urllib3_connection.allowed_gai_family = original  # type: ignore[assignment]
