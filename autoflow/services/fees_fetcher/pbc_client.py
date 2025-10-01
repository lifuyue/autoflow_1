"""Client utilities for fetching RMB central parity data from PBOC."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import re
import socket
import ssl
import time
from collections.abc import Iterator
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Mapping, Optional
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.exceptions import SSLError, Timeout

from . import tls_diag

LOGGER = logging.getLogger(__name__)

USER_AGENT = "AutoflowBot/1.0"
PAGE_DELAY_SECONDS = 0.5
FAILURE_STOP_THRESHOLD = 2

INDEX_ROOT = "https://www.pbc.gov.cn/zhengcehuobisi/125207/125213/125440/17105/"
KEYCHART_URL = "https://www.pbc.gov.cn/zhengcehuobisi/125207/125213/125440/4385116/index.html"

DATE_PATTERN = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")
RATE_PATTERN = re.compile(r"1美元对人民币(?P<val>\d+(?:\.\d+)?)元")


@dataclass
class RequestConfig:
    """Runtime configuration for outbound HTTP requests."""

    connect_timeout: float = 5.0
    read_timeout: float = 8.0
    attempts: int = 3
    backoff_base: float = 0.8
    jitter: float = 0.25
    total_deadline: float = 30.0
    ip_family: str = "auto"


@dataclass
class FetchMetrics:
    """Telemetry captured during scraping."""

    request_attempts: int = 0
    request_successes: int = 0
    request_failures: int = 0
    https_upgrades: int = 0
    https_fallbacks: int = 0
    deadline_exceeded: int = 0
    early_stop: bool = False
    tls_hostname_mismatch: int = 0
    dns_a_count: int = 0
    dns_aaaa_count: int = 0
    ip_family_used: str = "auto"
    rate_source: str | None = None
    fallback_used: str | None = None


DEFAULT_CONFIG = RequestConfig()
REQUEST_CONFIG = replace(DEFAULT_CONFIG)

_SESSION = requests.Session()
_SESSION.headers.update(
    {
        "User-Agent": USER_AGENT,
        "Accept": "text/html, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "close",
    }
)
_SESSION.trust_env = False
_ADAPTER = HTTPAdapter(max_retries=0, pool_connections=4, pool_maxsize=4)
_SESSION.mount("http://", _ADAPTER)
_SESSION.mount("https://", _ADAPTER)

_METRICS = FetchMetrics()
_CURRENT_DEADLINE_END: float | None = None
_DIAG_EMITTED = False


class PBOCClientError(RuntimeError):
    """Raised when PBOC content cannot be retrieved."""


class FetchTimeout(PBOCClientError):
    """Raised when a fetch exceeds the configured deadline."""


class CertHostnameMismatch(PBOCClientError):
    """Raised when TLS certificate hostname mismatches the target host."""

    def __init__(self, host: str, diagnostics: dict[str, object]) -> None:
        super().__init__(f"hostname mismatch for {host}")
        self.host = host
        self.diagnostics = diagnostics


def reset_metrics() -> None:
    """Clear accumulated telemetry."""

    global _METRICS
    _METRICS = FetchMetrics()


def get_metrics() -> FetchMetrics:
    """Return a snapshot of current telemetry."""

    return replace(_METRICS)


def configure_requests(
    *,
    connect_timeout: float | None = None,
    read_timeout: float | None = None,
    total_deadline: float | None = None,
    ip_family: str | None = None,
) -> None:
    """Update runtime request configuration."""

    global REQUEST_CONFIG
    cfg = REQUEST_CONFIG
    if connect_timeout is not None:
        cfg = replace(cfg, connect_timeout=connect_timeout)
    if read_timeout is not None:
        cfg = replace(cfg, read_timeout=read_timeout)
    if total_deadline is not None:
        cfg = replace(cfg, total_deadline=total_deadline)
    if ip_family is not None:
        cfg = replace(cfg, ip_family=ip_family)
    REQUEST_CONFIG = cfg


def reset_request_config() -> None:
    """Restore the default request configuration."""

    global REQUEST_CONFIG
    REQUEST_CONFIG = replace(DEFAULT_CONFIG)


def get_request_config() -> RequestConfig:
    """Return the current request configuration."""

    return replace(REQUEST_CONFIG)


def begin_request_cycle(total_deadline: float | None) -> None:
    """Start a timed network cycle with a shared deadline."""

    global _CURRENT_DEADLINE_END, _DIAG_EMITTED
    if total_deadline is None:
        total_deadline = REQUEST_CONFIG.total_deadline
    _CURRENT_DEADLINE_END = None if total_deadline is None else time.monotonic() + total_deadline
    _DIAG_EMITTED = False


def end_request_cycle() -> None:
    """Terminate the active request cycle deadline."""

    global _CURRENT_DEADLINE_END
    _CURRENT_DEADLINE_END = None


def _remaining_deadline(end_time: float | None) -> float | None:
    if end_time is None:
        return None
    return max(0.0, end_time - time.monotonic())


def _list_page_candidates(page: int) -> list[str]:
    if page == 0:
        return ["index.html", "index1.html", "index_1.html"]
    number = page + 1
    return [f"index{number}.html", f"index_{number}.html"]


def _resolve_for_host(host: str) -> tuple[list[str], list[str]]:
    cfg = REQUEST_CONFIG
    ipv4, ipv6 = tls_diag.resolve_ips(host, cfg.ip_family)
    _METRICS.dns_a_count = len(ipv4)
    _METRICS.dns_aaaa_count = len(ipv6)
    _METRICS.ip_family_used = cfg.ip_family
    return ipv4, ipv6


def _request(
    url: str,
    *,
    connect_timeout: float | None = None,
    read_timeout: float | None = None,
    attempts: int | None = None,
    backoff_base: float | None = None,
    jitter: float | None = None,
    total_deadline: float | None = None,
    method: str = "GET",
    params: Mapping[str, str] | None = None,
    data: Mapping[str, str] | None = None,
) -> requests.Response:
    cfg = REQUEST_CONFIG
    connect_timeout = connect_timeout or cfg.connect_timeout
    read_timeout = read_timeout or cfg.read_timeout
    attempts = attempts or cfg.attempts
    backoff_base = backoff_base or cfg.backoff_base
    jitter = jitter if jitter is not None else cfg.jitter
    method = method.upper()

    start_time = time.monotonic()
    deadline_end = _CURRENT_DEADLINE_END
    if deadline_end is None:
        if total_deadline is None:
            total_deadline = cfg.total_deadline
        deadline_end = None if total_deadline is None else start_time + total_deadline

    last_exc: Exception | None = None
    parsed = urlparse(url)
    host = parsed.hostname or ""
    ipv4, ipv6 = _resolve_for_host(host) if host else ([], [])

    with tls_diag.ip_family_guard(cfg.ip_family):
        for attempt in range(1, attempts + 1):
            remaining = _remaining_deadline(deadline_end)
            if remaining is not None and remaining <= 0:
                _METRICS.deadline_exceeded += 1
                raise FetchTimeout(f"deadline exceeded before requesting {url}")

            timeout_connect = connect_timeout if remaining is None else min(connect_timeout, remaining)
            timeout_read = read_timeout if remaining is None else min(read_timeout, remaining)
            if remaining is not None and (timeout_connect <= 0 or timeout_read <= 0):
                _METRICS.deadline_exceeded += 1
                raise FetchTimeout(f"deadline exceeded before requesting {url}")

            timeout = (timeout_connect, timeout_read)
            attempt_start = time.monotonic()
            _METRICS.request_attempts += 1
            LOGGER.debug(
                "Attempt %s -> %s %s (connect=%.2fs read=%.2fs remaining=%s)",
                attempt,
                url,
                method,
                timeout_connect,
                timeout_read,
                f"{remaining:.2f}" if remaining is not None else "None",
            )
            try:
                session_request = getattr(_SESSION, "request", None)
                if callable(session_request):
                    response = session_request(
                        method,
                        url,
                        timeout=timeout,
                        proxies={"http": None, "https": None},
                        params=params,
                        data=data,
                    )
                else:
                    response = _SESSION.get(  # type: ignore[attr-defined]
                        url,
                        timeout=timeout,
                        proxies={"http": None, "https": None},
                    )
                response.raise_for_status()
                response.encoding = response.apparent_encoding or "utf-8"
                _METRICS.request_successes += 1
                LOGGER.debug(
                    "Attempt %s succeeded in %.2fs for %s",
                    attempt,
                    time.monotonic() - attempt_start,
                    url,
                )
                return response
            except SSLError as exc:
                last_exc = exc
                _METRICS.request_failures += 1
                LOGGER.warning("TLS error on %s: %s", url, exc)
                if _is_hostname_mismatch(exc):
                    diag_info: dict[str, object] | None = None
                    if not _DIAG_EMITTED:
                        try:
                            diag_info = _handle_hostname_mismatch(host, ipv4, ipv6, exc)
                        except CertHostnameMismatch:
                            raise
                        alt = _maybe_retry_alternate_host(url)
                        if alt:
                            LOGGER.warning("Retrying with fallback host: %s", alt)
                            url = alt
                            parsed = urlparse(url)
                            host = parsed.hostname or host
                            ipv4, ipv6 = _resolve_for_host(host) if host else ([], [])
                            continue
                    raise CertHostnameMismatch(host, diag_info or _build_basic_diag(host, ipv4, ipv6)) from exc
                raise
            except Timeout as exc:
                last_exc = exc
                _METRICS.request_failures += 1
                LOGGER.debug(
                    "Attempt %s timeout after %.2fs for %s",
                    attempt,
                    time.monotonic() - attempt_start,
                    url,
                )
            except requests.RequestException as exc:
                last_exc = exc
                _METRICS.request_failures += 1
                LOGGER.warning("Request failed on %s: %s", url, exc)
                raise PBOCClientError(f"failed to fetch {url}") from exc

            if attempt < attempts:
                sleep_time = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, jitter)
                remaining = _remaining_deadline(deadline_end)
                if remaining is not None:
                    sleep_time = min(sleep_time, remaining)
                if sleep_time > 0:
                    time.sleep(sleep_time)

    raise PBOCClientError(f"failed to fetch {url}") from last_exc


def _is_hostname_mismatch(exc: SSLError) -> bool:
    message = str(exc).lower()
    return "hostname" in message and "match" in message


def _handle_hostname_mismatch(
    host: str, ipv4: list[str], ipv6: list[str], exc: SSLError
) -> dict[str, object]:
    global _DIAG_EMITTED
    _DIAG_EMITTED = True
    _METRICS.tls_hostname_mismatch += 1
    cfg = REQUEST_CONFIG
    diag_info: dict[str, object] = {}
    candidate_list = ipv4 or ipv6
    candidate_ip = candidate_list[0] if candidate_list else None
    try:
        cert_info = tls_diag.probe_cert(host, candidate_ip) if candidate_ip else {}
        diag_info = tls_diag.build_tls_diag_payload(
            host,
            ipv4,
            ipv6,
            cfg.ip_family,
            cert_info,
            error_code="CERT_HOSTNAME_MISMATCH",
        )
    except Exception as diag_exc:  # pragma: no cover - diagnostic failures
        diag_info = {
            "host": host,
            "resolved_ipv4": ipv4,
            "resolved_ipv6": ipv6,
            "ip_family_used": cfg.ip_family,
            "error_code": "CERT_HOSTNAME_MISMATCH",
            "diag_error": str(diag_exc),
        }
    fingerprint = _extract_fingerprint(diag_info)
    strict_mode = os.getenv("PBC_STRICT_TLS", "1") != "0"
    if not strict_mode and not fingerprint and candidate_ip:
        fingerprint = _compute_cert_sha256(host, candidate_ip)
        if fingerprint:
            diag_info["cert_sha256"] = fingerprint
    if "cert_sha256" not in diag_info:
        diag_info["cert_sha256"] = fingerprint
    LOGGER.info("tls_diag=%s", json.dumps(diag_info, ensure_ascii=False))
    if strict_mode:
        raise CertHostnameMismatch(host, diag_info) from exc
    allowed = _parse_allowed_fingerprints()
    if not fingerprint or fingerprint not in allowed:
        raise CertHostnameMismatch(host, diag_info) from exc
    LOGGER.warning("lenient accept: host=%s fp=%s", host, fingerprint)
    return diag_info


def _extract_fingerprint(diag_info: dict[str, object]) -> str | None:
    for key in ("cert_sha256", "cert_sha256_fingerprint", "sha256_fingerprint", "fingerprint_sha256"):
        value = diag_info.get(key)
        if isinstance(value, str):
            normalized = _normalize_fingerprint(value)
            if normalized:
                diag_info["cert_sha256"] = normalized
                return normalized
    return None


def _normalize_fingerprint(raw: str | None) -> str | None:
    if not raw:
        return None
    cleaned = re.sub(r"[^0-9A-Fa-f]", "", raw)
    if not cleaned:
        return None
    cleaned = cleaned.upper()
    if len(cleaned) != 64:
        return None
    return cleaned


def _compute_cert_sha256(host: str, ip: str, timeout: float = 3.0) -> str | None:
    family = socket.AF_INET6 if ":" in ip else socket.AF_INET
    context = ssl.create_default_context()
    sock = socket.socket(family, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((ip, 443))
        with context.wrap_socket(sock, server_hostname=host) as ssock:
            cert_bytes = ssock.getpeercert(True)
    except Exception:  # pragma: no cover - best effort fingerprint
        return None
    finally:
        sock.close()
    return hashlib.sha256(cert_bytes).hexdigest().upper()


def _parse_allowed_fingerprints() -> set[str]:
    raw = os.getenv("PBC_ALLOWED_CERT_FINGERPRINTS", "")
    tokens = [token.strip() for token in raw.split(",") if token.strip()]
    normalized = {_normalize_fingerprint(token) for token in tokens}
    return {token for token in normalized if token}


def _maybe_retry_alternate_host(url: str) -> str | None:
    fallback_raw = os.getenv("PBC_FALLBACK_HOSTS", "")
    if not fallback_raw:
        return None
    parsed = urlparse(url)
    current_host = parsed.hostname
    userinfo = ""
    if parsed.username:
        userinfo = parsed.username
        if parsed.password:
            userinfo = f"{userinfo}:{parsed.password}"
        userinfo = f"{userinfo}@"
    current_port = parsed.port
    for candidate in fallback_raw.split(","):
        host = candidate.strip()
        if not host:
            continue
        if current_host and host.lower() == current_host.lower():
            continue
        netloc = f"{userinfo}{host}"
        if current_port:
            netloc = f"{netloc}:{current_port}"
        return urlunparse(parsed._replace(netloc=netloc))
    return None


def _build_basic_diag(host: str, ipv4: list[str], ipv6: list[str]) -> dict[str, object]:
    return {
        "host": host,
        "resolved_ipv4": ipv4,
        "resolved_ipv6": ipv6,
        "ip_family_used": REQUEST_CONFIG.ip_family,
        "error_code": "CERT_HOSTNAME_MISMATCH",
        "cert_sha256": None,
    }


def iter_article_urls(max_pages: int = 15) -> Iterator[str]:
    """Yield candidate article URLs that mention the central parity announcement."""

    seen: set[str] = set()
    consecutive_failures = 0
    deadline_end = _CURRENT_DEADLINE_END

    for page in range(max_pages):
        html: str | None = None
        source_url: str | None = None
        candidates = [urljoin(INDEX_ROOT, suffix) for suffix in _list_page_candidates(page)]

        for candidate in candidates:
            try:
                response = _request(candidate)
            except CertHostnameMismatch:
                raise
            except FetchTimeout as exc:
                LOGGER.warning("Directory fetch timed out: %s", exc)
                raise
            except PBOCClientError:
                continue
            html = response.text
            source_url = candidate
            break

        if not html or not source_url:
            consecutive_failures += 1
            LOGGER.warning("Directory page %s unavailable (%s)", page, ", ".join(candidates))
            if consecutive_failures >= FAILURE_STOP_THRESHOLD:
                LOGGER.warning(
                    "Stopping directory scan after %s consecutive failures", consecutive_failures
                )
                _METRICS.early_stop = True
                break
            continue

        consecutive_failures = 0
        soup = BeautifulSoup(html, "html.parser")
        for anchor in soup.select("a"):
            text = anchor.get_text(strip=True)
            href = anchor.get("href")
            if not text or not href:
                continue
            if "人民币汇率中间价" not in text:
                continue
            article_url = urljoin(source_url, href)
            if article_url in seen:
                continue
            seen.add(article_url)
            yield article_url

        remaining = _remaining_deadline(deadline_end)
        delay = PAGE_DELAY_SECONDS if remaining is None else min(PAGE_DELAY_SECONDS, remaining)
        if delay > 0:
            time.sleep(delay)


def parse_article(url: str) -> tuple[str, Optional[Decimal]]:
    """Return the ISO date and USD/CNY midpoint parsed from an announcement article."""

    response = _request(url)
    soup = BeautifulSoup(response.text, "html.parser")
    body_text = soup.get_text("，", strip=True)

    date_match = DATE_PATTERN.search(body_text)
    date_iso = ""
    if date_match:
        year, month, day = date_match.groups()
        date_iso = f"{int(year):04d}-{int(month):02d}-{int(day):02d}"

    rate_match = RATE_PATTERN.search(body_text)
    if not rate_match:
        return date_iso, None

    rate_val = Decimal(rate_match.group("val"))
    return date_iso, rate_val


def probe_keychart(date: str) -> Optional[Decimal]:
    """Fallback parser for the key chart page that lists USD/CNY mid rates."""

    try:
        response = _request(KEYCHART_URL)
    except CertHostnameMismatch:
        raise
    except FetchTimeout:
        _METRICS.deadline_exceeded += 1
        return None
    except PBOCClientError:  # pragma: no cover - network error path
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")
    if not table:
        return None

    for row in table.find_all("tr"):
        cells = [cell.get_text(strip=True) for cell in row.find_all(["td", "th"])]
        if not cells:
            continue
        if cells[0].startswith(date):
            for cell in cells[1:]:
                match = RATE_PATTERN.search(cell)
                if match:
                    return Decimal(match.group("val"))
    return None
