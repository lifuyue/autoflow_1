"""Rate provider implementation backed by PBOC public data."""

from __future__ import annotations

import logging
from decimal import Decimal

from autoflow.services.form_processor.api import RateProvider
from autoflow.services.form_processor.providers import RateLookupError

from . import pbc_client

LOGGER = logging.getLogger(__name__)


class PBOCRateProvider(RateProvider):
    """Retrieve USD/CNY central parity quotes published by PBOC."""

    def __init__(self, max_pages: int = 15) -> None:
        self.max_pages = max_pages

    def get_rate(self, date: str, from_ccy: str, to_ccy: str) -> Decimal:
        from_code = from_ccy.upper()
        to_code = to_ccy.upper()

        if (from_code, to_code) != ("USD", "CNY"):
            raise NotImplementedError("PBOCRateProvider currently only supports USD/CNY")

        LOGGER.info("Fetching USD/CNY rate for %s", date)

        rate, _, _ = fetch_pbc_midpoint(date, max_pages=self.max_pages)
        return rate


def fetch_pbc_midpoint(
    target_date: str,
    *,
    max_pages: int = 15,
    manage_cycle: bool = True,
) -> tuple[Decimal, str, str]:
    """Return USD/CNY midpoint from PBOC sources."""

    config = pbc_client.get_request_config()
    tls_error: pbc_client.CertHostnameMismatch | None = None
    if manage_cycle:
        pbc_client.begin_request_cycle(config.total_deadline)
    try:
        try:
            for article_url in pbc_client.iter_article_urls(max_pages):
                try:
                    article_date, maybe_rate = pbc_client.parse_article(article_url)
                except pbc_client.CertHostnameMismatch as exc:
                    tls_error = exc
                    LOGGER.error("TLS hostname mismatch: %s", exc.diagnostics)
                    break
                if article_date != target_date:
                    continue
                if maybe_rate is None:
                    LOGGER.debug("Article %s lacks USD/CNY quote", article_url)
                    continue
                LOGGER.info("Matched PBOC announcement %s", article_url)
                return maybe_rate, article_date or target_date, "pbc_notice"
        except pbc_client.FetchTimeout as exc:
            LOGGER.warning("Directory scan timed out for %s: %s", target_date, exc)

        if tls_error:
            raise tls_error

        LOGGER.warning("Announcement not found, probing key chart for %s", target_date)
        try:
            fallback = pbc_client.probe_keychart(target_date)
        except pbc_client.CertHostnameMismatch as exc:
            LOGGER.error("TLS hostname mismatch during key chart probe: %s", exc.diagnostics)
            tls_error = exc
            fallback = None
        if fallback is not None:
            LOGGER.info("Using PBOC key chart value for %s", target_date)
            return fallback, target_date, "pbc_keychart"

        if tls_error:
            raise tls_error
    finally:
        if manage_cycle:
            pbc_client.end_request_cycle()

    raise RateLookupError(
        f"USD/CNY midpoint unavailable for {target_date}",
        original_date=target_date,
    )
