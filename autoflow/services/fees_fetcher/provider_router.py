"""Source routing for USD/CNY midpoint retrieval."""

from __future__ import annotations

import logging
from decimal import Decimal

from autoflow.services.form_processor.providers import RateLookupError

from . import cfets_provider, pbc_client, safe_provider
from .pbc_provider import fetch_pbc_midpoint

LOGGER = logging.getLogger(__name__)

VALID_SOURCES = {"auto", "pbc", "cfets", "safe"}


def fetch_with_fallback(
    target_date: str,
    *,
    prefer_source: str = "auto",
) -> tuple[Decimal, str, str, str]:
    """Retrieve USD/CNY midpoint with cascading fallbacks."""

    prefer = prefer_source.lower()
    if prefer not in VALID_SOURCES:
        raise ValueError(f"Unsupported prefer-source: {prefer_source}")

    order = _build_order(prefer)
    LOGGER.debug("Fetch order for %s: %s", target_date, ", ".join(order))

    config = pbc_client.get_request_config()
    pbc_client.begin_request_cycle(config.total_deadline)
    _reset_metrics_tracking()

    fallback_used = "none"
    last_lookup_error: Exception | None = None
    last_tls_error: pbc_client.CertHostnameMismatch | None = None

    try:
        for idx, source in enumerate(order):
            try:
                rate, source_date, rate_source = _invoke_source(source, target_date)
            except pbc_client.CertHostnameMismatch as exc:
                last_tls_error = exc
                LOGGER.warning("TLS mismatch via %s: %s", source, exc)
                continue
            except pbc_client.FetchTimeout as exc:
                last_lookup_error = exc
                LOGGER.warning("Timeout via %s: %s", source, exc)
                continue
            except (LookupError, RateLookupError) as exc:
                last_lookup_error = exc
                LOGGER.debug("Lookup failed via %s for %s: %s", source, target_date, exc)
                continue

            if idx > 0:
                fallback_used = source
            
            if rate_source == "safe_portal" and source_date != target_date:
                fallback_used = "forward"

            _note_result(rate_source, fallback_used)
            return rate, source_date, rate_source, fallback_used

        if last_tls_error is not None and prefer in {"auto", "pbc"}:
            raise last_tls_error
        if last_lookup_error is not None:
            raise RateLookupError(
                f"USD/CNY midpoint unavailable for {target_date}",
                original_date=target_date,
            ) from last_lookup_error
        raise RateLookupError(
            f"USD/CNY midpoint unavailable for {target_date}",
            original_date=target_date,
        )
    finally:
        pbc_client.end_request_cycle()


def _build_order(prefer: str) -> list[str]:
    if prefer == "auto":
        return ["pbc", "cfets", "safe"]
    if prefer == "pbc":
        return ["pbc", "cfets", "safe"]
    if prefer == "cfets":
        return ["cfets", "safe"]
    if prefer == "safe":
        return ["safe"]
    return ["pbc", "cfets", "safe"]


def _invoke_source(source: str, target_date: str) -> tuple[Decimal, str, str]:
    if source == "pbc":
        return fetch_pbc_midpoint(target_date, manage_cycle=False)
    if source == "cfets":
        return cfets_provider.get_usd_cny_midpoint_from_notice(None, target_date)
    if source == "safe":
        return safe_provider.get_usd_cny_midpoint_from_portal(None, target_date)
    raise ValueError(f"Unknown source: {source}")


def _note_result(rate_source: str, fallback_used: str) -> None:
    try:
        metrics = pbc_client._METRICS  # noqa: SLF001 - share telemetry bucket
    except AttributeError:  # pragma: no cover - defensive fallback
        return

    metrics.rate_source = rate_source
    metrics.fallback_used = fallback_used


def _reset_metrics_tracking() -> None:
    try:
        metrics = pbc_client._METRICS  # noqa: SLF001 - shared telemetry bucket
    except AttributeError:  # pragma: no cover - defensive fallback
        return
    metrics.rate_source = None
    metrics.fallback_used = None
