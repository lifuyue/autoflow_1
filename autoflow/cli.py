"""Typer based command line entry points for AutoFlow."""

from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import List, Tuple

import typer

from autoflow.core.logger import get_logger
from autoflow.services.fees_fetcher import fetch_with_fallback, pbc_client
from autoflow.services.fees_fetcher.monthly_builder import (
    fetch_month_rate,
    format_rate,
    load_cn_calendar,
    plan_missing_months,
    upsert_csv,
)
from autoflow.services.form_processor import FormProcessConfig, process_forms
from autoflow.services.form_processor.providers import RateLookupError, StaticRateProvider

ALLOWED_IP_FAMILIES = {"auto", "4", "6"}
PREFERRED_SOURCES = {"auto", "pbc", "cfets", "safe"}

app = typer.Typer(help="Utility CLI for AutoFlow services.")


def _validate_ip_family(value: str) -> str:
    value = value.lower()
    if value not in ALLOWED_IP_FAMILIES:
        raise typer.BadParameter("ip-family must be one of auto, 4, 6")
    return value


def _validate_prefer_source(value: str) -> str:
    value = value.lower()
    if value not in PREFERRED_SOURCES:
        raise typer.BadParameter("prefer-source must be one of auto, pbc, cfets, safe")
    return value


@app.command("get-rate")
def cli_get_rate(
    date: str = typer.Option(..., "--date", help="Target trading date in YYYY-MM-DD format"),
    from_ccy: str = typer.Option(..., "--from", help="Source currency code"),
    to_ccy: str = typer.Option(..., "--to", help="Target currency code"),
    connect_timeout: float = typer.Option(5.0, help="Connection timeout (seconds)", show_default=True),
    read_timeout: float = typer.Option(8.0, help="Read timeout (seconds)", show_default=True),
    total_deadline: float = typer.Option(30.0, help="Total deadline per lookup (seconds)", show_default=True),
    ip_family: str = typer.Option("auto", help="IP family preference (auto/4/6)", callback=_validate_ip_family),
    prefer_source: str = typer.Option(
        "auto",
        help="Preferred rate source (auto/pbc/cfets/safe)",
        callback=_validate_prefer_source,
    ),
) -> None:
    """Fetch USD/CNY central parity from PBOC."""

    logger = get_logger()

    try:
        date_obj = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError as exc:  # noqa: BLE001 - user input validation
        raise typer.BadParameter("date must be in YYYY-MM-DD format") from exc

    from_code = from_ccy.strip().upper()
    to_code = to_ccy.strip().upper()

    pbc_client.reset_metrics()
    pbc_client.configure_requests(
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        total_deadline=total_deadline,
        ip_family=ip_family,
    )

    started = time.monotonic()
    result: tuple[Decimal, str, str, str] | None = None
    exit_code = 0
    try:
        if (from_code, to_code) != ("USD", "CNY"):
            raise typer.BadParameter("Only USD/CNY pairs are supported")
        result = fetch_with_fallback(date_obj.isoformat(), prefer_source=prefer_source)
    except NotImplementedError as exc:  # noqa: BLE001 - surfaces to CLI
        raise typer.BadParameter(str(exc)) from exc
    except pbc_client.CertHostnameMismatch as exc:
        logger.error("TLS diagnostic: %s", json.dumps(exc.diagnostics, ensure_ascii=False))
        typer.secho("TLS hostname mismatch detected.", fg=typer.colors.RED)
        _print_tls_guidance(exc.diagnostics)
        exit_code = 2
    except RateLookupError as exc:
        logger.error("Unable to obtain rate for %s %s/%s: %s", date_obj, from_code, to_code, exc)
        exit_code = 1
    finally:
        duration = time.monotonic() - started
        metrics = pbc_client.get_metrics()
        logger.info(
            "Fetch summary: duration=%.2fs attempts=%d success=%d failure=%d deadline=%d upgrade=%d fallback=%d early_stop=%s tls_hostname_mismatch=%d dns_a_count=%d dns_aaaa_count=%d ip_family=%s rate_source=%s fallback_used=%s",
            duration,
            metrics.request_attempts,
            metrics.request_successes,
            metrics.request_failures,
            metrics.deadline_exceeded,
            metrics.https_upgrades,
            metrics.https_fallbacks,
            metrics.early_stop,
            metrics.tls_hostname_mismatch,
            metrics.dns_a_count,
            metrics.dns_aaaa_count,
            metrics.ip_family_used,
            metrics.rate_source or "unknown",
            metrics.fallback_used or "none",
        )
        pbc_client.reset_request_config()

    if result is not None and exit_code == 0:
        rate, source_date, rate_source, fallback_used = result
        typer.echo(
            f"{date_obj.isoformat()} USD/CNY midpoint = {rate:.4f} (source={rate_source}, "
            f"source_date={source_date}, fallback={fallback_used})"
        )
    if exit_code:
        raise typer.Exit(code=exit_code)


def _print_tls_guidance(diag: dict[str, object]) -> None:
    typer.echo("TLS diagnostics:")
    for key in [
        "host",
        "connected_ip",
        "server_cert_subject",
        "server_cert_issuer",
        "san_contains_host",
        "resolved_ipv4",
        "resolved_ipv6",
        "ip_family_used",
    ]:
        if key in diag:
            typer.echo(f"  {key}: {diag[key]}")
    typer.echo("Recommended actions: switch network出口, test with --ip-family 4, verify DNS, or contact network team.")


@app.command("build-monthly-rates")
def cli_build_monthly_rates(
    start: str = typer.Option(..., help="起始月份 YYYY-MM，如 2023-01"),
    output: Path | None = typer.Option(
        None,
        help="输出 CSV，默认 data/rates/monthly_usd_cny.csv",
        resolve_path=True,
    ),
    refresh: List[str] = typer.Option(
        [],
        "--refresh",
        help="需强制刷新 YYYY-MM，可多次传参",
    ),
    rebuild: bool = typer.Option(False, help="忽略现有 CSV 全量重建"),
    log_level: str = typer.Option("INFO", help="日志级别 (DEBUG/INFO/WARNING/ERROR)"),
    http_debug: bool = typer.Option(
        False,
        "--http-debug",
        help="开启 urllib3/http.client 调试输出",
        is_flag=True,
    ),
    connect_timeout: float = typer.Option(5.0, help="连接超时（秒）", show_default=True),
    read_timeout: float = typer.Option(8.0, help="读取超时（秒）", show_default=True),
    total_deadline: float = typer.Option(30.0, help="单次抓取总时长上限（秒）", show_default=True),
    ip_family: str = typer.Option("auto", help="IP 族偏好 (auto/4/6)", callback=_validate_ip_family),
    prefer_source: str = typer.Option(
        "auto",
        help="首选数据源 (auto/pbc/cfets/safe)",
        callback=_validate_prefer_source,
    ),
) -> None:
    """构建/补齐每月首个工作日 USD/CNY 中间价（仅月度，无每日缓存）。"""

    logger = get_logger()

    level_value = getattr(logging, log_level.upper(), None)
    if not isinstance(level_value, int):
        raise typer.BadParameter(f"Unknown log level: {log_level}")
    logging.getLogger().setLevel(level_value)

    if http_debug:
        try:
            import http.client as http_client

            http_client.HTTPConnection.debuglevel = 1
        except Exception:  # pragma: no cover - platform specific
            logger.warning("Failed to enable http.client debugging")
        logging.getLogger("urllib3").setLevel(logging.DEBUG)
        logging.getLogger("urllib3").propagate = True

    logger.info(
        "Starting monthly build: start=%s output=%s refresh=%s rebuild=%s http_debug=%s connect_timeout=%.2f read_timeout=%.2f total_deadline=%.2f ip_family=%s",
        start,
        output or "data/rates/monthly_usd_cny.csv",
        ",".join(refresh) if refresh else "<none>",
        rebuild,
        http_debug,
        connect_timeout,
        read_timeout,
        total_deadline,
        ip_family,
    )

    try:
        start_dt = datetime.strptime(start, "%Y-%m")
    except ValueError as exc:  # noqa: BLE001
        raise typer.BadParameter("start must be formatted as YYYY-MM") from exc

    start_marker = date(start_dt.year, start_dt.month, 1)
    current_month = date.today().replace(day=1)
    if start_marker > current_month:
        raise typer.BadParameter("start month cannot be in the future")

    output_path = output or Path("data/rates/monthly_usd_cny.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    months_to_process: set[tuple[int, int]] = set()
    pbc_client.reset_metrics()
    pbc_client.configure_requests(
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        total_deadline=total_deadline,
        ip_family=ip_family,
    )
    command_started = time.monotonic()

    if rebuild:
        if output_path.exists():
            output_path.unlink()
        cursor = start_marker
        while cursor <= current_month:
            months_to_process.add((cursor.year, cursor.month))
            cursor = cursor.replace(year=cursor.year + 1, month=1) if cursor.month == 12 else cursor.replace(month=cursor.month + 1)
    else:
        months_to_process.update(plan_missing_months(output_path, start_marker, date.today()))

    for item in refresh:
        try:
            refresh_dt = datetime.strptime(item, "%Y-%m")
        except ValueError as exc:  # noqa: BLE001
            raise typer.BadParameter(f"Invalid refresh month: {item}") from exc
        months_to_process.add((refresh_dt.year, refresh_dt.month))

    if not months_to_process:
        logger.info("Monthly USD/CNY rates already up to date; nothing to do.")
        _log_fetch_metrics(command_started)
        _cleanup_http_debug(http_debug)
        return

    ordered_months = sorted(months_to_process)
    logger.info(
        "Building monthly rates for: %s",
        ", ".join(f"{year}-{month:02d}" for year, month in ordered_months),
    )

    holidays, workdays = load_cn_calendar()
    success: list[tuple[int, int]] = []
    pending: list[tuple[int, int, str, str]] = []

    try:
        for year, month in ordered_months:
            try:
                rate, source_date, rate_source, fallback_used = fetch_month_rate(
                    year,
                    month,
                    holidays=holidays,
                    workdays=workdays,
                    prefer_source=prefer_source,
                )
            except pbc_client.CertHostnameMismatch as exc:
                logger.warning(
                    "%04d-%02d pending (tls hostname mismatch): %s",
                    year,
                    month,
                    json.dumps(exc.diagnostics, ensure_ascii=False),
                )
                pending.append((year, month, "CERT_HOSTNAME_MISMATCH", prefer_source))
                continue
            except RateLookupError as exc:
                logger.warning("%04d-%02d pending (no rate): %s", year, month, exc)
                pending.append((year, month, str(exc), prefer_source))
                continue

            rate_str = format_rate(rate)
            upsert_csv(output_path, [(year, month, rate_str, source_date)])
            logger.info(
                "Stored %04d-%02d rate %s from %s via %s (fallback=%s) into %s",
                year,
                month,
                rate_str,
                source_date,
                rate_source,
                fallback_used,
                output_path,
            )
            success.append((year, month))

        logger.info(
            "Monthly build finished: success=%d pending=%d output=%s",
            len(success),
            len(pending),
            output_path,
        )
        if pending:
            for year, month, message, source in pending:
                logger.warning(
                    "Pending month %04d-%02d (prefer=%s): %s",
                    year,
                    month,
                    source,
                    message,
                )
    finally:
        _log_fetch_metrics(command_started)
        pbc_client.reset_request_config()
        _cleanup_http_debug(http_debug)


def _log_fetch_metrics(started: float) -> None:
    metrics = pbc_client.get_metrics()
    logger = get_logger()
    duration = time.monotonic() - started
    logger.info(
        "Fetch metrics: duration=%.2fs attempts=%d success=%d failure=%d deadline=%d upgrade=%d fallback=%d early_stop=%s tls_hostname_mismatch=%d dns_a_count=%d dns_aaaa_count=%d ip_family=%s",
        duration,
        metrics.request_attempts,
        metrics.request_successes,
        metrics.request_failures,
        metrics.deadline_exceeded,
        metrics.https_upgrades,
        metrics.https_fallbacks,
        metrics.early_stop,
        metrics.tls_hostname_mismatch,
        metrics.dns_a_count,
        metrics.dns_aaaa_count,
        metrics.ip_family_used,
    )


def _cleanup_http_debug(enabled: bool) -> None:
    if enabled:
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("urllib3").propagate = False
        try:  # pragma: no cover - platform specific
            import http.client as http_client

            http_client.HTTPConnection.debuglevel = 0
        except Exception:
            pass


@app.command("process-forms")
def cli_process_forms(
    input_files: List[Path] = typer.Option(
        [],
        "--input",
        "-i",
        help="Input Excel/CSV files (repeat for multiple)",
        exists=True,
        readable=True,
        resolve_path=True,
        file_okay=True,
        dir_okay=False,
    ),
    output: Path = typer.Option(..., "--output", "-o", help="Directory for generated files", resolve_path=True),
    mapping: Path = typer.Option(..., "--mapping", help="Mapping YAML file", exists=True, readable=True, resolve_path=True),
    base_currency: str = typer.Option("CNY", help="Target/base currency code"),
    round_digits: int = typer.Option(2, help="Decimal precision for monetary values"),
    confirm_threshold: str = typer.Option(
        "20000", help="Confirmation threshold in base currency"
    ),
    default_rate: str = typer.Option(
        "1", help="Default conversion rate when no override is provided"
    ),
    rates: List[str] = typer.Option(
        [],
        "--rate",
        help="Override rates as FROM:TO=VALUE (e.g. USD:CNY=7.12)",
    ),
    non_interactive: bool = typer.Option(False, help="Skip interactive confirmation prompts"),
) -> None:
    """Process fee forms according to a mapping configuration."""

    logger = get_logger()

    if not input_files:
        raise typer.BadParameter("At least one --input/-i file is required")

    overrides: dict[Tuple[str, str], Decimal] = {}
    if rates:
        for item in rates:
            try:
                pair, value = item.split("=")
                from_ccy, to_ccy = pair.split(":")
                overrides[(from_ccy.strip().upper(), to_ccy.strip().upper())] = Decimal(value)
            except Exception as exc:  # noqa: BLE001
                raise typer.BadParameter(f"Invalid rate override: {item}") from exc

    try:
        confirm_threshold_value = Decimal(confirm_threshold)
    except InvalidOperation as exc:  # noqa: BLE001
        raise typer.BadParameter(
            f"Invalid decimal for confirm-threshold: {confirm_threshold}"
        ) from exc

    try:
        default_rate_value = Decimal(default_rate)
    except InvalidOperation as exc:  # noqa: BLE001
        raise typer.BadParameter(f"Invalid decimal for default-rate: {default_rate}") from exc

    provider = StaticRateProvider(default_rate=default_rate_value, overrides=overrides)
    cfg = FormProcessConfig(
        mapping_path=str(mapping),
        base_currency=base_currency.upper(),
        round_digits=round_digits,
        confirm_over_amount_cny=confirm_threshold_value,
    )

    result = process_forms(
        input_paths=[str(p) for p in input_files],
        output_dir=str(output),
        config=cfg,
        rate_provider=provider,
        non_interactive=non_interactive,
    )

    typer.echo("Processing finished")
    typer.echo(f"Accepted rows: {result.ok_rows}")
    typer.echo(f"Rejected rows: {result.rejected_rows}")
    typer.echo(f"Need confirm rows: {result.need_confirm_rows}")
    typer.echo(f"Template output: {result.output_template_path}")
    typer.echo(f"Report: {result.report_path}")
    if result.rejects_csv_path:
        typer.echo(f"Rejected rows CSV: {result.rejects_csv_path}")
    if result.confirm_csv_path:
        typer.echo(f"Need confirm CSV: {result.confirm_csv_path}")
    logger.info("CLI form-processing completed: output=%s", result.output_template_path)


if __name__ == "__main__":
    app()
