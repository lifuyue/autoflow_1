"""
RESPONSIBILITIES
- Minimal Typer CLI for managing the FX rates XLSX store.
- Supports initialization, CSV import, single-record upsert, and query preview.
PROCESS OVERVIEW
1. init -> ensure stores exist (rates/pdf/xlsx when requested).
2. import-csv -> canonicalize CSV payloads and upsert them into the rates store.
3. upsert -> merge one record with download_url placeholder retention.
4. query -> filter by pair/date range and preview results.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

import pandas as pd
import typer

from autoflow_persist.schemas.rates import RatesQuery, RatesRecord
from autoflow_persist.stores.pdf_store import init_pdf_store
from autoflow_persist.stores.rates_store import (
    bulk_import_csv as import_rates_csv,
    init_rates_store,
    query_rates,
    upsert_rate,
)
from autoflow_persist.stores.xlsx_store import init_xlsx_store
from autoflow_persist.utils.log import get_logger

app = typer.Typer(help="Manage the local XLSX stores for FX rates.")
logger = get_logger("tools.rates_cli")


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"Invalid ISO date: {value}") from exc


def _parse_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"Invalid ISO datetime: {value}") from exc


@app.command("init")
def init_command(
    root: Optional[Path] = typer.Option(None, help="Alternate persistence root (defaults to ~/AutoFlow)."),
    all_stores: bool = typer.Option(True, help="Initialize pdf/xlsx placeholder stores as well."),
) -> None:
    """Ensure the XLSX stores exist."""

    path = init_rates_store(root)
    typer.echo(f"rates_store.xlsx ready: {path}")
    if all_stores:
        pdf_path = init_pdf_store(root)
        xlsx_path = init_xlsx_store(root)
        typer.echo(f"pdf_store.xlsx ready: {pdf_path}")
        typer.echo(f"xlsx_store.xlsx ready: {xlsx_path}")


@app.command("import-csv")
def import_csv_command(
    path: Path = typer.Option(..., exists=True, file_okay=True, readable=True, help="CSV file to import."),
    base: str = typer.Option(..., help="Base currency, e.g. USD."),
    quote: str = typer.Option(..., help="Quote currency, e.g. CNY."),
    source: str = typer.Option(..., help="Primary data source identifier."),
    fallback: Optional[str] = typer.Option(None, help="Fallback strategy label."),
    root: Optional[Path] = typer.Option(None, help="Alternate persistence root."),
    download_url: Optional[str] = typer.Option(None, help="Download URL placeholder to persist."),
) -> None:
    """Import historical rates from CSV with upsert semantics."""

    logger.info("Importing CSV %s", path)
    count = import_rates_csv(
        path,
        base=base,
        quote=quote,
        source=source,
        fallback=fallback,
        root=root,
        download_url=download_url,
    )
    typer.echo(f"Imported or updated {count} rows from {path}")


@app.command("upsert")
def upsert_command(
    base: str = typer.Option(..., help="Base currency."),
    quote: str = typer.Option(..., help="Quote currency."),
    rate: Decimal = typer.Option(..., help="Mid rate value (decimal)."),
    rate_date: str = typer.Option(..., help="Rate effective date (ISO)."),
    fetch_date: str = typer.Option(..., help="Fetch date (ISO date or datetime)."),
    source: str = typer.Option(..., help="Data source identifier."),
    fallback: Optional[str] = typer.Option(None, help="Fallback strategy label."),
    root: Optional[Path] = typer.Option(None, help="Alternate persistence root."),
    download_url: Optional[str] = typer.Option(None, help="Download URL placeholder to persist."),
) -> None:
    """Upsert a single FX rate record."""

    rate_dt = _parse_date(rate_date)
    fetch_dt: datetime | date
    try:
        fetch_dt = _parse_datetime(fetch_date)
    except typer.BadParameter:
        fetch_dt = _parse_date(fetch_date)
    record = RatesRecord(
        base_currency=base,
        quote_currency=quote,
        rate_mid=rate,
        rate_date=rate_dt,
        fetch_date=fetch_dt,
        source=source,
        fallback_strategy=fallback,
        download_url=download_url,
    )
    upsert_rate(record, root=root, download_url=download_url)
    typer.echo(
        f"Upserted {base}/{quote} {rate:.4f} for {rate_dt.isoformat()} (download_url={download_url or 'None'})"
    )


@app.command("query")
def query_command(
    pair: Optional[str] = typer.Option(None, help="Currency pair in BASE/QUOTE format."),
    base: Optional[str] = typer.Option(None, help="Base currency override."),
    quote: Optional[str] = typer.Option(None, help="Quote currency override."),
    from_date: Optional[str] = typer.Option(None, "--from", help="Start date inclusive (ISO)."),
    to_date: Optional[str] = typer.Option(None, "--to", help="End date inclusive (ISO)."),
    root: Optional[Path] = typer.Option(None, help="Alternate persistence root."),
    limit: int = typer.Option(5, help="Preview row limit."),
) -> None:
    """Run a filtered query and show a quick preview."""

    query = RatesQuery()
    if pair:
        try:
            pair_base, pair_quote = pair.split("/")
        except ValueError as exc:
            raise typer.BadParameter("Pair must be in BASE/QUOTE format") from exc
        query.base_currency = pair_base
        query.quote_currency = pair_quote
    if base:
        query.base_currency = base
    if quote:
        query.quote_currency = quote
    if from_date:
        query.start_date = _parse_date(from_date)
    if to_date:
        query.end_date = _parse_date(to_date)

    frame = query_rates(query, root=root)
    typer.echo(f"Matched {len(frame)} rows")
    if not frame.empty:
        preview = frame.head(limit)
        preview = preview.assign(
            rate_mid=lambda df: df["rate_mid"].apply(lambda val: f"{val:.4f}" if isinstance(val, Decimal) else val)
        )
        typer.echo(preview.to_string(index=False))


def main() -> None:
    app()


if __name__ == "__main__":
    main()
