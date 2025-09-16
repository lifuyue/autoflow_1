"""Typer based command line entry points for AutoFlow."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import List, Tuple

import typer

from autoflow.core.logger import get_logger
from autoflow.services.form_processor import FormProcessConfig, process_forms
from autoflow.services.form_processor.providers import StaticRateProvider

app = typer.Typer(help="Utility CLI for AutoFlow services.")


@app.command("process-forms")
def cli_process_forms(
    input_files: List[Path] = typer.Option(
        ..., "--input", "-i", help="Input Excel/CSV files", exists=True, readable=True, resolve_path=True, nargs=-1
    ),
    output: Path = typer.Option(..., "--output", "-o", help="Directory for generated files", resolve_path=True),
    mapping: Path = typer.Option(..., "--mapping", help="Mapping YAML file", exists=True, readable=True, resolve_path=True),
    base_currency: str = typer.Option("CNY", help="Target/base currency code"),
    round_digits: int = typer.Option(2, help="Decimal precision for monetary values"),
    confirm_threshold: Decimal = typer.Option(Decimal("20000"), help="Confirmation threshold in base currency"),
    default_rate: Decimal = typer.Option(
        Decimal("1"), help="Default conversion rate when no override is provided"
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

    overrides: dict[Tuple[str, str], Decimal] = {}
    if rates:
        for item in rates:
            try:
                pair, value = item.split("=")
                from_ccy, to_ccy = pair.split(":")
                overrides[(from_ccy.strip().upper(), to_ccy.strip().upper())] = Decimal(value)
            except Exception as exc:  # noqa: BLE001
                raise typer.BadParameter(f"Invalid rate override: {item}") from exc

    provider = StaticRateProvider(default_rate=default_rate, overrides=overrides)
    cfg = FormProcessConfig(
        mapping_path=str(mapping),
        base_currency=base_currency.upper(),
        round_digits=round_digits,
        confirm_over_amount_cny=confirm_threshold,
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
