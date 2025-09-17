"""CLI integration tests for monthly rate building."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
from typer.testing import CliRunner

from autoflow import cli
from autoflow.services.fees_fetcher.monthly_builder import MonthlyRateResult


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


def test_build_monthly_rates_append_and_dedup(
    cli_runner: CliRunner,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "rates.csv"

    # Ensure logging does not write into the project workspace.
    import autoflow.core.logger as core_logger

    monkeypatch.setattr(core_logger, "_work_dir", lambda: tmp_path / "work")
    monkeypatch.setattr(core_logger, "_LOGGER", None, raising=False)

    def fake_fetch_month_rate(
        year: int,
        month: int,
        **_: object,
    ) -> MonthlyRateResult:
        query_date = f"{year:04d}-{month:02d}-01"
        return MonthlyRateResult(
            year=year,
            month=month,
            query_date=query_date,
            request_date=query_date,
            mid_rate=Decimal(f"7.{month:02d}{month:02d}"),
            source_date=f"{year:04d}-{month:02d}-05",
            rate_source="stub_source",
            fallback_used="none",
        )

    monkeypatch.setattr(cli, "fetch_month_rate", fake_fetch_month_rate)
    monkeypatch.setattr(cli, "load_cn_calendar", lambda: (set(), set()))

    result = cli_runner.invoke(
        cli.app,
        [
            "build-monthly-rates",
            "--start",
            "2023-01",
            "--end",
            "2023-03",
            "--output",
            str(output),
        ],
    )
    assert result.exit_code == 0, result.stdout

    lines_first = output.read_text(encoding="utf-8").splitlines()
    assert len(lines_first) == 4  # header + 3 months

    result_repeat = cli_runner.invoke(
        cli.app,
        [
            "build-monthly-rates",
            "--start",
            "2023-01",
            "--end",
            "2023-03",
            "--output",
            str(output),
        ],
    )
    assert result_repeat.exit_code == 0, result_repeat.stdout

    lines_second = output.read_text(encoding="utf-8").splitlines()
    assert lines_second == lines_first
    # Ensure no duplicate months were appended.
    assert sum(1 for line in lines_second if "stub_source" in line) == 3
