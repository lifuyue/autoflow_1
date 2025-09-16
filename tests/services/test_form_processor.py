from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Iterator

import pandas as pd
import pytest

from autoflow.services.form_processor import FormProcessConfig, process_forms
from autoflow.services.form_processor.providers import MockRateProvider

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "fixtures" / "form_processor"


@pytest.fixture()
def sample_inputs() -> list[str]:
    return [str(FIXTURE_DIR / "sample_input.xlsx"), str(FIXTURE_DIR / "sample_input.csv")]


@pytest.fixture()
def rate_provider() -> MockRateProvider:
    rates = {
        ("USD", "CNY"): {"2024-05-10": Decimal("7.20"), "2024-05-11": Decimal("7.25")},
        ("EUR", "CNY"): {"2024-05-10": Decimal("8.00")},
    }
    return MockRateProvider(rates=rates, fallback_window_days=5)


def _config(mapping_path: Path) -> FormProcessConfig:
    return FormProcessConfig(
        mapping_path=str(mapping_path),
        base_currency="CNY",
        round_digits=2,
        confirm_over_amount_cny=Decimal("20000"),
    )


def test_process_forms_mixed_sources(tmp_path, sample_inputs, rate_provider):
    cfg = _config(Path("autoflow/config/mapping.yaml"))
    result = process_forms(
        input_paths=sample_inputs,
        output_dir=str(tmp_path),
        config=cfg,
        rate_provider=rate_provider,
        non_interactive=True,
    )

    assert result.ok_rows == 4
    assert result.rejected_rows == 4
    assert result.need_confirm_rows == 3
    assert Path(result.output_template_path).exists()
    assert Path(result.report_path).exists()
    assert result.rejects_csv_path is not None and Path(result.rejects_csv_path).exists()
    assert result.confirm_csv_path is not None and Path(result.confirm_csv_path).exists()

    processed_df = result.processed_frame.dataframe
    assert set(processed_df["need_confirm"]) == {False, True}

    eur_row = processed_df.loc[processed_df["currency"] == "EUR"].iloc[0]
    assert Decimal(str(eur_row["base_amount"])) == Decimal("24000.00")
    assert eur_row["rate_date_used"] == "2024-05-10"
    assert "rate_fallback" in eur_row["issues"]

    report_text = Path(result.report_path).read_text(encoding="utf-8")
    assert "Need confirm rows" in report_text

    rejects_df = pd.read_csv(result.rejects_csv_path)
    issues_joined = " ".join(rejects_df.get("issues", pd.Series(dtype=str)).astype(str).tolist())
    for token in ["negative_amount", "missing_project", "missing_date", "rate_unavailable"]:
        assert token in issues_joined


def test_process_forms_interactive_confirmation(tmp_path, sample_inputs, rate_provider, monkeypatch):
    responses: Iterator[str] = iter(["y", "n", "y"])
    monkeypatch.setattr("builtins.input", lambda prompt: next(responses))

    cfg = _config(Path("autoflow/config/mapping.yaml"))
    result = process_forms(
        input_paths=sample_inputs,
        output_dir=str(tmp_path),
        config=cfg,
        rate_provider=rate_provider,
        non_interactive=False,
    )

    assert result.need_confirm_rows == 1
    assert result.confirm_csv_path is not None and Path(result.confirm_csv_path).exists()
    confirm_df = pd.read_csv(result.confirm_csv_path)
    assert len(confirm_df) == 1
    assert "confirmation_declined" in " ".join(confirm_df.get("issues", pd.Series(dtype=str)).astype(str).tolist())
