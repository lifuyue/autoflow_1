"""Public API for the form processor service."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Callable, Iterable, Protocol
import logging

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from .cleaning import clean_dataframe
from .compute import compute_base_amounts
from .exporter import export_template
from .mapping import MappingConfig, apply_column_mapping, load_mapping_config
from .models import ProcessedFrame
from .report import generate_report
from .validate import ValidationOutcome, apply_validations

LOGGER = logging.getLogger(__name__)


class RateProvider(Protocol):
    """Protocol for currency rate providers."""

    def get_rate(self, date: str, from_ccy: str, to_ccy: str) -> Decimal:  # pragma: no cover - interface definition
        ...


class FormProcessConfig(BaseModel):
    """Configuration needed to run form processing."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    mapping_path: str
    base_currency: str = "CNY"
    round_digits: int = 2
    confirm_over_amount_cny: Decimal = Field(default_factory=lambda: Decimal("20000"))


class ProcessResult(BaseModel):
    """Aggregated outcome returned to callers."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    ok_rows: int
    rejected_rows: int
    need_confirm_rows: int
    output_template_path: str
    report_path: str
    rejects_csv_path: str | None = None
    confirm_csv_path: str | None = None
    processed_frame: ProcessedFrame


@dataclass(slots=True)
class _MappingDiagnostics:
    file: str
    missing_columns: list[str]
    unmatched_columns: list[str]


def _read_input_file(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    if path.suffix.lower() in {".csv"}:
        return pd.read_csv(path)
    raise ValueError(f"unsupported input file: {path}")


def _build_confirmation_callback(non_interactive: bool) -> Callable[[pd.Series], bool] | None:
    if non_interactive:
        return None

    def _callback(row: pd.Series) -> bool:
        amount = row.get("base_amount")
        project = row.get("project") or ""
        prompt = (
            f"Record project={project} base_amount={amount} exceeds confirmation threshold. "
            "Proceed? [y/N]: "
        )
        while True:
            choice = input(prompt).strip().lower()  # noqa: PLW1514 - interactive by design
            if choice in {"y", "yes"}:
                return True
            if choice in {"", "n", "no"}:
                return False
            print("Please respond with 'y' or 'n'.")

    return _callback


def _apply_computed_columns(frame: pd.DataFrame, mapping_conf: MappingConfig) -> pd.DataFrame:
    df = frame.copy()
    for key, value in mapping_conf.computed.items():
        df[key] = value
    return df


def process_forms(
    input_paths: Iterable[str],
    output_dir: str,
    config: FormProcessConfig,
    rate_provider: RateProvider,
    non_interactive: bool = False,
) -> ProcessResult:
    """Process raw fee forms into the invoice template and report."""

    paths = [Path(p) for p in input_paths]
    if not paths:
        raise ValueError("no input files provided")

    mapping_conf = load_mapping_config(config.mapping_path)
    threshold = config.confirm_over_amount_cny
    if mapping_conf.thresholds.confirm_over_amount_cny is not None:
        threshold = Decimal(str(mapping_conf.thresholds.confirm_over_amount_cny))

    mapping_meta: list[_MappingDiagnostics] = []
    mapped_frames: list[pd.DataFrame] = []

    for path in paths:
        LOGGER.info("Reading input file: %s", path)
        raw_df = _read_input_file(path)
        mapping_result = apply_column_mapping(raw_df, mapping_conf)
        df = mapping_result.dataframe.copy()
        df["source_file"] = path.name
        df["source_row"] = list(range(2, len(df) + 2))
        df = _apply_computed_columns(df, mapping_conf)
        mapped_frames.append(df)
        mapping_meta.append(
            _MappingDiagnostics(
                file=path.name,
                missing_columns=mapping_result.missing_columns,
                unmatched_columns=mapping_result.unmatched_columns,
            )
        )
        if mapping_result.missing_columns:
            LOGGER.warning("Missing columns for %s: %s", path.name, mapping_result.missing_columns)

    combined = pd.concat(mapped_frames, ignore_index=True) if mapped_frames else pd.DataFrame()
    if combined.empty:
        raise ValueError("no data rows found in inputs")

    cleaned = clean_dataframe(combined)
    computed = compute_base_amounts(
        cleaned,
        base_currency=config.base_currency,
        round_digits=config.round_digits,
        rate_provider=rate_provider,
    )

    confirm_callback = _build_confirmation_callback(non_interactive)

    outcome: ValidationOutcome = apply_validations(
        computed,
        rules=mapping_conf.validations,
        round_digits=config.round_digits,
        confirm_threshold=threshold,
        confirm_callback=confirm_callback,
    )

    output_path = export_template(outcome.accepted, Path(output_dir))
    report_path, rejects_path, confirm_path = generate_report(
        Path(output_dir),
        processed=outcome.accepted,
        rejected=outcome.rejected,
        need_confirm=outcome.need_confirm,
        mapping_issues=[asdict(m) for m in mapping_meta],
    )

    LOGGER.info(
        "Processed %s rows (%s ok / %s rejected / %s need confirm)",
        len(combined),
        len(outcome.accepted),
        len(outcome.rejected),
        len(outcome.need_confirm),
    )

    return ProcessResult(
        ok_rows=len(outcome.accepted),
        rejected_rows=len(outcome.rejected),
        need_confirm_rows=len(outcome.need_confirm),
        output_template_path=str(output_path),
        report_path=str(report_path),
        rejects_csv_path=str(rejects_path) if rejects_path else None,
        confirm_csv_path=str(confirm_path) if confirm_path else None,
        processed_frame=ProcessedFrame(dataframe=outcome.accepted),
    )
