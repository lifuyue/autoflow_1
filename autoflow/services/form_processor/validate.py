"""Validation layer for processed forms."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Callable, List

import pandas as pd

from .mapping import ValidationRules


@dataclass(slots=True)
class ValidationOutcome:
    """Container for validation results."""

    accepted: pd.DataFrame
    rejected: pd.DataFrame
    need_confirm: pd.DataFrame


ConfirmCallback = Callable[[pd.Series], bool]


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False


def apply_validations(
    frame: pd.DataFrame,
    rules: ValidationRules,
    round_digits: int,
    confirm_threshold: Decimal,
    confirm_callback: ConfirmCallback | None,
) -> ValidationOutcome:
    """Validate rows and split accepted / rejected sets."""

    df = frame.copy()
    statuses: List[str] = []
    confirm_flags: List[bool] = []

    # Ensure issues column exists for appending messages.
    if "issues" not in df.columns:
        df["issues"] = [[] for _ in range(len(df))]

    for idx, row in df.iterrows():
        row_issues = row.get("issues", [])
        status = "ok"

        # Required fields validation.
        for col in rules.required:
            if _is_missing(row.get(col)):
                row_issues.append(f"missing_{col}")
                status = "reject"

        # Non-negative validation for Decimal columns.
        for col in rules.non_negative:
            value = row.get(col)
            if isinstance(value, Decimal) and value < 0:
                row_issues.append(f"negative_{col}")
                status = "reject"

        # Rounding enforcement.
        for col, digits in rules.round.items():
            value = row.get(col)
            if isinstance(value, Decimal):
                quant_local = Decimal("1").scaleb(-digits)
                df.at[idx, col] = value.quantize(quant_local, rounding=ROUND_HALF_UP)

        df.at[idx, "issues"] = row_issues

        base_amount = row.get("base_amount")
        if "rate_unavailable" in row_issues or (
            isinstance(row.get("amount"), Decimal) and base_amount is None
        ):
            if "rate_unavailable" not in row_issues:
                row_issues.append("missing_base_amount")
            status = "reject"
        need_confirm = False
        if isinstance(base_amount, Decimal) and base_amount >= confirm_threshold:
            if confirm_callback is None:
                need_confirm = True
                row_issues.append("requires_confirmation")
            else:
                confirmed = confirm_callback(row)
                if not confirmed:
                    need_confirm = True
                    row_issues.append("confirmation_declined")
            df.at[idx, "base_amount"] = Decimal(base_amount).quantize(
                Decimal("1").scaleb(-round_digits),
                rounding=ROUND_HALF_UP,
            )
        confirm_flags.append(need_confirm)
        statuses.append(status)

    df["status"] = statuses
    df["need_confirm"] = confirm_flags

    accepted = df[df["status"] == "ok"].copy()
    rejected = df[df["status"] != "ok"].copy()
    need_confirm_df = accepted[accepted["need_confirm"]].copy()

    return ValidationOutcome(accepted=accepted, rejected=rejected, need_confirm=need_confirm_df)
