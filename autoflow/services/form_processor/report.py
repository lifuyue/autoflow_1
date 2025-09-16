"""Reporting utilities for the form processor."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


def _serialize_issues(frame: pd.DataFrame) -> pd.DataFrame:
    df = frame.copy()
    if "issues" in df.columns:
        df["issues"] = df["issues"].map(
            lambda x: "; ".join(str(item) for item in x) if isinstance(x, Iterable) and not isinstance(x, str) else x
        )
    return df


def generate_report(
    output_dir: Path,
    processed: pd.DataFrame,
    rejected: pd.DataFrame,
    need_confirm: pd.DataFrame,
    mapping_issues: list[dict[str, object]],
) -> tuple[Path, Path | None, Path | None]:
    """Generate Markdown report and side CSV exports."""

    output_dir.mkdir(parents=True, exist_ok=True)

    reject_path: Path | None = None
    confirm_path: Path | None = None

    if not rejected.empty:
        reject_path = output_dir / "processed_forms_rejects.csv"
        _serialize_issues(rejected).to_csv(reject_path, index=False)

    if not need_confirm.empty:
        confirm_path = output_dir / "processed_forms_need_confirm.csv"
        _serialize_issues(need_confirm).to_csv(confirm_path, index=False)

    report_path = output_dir / "processed_forms_report.md"

    lines = ["# Form Processing Report", ""]
    lines.append(f"- Accepted rows: {len(processed)}")
    lines.append(f"- Rejected rows: {len(rejected)}")
    lines.append(f"- Need confirm rows: {len(need_confirm)}")
    lines.append("")

    if mapping_issues:
        lines.append("## Mapping diagnostics")
        for item in mapping_issues:
            lines.append(f"- **{item['file']}**")
            missing = item.get("missing_columns") or []
            unmatched = item.get("unmatched_columns") or []
            if missing:
                lines.append(f"  - Missing columns: {', '.join(missing)}")
            if unmatched:
                lines.append(f"  - Unmapped source columns: {', '.join(unmatched)}")
        lines.append("")

    if reject_path:
        lines.append(f"Rejected rows exported to `{reject_path.name}`.")
    if confirm_path:
        lines.append(f"Records requiring confirmation exported to `{confirm_path.name}`.")

    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path, reject_path, confirm_path
