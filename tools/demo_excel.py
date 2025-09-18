"""CLI demo for Excel fixed mapping write."""

# Module responsibilities:
# - Provide a CLI that loads a source workbook, applies fixed mapping, and writes into a template.
# - Generate placeholder sample files when requested paths do not exist.

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from openpyxl import Workbook

from autoflow_io.excel_reader import read_table
from autoflow_io.excel_writer import write_fixed
from autoflow_io.mapping import FixedMapping
from autoflow_io.utils.log import get_logger
from autoflow_io.utils.paths import ensure_default_structure, prepare_output_path

logger = get_logger("tools.demo_excel")


def _generate_source_example(path: Path) -> None:
    data = pd.DataFrame(
        [
            {"项目名称": "服务费", "数量": 1, "金额(USD)": 1200},
            {"项目名称": "备件", "数量": 4, "金额(USD)": 800},
            {"项目名称": "咨询", "数量": 2, "金额(USD)": 500},
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    data.to_excel(path, index=False)
    logger.info("Generated example source workbook", extra={"path": str(path)})


def _generate_template_example(path: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Invoice"
    for _ in range(8):
        ws.append([None, None, None])
    headers = ["项目名称", "数量", "金额(USD)"]
    for idx, header in enumerate(headers, start=1):
        ws.cell(row=9, column=idx, value=header)
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)
    logger.info("Generated example template workbook", extra={"path": str(path)})


def ensure_examples(source: Path, template: Path) -> None:
    if not source.exists():
        _generate_source_example(source)
    if not template.exists():
        _generate_template_example(template)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Excel fixed mapping demo")
    parser.add_argument("--source", type=Path, default=Path("examples/source_a.xlsx"))
    parser.add_argument("--template", type=Path, default=Path("examples/target_tpl.xlsx"))
    parser.add_argument("--mapping", type=Path, default=Path("examples/fixed_mapping.yaml"))
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument("--workspace", type=Path, default=None, help="Override AutoFlow base directory")
    parser.add_argument("--dry-run", action="store_true", help="Simulate write without emitting files")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    ensure_default_structure(args.workspace)

    try:
        ensure_examples(args.source, args.template)
        mapping = FixedMapping.from_yaml(args.mapping)
        df = read_table(args.source)

        if args.out:
            out_path = args.out
        else:
            filename = mapping.output_name or f"{args.source.stem}_to_{mapping.sheet}.xlsx"
            out_path = prepare_output_path(filename, args.workspace)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        outputs = write_fixed(
            df=df,
            template_path=args.template,
            mapping=mapping,
            out_path=out_path,
            dry_run=args.dry_run,
        )
        logger.info(
            "Excel processing complete",
            extra={"outputs": [str(p) for p in outputs], "row_count": len(df)},
        )
        print(f"Rows processed: {len(df)}")
        print("Outputs:")
        for path in outputs:
            print(f"  - {path}")
        return 0
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Excel demo failed", extra={"error": str(exc)})
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
