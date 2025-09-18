"""CLI demo for PDF basic operations."""

# Module responsibilities:
# - Read metadata/text from PDFs and showcase export/metadata update utilities.
# - Autogenerate a lightweight sample PDF when the expected input is absent.

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, Iterable, List

from autoflow_io.pdf_io import PdfInfo, export_pages, extract_text, read_info, set_metadata
from autoflow_io.utils.log import get_logger
from autoflow_io.utils.paths import ensure_default_structure

logger = get_logger("tools.demo_pdf")


def _encode_pdf_text(text: str) -> bytes:
    escaped = (
        text.replace("\\", "\\\\")
        .replace("(", r"\(")
        .replace(")", r"\)")
    )
    content = f"BT\n/F1 18 Tf\n72 720 Td\n({escaped}) Tj\nET\n"
    return content.encode("utf-8")


def _generate_sample_pdf(path: Path) -> None:
    header = b"%PDF-1.4\n"
    text_stream = _encode_pdf_text("AutoFlow Sample PDF -- page 1")
    obj1 = b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n"
    obj2 = b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n"
    obj3 = (
        b"3 0 obj\n"
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792]"
        b" /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>\n"
        b"endobj\n"
    )
    obj4 = (
        f"4 0 obj\n<< /Length {len(text_stream)} >>\nstream\n".encode("utf-8")
        + text_stream
        + b"endstream\nendobj\n"
    )
    obj5 = (
        b"5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n"
    )

    objects = [obj1, obj2, obj3, obj4, obj5]
    offsets = []
    buffer = bytearray()
    buffer.extend(header)
    for obj in objects:
        offsets.append(len(buffer))
        buffer.extend(obj)
    xref_offset = len(buffer)
    buffer.extend(f"xref\n0 6\n".encode("utf-8"))
    buffer.extend(b"0000000000 65535 f \n")
    for offset in offsets:
        buffer.extend(f"{offset:010d} 00000 n \n".encode("utf-8"))
    buffer.extend(b"trailer\n<< /Size 6 /Root 1 0 R >>\n")
    buffer.extend(f"startxref\n{xref_offset}\n%%EOF\n".encode("utf-8"))

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(buffer)
    logger.info("Generated sample PDF", extra={"path": str(path)})


def ensure_sample(path: Path) -> None:
    if not path.exists():
        _generate_sample_pdf(path)


def _parse_meta(meta_string: str | None) -> Dict[str, str]:
    if not meta_string:
        return {}
    entries = [chunk.strip() for chunk in meta_string.split(";") if chunk.strip()]
    meta: Dict[str, str] = {}
    for entry in entries:
        if "=" not in entry:
            raise ValueError(f"Invalid metadata expression: {entry}")
        key, value = entry.split("=", 1)
        meta[key.strip()] = value.strip()
    return meta


def _parse_pages(raw: Iterable[str]) -> List[int]:
    pages: List[int] = []
    for token in raw:
        for part in token.split(","):
            part = part.strip()
            if not part:
                continue
            pages.append(int(part))
    return pages


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PDF processing demo")
    parser.add_argument("--pdf", type=Path, default=Path("examples/sample.pdf"))
    parser.add_argument("--out", type=Path, default=Path("out"))
    parser.add_argument("--export-pages", nargs="*", default=[], help="Pages to export (1-based)")
    parser.add_argument("--set-meta", default=None, help="Metadata assignments, e.g. 'Title=a;Author=b'")
    parser.add_argument(
        "--max-chars", type=int, default=100, help="Max characters per page when printing text"
    )
    parser.add_argument("--workspace", type=Path, default=None)
    return parser.parse_args(argv)


def _export_selected(pdf_path: Path, out_dir: Path, pages: List[int]) -> List[Path]:
    if not pages:
        return []
    pages_sorted = sorted(set(pages))
    if len(pages_sorted) == 1:
        suffix = f"_p{pages_sorted[0]}"
    else:
        suffix = f"_p{pages_sorted[0]}-{pages_sorted[-1]}"
    filename = f"{pdf_path.stem}{suffix}.pdf"
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / filename
    export_pages(pdf_path, pages_sorted, target)
    return [target]


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    ensure_default_structure(args.workspace)
    pdf_path = args.pdf
    ensure_sample(pdf_path)

    try:
        info: PdfInfo = read_info(pdf_path)
        print(f"PDF: {pdf_path}")
        print(f"Pages: {info.page_count}")
        print(f"Metadata: {info.metadata}")

        snippets = extract_text(pdf_path, max_chars_per_page=args.max_chars)
        for idx, snippet in enumerate(snippets, start=1):
            preview = snippet[: args.max_chars]
            print(f"Page {idx} snippet: {preview}")

        exports = _export_selected(pdf_path, args.out, _parse_pages(args.export_pages))

        meta_payload = _parse_meta(args.set_meta)
        meta_output = None
        if meta_payload:
            meta_output = set_metadata(pdf_path, meta_payload, out_path=args.out / f"{pdf_path.stem}_meta.pdf")

        if exports:
            print("Exported files:")
            for path in exports:
                print(f"  - {path}")
        if meta_output:
            print(f"Metadata written to: {meta_output}")
        return 0
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("PDF demo failed", extra={"error": str(exc)})
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
