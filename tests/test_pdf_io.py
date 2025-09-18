"""Unit tests for PDF utilities."""

# Module responsibilities:
# - Ensure happy path metadata/text extraction works on generated PDFs.
# - Validate defensive behaviour for missing files.

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("PyPDF2")

from autoflow_io.pdf_io import export_pages, extract_text, read_info, set_metadata


def _create_sample_pdf(path: Path) -> None:
    header = b"%PDF-1.4\n"
    text = "AutoFlow PDF Test"
    escaped = (
        text.replace("\\", "\\\\")
        .replace("(", r"\(")
        .replace(")", r"\)")
    )
    stream = f"BT\n/F1 14 Tf\n72 720 Td\n({escaped}) Tj\nET\n".encode("utf-8")
    obj1 = b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n"
    obj2 = b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n"
    obj3 = (
        b"3 0 obj\n"
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792]"
        b" /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>\n"
        b"endobj\n"
    )
    obj4 = (
        f"4 0 obj\n<< /Length {len(stream)} >>\nstream\n".encode("utf-8")
        + stream
        + b"endstream\nendobj\n"
    )
    obj5 = b"5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n"

    objects = [obj1, obj2, obj3, obj4, obj5]
    offsets = []
    data = bytearray()
    data.extend(header)
    for obj in objects:
        offsets.append(len(data))
        data.extend(obj)
    xref_offset = len(data)
    data.extend(b"xref\n0 6\n")
    data.extend(b"0000000000 65535 f \n")
    for offset in offsets:
        data.extend(f"{offset:010d} 00000 n \n".encode("utf-8"))
    data.extend(b"trailer\n<< /Size 6 /Root 1 0 R >>\n")
    data.extend(f"startxref\n{xref_offset}\n%%EOF\n".encode("utf-8"))
    path.write_bytes(data)


def test_pdf_read_and_export(tmp_path: Path) -> None:
    pdf_path = tmp_path / "sample.pdf"
    _create_sample_pdf(pdf_path)

    info = read_info(pdf_path)
    assert info.page_count == 1
    assert not info.encrypted

    snippets = extract_text(pdf_path, max_chars_per_page=200)
    assert len(snippets) == 1
    assert "AutoFlow" in snippets[0]

    out_dir = tmp_path / "out"
    exported = export_pages(pdf_path, [1], out_dir / "sample_p1.pdf")
    assert exported.exists()

    meta_path = set_metadata(pdf_path, {"Title": "Demo"}, out_path=tmp_path / "sample_meta.pdf")
    updated_info = read_info(meta_path)
    assert updated_info.metadata.get("Title") == "Demo"


def test_pdf_missing_file_raises(tmp_path: Path) -> None:
    missing = tmp_path / "missing.pdf"
    with pytest.raises(FileNotFoundError):
        read_info(missing)
