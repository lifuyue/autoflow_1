"""PDF basic read/write utilities."""

# Module responsibilities:
# - Surface minimal metadata/text extraction with optional pdfplumber support.
# - Support exporting selected pages and updating metadata with PyPDF2 when available.
# - Guard against encrypted or malformed PDFs with explicit failures.

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

try:  # pdfplumber is optional; fall back when absent.
    import pdfplumber  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - exercised via optional dependency
    pdfplumber = None

try:  # PyPDF2 is optional but required for export/metadata updates.
    from PyPDF2 import PdfReader, PdfWriter
    from PyPDF2.errors import PdfReadError

    HAS_PYPDF2 = True
except ModuleNotFoundError:  # pragma: no cover - handled by runtime checks
    PdfReader = PdfWriter = None  # type: ignore
    PdfReadError = Exception  # type: ignore
    HAS_PYPDF2 = False

from .utils.log import get_logger

logger = get_logger("pdf_io")


class PdfProcessingError(RuntimeError):
    """Raised when PDF operations fail."""


@dataclass(frozen=True)
class PdfInfo:
    """Metadata summary for a PDF file."""

    path: Path
    page_count: int
    metadata: Dict[str, str]
    encrypted: bool


def _resolve_pdf_reader(path: Path):  # type: ignore[no-untyped-def]
    if not HAS_PYPDF2:
        raise PdfProcessingError("PyPDF2 is required for this operation. Install PyPDF2>=3.0.")
    if not path.exists():
        raise FileNotFoundError(f"PDF file not found: {path}")
    try:
        reader = PdfReader(path)  # type: ignore[operator]
    except PdfReadError as exc:  # type: ignore[arg-type]
        raise PdfProcessingError(f"Failed to open PDF: {exc}") from exc
    if reader.is_encrypted:  # type: ignore[union-attr]
        raise PdfProcessingError("Encrypted PDFs are not supported")
    return reader


def read_info(path: Path) -> PdfInfo:
    """Read metadata and page count for a PDF file."""

    if HAS_PYPDF2:
        reader = _resolve_pdf_reader(path)
        metadata = {k.lstrip("/"): str(v) for k, v in (reader.metadata or {}).items()}
        page_count = len(reader.pages)
    elif pdfplumber:
        with pdfplumber.open(path) as pdf:  # type: ignore[attr-defined]
            if pdf.is_encrypted:
                raise PdfProcessingError("Encrypted PDFs are not supported")
            metadata = {k.lstrip("/"): str(v) for k, v in (pdf.metadata or {}).items()}
            page_count = len(pdf.pages)
    else:
        raise PdfProcessingError("No PDF backend available. Install PyPDF2 or pdfplumber.")

    logger.info(
        "PDF info read",
        extra={"path": str(path), "page_count": page_count},
    )
    return PdfInfo(
        path=path,
        page_count=page_count,
        metadata=metadata,
        encrypted=False,
    )


def extract_text(path: Path, max_chars_per_page: int = 1000) -> List[str]:
    """Extract text snippets from each page of the PDF."""

    if max_chars_per_page <= 0:
        raise ValueError("max_chars_per_page must be positive")

    snippets: List[str] = []
    if pdfplumber:
        try:
            with pdfplumber.open(path) as pdf:  # type: ignore[attr-defined]
                if pdf.is_encrypted:
                    raise PdfProcessingError("Encrypted PDFs are not supported")
                for idx, page in enumerate(pdf.pages, start=1):
                    text = page.extract_text() or ""
                    snippet = text[:max_chars_per_page]
                    snippets.append(snippet)
                    logger.info(
                        "Extracted text from page",
                        extra={"path": str(path), "page": idx, "chars": len(snippet)},
                    )
        except PdfReadError as exc:  # type: ignore[arg-type]
            raise PdfProcessingError(f"Failed to read PDF: {exc}") from exc
        return snippets

    if HAS_PYPDF2:
        reader = _resolve_pdf_reader(path)
        for idx, page in enumerate(reader.pages, start=1):
            text = page.extract_text() or ""
            snippet = text[:max_chars_per_page]
            snippets.append(snippet)
            logger.info(
                "Extracted text from page (PyPDF2 fallback)",
                extra={"path": str(path), "page": idx, "chars": len(snippet)},
            )
        return snippets

    raise PdfProcessingError("No PDF backend available for text extraction.")


def export_pages(path: Path, pages: Iterable[int], out_path: Path) -> Path:
    """Export selected 1-based pages to a new PDF file."""

    if not HAS_PYPDF2:
        raise PdfProcessingError("PyPDF2 is required for exporting pages.")

    page_list = sorted(set(pages))
    if not page_list:
        raise ValueError("No pages provided for export")

    reader = _resolve_pdf_reader(path)
    writer = PdfWriter()  # type: ignore[operator]

    total_pages = len(reader.pages)
    for page_number in page_list:
        if page_number < 1 or page_number > total_pages:
            raise PdfProcessingError(
                f"Page {page_number} out of range (document has {total_pages} pages)"
            )
        writer.add_page(reader.pages[page_number - 1])  # type: ignore[index]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("wb") as fh:
        writer.write(fh)

    logger.info(
        "Exported pages",
        extra={"source": str(path), "pages": page_list, "output": str(out_path)},
    )
    return out_path


def set_metadata(path: Path, meta: Dict[str, str], out_path: Optional[Path] = None) -> Path:
    """Write basic metadata into a PDF copy."""

    if not meta:
        raise ValueError("Metadata payload is empty")
    if not HAS_PYPDF2:
        raise PdfProcessingError("PyPDF2 is required for metadata updates.")

    reader = _resolve_pdf_reader(path)
    writer = PdfWriter()  # type: ignore[operator]
    for page in reader.pages:
        writer.add_page(page)

    cleaned_meta = {f"/{k}": str(v) for k, v in meta.items()}
    existing = reader.metadata or {}
    merged_meta = {key: str(value) for key, value in existing.items()}
    merged_meta.update(cleaned_meta)
    writer.add_metadata(merged_meta)

    destination = out_path or path.with_name(f"{path.stem}_meta{path.suffix}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as fh:
        writer.write(fh)

    logger.info(
        "Updated PDF metadata",
        extra={"source": str(path), "output": str(destination), "keys": list(meta.keys())},
    )
    return destination
