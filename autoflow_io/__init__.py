"""`autoflow_io` top-level package exports the IO helpers for Excel and PDF flows."""

# Module responsibilities:
# - Re-export high-level interfaces for Excel/PDF I/O and mapping utilities so consumers have a stable API surface.
# - Provide package version placeholder for future packaging.

from __future__ import annotations

from .excel_reader import read_table
from .excel_writer import write_fixed
from .mapping import FixedMapping, FixedMappingConfig, FixedMappingStrategy
from .pdf_io import (
    PdfInfo,
    export_pages,
    extract_text,
    read_info,
    set_metadata,
)

__all__ = [
    "read_table",
    "write_fixed",
    "FixedMapping",
    "FixedMappingConfig",
    "FixedMappingStrategy",
    "PdfInfo",
    "read_info",
    "extract_text",
    "export_pages",
    "set_metadata",
]

__version__ = "0.1.0"
