"""Shared schemas for Excel and PDF data structures."""

# Module responsibilities:
# - Provide strongly typed configuration containers for mapping behaviours.
# - Define lightweight data schemas that make validation explicit.

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, NotRequired, Optional, TypedDict


class FixedMappingConfig(TypedDict):
    """Schema for fixed cell mapping YAML payloads."""

    sheet: str
    start_row: int
    columns: Dict[str, str]
    max_rows_per_sheet: NotRequired[int]
    header_row: NotRequired[int]
    output_name: NotRequired[str]


@dataclass(frozen=True)
class TargetSchema:
    """Target worksheet layout metadata used by mapping strategies."""

    sheet: str
    start_row: int
    header_row: Optional[int] = None


@dataclass(frozen=True)
class MappingContext:
    """Context object passed to mapping strategies when calculating placements."""

    source_path: Path
    template_path: Path
    target: TargetSchema
