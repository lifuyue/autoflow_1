"""Mapping strategies for Excel data transfer."""

# Module responsibilities:
# - Define abstract mapping strategies to translate source DataFrames to template schemas.
# - Provide a concrete FixedMapping implementation backed by YAML configuration.
# - Reserve extension points for header-based automatic mapping.

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import yaml

from .schema import FixedMappingConfig, MappingContext, TargetSchema

ColumnMap = Dict[str, str]


class MappingError(RuntimeError):
    """Raised when mapping configuration is invalid or cannot be applied."""


class BaseMappingStrategy(ABC):
    """Abstract base class for mapping strategies."""

    @abstractmethod
    def map(self, df: pd.DataFrame, target_schema: TargetSchema) -> ColumnMap:
        """Compute a column mapping for the given DataFrame."""


@dataclass(frozen=True)
class FixedMappingStrategy(BaseMappingStrategy):
    """Concrete mapping strategy using explicit YAML configuration."""

    config: FixedMappingConfig

    def map(self, df: pd.DataFrame, target_schema: TargetSchema) -> ColumnMap:
        """Validate the source DataFrame against the fixed mapping.

        Args:
            df: Source DataFrame loaded from the Excel workbook.
            target_schema: Target worksheet schema metadata.

        Returns:
            Column map of ``source_column -> target_column_letter``.
        """

        missing = [col for col in self.config["columns"] if col not in df.columns]
        if missing:
            raise MappingError(
                f"Source data missing required columns: {', '.join(sorted(missing))}"
            )
        return dict(self.config["columns"])

    @property
    def sheet(self) -> str:
        return self.config["sheet"]

    @property
    def start_row(self) -> int:
        return self.config["start_row"]

    @property
    def header_row(self) -> Optional[int]:
        return self.config.get("header_row")

    @property
    def max_rows_per_sheet(self) -> Optional[int]:
        return self.config.get("max_rows_per_sheet")

    @property
    def output_name(self) -> Optional[str]:
        return self.config.get("output_name")


class HeaderAutoMappingStrategy(BaseMappingStrategy):
    """Placeholder for future header-based auto mapping strategy."""

    def __init__(self, *_: object, **__: object) -> None:
        # TODO: Implement header-driven matching by similarity/alias dictionary.
        pass

    def map(self, df: pd.DataFrame, target_schema: TargetSchema) -> ColumnMap:
        raise NotImplementedError("HeaderAutoMappingStrategy is not yet implemented")


@dataclass(frozen=True)
class FixedMapping(FixedMappingStrategy):
    """Helper dataclass bundling YAML loading for fixed mapping."""

    @classmethod
    def from_yaml(cls, path: Path) -> "FixedMapping":
        """Load mapping configuration from YAML file."""

        with path.open("r", encoding="utf-8") as fh:
            payload = yaml.safe_load(fh)
        if not isinstance(payload, dict):
            raise MappingError("Invalid mapping YAML structure (expected mapping)")
        required = {"sheet", "start_row", "columns"}
        if missing := required - payload.keys():
            raise MappingError(
                f"Mapping YAML missing required keys: {', '.join(sorted(missing))}"
            )
        config: FixedMappingConfig = {
            "sheet": str(payload["sheet"]),
            "start_row": int(payload["start_row"]),
            "columns": {str(k): str(v) for k, v in payload["columns"].items()},
        }
        if "max_rows_per_sheet" in payload and payload["max_rows_per_sheet"] is not None:
            config["max_rows_per_sheet"] = int(payload["max_rows_per_sheet"])
        if "header_row" in payload and payload["header_row"] is not None:
            config["header_row"] = int(payload["header_row"])
        if "output_name" in payload and payload["output_name"]:
            config["output_name"] = str(payload["output_name"])
        return cls(config=config)

    def build_context(self, source_path: Path, template_path: Path) -> MappingContext:
        """Create a mapping context for downstream processing."""

        target = TargetSchema(
            sheet=self.sheet, start_row=self.start_row, header_row=self.header_row
        )
        return MappingContext(
            source_path=source_path, template_path=template_path, target=target
        )
