"""Column mapping utilities for the form processor."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
from ruamel.yaml import YAML


class ValidationRules(BaseModel):
    """Validation directives parsed from the mapping file."""

    model_config = ConfigDict(extra="allow")

    required: List[str] = Field(default_factory=list)
    non_negative: List[str] = Field(default_factory=list)
    round: Dict[str, int] = Field(default_factory=dict)


class ThresholdRules(BaseModel):
    """Threshold directives parsed from mapping.yaml."""

    model_config = ConfigDict(extra="allow")

    confirm_over_amount_cny: float | None = None


class MappingConfig(BaseModel):
    """Complete mapping file model."""

    model_config = ConfigDict(extra="allow")

    input_columns: Dict[str, List[str]] = Field(default_factory=dict)
    computed: Dict[str, str] = Field(default_factory=dict)
    validations: ValidationRules = Field(default_factory=ValidationRules)
    thresholds: ThresholdRules = Field(default_factory=ThresholdRules)


@dataclass(slots=True)
class ColumnMappingResult:
    """Outcome of mapping raw columns to canonical ones."""

    dataframe: pd.DataFrame
    matched_columns: Dict[str, str]
    missing_columns: List[str]
    unmatched_columns: List[str]


def load_mapping_config(mapping_path: str | Path) -> MappingConfig:
    """Load mapping configuration from YAML."""

    yaml = YAML(typ="safe")
    with Path(mapping_path).open("r", encoding="utf-8") as fh:
        data = yaml.load(fh) or {}
    return MappingConfig.model_validate(data)


def _normalize(label: str) -> str:
    return label.strip().lower().replace(" ", "")


def apply_column_mapping(frame: pd.DataFrame, config: MappingConfig) -> ColumnMappingResult:
    """Rename columns based on ``input_columns`` mapping rules."""

    original_columns = list(frame.columns)
    normalized_map = {_normalize(col): col for col in original_columns}

    matched: Dict[str, str] = {}
    missing: List[str] = []

    for canonical, candidates in config.input_columns.items():
        found = None
        for candidate in candidates:
            normalized = _normalize(candidate)
            if normalized in normalized_map:
                found = normalized_map[normalized]
                break
        if found is not None:
            matched[canonical] = found
        else:
            missing.append(canonical)

    rename_map = {original: canonical for canonical, original in matched.items()}
    renamed = frame.rename(columns=rename_map)
    # Ensure canonical names exist even when missing by adding NA columns.
    for canonical in config.input_columns.keys():
        if canonical not in renamed.columns:
            renamed[canonical] = pd.NA
    # Keep canonical columns first for readability.
    canonical_order = list(config.input_columns.keys())
    ordered_columns = canonical_order + [c for c in renamed.columns if c not in canonical_order]
    ordered = renamed[ordered_columns]

    used_originals = set(matched.values())
    unmatched = [col for col in original_columns if col not in used_originals]

    return ColumnMappingResult(
        dataframe=ordered,
        matched_columns=matched,
        missing_columns=missing,
        unmatched_columns=unmatched,
    )
