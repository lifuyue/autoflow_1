"""Configuration helpers for AutoFlow runtime files.

Provides loader utilities for Playwright selector YAMLs with schema
validation and optional gray release overrides so hotfixes can be applied
without redeploying the application.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping

import yaml

from autoflow.core.errors import ConfigError


CONFIG_DIR = Path(__file__).resolve().parent
DEFAULT_DRIVE_SELECTOR_PATH = CONFIG_DIR / "selectors" / "drive_upload.yaml"


class SelectorValidationError(ConfigError):
    """Raised when a selector configuration fails validation."""


@dataclass(frozen=True)
class SelectorSet:
    """Normalized selector definitions with primary/fallback entries."""

    primary: Mapping[str, Any]
    fallbacks: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True)
class SpaceDefinition:
    """Navigation anchors for a top-level drive space."""

    key: str
    labels: tuple[str, ...]
    anchors: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True)
class RowSelectorSet:
    """Selector templates for locating folder rows by name."""

    primary: Mapping[str, Any]
    fallbacks: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True)
class DriveUploadSelectors:
    """Structured selectors for DingTalk drive uploads."""

    spaces: Mapping[str, SpaceDefinition]
    frames: Mapping[str, SelectorSet]
    actions: Mapping[str, SelectorSet]
    inputs: Mapping[str, SelectorSet]
    listing: Mapping[str, RowSelectorSet]
    conflicts: Mapping[str, SelectorSet]
    exports: Mapping[str, SelectorSet]
    metadata: Mapping[str, SelectorSet]
    toasts: Mapping[str, tuple[str, ...]]
    raw: Mapping[str, Any]

    def action(self, name: str) -> SelectorSet:
        """Return action selectors by key."""

        return self.actions[name]


def load_drive_upload_selectors(
    path: str | Path | None = None,
    *,
    enable_gray_release: bool | None = None,
) -> DriveUploadSelectors:
    """Load drive upload selectors with optional gray release overrides."""

    selectors_path = Path(path) if path else DEFAULT_DRIVE_SELECTOR_PATH
    raw = _load_yaml(selectors_path)
    merged = _resolve_variant(raw, enable_gray_release=bool(enable_gray_release))
    return _build_drive_selectors(merged, raw)


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"选择器文件未找到: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ConfigError("选择器配置必须是字典结构")
    return data


def _resolve_variant(data: Mapping[str, Any], *, enable_gray_release: bool) -> Dict[str, Any]:
    variants = data.get("variants")
    if variants is None:
        return deepcopy(data)
    if not isinstance(variants, Mapping):
        raise SelectorValidationError("variants 节点必须为映射类型")
    base_variant = variants.get("base")
    if not isinstance(base_variant, Mapping):
        raise SelectorValidationError("variants.base 缺失或格式错误")
    merged = deepcopy(base_variant)
    if enable_gray_release:
        override = variants.get("gray_release")
        if override is None:
            return merged
        if not isinstance(override, Mapping):
            raise SelectorValidationError("variants.gray_release 必须是映射类型")
        merged = _deep_merge(merged, override)
    return merged


def _deep_merge(base: Dict[str, Any], override: Mapping[str, Any]) -> Dict[str, Any]:
    for key, value in override.items():
        if key not in base:
            base[key] = deepcopy(value)
            continue
        base_value = base[key]
        if isinstance(base_value, dict) and isinstance(value, Mapping):
            base[key] = _deep_merge(dict(base_value), value)
        else:
            base[key] = deepcopy(value)
    return base


def _build_drive_selectors(data: Mapping[str, Any], raw: Mapping[str, Any]) -> DriveUploadSelectors:
    spaces = _normalize_spaces(data.get("spaces"))
    frames = _normalize_selector_sets(data.get("frames"), category="frame", require_role=False)
    actions = _normalize_selector_sets(data.get("actions"), category="action", require_role=True)
    inputs = _normalize_selector_sets(data.get("inputs"), category="input", require_role=False)
    listing = _normalize_listing(data.get("listing"))
    conflicts = _normalize_selector_sets(data.get("conflicts"), category="conflict", require_role=True)
    exports = _normalize_selector_sets(data.get("exports"), category="export", require_role=True)
    metadata = _normalize_selector_sets(data.get("metadata"), category="metadata", require_role=False, required=False)
    toasts = _normalize_toasts(data.get("toasts"))
    return DriveUploadSelectors(
        spaces=spaces,
        frames=frames,
        actions=actions,
        inputs=inputs,
        listing=listing,
        conflicts=conflicts,
        exports=exports,
        metadata=metadata,
        toasts=toasts,
        raw=raw,
    )


def _normalize_spaces(spaces_node: Any) -> Mapping[str, SpaceDefinition]:
    if not isinstance(spaces_node, Mapping):
        raise SelectorValidationError("spaces 节点缺失或格式错误")
    normalized: Dict[str, SpaceDefinition] = {}
    for key, value in spaces_node.items():
        if not isinstance(value, Mapping):
            raise SelectorValidationError(f"space {key} 必须是映射")
        labels = value.get("labels")
        if not isinstance(labels, list) or not labels:
            raise SelectorValidationError(f"space {key} 缺少 labels")
        anchors = value.get("anchors")
        if not isinstance(anchors, list) or not anchors:
            raise SelectorValidationError(f"space {key} 未定义 anchors")
        normalized_anchors = []
        for idx, anchor in enumerate(anchors):
            if not isinstance(anchor, Mapping):
                raise SelectorValidationError(f"space {key} anchors[{idx}] 必须是映射")
            if not _anchor_has_signal(anchor):
                raise SelectorValidationError(f"space {key} anchors[{idx}] 缺少定位信息")
            normalized_anchors.append(dict(anchor))
        label_values = tuple(str(label).strip() for label in labels if str(label).strip())
        if not label_values:
            raise SelectorValidationError(f"space {key} labels 不能为空")
        normalized[key] = SpaceDefinition(
            key=key,
            labels=label_values,
            anchors=tuple(normalized_anchors),
        )
    return normalized


def _anchor_has_signal(anchor: Mapping[str, Any]) -> bool:
    allowed_keys = {
        "role",
        "name",
        "name_regex",
        "url_contains",
        "text",
        "text_regex",
        "test_id",
        "selector",
    }
    return any(anchor.get(key) for key in allowed_keys)


def _normalize_selector_sets(
    node: Any,
    *,
    category: str,
    require_role: bool,
    required: bool = True,
) -> Mapping[str, SelectorSet]:
    if node is None:
        if required:
            raise SelectorValidationError(f"{category}s 节点缺失或格式错误")
        return {}
    if not isinstance(node, Mapping) or not node:
        if required:
            raise SelectorValidationError(f"{category}s 节点缺失或格式错误")
        return {}
    result: Dict[str, SelectorSet] = {}
    for key, spec in node.items():
        result[key] = _build_selector_set(key, spec, category=category, require_role=require_role)
    return result


def _build_selector_set(
    key: str,
    spec: Any,
    *,
    category: str,
    require_role: bool,
) -> SelectorSet:
    if not isinstance(spec, Mapping):
        raise SelectorValidationError(f"{category} {key} 必须是映射")
    primary = spec.get("primary")
    if not isinstance(primary, Mapping) or not primary:
        raise SelectorValidationError(f"{category} {key} 缺少 primary 选择器")
    if require_role:
        _assert_role_and_name(key, primary, category)
    else:
        _assert_generic_fields(key, primary, category)
    fallbacks_node = spec.get("fallbacks", [])
    if fallbacks_node in (None, ""):
        fallbacks_node = []
    if not isinstance(fallbacks_node, list):
        raise SelectorValidationError(f"{category} {key} fallbacks 必须是列表")
    fallbacks = []
    for idx, fallback in enumerate(fallbacks_node):
        if not isinstance(fallback, Mapping) or not fallback:
            raise SelectorValidationError(f"{category} {key} fallback[{idx}] 必须是映射并且非空")
        if require_role:
            _assert_role_and_name(key, fallback, category)
        else:
            _assert_generic_fields(key, fallback, category)
        fallbacks.append(dict(fallback))
    return SelectorSet(primary=dict(primary), fallbacks=tuple(fallbacks))


def _assert_role_and_name(key: str, spec: Mapping[str, Any], category: str) -> None:
    role = spec.get("role")
    if not isinstance(role, str) or not role:
        raise SelectorValidationError(f"{category} {key} 的 role 必须为字符串")
    has_name = isinstance(spec.get("name"), str) and spec.get("name")
    has_regex = isinstance(spec.get("name_regex"), str) and spec.get("name_regex")
    if not (has_name or has_regex):
        raise SelectorValidationError(f"{category} {key} 需要提供 name 或 name_regex")


def _assert_generic_fields(key: str, spec: Mapping[str, Any], category: str) -> None:
    allowed_by_category = {
        "frame": {
            "selector",
            "url_contains",
            "title",
            "title_regex",
            "name",
            "name_regex",
        },
        "input": {
            "css",
            "selector",
            "role",
            "name",
            "name_regex",
            "placeholder",
            "placeholder_regex",
            "label",
            "label_regex",
            "test_id",
            "use_file_chooser",
        },
    }
    allowed = allowed_by_category.get(category, set())
    if not allowed:
        return
    if not any(spec.get(field) for field in allowed):
        raise SelectorValidationError(f"{category} {key} 缺少定位字段: {', '.join(sorted(allowed))}")


def _normalize_listing(node: Any) -> Mapping[str, RowSelectorSet]:
    if not isinstance(node, Mapping):
        raise SelectorValidationError("listing 节点缺失或格式错误")
    result: Dict[str, RowSelectorSet] = {}
    for key, spec in node.items():
        result[key] = _build_row_selector_set(key, spec)
    return result


def _build_row_selector_set(key: str, spec: Any) -> RowSelectorSet:
    if not isinstance(spec, Mapping):
        raise SelectorValidationError(f"listing {key} 必须是映射")
    primary = spec.get("primary")
    if not isinstance(primary, Mapping) or not primary:
        raise SelectorValidationError(f"listing {key} 缺少 primary")
    _assert_row_template(key, primary)
    fallbacks_node = spec.get("fallbacks", [])
    if fallbacks_node in (None, ""):
        fallbacks_node = []
    if not isinstance(fallbacks_node, list):
        raise SelectorValidationError(f"listing {key} fallbacks 必须是列表")
    fallbacks = []
    for idx, fallback in enumerate(fallbacks_node):
        if not isinstance(fallback, Mapping) or not fallback:
            raise SelectorValidationError(f"listing {key} fallback[{idx}] 必须是映射并且非空")
        _assert_row_template(key, fallback)
        fallbacks.append(dict(fallback))
    return RowSelectorSet(primary=dict(primary), fallbacks=tuple(fallbacks))


def _assert_row_template(key: str, spec: Mapping[str, Any]) -> None:
    template_keys = (
        "name_template",
        "name_regex_template",
        "test_id_template",
        "aria_label_template",
    )
    if not any(spec.get(k) for k in template_keys):
        raise SelectorValidationError(f"listing {key} 需要提供模板字段 {template_keys}")
    for template_key in template_keys:
        template = spec.get(template_key)
        if template is None:
            continue
        if not isinstance(template, str) or "{name}" not in template:
            raise SelectorValidationError(
                f"listing {key} 的 {template_key} 必须为包含 '{{name}}' 的字符串"
            )


def _normalize_toasts(node: Any) -> Mapping[str, tuple[str, ...]]:
    if not isinstance(node, Mapping):
        raise SelectorValidationError("toasts 节点缺失或格式错误")
    result: Dict[str, tuple[str, ...]] = {}
    for key, spec in node.items():
        patterns = spec.get("regex") if isinstance(spec, Mapping) else None
        if not isinstance(patterns, list) or not patterns:
            raise SelectorValidationError(f"toast {key} 必须提供 regex 列表")
        normalized = []
        for idx, pattern in enumerate(patterns):
            if not isinstance(pattern, str) or not pattern:
                raise SelectorValidationError(f"toast {key} regex[{idx}] 必须是非空字符串")
            normalized.append(pattern)
        result[key] = tuple(normalized)
    return result


__all__ = [
    "DriveUploadSelectors",
    "RowSelectorSet",
    "SelectorSet",
    "SelectorValidationError",
    "SpaceDefinition",
    "load_drive_upload_selectors",
]
