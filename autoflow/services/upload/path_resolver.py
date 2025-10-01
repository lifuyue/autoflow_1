"""Path resolution utilities for DingTalk drive uploads via Playwright."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

try:  # pragma: no cover - typing aid when Playwright is available
    from playwright.sync_api import FrameLocator, Locator, Page, TimeoutError as PlaywrightTimeoutError
except Exception:  # pragma: no cover - fallback for tests without Playwright
    FrameLocator = Any  # type: ignore[misc,assignment]
    Locator = Any  # type: ignore[misc,assignment]
    Page = Any  # type: ignore[misc,assignment]

    class PlaywrightTimeoutError(Exception):  # type: ignore[override]
        """Placeholder timeout error when Playwright is absent."""

from autoflow.config import DriveUploadSelectors, RowSelectorSet, SelectorSet, SpaceDefinition
from autoflow.core.errors import BrowserError
from autoflow.core.logger import get_logger


_ILLEGAL_CHARS = re.compile(r'[<>:"|?*]')


class PathResolverError(BrowserError):
    """Base error for path resolution failures."""


class PathValidationError(PathResolverError):
    """Raised when the provided path string is invalid."""


class PathNotFoundError(PathResolverError):
    """Raised when a folder path does not exist and auto-create is disabled."""


@dataclass(frozen=True)
class PathPlan:
    """Parsed representation of a drive path."""

    space_label: str
    folders: tuple[str, ...]


class PathResolver:
    """Validate and traverse DingTalk drive folder paths via Playwright."""

    def __init__(
        self,
        page: Page,
        selectors: DriveUploadSelectors,
        *,
        timeout_ms: int = 10_000,
        logger=None,
    ) -> None:
        self.page = page
        self.selectors = selectors
        self.timeout_ms = timeout_ms
        self.logger = logger or get_logger()

    # ------------------------------------------------------------------
    def resolve(self, path: str, *, create_missing: bool = False) -> None:
        """Ensure the path exists by navigating/creating folders as needed."""

        plan = self._parse_path(path)
        space_def = self._match_space(plan.space_label)
        self.logger.info("drive.path_resolver enter_space label=%s", plan.space_label)
        self._select_space(plan.space_label, space_def)
        frame = self._ensure_workspace_frame()

        for folder in plan.folders:
            self.logger.info("drive.path_resolver descend folder=%s", folder)
            if self._enter_existing_folder(frame, folder):
                continue
            if not create_missing:
                raise PathNotFoundError(f"路径不存在: {folder}")
            self.logger.info("drive.path_resolver create_missing folder=%s", folder)
            self._create_folder(frame, folder)

    def workspace_frame(self) -> FrameLocator:
        """Return the Playwright frame locator for the drive workspace."""

        return self._ensure_workspace_frame()

    # ------------------------------------------------------------------
    def _parse_path(self, path: str) -> PathPlan:
        if not path or not isinstance(path, str):
            raise PathValidationError("路径不能为空")
        if path.endswith("/"):
            raise PathValidationError("路径尾部存在空段")
        raw_segments = path.split("/")
        if any(segment.strip() == "" for segment in raw_segments):
            raise PathValidationError("路径包含空段")

        segments: list[str] = []
        seen_folders: set[str] = set()
        for idx, segment in enumerate(raw_segments):
            cleaned = segment.strip()
            if _ILLEGAL_CHARS.search(cleaned):
                raise PathValidationError(f"路径段包含非法字符: {cleaned}")
            if idx > 0:
                if cleaned in seen_folders:
                    raise PathValidationError(f"路径存在重复段: {cleaned}")
                seen_folders.add(cleaned)
            segments.append(cleaned)

        if len(segments) < 1:
            raise PathValidationError("路径必须包含盘别")
        space_label = segments[0]
        folders = tuple(segments[1:])
        return PathPlan(space_label=space_label, folders=folders)

    def _match_space(self, label: str) -> SpaceDefinition:
        for space in self.selectors.spaces.values():
            if label in space.labels:
                return space
        raise PathValidationError(f"路径首段应为已配置的盘别，未识别: {label}")

    def _select_space(self, label: str, space: SpaceDefinition) -> None:
        page = self.page
        current_url = getattr(page, "url", "")
        for anchor in space.anchors:
            url_contains = anchor.get("url_contains")
            if url_contains and url_contains in current_url:
                self.logger.debug("drive.path_resolver already_in_space label=%s", label)
                return
            locator = self._build_locator(page, anchor)
            if locator is None:
                continue
            if self._click_locator(locator):
                return
        raise PathNotFoundError(f"无法定位盘别入口: {label}")

    def _ensure_workspace_frame(self) -> FrameLocator:
        frame_set = self.selectors.frames.get("drive_workspace")
        if frame_set is None:
            raise PathResolverError("未配置 drive_workspace frame 选择器")
        for candidate in self._iter_selector_candidates(frame_set):
            selector = candidate.get("selector")
            if not selector:
                continue
            try:
                frame_locator = self.page.frame_locator(selector)
                frame_locator.locator("body").wait_for(state="attached", timeout=self.timeout_ms)
                return frame_locator
            except Exception:  # noqa: BLE001
                continue
        raise PathNotFoundError("无法定位网盘工作区 iframe")

    def _enter_existing_folder(self, frame: FrameLocator, name: str) -> bool:
        listing = self.selectors.listing.get("row_by_name")
        if listing is None:
            raise PathResolverError("未配置 row_by_name 选择器")
        for spec in self._iter_row_candidates(listing, name):
            locator = self._build_locator(frame, spec)
            if locator is None:
                continue
            if self._click_locator(locator):
                return True
        return False

    def _create_folder(self, frame: FrameLocator, name: str) -> None:
        new_action = self.selectors.actions.get("new_folder")
        confirm_action = self.selectors.actions.get("confirm_button")
        folder_input = self.selectors.inputs.get("folder_name")
        if not (new_action and confirm_action and folder_input):
            raise PathResolverError("缺少创建文件夹所需的选择器配置")

        if not self._click_using_selector_set(frame, new_action):
            raise PathResolverError("无法点击新建文件夹按钮")
        if not self._fill_input(frame, folder_input, name):
            raise PathResolverError("无法填写文件夹名称")
        if not self._click_using_selector_set(frame, confirm_action):
            raise PathResolverError("无法确认新建文件夹")
        if not self._enter_existing_folder(frame, name):
            raise PathResolverError("新建文件夹后未找到目标目录")

    # ------------------------------------------------------------------
    def _click_using_selector_set(self, container: Any, selector_set: SelectorSet) -> bool:
        for candidate in self._iter_selector_candidates(selector_set):
            locator = self._build_locator(container, candidate)
            if locator is None:
                continue
            if self._click_locator(locator):
                return True
        return False

    def _fill_input(self, container: Any, selector_set: SelectorSet, value: str) -> bool:
        for candidate in self._iter_selector_candidates(selector_set):
            locator = self._build_locator(container, candidate)
            if locator is None:
                continue
            try:
                locator.click()
            except Exception:  # noqa: BLE001
                pass
            try:
                locator.fill(value)
                return True
            except Exception:  # noqa: BLE001
                continue
        return False

    def _click_locator(self, locator: Locator) -> bool:
        try:
            locator.wait_for(state="visible", timeout=self.timeout_ms)
        except (PlaywrightTimeoutError, AttributeError):
            pass
        try:
            locator.scroll_into_view_if_needed(timeout=self.timeout_ms)
        except Exception:  # noqa: BLE001
            pass
        try:
            locator.click(timeout=self.timeout_ms)
            return True
        except Exception:  # noqa: BLE001
            return False

    def _build_locator(self, container: Any, spec: Mapping[str, Any]) -> Locator | None:
        role = spec.get("role")
        if role:
            name = spec.get("name")
            name_regex = spec.get("name_regex")
            if name is not None:
                return container.get_by_role(role, name=name)
            if name_regex is not None:
                return container.get_by_role(role, name=re.compile(name_regex))
        if spec.get("test_id"):
            return container.get_by_test_id(spec["test_id"])
        if spec.get("text"):
            return container.get_by_text(spec["text"], exact=True)
        if spec.get("text_regex"):
            return container.get_by_text(re.compile(spec["text_regex"]))
        if spec.get("label"):
            return container.get_by_label(spec["label"])
        if spec.get("label_regex"):
            return container.get_by_label(re.compile(spec["label_regex"]))
        if spec.get("placeholder"):
            return container.get_by_placeholder(spec["placeholder"])
        if spec.get("placeholder_regex"):
            return container.get_by_placeholder(re.compile(spec["placeholder_regex"]))
        selector = spec.get("selector") or spec.get("css")
        if selector:
            return container.locator(selector)
        return None

    # ------------------------------------------------------------------
    @staticmethod
    def _iter_selector_candidates(selector_set: SelectorSet) -> Iterable[Mapping[str, Any]]:
        yield selector_set.primary
        yield from selector_set.fallbacks

    @staticmethod
    def _iter_row_candidates(selector_set: RowSelectorSet, name: str) -> Iterable[Mapping[str, Any]]:
        for spec in PathResolver._iter_selector_candidates(selector_set):
            yield PathResolver._apply_name_template(spec, name)

    @staticmethod
    def _apply_name_template(spec: Mapping[str, Any], name: str) -> Mapping[str, Any]:
        data = dict(spec)
        template = data.pop("name_template", None)
        if template:
            data["name"] = template.format(name=name)
        regex_template = data.pop("name_regex_template", None)
        if regex_template:
            data["name_regex"] = regex_template.format(name=re.escape(name))
        test_id_template = data.pop("test_id_template", None)
        if test_id_template:
            data["test_id"] = test_id_template.format(name=name)
        return data


__all__ = [
    "PathResolver",
    "PathResolverError",
    "PathValidationError",
    "PathNotFoundError",
]
