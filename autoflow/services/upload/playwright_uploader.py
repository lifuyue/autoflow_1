"""Playwright-based uploader that orchestrates DingTalk drive interactions."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Literal, Mapping

try:  # pragma: no cover - simplify type checking when Playwright is absent
    from playwright.sync_api import (
        FileChooser,
        FrameLocator,
        Locator,
        Page,
        TimeoutError as PlaywrightTimeoutError,
        Error as PlaywrightError,
    )
except Exception:  # pragma: no cover - provide fallbacks for tests without Playwright
    FileChooser = object  # type: ignore[misc,assignment]
    FrameLocator = object  # type: ignore[misc,assignment]
    Locator = object  # type: ignore[misc,assignment]
    Page = object  # type: ignore[misc,assignment]

    class PlaywrightTimeoutError(Exception):  # type: ignore[override]
        """Timeout placeholder when Playwright is unavailable."""

    class PlaywrightError(Exception):  # type: ignore[override]
        """Generic error placeholder."""

from autoflow.config import DriveUploadSelectors, RowSelectorSet, SelectorSet
from autoflow.core.errors import BrowserError
from autoflow.core.logger import get_logger
from .path_resolver import PathResolver

if TYPE_CHECKING:  # pragma: no cover - type hints only
    from browser.playwright_flow import PlaywrightFlow


@dataclass(slots=True)
class ConflictResolution:
    """Conflict-dialog handling outcome."""

    status: Literal["uploaded", "skipped"]
    strategy: str
    final_name: str


@dataclass(slots=True)
class TraceSession:
    """Holds tracing metadata for later export."""

    active: bool
    path: Path | None


@dataclass(slots=True)
class ConsoleCaptureState:
    """In-memory buffer for capturing browser console output."""

    enabled: bool
    entries: list[str]
    handler: object | None


@dataclass(slots=True)
class UploadResult:
    """Outcome payload from PlaywrightUploader.upload."""

    status: Literal["uploaded", "skipped", "failed"]
    requested_path: str
    requested_name: str
    final_name: str | None
    conflict_strategy: str
    screenshot: Path | None
    trace: Path | None
    downloads: tuple[Path, ...]
    message: str | None = None
    console_log: Path | None = None


class UploadFlowError(BrowserError):
    """Raised when the UI upload flow fails."""

    def __init__(self, message: str, *, result: UploadResult | None = None) -> None:
        super().__init__(message)
        self.result = result


class PlaywrightUploader:
    """Encapsulate file upload, conflict handling, and evidence capture."""

    def __init__(
        self,
        flow: "PlaywrightFlow",
        selectors: DriveUploadSelectors,
        *,
        timeout_ms: int = 12_000,
        toast_timeout_ms: int | None = None,
        logger=None,
    ) -> None:
        self.flow = flow
        self.selectors = selectors
        self.timeout_ms = timeout_ms
        self.toast_timeout_ms = toast_timeout_ms or max(timeout_ms, 12_000)
        self.logger = logger or get_logger()

    # ------------------------------------------------------------------
    def upload(
        self,
        *,
        path: str,
        file_path: Path,
        conflict_strategy: Literal["skip", "overwrite", "rename"] = "skip",
        create_missing: bool = False,
        export_results: bool = False,
    ) -> UploadResult:
        if conflict_strategy not in {"skip", "overwrite", "rename"}:
            raise ValueError(f"未知的冲突策略: {conflict_strategy}")

        requested_name = file_path.name
        page = self.flow.ensure_ready()
        trace_session = self._start_trace(page)
        downloads: list[Path] = []
        console_state = self._start_console_capture(page)

        result = UploadResult(
            status="failed",
            requested_path=path,
            requested_name=requested_name,
            final_name=None,
            conflict_strategy=conflict_strategy,
            screenshot=None,
            trace=None,
            downloads=(),
            message=None,
            console_log=None,
        )

        error: Exception | None = None
        final_name = requested_name
        applied_strategy = conflict_strategy

        try:
            resolver = PathResolver(page, self.selectors, timeout_ms=self.timeout_ms, logger=self.logger)
            resolver.resolve(path, create_missing=create_missing)
            frame = resolver.workspace_frame()

            if conflict_strategy == "skip" and self._file_exists(frame, requested_name):
                self.logger.info(
                    "drive.uploader conflict skip, file already exists",
                    extra={"drive_file": requested_name},
                )
                result.status = "skipped"
                result.final_name = requested_name
                return result

            if not self._perform_upload(frame, file_path):
                raise UploadFlowError("未能定位上传入口或设置文件")

            conflict_resolution = self._handle_conflict_dialog(page, conflict_strategy, requested_name)
            applied_strategy = conflict_resolution.strategy
            if conflict_resolution.status == "skipped":
                result.status = "skipped"
                result.final_name = requested_name
            else:
                final_name = conflict_resolution.final_name
                self._wait_for_toasts(page)
                final_name = self._await_listing(page, frame, final_name, requested_name)
                frame, final_name = self._reload_and_verify(page, path, final_name)
                if export_results:
                    exported = self._download_results(page, frame)
                    if exported:
                        downloads.append(exported)
                result.status = "uploaded"
                result.final_name = final_name
        except Exception as exc:  # noqa: BLE001
            error = exc
            result.status = "failed"
            result.message = str(exc)
            self.logger.error("drive.uploader upload failed", exc_info=True)
        finally:
            screenshot_label = "success" if result.status != "failed" else "failed"
            result.screenshot = self._capture_screenshot(page, screenshot_label)
            if trace_session.active:
                trace_path = self._stop_trace(page, trace_session, error is not None)
                if trace_path is not None:
                    result.trace = trace_path
            console_log_path = self._finalize_console_capture(page, console_state)
            if console_log_path is not None:
                result.console_log = console_log_path
            result.downloads = tuple(downloads)
            result.conflict_strategy = applied_strategy

        if error:
            suffix = []
            if result.screenshot:
                suffix.append(f"截图: {result.screenshot}")
            if result.trace:
                suffix.append(f"trace: {result.trace}")
            if result.console_log:
                suffix.append(f"console: {result.console_log}")
            message = result.message or "上传失败"
            if suffix:
                message = f"{message} ({'; '.join(suffix)})"
            raise UploadFlowError(message, result=result) from error

        return result

    # ------------------------------------------------------------------
    def _perform_upload(self, frame: FrameLocator, file_path: Path) -> bool:
        file_input = self.selectors.inputs.get("file_input")
        upload_action = self.selectors.actions.get("upload_button")

        used_direct = False
        requires_file_chooser = False

        if file_input:
            for candidate in self._iter_selector_candidates(file_input):
                if candidate.get("use_file_chooser"):
                    requires_file_chooser = True
                    continue
                locator = self._build_locator(frame, candidate)
                if locator is None:
                    continue
                try:
                    locator.set_input_files(str(file_path))
                    self.logger.info("drive.uploader file selected via hidden input")
                    used_direct = True
                    break
                except Exception:  # noqa: BLE001
                    continue

        if used_direct:
            return True

        if not upload_action:
            return False

        if not requires_file_chooser:
            requires_file_chooser = True  # fallback safeguard

        if not requires_file_chooser:
            return False

        return self._upload_with_file_chooser(frame, upload_action, file_path)

    def _upload_with_file_chooser(
        self,
        frame: FrameLocator,
        upload_action: SelectorSet,
        file_path: Path,
    ) -> bool:
        page = self.flow.ensure_ready()
        try:
            with page.expect_file_chooser(timeout=self.timeout_ms) as chooser_info:
                clicked = self._click_selector_set(frame, upload_action) or self._click_selector_set(page, upload_action)
            if not clicked:
                return False
            chooser: FileChooser = chooser_info.value
            chooser.set_files(str(file_path))
            self.logger.info("drive.uploader file chooser used for selection")
            return True
        except PlaywrightTimeoutError:
            self.logger.warning("drive.uploader file chooser not triggered within timeout")
            return False
        except Exception:  # noqa: BLE001
            self.logger.exception("drive.uploader file chooser flow failed")
            return False

    # ------------------------------------------------------------------
    def _handle_conflict_dialog(
        self,
        page: Page,
        strategy: str,
        requested_name: str,
    ) -> ConflictResolution:
        conflicts = self.selectors.conflicts
        dialog_set = conflicts.get("dialog")
        if not dialog_set:
            return ConflictResolution("uploaded", strategy, requested_name)

        dialog = self._wait_for_locator(page, dialog_set, timeout_ms=3000)
        if dialog is None:
            return ConflictResolution("uploaded", strategy, requested_name)

        self.logger.info("drive.uploader conflict dialog visible, strategy=%s", strategy)
        if strategy == "skip":
            skip_set = conflicts.get("skip")
            if skip_set and self._click_selector_set(page, skip_set):
                return ConflictResolution("skipped", strategy, requested_name)
            return ConflictResolution("skipped", strategy, requested_name)

        if strategy == "overwrite":
            overwrite_set = conflicts.get("overwrite")
            if overwrite_set:
                self._click_selector_set(page, overwrite_set)
            confirm_set = conflicts.get("confirm")
            if confirm_set:
                self._click_selector_set(page, confirm_set)
            return ConflictResolution("uploaded", strategy, requested_name)

        # rename strategy
        rename_set = conflicts.get("rename")
        if rename_set:
            self._click_selector_set(page, rename_set)
        rename_input = conflicts.get("rename_input")
        new_name = self._generate_timestamp_name(requested_name)
        if rename_input:
            self._fill_input(page, rename_input, new_name)
        confirm_set = conflicts.get("confirm")
        if confirm_set:
            self._click_selector_set(page, confirm_set)
        return ConflictResolution("uploaded", strategy, new_name)

    # ------------------------------------------------------------------
    def _wait_for_toasts(self, page: Page) -> None:
        success_patterns = [re.compile(pattern) for pattern in self.selectors.toasts.get("success", ())]
        failure_patterns = [re.compile(pattern) for pattern in self.selectors.toasts.get("failure", ())]
        deadline = time.time() + (self.toast_timeout_ms / 1000.0)

        while time.time() < deadline:
            for failure in failure_patterns:
                locator = page.get_by_text(failure)
                if self._locator_visible(locator, timeout_ms=500):
                    raise UploadFlowError(f"检测到上传失败提示: {failure.pattern}")
            for success in success_patterns:
                locator = page.get_by_text(success)
                if self._locator_visible(locator, timeout_ms=500):
                    self.logger.info("drive.uploader success toast pattern=%s", success.pattern)
                    return
            page.wait_for_timeout(200)
        raise UploadFlowError("未检测到上传成功提示")

    def _await_listing(self, page: Page, frame: FrameLocator, expected_name: str, base_name: str) -> str:
        listing = self.selectors.listing.get("row_by_name")
        if not listing:
            return expected_name
        deadline = time.time() + (self.timeout_ms / 1000.0)
        while time.time() < deadline:
            locator = self._build_row_locator(frame, listing, expected_name)
            if locator and self._locator_visible(locator, timeout_ms=500):
                return self._extract_name(locator, expected_name)
            if base_name != expected_name:
                locator = self._build_row_locator(frame, listing, base_name)
                if locator and self._locator_visible(locator, timeout_ms=500):
                    return self._extract_name(locator, base_name)
            page.wait_for_timeout(200)
        raise UploadFlowError("上传文件未出现在当前目录")

    def _reload_and_verify(self, page: Page, path: str, expected_name: str) -> tuple[FrameLocator, str]:
        page.reload(wait_until="domcontentloaded")
        resolver = PathResolver(page, self.selectors, timeout_ms=self.timeout_ms, logger=self.logger)
        resolver.resolve(path, create_missing=False)
        frame = resolver.workspace_frame()
        confirmed_name = self._await_listing(page, frame, expected_name, expected_name)
        return frame, confirmed_name

    def _download_results(self, page: Page, frame: FrameLocator) -> Path | None:
        export_set = self.selectors.exports.get("result_sheet")
        if not export_set:
            return None
        containers = (frame, page)
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        for container in containers:
            try:
                with page.expect_download(timeout=self.timeout_ms) as download_info:
                    clicked = self._click_selector_set(container, export_set)  # type: ignore[arg-type]
                if not clicked:
                    continue
                download = download_info.value
                suggested = download.suggested_filename or f"drive_result_{timestamp}.csv"
                target = self.flow.downloads_dir / f"{timestamp}_{suggested}"
                download.save_as(str(target))
                self.logger.info("drive.uploader result exported path=%s", target)
                return target
            except PlaywrightTimeoutError:
                continue
            except Exception:  # noqa: BLE001
                self.logger.warning("drive.uploader result export failed", exc_info=True)
                break
        return None

    # ------------------------------------------------------------------
    def _file_exists(self, frame: FrameLocator, name: str) -> bool:
        listing = self.selectors.listing.get("row_by_name")
        if not listing:
            return False
        locator = self._build_row_locator(frame, listing, name)
        if locator is None:
            return False
        try:
            return locator.count() > 0
        except Exception:  # noqa: BLE001
            return False

    # ------------------------------------------------------------------
    def _start_trace(self, page: Page) -> TraceSession:
        context = getattr(page, "context", None)
        if context is None:
            return TraceSession(False, None)
        trace_path = self.flow.trace_dir / f"drive-upload_{time.strftime('%Y%m%d-%H%M%S')}.zip"
        try:
            context.tracing.start(screenshots=True, snapshots=True, sources=True)
            return TraceSession(True, trace_path)
        except PlaywrightError:
            self.logger.warning("drive.uploader failed to start tracing", exc_info=True)
            return TraceSession(False, trace_path)

    def _stop_trace(self, page: Page, session: TraceSession, export: bool) -> Path | None:
        if not session.active:
            return None
        context = getattr(page, "context", None)
        if context is None:
            return None
        try:
            if export and session.path is not None:
                context.tracing.stop(path=str(session.path))
                self.logger.info("drive.uploader trace exported path=%s", session.path)
                return session.path
            context.tracing.stop()
        except PlaywrightError:
            self.logger.warning("drive.uploader failed to stop tracing", exc_info=True)
        return None

    def _start_console_capture(self, page: Page) -> ConsoleCaptureState:
        entries: list[str] = []
        if not hasattr(page, "on") or not callable(getattr(page, "on")):
            return ConsoleCaptureState(False, entries, None)

        def _listener(message: object) -> None:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            msg_type = self._extract_console_field(message, "type", default="log")
            text = self._extract_console_field(message, "text", default="")
            sanitized = self._sanitize_console_text(text)
            entries.append(f"[{timestamp}] {msg_type}: {sanitized}")

        try:
            page.on("console", _listener)  # type: ignore[attr-defined]
            return ConsoleCaptureState(True, entries, _listener)
        except Exception:  # noqa: BLE001
            return ConsoleCaptureState(False, entries, None)

    def _finalize_console_capture(self, page: Page, state: ConsoleCaptureState) -> Path | None:
        if state.enabled and hasattr(page, "off") and callable(getattr(page, "off")):
            try:
                page.off("console", state.handler)  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001
                pass
        if not state.entries:
            return None
        return self._write_console_log(state.entries)

    def _capture_screenshot(self, page: Page, label: str) -> Path | None:
        try:
            filename = f"drive-upload-{label}_{time.strftime('%Y%m%d-%H%M%S')}.png"
            target = self.flow.screenshots_dir / filename
            page.screenshot(path=str(target), full_page=True)
            return target
        except Exception:  # noqa: BLE001
            self.logger.warning("drive.uploader failed to capture screenshot", exc_info=True)
            return None

    # ------------------------------------------------------------------
    def _wait_for_locator(
        self,
        container: Page | FrameLocator,
        selector_set: SelectorSet,
        *,
        timeout_ms: int | None = None,
    ) -> Locator | None:
        for candidate in self._iter_selector_candidates(selector_set):
            locator = self._build_locator(container, candidate)
            if locator is None:
                continue
            try:
                locator.wait_for(state="visible", timeout=timeout_ms or self.timeout_ms)
                return locator
            except PlaywrightTimeoutError:
                continue
            except Exception:  # noqa: BLE001
                continue
        return None

    def _click_selector_set(self, container: Page | FrameLocator, selector_set: SelectorSet) -> bool:
        for candidate in self._iter_selector_candidates(selector_set):
            locator = self._build_locator(container, candidate)
            if locator is None:
                continue
            try:
                locator.click(timeout=self.timeout_ms)
                return True
            except Exception:  # noqa: BLE001
                continue
        return False

    def _fill_input(self, container: Page | FrameLocator, selector_set: SelectorSet, value: str) -> bool:
        for candidate in self._iter_selector_candidates(selector_set):
            locator = self._build_locator(container, candidate)
            if locator is None:
                continue
            try:
                locator.fill(value, timeout=self.timeout_ms)
                return True
            except Exception:  # noqa: BLE001
                continue
        return False

    def _build_row_locator(self, container: Page | FrameLocator, selector_set: RowSelectorSet, name: str) -> Locator | None:
        for spec in self._iter_row_candidates(selector_set, name):
            locator = self._build_locator(container, spec)
            if locator is not None:
                return locator
        return None

    def _extract_name(self, locator: Locator, fallback: str) -> str:
        try:
            text = locator.first.inner_text(timeout=self.timeout_ms)
        except Exception:  # noqa: BLE001
            text = None
        if not text:
            try:
                text = locator.first.text_content(timeout=self.timeout_ms)
            except Exception:  # noqa: BLE001
                text = None
        return text.strip() if isinstance(text, str) and text.strip() else fallback

    def _locator_visible(self, locator: Locator, *, timeout_ms: int) -> bool:
        try:
            locator.wait_for(state="visible", timeout=timeout_ms)
            return True
        except PlaywrightTimeoutError:
            return False
        except Exception:  # noqa: BLE001
            return False

    @staticmethod
    def _iter_selector_candidates(selector_set: SelectorSet) -> Iterable[Mapping[str, object]]:
        yield selector_set.primary
        yield from selector_set.fallbacks

    @staticmethod
    def _iter_row_candidates(selector_set: RowSelectorSet, name: str) -> Iterable[Mapping[str, object]]:
        for spec in PlaywrightUploader._iter_selector_candidates(selector_set):
            yield PlaywrightUploader._apply_name_template(spec, name)

    @staticmethod
    def _apply_name_template(spec: Mapping[str, object], name: str) -> Mapping[str, object]:
        data = dict(spec)
        template = data.pop("name_template", None)
        if isinstance(template, str):
            data["name"] = template.format(name=name)
        regex_template = data.pop("name_regex_template", None)
        if isinstance(regex_template, str):
            data["name_regex"] = regex_template.format(name=re.escape(name))
        test_id_template = data.pop("test_id_template", None)
        if isinstance(test_id_template, str):
            data["test_id"] = test_id_template.format(name=name)
        return data

    @staticmethod
    def _extract_console_field(message: object, attr: str, default: str) -> str:
        value = getattr(message, attr, None)
        if callable(value):
            try:
                value = value()
            except Exception:  # noqa: BLE001
                value = None
        return str(value) if value else default

    @staticmethod
    def _sanitize_console_text(text: str) -> str:
        sanitized = re.sub(r"\d{3,}", "***", text or "")
        return sanitized.replace("\n", " ").strip()

    def _write_console_log(self, entries: list[str]) -> Path | None:
        try:
            filename = f"drive-console_{time.strftime('%Y%m%d-%H%M%S')}.log"
            target = self.flow.console_dir / filename
            target.write_text("\n".join(entries), encoding="utf-8")
            self.logger.info("drive.uploader console log captured path=%s", target)
            return target
        except Exception:  # noqa: BLE001
            self.logger.warning("drive.uploader failed to persist console log", exc_info=True)
            return None

    @staticmethod
    def _build_locator(container: Page | FrameLocator, spec: Mapping[str, object]) -> Locator | None:
        role = spec.get("role")
        if isinstance(role, str) and role:
            name = spec.get("name")
            if isinstance(name, str):
                return container.get_by_role(role, name=name)  # type: ignore[return-value]
            name_regex = spec.get("name_regex")
            if isinstance(name_regex, str):
                return container.get_by_role(role, name=re.compile(name_regex))  # type: ignore[return-value]
        text = spec.get("text")
        if isinstance(text, str) and text:
            return container.get_by_text(text, exact=True)  # type: ignore[return-value]
        text_regex = spec.get("text_regex")
        if isinstance(text_regex, str) and text_regex:
            return container.get_by_text(re.compile(text_regex))  # type: ignore[return-value]
        label = spec.get("label")
        if isinstance(label, str) and label:
            return container.get_by_label(label)  # type: ignore[return-value]
        label_regex = spec.get("label_regex")
        if isinstance(label_regex, str) and label_regex:
            return container.get_by_label(re.compile(label_regex))  # type: ignore[return-value]
        placeholder = spec.get("placeholder")
        if isinstance(placeholder, str) and placeholder:
            return container.get_by_placeholder(placeholder)  # type: ignore[return-value]
        placeholder_regex = spec.get("placeholder_regex")
        if isinstance(placeholder_regex, str) and placeholder_regex:
            return container.get_by_placeholder(re.compile(placeholder_regex))  # type: ignore[return-value]
        test_id = spec.get("test_id")
        if isinstance(test_id, str) and test_id:
            return container.get_by_test_id(test_id)  # type: ignore[return-value]
        selector = spec.get("selector") or spec.get("css")
        if isinstance(selector, str) and selector:
            return container.locator(selector)  # type: ignore[return-value]
        return None

    @staticmethod
    def _generate_timestamp_name(name: str) -> str:
        stem, suffix = PlaywrightUploader._split_name(name)
        return f"{stem}_{time.strftime('%Y%m%d-%H%M%S')}{suffix}"

    @staticmethod
    def _split_name(name: str) -> tuple[str, str]:
        if "." not in name:
            return name, ""
        idx = name.rfind(".")
        return name[:idx], name[idx:]


__all__ = ["PlaywrightUploader", "UploadResult", "UploadFlowError"]
