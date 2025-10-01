"""Playwright flow helpers for browser-based automations.

This module provides a structured wrapper to initialise a browser context,
reuse login state, detect session expiry, and capture failure artefacts such
as screenshots and Playwright traces. Interactions rely on the Locator API so
that selectors remain resilient against DOM changes.
"""

from __future__ import annotations

import re
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, ContextManager, Generator
import platform

from playwright.sync_api import (
    Browser,
    BrowserContext,
    Download,
    Locator,
    Page,
    Playwright,
    Response,
    TimeoutError as PlaywrightTimeoutError,
    Error as PlaywrightError,
    sync_playwright,
)

from autoflow.core.errors import BrowserError
from autoflow.core.logger import get_logger
from autoflow.core.profiles import ensure_work_dirs


DEFAULT_STORAGE_STATE = Path("browser") / "storageState.json"


class SessionExpiredError(BrowserError):
    """Raised when a stored Playwright session is no longer valid."""


class PlaywrightFlow:
    """Orchestrates Playwright context lifecycle and common interactions.

    The flow loads a persisted storage state to reuse authenticated sessions,
    falls back to headless Chromium launch, and records diagnostics when
    session expiry or other automation errors occur.
    """

    _SESSION_ERROR_STATUS = {401, 403, 440, 499, 500, 302}

    def __init__(
        self,
        *,
        headless: bool = True,
        storage_state_path: Path | str | None = DEFAULT_STORAGE_STATE,
        downloads_dir: Path | None = None,
        screenshots_dir: Path | None = None,
        trace_dir: Path | None = None,
        login_url_markers: tuple[str, ...] | None = None,
        login_button_texts: tuple[str, ...] | None = None,
        password_hints: tuple[str, ...] | None = None,
        default_timeout_ms: int = 15_000,
        on_session_expired: Callable[[str], None] | None = None,
    ) -> None:
        self.logger = get_logger()
        self.headless = headless
        self._storage_state_path = Path(storage_state_path) if storage_state_path else None
        work_dirs = ensure_work_dirs()
        self.downloads_dir = downloads_dir or (work_dirs["tmp"] / "browser_downloads")
        self.screenshots_dir = screenshots_dir or work_dirs["shot"]
        self.trace_dir = trace_dir or (work_dirs["logs"] / "trace")
        self.console_dir = work_dirs["logs"] / "console"
        for directory in (self.downloads_dir, self.screenshots_dir, self.trace_dir, self.console_dir):
            directory.mkdir(parents=True, exist_ok=True)

        self.default_timeout_ms = default_timeout_ms
        self.on_session_expired = on_session_expired

        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None
        self._tracing_active = False
        self._browser_channel: str | None = None

        self._login_url_markers = tuple(m.lower() for m in (login_url_markers or ("login", "sso", "auth")))
        self._login_button_regex = re.compile("|".join(login_button_texts or ("登录", "登陆", "login", "sign in")), re.IGNORECASE)
        self._password_regex = re.compile("|".join(password_hints or ("密码", "password")), re.IGNORECASE)

    # ------------------------------------------------------------------
    # Lifecycle helpers
    def __enter__(self) -> "PlaywrightFlow":
        self.ensure_ready()
        return self

    def __exit__(self, exc_type, exc, _tb) -> bool:
        if exc is not None:
            reason = str(exc)
            self.logger.error("Playwright flow failed: %s", reason)
            self._record_failure_artifacts("exception")
        self.close()
        # Do not suppress exceptions
        return False

    def ensure_ready(self) -> Page:
        """Ensure the browser context and page are initialised."""
        if self._page is not None:
            return self._page

        try:
            self._playwright = sync_playwright().start()
        except Exception as exc:  # noqa: BLE001
            raise BrowserError("Playwright 未安装或初始化失败，请运行 python -m playwright install chromium") from exc

        browser = self._launch_browser(self._playwright)
        context = self._new_context(browser)
        page = context.new_page()
        page.set_default_timeout(self.default_timeout_ms)

        self._browser = browser
        self._context = context
        self._page = page
        return page

    def close(self) -> None:
        """Release Playwright resources."""
        if self._context is not None:
            try:
                self._context.close()
            except Exception:  # noqa: BLE001
                self.logger.warning("关闭 BrowserContext 失败", exc_info=True)
        if self._browser is not None:
            try:
                self._browser.close()
            except Exception:  # noqa: BLE001
                self.logger.warning("关闭 Browser 实例失败", exc_info=True)
        if self._playwright is not None:
            try:
                self._playwright.stop()
            except Exception:  # noqa: BLE001
                self.logger.warning("停止 Playwright 失败", exc_info=True)

        self._context = None
        self._browser = None
        self._playwright = None
        self._page = None
        self._tracing_active = False
        self._browser_channel = None

    # ------------------------------------------------------------------
    # Navigation helpers
    def goto(
        self,
        url: str,
        *,
        wait_for: Callable[[Page], Locator] | None = None,
        timeout_ms: int | None = None,
        description: str | None = None,
    ) -> Page:
        """Navigate to a URL and optionally wait for a business signal.

        Args:
            url: Target URL.
            wait_for: Callback returning a locator that must become visible to
                deem navigation successful.
            timeout_ms: Optional override for the default timeout when
                waiting on the business signal.
            description: Log-friendly description of the navigation target.

        Raises:
            SessionExpiredError: Stored session has expired.
            BrowserError: Navigation or waiting failed.
        """

        page = self.ensure_ready()
        friendly = description or url
        self.logger.info("打开页面: %s", friendly)

        try:
            response = page.goto(url, wait_until="domcontentloaded")
        except PlaywrightTimeoutError as exc:
            self._record_failure_artifacts("goto-timeout")
            raise BrowserError(f"页面跳转超时: {url}") from exc
        except PlaywrightError as exc:  # noqa: BLE001
            self._record_failure_artifacts("goto-error")
            raise BrowserError(f"页面跳转失败: {url}") from exc

        self._verify_session(page, response)

        if wait_for is not None:
            locator = wait_for(page)
            self.logger.debug("等待业务信号: %s", locator)
            try:
                locator.wait_for(state="visible", timeout=timeout_ms or self.default_timeout_ms)
            except PlaywrightTimeoutError as exc:
                self._record_failure_artifacts("signal-timeout")
                raise BrowserError(f"业务元素未及时出现: {friendly}") from exc
        return page

    def wait_for_text(self, text: str, *, exact: bool = False, timeout_ms: int | None = None) -> Locator:
        """Wait for a text node using Locator API."""

        page = self.ensure_ready()
        locator = page.get_by_text(text, exact=exact)
        locator.wait_for(state="visible", timeout=timeout_ms or self.default_timeout_ms)
        return locator

    def run_with_trace(self, label: str) -> ContextManager[None]:
        """Context manager that records a trace only if an error bubbles up."""

        @contextmanager
        def _runner() -> Generator[None, None, None]:
            context = self._context
            if context is None:
                raise BrowserError("Playwright context 未初始化")

            try:
                context.tracing.start(name=label, screenshots=True, snapshots=True, sources=True)
                self._tracing_active = True
            except PlaywrightError:
                # Logging is deferred to keep normal path clean; trace export
                # still happens via _record_failure_artifacts when possible.
                self._tracing_active = False
            try:
                yield
            except Exception:
                self._record_failure_artifacts(label)
                raise
            else:
                if self._tracing_active:
                    try:
                        context.tracing.stop()
                    except PlaywrightError:
                        self.logger.warning("停止 trace 失败", exc_info=True)
                self._tracing_active = False

        return _runner()

    # ------------------------------------------------------------------
    # Downloads
    def download_via(self, trigger: Callable[[Page], None], *, filename: str | None = None) -> Path:
        """Trigger a download and save the file to the configured directory."""

        page = self.ensure_ready()
        with page.expect_download() as download_info:
            trigger(page)
        download = download_info.value
        target = self._resolve_download_path(download, filename)
        download.save_as(str(target))
        self.logger.info("下载完成: %s", target)
        return target

    # ------------------------------------------------------------------
    # Internal helpers
    def _launch_browser(self, playwright: Playwright) -> Browser:
        attempts: list[tuple[str | None, str]] = [
            (None, "chromium"),
            ("msedge", "msedge"),
            ("chrome", "chrome"),
        ]
        last_exc: PlaywrightError | None = None
        for channel, label in attempts:
            try:
                if channel is None:
                    browser = playwright.chromium.launch(headless=self.headless)
                else:
                    browser = playwright.chromium.launch(headless=self.headless, channel=channel)
                self._browser_channel = label
                return browser
            except PlaywrightError as exc:
                last_exc = exc
                continue
        raise BrowserError(
            "无法启动 Chromium，请执行 python -m playwright install chromium 或安装 Edge/Chrome"
        ) from last_exc

    def _new_context(self, browser: Browser) -> BrowserContext:
        storage_state: str | None = None
        if self._storage_state_path and self._storage_state_path.exists():
            storage_state = str(self._storage_state_path)
            self.logger.info("加载 storage state: %s", storage_state)
        else:
            if self._storage_state_path is not None:
                self.logger.warning("storageState 未找到，可能需要先手动登录: %s", self._storage_state_path)
        context = browser.new_context(storage_state=storage_state, accept_downloads=True)
        context.set_default_timeout(self.default_timeout_ms)
        return context

    def _verify_session(self, page: Page, response: Response | None) -> None:
        if response and response.status in self._SESSION_ERROR_STATUS:
            self._handle_session_expired(f"HTTP {response.status}")
            return

        target = page.url.lower()
        if any(marker in target for marker in self._login_url_markers):
            self._handle_session_expired(f"命中登录页: {page.url}")
            return

        if self._detect_login_form(page):
            self._handle_session_expired("检测到登录表单")

    def _detect_login_form(self, page: Page) -> bool:
        try:
            password_by_label = page.get_by_label(self._password_regex)
            if password_by_label.count() > 0:
                return True
        except PlaywrightError:
            pass

        try:
            password_by_placeholder = page.get_by_placeholder(self._password_regex)
            if password_by_placeholder.count() > 0:
                return True
        except PlaywrightError:
            pass

        try:
            login_button = page.get_by_role("button", name=self._login_button_regex)
            if login_button.count() > 0:
                return True
        except PlaywrightError:
            pass

        return False

    def _handle_session_expired(self, reason: str) -> None:
        message = f"检测到登录态失效({reason})，请通过浏览器重新登录并刷新 storageState.json。"
        self.logger.warning(message)
        if self.on_session_expired is not None:
            try:
                self.on_session_expired(reason)
            except Exception:  # noqa: BLE001
                self.logger.warning("会话过期回调执行失败", exc_info=True)
        self._record_failure_artifacts("session-expired")
        raise SessionExpiredError(message)

    def _record_failure_artifacts(self, label: str) -> None:
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        page = self._page
        context = self._context

        if page is not None:
            snap_path = self.screenshots_dir / f"{label}_{timestamp}.png"
            try:
                page.screenshot(path=str(snap_path), full_page=True)
                self.logger.info("故障截图已保存: %s", snap_path)
            except PlaywrightError:
                self.logger.warning("截图失败", exc_info=True)

        if context is not None:
            trace_path = self.trace_dir / f"{label}_{timestamp}.zip"
            try:
                if not self._tracing_active:
                    context.tracing.start(name=label, screenshots=True, snapshots=True, sources=True)
                    self._tracing_active = True
                    if page is not None:
                        page.wait_for_timeout(200)
                context.tracing.stop(path=str(trace_path))
                self.logger.info("Playwright trace 导出: %s", trace_path)
            except PlaywrightError:
                self.logger.warning("导出 trace 失败", exc_info=True)
            finally:
                self._tracing_active = False

    def _resolve_download_path(self, download: Download, filename: str | None) -> Path:
        suggested = filename or download.suggested_filename
        target = self.downloads_dir / suggested
        counter = 1
        while target.exists():
            stem = Path(suggested).stem
            suffix = Path(suggested).suffix
            target = self.downloads_dir / f"{stem}_{counter}{suffix}"
            counter += 1
        return target

    # ------------------------------------------------------------------
    # Diagnostics
    def environment_snapshot(self) -> dict[str, str | None]:
        """Return Playwright/browser/OS metadata for diagnostics."""

        info: dict[str, str | None] = {
            "playwrightVersion": self._playwright_version(),
            "browserName": None,
            "browserVersion": None,
            "browserChannel": self._browser_channel,
            "os": platform.platform(),
        }
        browser = getattr(self, "_browser", None)
        if browser is not None:
            browser_type = getattr(browser, "browser_type", None)
            info["browserName"] = getattr(browser_type, "name", None)
            try:
                version = browser.version  # type: ignore[attr-defined]
                info["browserVersion"] = version() if callable(version) else version
            except Exception:  # noqa: BLE001
                info["browserVersion"] = None
        return info

    @staticmethod
    def _playwright_version() -> str | None:
        try:
            import playwright  # type: ignore

            return getattr(playwright, "__version__", None)
        except Exception:  # noqa: BLE001
            return None
