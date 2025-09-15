from __future__ import annotations

from pathlib import Path
from typing import Any, Callable
import time
import yaml

from autoflow.core.logger import get_logger
from autoflow.core.profiles import resolve_config_path
from autoflow.core.errors import BrowserError


class BrowserRunner:
    """A thin wrapper around Playwright for open/login/upload/download.

    For MVP, focuses on upload; selectors are read from YAML files.
    """

    def __init__(self, headless: bool = False, shots_dir: Path | None = None):
        self.headless = headless
        try:
            from autoflow.core.profiles import ensure_work_dirs
            if shots_dir is None:
                self.shots_dir = ensure_work_dirs()["shot"]
            else:
                self.shots_dir = shots_dir
        except Exception:
            self.shots_dir = shots_dir or Path("autoflow/work/logs/shot")
        self.shots_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger()
        self._browser = None
        self._page = None

    def _ensure(self):
        if self._page is not None:
            return
        try:
            from playwright.sync_api import sync_playwright  # type: ignore
        except Exception as e:  # noqa: BLE001
            raise BrowserError(
                "未安装 Playwright。请执行: pip install playwright && python -m playwright install chromium"
            ) from e
        pw = sync_playwright().start()
        # Prefer bundled Chromium; fallback to system Edge/Chrome if not installed
        try:
            browser = pw.chromium.launch(headless=self.headless)
        except Exception:
            try:
                browser = pw.chromium.launch(headless=self.headless, channel="msedge")
            except Exception:
                try:
                    browser = pw.chromium.launch(headless=self.headless, channel="chrome")
                except Exception as e:  # noqa: BLE001
                    pw.stop()
                    raise BrowserError(
                        "无法启动浏览器。请安装 Chromium: python -m playwright install chromium，或安装系统 Edge/Chrome。"
                    ) from e
        context = browser.new_context()
        page = context.new_page()
        self._playwright = pw
        self._browser = browser
        self._context = context
        self._page = page

    def open(self, url: str | None):
        if not url:
            raise BrowserError("未提供 URL")
        self._ensure()
        assert self._page is not None
        self.logger.info("打开页面: %s", url)
        self._page.goto(url)

    def login_if_needed(
        self,
        config: dict[str, Any] | None,
        credentials_provider: Callable[[bool], dict[str, str] | None] | None = None,
    ) -> None:
        if not config:
            return
        page = self._page
        assert page is not None
        u_sel = config.get("username_selector")
        p_sel = config.get("password_selector")
        s_sel = config.get("submit_selector")
        if not (u_sel and p_sel and s_sel):
            # Assume session is active
            self.logger.info("未配置登录选择器，跳过登录。")
            return
        creds = credentials_provider(True) if credentials_provider else None
        if not creds:
            raise BrowserError("需要登录凭据，但未获取到用户名/密码")
        self.logger.info("执行登录…")
        page.fill(u_sel, creds["username"])  # type: ignore[arg-type]
        page.fill(p_sel, creds["password"])  # type: ignore[arg-type]
        page.click(s_sel)  # type: ignore[arg-type]
        time.sleep(1)

    def do_upload(self, selectors_file: str | Path, file_path: Path):
        page = self._page
        assert page is not None
        sels = self._load_selectors(selectors_file)
        up_sel = sels.get("upload_input_selector")
        submit_sel = sels.get("submit_selector")
        if not up_sel:
            raise BrowserError("选择器文件中缺少 upload_input_selector")
        self.logger.info("设置上传文件并提交…")
        page.set_input_files(up_sel, str(file_path))  # type: ignore[arg-type]
        if submit_sel:
            page.click(submit_sel)  # type: ignore[arg-type]
        time.sleep(sels.get("post_submit_wait", 1))

    def screenshot(self, name_prefix: str = "shot") -> Path:
        page = self._page
        assert page is not None
        out = self.shots_dir / f"{name_prefix}_{int(time.time())}.png"
        page.screenshot(path=str(out))
        return out

    def html_dump(self, name_prefix: str = "dump") -> Path:
        page = self._page
        assert page is not None
        out = self.shots_dir / f"{name_prefix}_{int(time.time())}.html"
        out.write_text(page.content(), encoding="utf-8")
        return out

    def close(self):
        try:
            if getattr(self, "_context", None):
                self._context.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            if getattr(self, "_browser", None):
                self._browser.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            if getattr(self, "_playwright", None):
                self._playwright.stop()
        except Exception:  # noqa: BLE001
            pass

    @staticmethod
    def _load_selectors(path: str | Path) -> dict[str, Any]:
        p = resolve_config_path(path)
        if not p.exists():
            raise BrowserError(f"未找到选择器文件: {p}")
        with p.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

