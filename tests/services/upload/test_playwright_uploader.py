from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
import zipfile

import pytest

from autoflow.config import load_drive_upload_selectors
from autoflow.services.upload.playwright_uploader import (
    ConflictResolution,
    PlaywrightUploader,
    TraceSession,
    UploadFlowError,
)


@pytest.fixture(scope="module")
def drive_selectors():
    return load_drive_upload_selectors()


class DummyFlow:
    def __init__(self, page: MagicMock, base_dir: Path):
        self._page = page
        self.screenshots_dir = base_dir / "shots"
        self.screenshots_dir.mkdir(parents=True, exist_ok=True)
        self.downloads_dir = base_dir / "downloads"
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
        self.trace_dir = base_dir / "traces"
        self.trace_dir.mkdir(parents=True, exist_ok=True)

    def ensure_ready(self):
        return self._page


def _stub_page():
    page = MagicMock()
    page.context = MagicMock()
    page.wait_for_timeout = MagicMock()
    page.reload = MagicMock()
    page.expect_file_chooser = MagicMock()
    page.expect_download = MagicMock()
    page.get_by_text = MagicMock()
    page.get_by_role = MagicMock()
    page.screenshot = MagicMock()
    return page


def test_upload_skip_when_conflict_detected(monkeypatch, tmp_path, drive_selectors):
    page = _stub_page()
    flow = DummyFlow(page, tmp_path)
    uploader = PlaywrightUploader(flow, drive_selectors)

    class FakeResolver:
        def __init__(self, *args, **kwargs):
            self.frame = MagicMock()

        def resolve(self, path, create_missing=False):
            pass

        def workspace_frame(self):
            return self.frame

    monkeypatch.setattr("autoflow.services.upload.playwright_uploader.PathResolver", FakeResolver)
    monkeypatch.setattr(PlaywrightUploader, "_file_exists", lambda self, frame, name: True)
    called = {"perform": False}

    def _fail_perform(self, frame, file_path):
        called["perform"] = True
        return True

    monkeypatch.setattr(PlaywrightUploader, "_perform_upload", _fail_perform)
    monkeypatch.setattr(PlaywrightUploader, "_capture_screenshot", lambda self, page, label: flow.screenshots_dir / "skip.png")
    monkeypatch.setattr(PlaywrightUploader, "_start_trace", lambda self, page: TraceSession(False, None))
    monkeypatch.setattr(PlaywrightUploader, "_stop_trace", lambda self, page, session, export: None)

    file_path = tmp_path / "dummy.xlsx"
    file_path.write_text("demo")
    result = uploader.upload(path="企业盘/财务", file_path=file_path, conflict_strategy="skip")

    assert result.status == "skipped"
    assert result.final_name == "dummy.xlsx"
    assert result.screenshot == flow.screenshots_dir / "skip.png"
    assert result.console_log is None
    assert not called["perform"]


def test_upload_success(monkeypatch, tmp_path, drive_selectors):
    page = _stub_page()
    flow = DummyFlow(page, tmp_path)
    uploader = PlaywrightUploader(flow, drive_selectors)
    frame = MagicMock()

    class FakeResolver:
        def __init__(self, *args, **kwargs):
            pass

        def resolve(self, path, create_missing=False):
            return None

        def workspace_frame(self):
            return frame

    monkeypatch.setattr("autoflow.services.upload.playwright_uploader.PathResolver", FakeResolver)
    monkeypatch.setattr(PlaywrightUploader, "_file_exists", lambda self, frame, name: False)
    monkeypatch.setattr(PlaywrightUploader, "_perform_upload", lambda self, frame, file_path: True)
    monkeypatch.setattr(
        PlaywrightUploader,
        "_handle_conflict_dialog",
        lambda self, page, strategy, requested_name: ConflictResolution("uploaded", "overwrite", "uploaded_final.xlsx"),
    )
    monkeypatch.setattr(PlaywrightUploader, "_wait_for_toasts", lambda self, page: None)
    monkeypatch.setattr(PlaywrightUploader, "_await_listing", lambda self, page, frame, expected, base: "uploaded_final.xlsx")
    monkeypatch.setattr(PlaywrightUploader, "_reload_and_verify", lambda self, page, path, expected: (frame, "uploaded_final.xlsx"))
    exported_path = flow.downloads_dir / "result.csv"
    monkeypatch.setattr(PlaywrightUploader, "_download_results", lambda self, page, frame: exported_path)
    monkeypatch.setattr(PlaywrightUploader, "_capture_screenshot", lambda self, page, label: flow.screenshots_dir / "success.png")
    monkeypatch.setattr(PlaywrightUploader, "_start_trace", lambda self, page: TraceSession(False, None))
    monkeypatch.setattr(PlaywrightUploader, "_stop_trace", lambda self, page, session, export: None)

    file_path = tmp_path / "source.xlsx"
    file_path.write_text("demo")
    result = uploader.upload(
        path="企业盘/财务",
        file_path=file_path,
        conflict_strategy="overwrite",
        export_results=True,
    )

    assert result.status == "uploaded"
    assert result.final_name == "uploaded_final.xlsx"
    assert result.conflict_strategy == "overwrite"
    assert result.downloads == (exported_path,)
    assert result.screenshot == flow.screenshots_dir / "success.png"
    assert result.console_log is None


def test_upload_conflict_rename(monkeypatch, tmp_path, drive_selectors):
    page = _stub_page()
    flow = DummyFlow(page, tmp_path)
    uploader = PlaywrightUploader(flow, drive_selectors)
    frame = MagicMock()

    class FakeResolver:
        def __init__(self, *args, **kwargs):
            pass

        def resolve(self, path, create_missing=False):
            return None

        def workspace_frame(self):
            return frame

    monkeypatch.setattr("autoflow.services.upload.playwright_uploader.PathResolver", FakeResolver)
    monkeypatch.setattr(PlaywrightUploader, "_file_exists", lambda self, frame, name: False)
    monkeypatch.setattr(PlaywrightUploader, "_perform_upload", lambda self, frame, file_path: True)
    monkeypatch.setattr(
        PlaywrightUploader,
        "_handle_conflict_dialog",
        lambda self, page, strategy, requested_name: ConflictResolution("uploaded", "rename", "renamed.xlsx"),
    )
    monkeypatch.setattr(PlaywrightUploader, "_wait_for_toasts", lambda self, page: None)
    monkeypatch.setattr(PlaywrightUploader, "_await_listing", lambda self, page, frame, expected, base: "renamed.xlsx")
    monkeypatch.setattr(PlaywrightUploader, "_reload_and_verify", lambda self, page, path, expected: (frame, "renamed.xlsx"))
    monkeypatch.setattr(PlaywrightUploader, "_download_results", lambda self, page, frame: None)
    monkeypatch.setattr(PlaywrightUploader, "_capture_screenshot", lambda self, page, label: None)
    monkeypatch.setattr(PlaywrightUploader, "_start_trace", lambda self, page: TraceSession(False, None))
    monkeypatch.setattr(PlaywrightUploader, "_stop_trace", lambda self, page, session, export: None)

    file_path = tmp_path / "doc.xlsx"
    file_path.write_text("demo")
    result = uploader.upload(
        path="企业盘/财务",
        file_path=file_path,
        conflict_strategy="rename",
        export_results=False,
    )

    assert result.status == "uploaded"
    assert result.final_name == "renamed.xlsx"
    assert result.conflict_strategy == "rename"


def test_upload_failure_raises(monkeypatch, tmp_path, drive_selectors):
    page = _stub_page()
    flow = DummyFlow(page, tmp_path)
    uploader = PlaywrightUploader(flow, drive_selectors)

    class FakeResolver:
        def __init__(self, *args, **kwargs):
            pass

        def resolve(self, path, create_missing=False):
            return None

        def workspace_frame(self):
            return MagicMock()

    monkeypatch.setattr("autoflow.services.upload.playwright_uploader.PathResolver", FakeResolver)
    monkeypatch.setattr(PlaywrightUploader, "_file_exists", lambda self, frame, name: False)
    monkeypatch.setattr(PlaywrightUploader, "_perform_upload", lambda self, frame, file_path: False)
    screenshot_path = flow.screenshots_dir / "failed.png"
    trace_path = flow.trace_dir / "trace.zip"
    trace_path.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(PlaywrightUploader, "_capture_screenshot", lambda self, page, label: screenshot_path)
    monkeypatch.setattr(PlaywrightUploader, "_start_trace", lambda self, page: TraceSession(True, trace_path))

    def _stop_trace(self, page, session, export):  # noqa: ANN001
        with zipfile.ZipFile(trace_path, "w") as handler:
            handler.writestr("trace.json", "{}")
        return trace_path

    monkeypatch.setattr(PlaywrightUploader, "_stop_trace", _stop_trace)

    file_path = tmp_path / "bad.xlsx"
    file_path.write_text("demo")

    with pytest.raises(UploadFlowError) as exc:
        uploader.upload(path="企业盘/财务", file_path=file_path, conflict_strategy="overwrite")

    assert "截图:" in str(exc.value)
    assert str(screenshot_path) in str(exc.value)
    assert str(trace_path) in str(exc.value)
    assert exc.value.result is not None
    assert exc.value.result.screenshot == screenshot_path
    assert exc.value.result.trace == trace_path
    assert trace_path.exists() and zipfile.is_zipfile(trace_path)


def test_upload_toast_timeout(monkeypatch, tmp_path, drive_selectors):
    page = _stub_page()
    flow = DummyFlow(page, tmp_path)
    uploader = PlaywrightUploader(flow, drive_selectors)
    frame = MagicMock()

    class FakeResolver:
        def __init__(self, *args, **kwargs):
            pass

        def resolve(self, path, create_missing=False):
            return None

        def workspace_frame(self):
            return frame

    monkeypatch.setattr("autoflow.services.upload.playwright_uploader.PathResolver", FakeResolver)
    monkeypatch.setattr(PlaywrightUploader, "_file_exists", lambda self, frame, name: False)
    monkeypatch.setattr(PlaywrightUploader, "_perform_upload", lambda self, frame, file_path: True)
    monkeypatch.setattr(
        PlaywrightUploader,
        "_handle_conflict_dialog",
        lambda self, page, strategy, requested_name: ConflictResolution("uploaded", strategy, requested_name),
    )

    def raise_timeout(self, page):  # noqa: ANN001
        raise UploadFlowError("未检测到上传成功提示")

    monkeypatch.setattr(PlaywrightUploader, "_wait_for_toasts", raise_timeout)
    screenshot_path = flow.screenshots_dir / "toast.png"
    trace_path = flow.trace_dir / "toast.zip"
    monkeypatch.setattr(PlaywrightUploader, "_capture_screenshot", lambda self, page, label: screenshot_path)
    monkeypatch.setattr(PlaywrightUploader, "_start_trace", lambda self, page: TraceSession(True, trace_path))
    monkeypatch.setattr(PlaywrightUploader, "_stop_trace", lambda self, page, session, export: trace_path)

    file_path = tmp_path / "toast.xlsx"
    file_path.write_text("demo")

    with pytest.raises(UploadFlowError) as exc:
        uploader.upload(path="企业盘/财务", file_path=file_path, conflict_strategy="skip")

    assert "未检测到上传成功提示" in str(exc.value)
    assert exc.value.result is not None


def test_upload_refresh_missing(monkeypatch, tmp_path, drive_selectors):
    page = _stub_page()
    flow = DummyFlow(page, tmp_path)
    uploader = PlaywrightUploader(flow, drive_selectors)
    frame = MagicMock()

    class FakeResolver:
        def __init__(self, *args, **kwargs):
            pass

        def resolve(self, path, create_missing=False):
            return None

        def workspace_frame(self):
            return frame

    monkeypatch.setattr("autoflow.services.upload.playwright_uploader.PathResolver", FakeResolver)
    monkeypatch.setattr(PlaywrightUploader, "_file_exists", lambda self, frame, name: False)
    monkeypatch.setattr(PlaywrightUploader, "_perform_upload", lambda self, frame, file_path: True)
    monkeypatch.setattr(
        PlaywrightUploader,
        "_handle_conflict_dialog",
        lambda self, page, strategy, requested_name: ConflictResolution("uploaded", strategy, requested_name),
    )
    monkeypatch.setattr(PlaywrightUploader, "_wait_for_toasts", lambda self, page: None)
    monkeypatch.setattr(PlaywrightUploader, "_await_listing", lambda self, page, frame, expected, base: expected)

    def fail_reload(self, page, path, expected):  # noqa: ANN001
        raise UploadFlowError("上传文件未出现在当前目录")

    monkeypatch.setattr(PlaywrightUploader, "_reload_and_verify", fail_reload)
    monkeypatch.setattr(PlaywrightUploader, "_capture_screenshot", lambda self, page, label: flow.screenshots_dir / "refresh.png")
    monkeypatch.setattr(PlaywrightUploader, "_start_trace", lambda self, page: TraceSession(False, None))
    monkeypatch.setattr(PlaywrightUploader, "_stop_trace", lambda self, page, session, export: None)

    file_path = tmp_path / "missing.xlsx"
    file_path.write_text("demo")

    with pytest.raises(UploadFlowError) as exc:
        uploader.upload(path="企业盘/财务", file_path=file_path, conflict_strategy="skip")

    assert "上传文件未出现在当前目录" in str(exc.value)


def test_download_results_saves_file(monkeypatch, tmp_path, drive_selectors):
    page = _stub_page()
    flow = DummyFlow(page, tmp_path)
    uploader = PlaywrightUploader(flow, drive_selectors)
    frame = MagicMock()

    download = MagicMock()
    download.suggested_filename = "report.csv"
    download.save_as = MagicMock()
    download_context = MagicMock()
    download_context.__enter__.return_value = SimpleNamespace(value=download)
    download_context.__exit__.return_value = False
    page.expect_download.return_value = download_context

    monkeypatch.setattr(PlaywrightUploader, "_click_selector_set", lambda self, container, selectors: True)

    target = uploader._download_results(page, frame)

    assert target is not None
    download.save_as.assert_called_once()
    assert target.parent == flow.downloads_dir
