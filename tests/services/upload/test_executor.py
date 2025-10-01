from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from autoflow.config import load_drive_upload_selectors
from autoflow.services.upload.executor import DriveUploadExecutor, UploadWorkItem
from autoflow.services.upload.playwright_uploader import UploadFlowError, UploadResult


@pytest.fixture(scope="module")
def drive_selectors():
    return load_drive_upload_selectors()


class StubFlow:
    def __init__(self, base_dir: Path):
        self._page = MagicMock()
        self.screenshots_dir = base_dir / "shots"
        self.screenshots_dir.mkdir(parents=True, exist_ok=True)
        self.downloads_dir = base_dir / "downloads"
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
        self.trace_dir = base_dir / "traces"
        self.trace_dir.mkdir(parents=True, exist_ok=True)
        self.console_dir = base_dir / "console"
        self.console_dir.mkdir(parents=True, exist_ok=True)

    def ensure_ready(self):
        return self._page

    def environment_snapshot(self):
        return {
            "playwrightVersion": "1.42.0",
            "browserName": "chromium",
            "browserVersion": "123",
            "browserChannel": "chromium",
            "os": "TestOS",
        }


class DummyUploader:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = 0

    def upload(self, **kwargs):
        self.calls += 1
        action = next(self.responses)
        if isinstance(action, Exception):
            raise action
        return action


def _result(
    *,
    status: str,
    path: str,
    requested: str,
    final: str | None = None,
    screenshot: Path | None = None,
    trace: Path | None = None,
    console: Path | None = None,
):
    return UploadResult(
        status=status,  # type: ignore[arg-type]
        requested_path=path,
        requested_name=requested,
        final_name=final,
        conflict_strategy="skip",
        screenshot=screenshot,
        trace=trace,
        downloads=(),
        message=None,
        console_log=console,
    )


def test_executor_retries_transient_failure(monkeypatch, tmp_path, drive_selectors):
    flow = StubFlow(tmp_path)
    upload_file = tmp_path / "report.xlsx"
    upload_file.write_text("demo")

    transient = UploadFlowError("timeout", result=_result(status="failed", path="企业盘/财务", requested="report.xlsx"))
    transient.__cause__ = TimeoutError("transient")
    success = _result(status="uploaded", path="企业盘/财务", requested="report.xlsx", final="report.xlsx")
    uploader = DummyUploader([transient, transient, success])
    monkeypatch.setattr("autoflow.services.upload.executor.time.sleep", lambda _: None)

    executor = DriveUploadExecutor(flow, drive_selectors, uploader=uploader, max_retries=3, base_backoff=0.1)
    report = executor.run_batch(dest_path="企业盘/财务", files=[UploadWorkItem(upload_file)], conflict_strategy="overwrite")

    assert uploader.calls == 3
    assert len(report["success"]) == 1
    assert report["failed"] == []
    assert report["renamed"] == []
    assert report["debugChecklist"]["environment"]["browserName"] == "chromium"


def test_executor_records_failure_artifacts(tmp_path, drive_selectors):
    flow = StubFlow(tmp_path)
    upload_file = tmp_path / "客户名单.xlsx"
    upload_file.write_text("demo")

    screenshot = flow.screenshots_dir / "failed.png"
    screenshot.touch()
    trace = flow.trace_dir / "trace.zip"
    trace.touch()
    console = flow.console_dir / "console.log"
    console.touch()

    failure_result = _result(
        status="failed",
        path="企业盘/财务",
        requested="客户名单.xlsx",
        screenshot=screenshot,
        trace=trace,
        console=console,
    )
    failure = UploadFlowError("network issue", result=failure_result)
    uploader = DummyUploader([failure])
    executor = DriveUploadExecutor(flow, drive_selectors, uploader=uploader, max_retries=0)

    report = executor.run_batch(dest_path="企业盘/财务", files=[UploadWorkItem(upload_file)])

    assert len(report["failed"]) == 1
    failure_entry = report["failed"][0]
    assert "客户名单.xlsx" not in failure_entry["reason"]
    debug_artifacts = report["debugChecklist"]["artifacts"]["failures"]["客户名单.xlsx"]
    assert debug_artifacts["screenshot"] == str(screenshot)
    assert debug_artifacts["trace"] == str(trace)
    assert debug_artifacts["console"] == str(console)


def test_executor_redacts_paths(tmp_path, drive_selectors):
    flow = StubFlow(tmp_path)
    upload_file = tmp_path / "客户12345.xlsx"
    upload_file.write_text("demo")

    failure_result = _result(status="failed", path="企业盘/财务", requested="客户12345.xlsx")
    error = UploadFlowError(f"failed processing {upload_file}", result=failure_result)
    uploader = DummyUploader([error])
    executor = DriveUploadExecutor(flow, drive_selectors, uploader=uploader, max_retries=0)

    report = executor.run_batch(dest_path="企业盘/财务", files=[UploadWorkItem(upload_file)])

    reason = report["failed"][0]["reason"]
    assert str(upload_file) not in reason
    assert "***.xlsx" in reason
