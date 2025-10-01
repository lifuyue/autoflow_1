"""Batch upload executor with retry, reporting, and redacted logging."""

from __future__ import annotations

import platform
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Sequence

try:  # pragma: no cover - optional Playwright availability
    from playwright.sync_api import Error as PlaywrightError, TimeoutError as PlaywrightTimeoutError
except Exception:  # pragma: no cover - fallback types for test environments
    PlaywrightTimeoutError = TimeoutError

    class PlaywrightError(Exception):  # type: ignore[override]
        """Placeholder Playwright error when library is absent."""

from autoflow.config import DriveUploadSelectors
from autoflow.core.logger import get_logger

from .playwright_uploader import PlaywrightUploader, UploadFlowError, UploadResult


RETRYABLE_EXCEPTIONS = (
    PlaywrightTimeoutError,
    PlaywrightError,
    TimeoutError,
    ConnectionError,
)


@dataclass(slots=True)
class UploadWorkItem:
    """Single file upload task definition."""

    file_path: Path
    name: str | None = None


class DriveUploadExecutor:
    """Coordinate Playwright-based uploads with retries and reporting."""

    def __init__(
        self,
        flow,
        selectors: DriveUploadSelectors,
        *,
        uploader: PlaywrightUploader | None = None,
        max_retries: int = 2,
        base_backoff: float = 1.0,
        logger=None,
    ) -> None:
        self.flow = flow
        self.selectors = selectors
        self.logger = logger or get_logger()
        self.max_retries = max(0, max_retries)
        self.base_backoff = max(0.1, base_backoff)
        self.uploader = uploader or PlaywrightUploader(flow, selectors, logger=self.logger)

    # ------------------------------------------------------------------
    def run_batch(
        self,
        *,
        dest_path: str,
        files: Sequence[UploadWorkItem],
        conflict_strategy: Literal["skip", "overwrite", "rename"] = "skip",
        create_missing: bool = False,
        export_results: bool = False,
        tenant: str | None = None,
        batch_id: str | None = None,
    ) -> dict[str, object]:
        start = time.monotonic()
        batch_key = batch_id or uuid.uuid4().hex
        environment = self._collect_environment_info()
        artifacts = {"success": {}, "failures": {}}
        successes: list[dict[str, str]] = []
        failures: list[dict[str, object]] = []
        skipped: list[str] = []
        renamed: list[dict[str, str]] = []

        for item in files:
            file_path = item.file_path
            requested_name = item.name or file_path.name
            sanitized_path = self._redact_path(file_path)
            attempts = 0
            while True:
                attempts += 1
                try:
                    result = self.uploader.upload(
                        path=dest_path,
                        file_path=file_path,
                        conflict_strategy=conflict_strategy,
                        create_missing=create_missing,
                        export_results=export_results,
                    )
                    if result.status == "uploaded":
                        successes.append({"name": result.final_name or requested_name, "requested": requested_name})
                        artifacts["success"][requested_name] = self._artifact_payload(result)
                        if result.final_name and result.final_name != requested_name:
                            renamed.append({"old": requested_name, "new": result.final_name})
                    elif result.status == "skipped":
                        skipped.append(requested_name)
                        artifacts["success"][requested_name] = self._artifact_payload(result)
                    break
                except UploadFlowError as exc:  # noqa: BLE001
                    reason = self._sanitize_reason(str(exc), file_path)
                    if attempts <= self.max_retries and self._is_retryable(exc):
                        delay = self.base_backoff * (2 ** (attempts - 1))
                        self.logger.warning(
                            "drive.executor transient_failure attempt=%s/%s file=%s reason=%s",
                            attempts,
                            self.max_retries + 1,
                            sanitized_path,
                            reason,
                        )
                        time.sleep(delay)
                        continue

                    failure_record: dict[str, object] = {
                        "name": requested_name,
                        "reason": reason,
                    }
                    result_obj = exc.result
                    if result_obj is not None:
                        failure_record["artifacts"] = self._artifact_payload(result_obj)
                        artifacts["failures"][requested_name] = self._artifact_payload(result_obj)
                    failures.append(failure_record)
                    break

        duration = time.monotonic() - start
        report: dict[str, object] = {
            "batchId": batch_key,
            "destPath": dest_path,
            "tenant": tenant,
            "total": len(files),
            "success": successes,
            "failed": failures,
            "skipped": skipped,
            "renamed": renamed,
            "duration": round(duration, 3),
            "debugChecklist": {
                "environment": environment,
                "artifacts": artifacts,
            },
        }
        return report

    # ------------------------------------------------------------------
    def _is_retryable(self, exc: UploadFlowError) -> bool:
        cause = exc.__cause__ or exc.__context__ or exc
        if isinstance(cause, RETRYABLE_EXCEPTIONS):
            return True
        reason = (str(exc) or "").lower()
        for keyword in ("timeout", "超时", "temporarily", "网络", "connection"):
            if keyword in reason:
                return True
        return False

    @staticmethod
    def _artifact_payload(result: UploadResult | None) -> dict[str, object]:
        if result is None:
            return {}
        payload: dict[str, object] = {}
        if result.screenshot:
            payload["screenshot"] = str(result.screenshot)
        if result.trace:
            payload["trace"] = str(result.trace)
        if result.console_log:
            payload["console"] = str(result.console_log)
        if result.downloads:
            payload["downloads"] = [str(path) for path in result.downloads]
        return payload

    def _collect_environment_info(self) -> dict[str, str | None]:
        if hasattr(self.flow, "environment_snapshot") and callable(getattr(self.flow, "environment_snapshot")):
            try:
                info = dict(self.flow.environment_snapshot())
            except Exception:  # noqa: BLE001
                info = {}
        else:
            info = {}
        info.setdefault("os", platform.platform())
        return info

    @staticmethod
    def _redact_path(path: Path) -> str:
        return DriveUploadExecutor._mask_filename(path)

    def _sanitize_reason(self, message: str, file_path: Path) -> str:
        masked_name = self._mask_filename(file_path)
        sanitized = message.replace(str(file_path), masked_name)
        sanitized = sanitized.replace(file_path.name, masked_name)
        return sanitized.replace("\n", " ").strip()

    @staticmethod
    def _mask_filename(path: Path) -> str:
        suffix = path.suffix
        return f"***{suffix}" if suffix else "***"


__all__ = ["DriveUploadExecutor", "UploadWorkItem"]
