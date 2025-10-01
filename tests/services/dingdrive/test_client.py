from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from autoflow.services.dingdrive.auth import AuthClient
from autoflow.services.dingdrive.client import DingDriveClient
from autoflow.services.dingdrive.config import DingDriveConfig, RetryConfig
from autoflow.services.dingdrive.http import HttpClient
from autoflow.services.dingdrive.models import DriveNotFound
from autoflow.services.dingdrive.uploader import DingDriveUploader


@dataclass
class MockResponse:
    status_code: int = 200
    json_data: dict[str, Any] | None = None
    text_data: str | None = None
    headers: dict[str, str] | None = None

    def json(self) -> dict[str, Any]:
        if self.json_data is None:
            raise ValueError("JSON body not set")
        return self.json_data

    @property
    def content(self) -> bytes:
        if self.json_data is not None:
            return json.dumps(self.json_data).encode("utf-8")
        if self.text_data is not None:
            return self.text_data.encode("utf-8")
        return b""

    @property
    def text(self) -> str:
        if self.text_data is not None:
            return self.text_data
        if self.json_data is not None:
            return json.dumps(self.json_data)
        return ""

    def __post_init__(self) -> None:
        if self.headers is None:
            self.headers = {}

    def __enter__(self) -> "MockResponse":  # pragma: no cover - context helper
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # pragma: no cover - context helper
        return False

    def iter_content(self, chunk_size: int = 8192):  # pragma: no cover - stream helper
        if self.text_data is not None:
            data = self.text_data.encode("utf-8")
        elif self.json_data is not None:
            data = json.dumps(self.json_data).encode("utf-8")
        else:
            data = b""
        if not data:
            yield b""
            return
        for idx in range(0, len(data), chunk_size):
            yield data[idx : idx + chunk_size]


class FakeSession:
    def __init__(self, responses: list[MockResponse]) -> None:
        self._responses = responses
        self.headers: dict[str, str] = {}
        self.verify = True
        self.trust_env = False
        self.proxies: dict[str, str] = {}
        self.calls: list[tuple[str, str]] = []
        self.call_kwargs: list[dict[str, Any]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> MockResponse:
        if not self._responses:
            raise AssertionError("No more responses queued")
        self.calls.append((method, url))
        self.call_kwargs.append(kwargs)
        return self._responses.pop(0)

    def get(self, url: str, **kwargs: Any) -> MockResponse:
        return self.request("GET", url, **kwargs)

    def close(self) -> None:  # pragma: no cover - nothing to close in tests
        pass


def _build_client(responses: list[MockResponse]) -> tuple[DingDriveClient, FakeSession]:
    config = DingDriveConfig(
        app_key="app",
        app_secret="secret",
        space_id="space123",
        timeout_sec=1.0,
        retries=RetryConfig(max_attempts=3, backoff_ms=1, max_backoff_ms=1),
    )
    session = FakeSession(responses)
    auth = AuthClient(config, session=session)
    http_client = HttpClient(config, session=session, auth_client=auth)
    client = DingDriveClient(config, http_client=http_client)
    return client, session


def test_list_refreshes_token_on_unauthorized() -> None:
    client, session = _build_client(
        [
            MockResponse(json_data={"access_token": "token1", "expires_in": 1}),
            MockResponse(status_code=401, json_data={"code": "AccessDenied"}),
            MockResponse(json_data={"access_token": "token2", "expires_in": 3600}),
            MockResponse(json_data={"items": [{"id": "a", "name": "FileA", "type": "file"}]}),
        ]
    )
    items = client.list_children("root")
    assert [call for call in session.calls if call[0] == "GET" and "files" in call[1]]
    assert items[0]["name"] == "FileA"
    client.close()


def test_delete_raises_not_found() -> None:
    client, session = _build_client(
        [
            MockResponse(json_data={"access_token": "token", "expires_in": 3600}),
            MockResponse(status_code=404, json_data={"code": "NotFound"}),
        ]
    )
    with pytest.raises(DriveNotFound):
        client.delete("missing")
    assert session.calls[-1][0] == "DELETE"
    client.close()


def test_upload_single_part_success(tmp_path: Path) -> None:
    artifact = tmp_path / "sample.txt"
    artifact.write_text("hello drive", encoding="utf-8")

    client, session = _build_client(
        [
            MockResponse(json_data={"access_token": "token", "expires_in": 3600}),
            MockResponse(
                json_data={
                    "uploadKey": "key123",
                    "uploadUrl": "https://upload.example/object",
                    "httpMethod": "PUT",
                    "resourceId": "file123",
                    "name": "sample.txt",
                }
            ),
            MockResponse(status_code=200),
            MockResponse(json_data={"fileId": "file123"}),
        ]
    )

    file_id = client.upload_file("root", str(artifact))
    assert file_id == "file123"
    assert any(call for call in session.calls if call[0] == "PUT" and "upload.example" in call[1])
    client.close()


def test_retry_on_server_error(monkeypatch: pytest.MonkeyPatch) -> None:
    client, session = _build_client(
        [
            MockResponse(json_data={"access_token": "token", "expires_in": 3600}),
            MockResponse(status_code=500, text_data="server error"),
            MockResponse(json_data={"items": []}),
        ]
    )
    monkeypatch.setattr("autoflow.services.dingdrive.http.time.sleep", lambda *_: None)
    items = client.list_children("root")
    assert items == []
    assert session.calls.count(("GET", "https://api.dingtalk.com/v1.0/drive/spaces/space123/files")) == 2
    client.close()


def test_auth_client_caches_token(monkeypatch: pytest.MonkeyPatch) -> None:
    config = DingDriveConfig(
        app_key="app",
        app_secret="secret",
        space_id="space123",
        timeout_sec=1.0,
        retries=RetryConfig(max_attempts=2, backoff_ms=1, max_backoff_ms=1),
    )
    session = FakeSession(
        [
            MockResponse(json_data={"access_token": "tok1", "expires_in": 120}),
            MockResponse(json_data={"access_token": "tok2", "expires_in": 120}),
        ]
    )
    auth = AuthClient(config, session=session)
    clock = {"now": 0.0}

    def fake_monotonic() -> float:
        return clock["now"]

    monkeypatch.setattr("autoflow.services.dingdrive.auth.time.monotonic", fake_monotonic)

    first = auth.get_token()
    clock["now"] = 30.0
    second = auth.get_token()
    assert first == second
    clock["now"] = 200.0
    third = auth.get_token()
    assert third != first
    fourth = auth.get_token()
    assert fourth == third
    token_calls = [call for call in session.calls if call[0] == "GET" and "gettoken" in call[1]]
    assert len(token_calls) == 2


def test_uploader_switches_threshold(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = DingDriveConfig(
        app_key="app",
        app_secret="secret",
        space_id="space123",
        timeout_sec=1.0,
        retries=RetryConfig(max_attempts=2, backoff_ms=1, max_backoff_ms=1),
        multipart_threshold=8,
        upload_chunk_size=4,
        upload_concurrency=1,
    )
    session = FakeSession([])
    auth = AuthClient(config, session=session)
    http_client = HttpClient(config, session=session, auth_client=auth)
    uploader = DingDriveUploader(config, http_client)

    called: dict[str, bool] = {}

    def fake_small(parent_id: str, file_path: Path, *, display_name: str, progress_cb=None) -> str:  # type: ignore[override]
        called["small"] = True
        return "small-id"

    def fake_multi(parent_id: str, file_path: Path, *, display_name: str, progress_cb=None) -> str:  # type: ignore[override]
        called["multi"] = True
        return "multi-id"

    monkeypatch.setattr(uploader, "upload_small", fake_small)
    monkeypatch.setattr(uploader, "upload_multipart", fake_multi)

    small_file = tmp_path / "small.bin"
    small_file.write_bytes(b"a" * 4)
    uploader.upload("root", small_file, display_name="small.bin")
    assert called.get("small") is True

    large_file = tmp_path / "large.bin"
    large_file.write_bytes(b"b" * 16)
    uploader.upload("root", large_file, display_name="large.bin")
    assert called.get("multi") is True


def test_multipart_retries_failed_part(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = DingDriveConfig(
        app_key="app",
        app_secret="secret",
        space_id="space123",
        timeout_sec=1.0,
        retries=RetryConfig(max_attempts=2, backoff_ms=1, max_backoff_ms=1),
        multipart_threshold=4,
        upload_chunk_size=5,
        upload_concurrency=1,
    )
    responses = [
        MockResponse(json_data={"access_token": "token", "expires_in": 3600}),
        MockResponse(
            json_data={
                "uploadKey": "key123",
                "parts": [
                    {
                        "uploadUrl": "https://upload.example/part1",
                        "partNumber": 1,
                        "httpMethod": "PUT",
                        "headers": {},
                    }
                ],
                "resourceId": "file123",
                "name": "retry.bin",
            }
        ),
        MockResponse(status_code=500, text_data="server error"),
        MockResponse(status_code=200),
        MockResponse(json_data={"fileId": "file123"}),
    ]
    session = FakeSession(responses)
    auth = AuthClient(config, session=session)
    http_client = HttpClient(config, session=session, auth_client=auth)
    uploader = DingDriveUploader(config, http_client)
    monkeypatch.setattr("autoflow.services.dingdrive.uploader.time.sleep", lambda *_: None)

    artifact = tmp_path / "retry.bin"
    artifact.write_bytes(b"x" * 5)

    file_id = uploader.upload("root", artifact, display_name="retry.bin")
    assert file_id == "file123"
    put_calls = [call for call in session.calls if call[0] == "PUT" and "upload.example" in call[1]]
    assert len(put_calls) == 2
