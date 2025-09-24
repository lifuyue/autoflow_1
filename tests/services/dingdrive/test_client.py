from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from autoflow.services.dingdrive.client import DingDriveClient
from autoflow.services.dingdrive.config import DingDriveConfig, RetryConfig
from autoflow.services.dingdrive.http import DingTalkAuth, HttpClient
from autoflow.services.dingdrive.models import DriveNotFound


@dataclass
class MockResponse:
    status_code: int = 200
    json_data: dict[str, Any] | None = None
    text_data: str | None = None

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


class FakeSession:
    def __init__(self, responses: list[MockResponse]) -> None:
        self._responses = responses
        self.headers: dict[str, str] = {}
        self.verify = True
        self.trust_env = False
        self.proxies: dict[str, str] = {}
        self.calls: list[tuple[str, str]] = []

    def request(self, method: str, url: str, **kwargs: Any) -> MockResponse:
        if not self._responses:
            raise AssertionError("No more responses queued")
        self.calls.append((method, url))
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
    http_client = HttpClient(config, session=session)
    auth = DingTalkAuth(config, session=session)
    client = DingDriveClient(config, http_client=http_client, auth=auth)
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
