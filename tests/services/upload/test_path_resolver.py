from unittest.mock import MagicMock

import pytest

from autoflow.config import load_drive_upload_selectors
from autoflow.services.upload.path_resolver import (
    PathNotFoundError,
    PathResolver,
    PathResolverError,
    PathValidationError,
)


@pytest.fixture(scope="module")
def drive_selectors():
    return load_drive_upload_selectors()


@pytest.fixture()
def resolver(drive_selectors):
    page = MagicMock()
    return PathResolver(page=page, selectors=drive_selectors)


def test_resolve_existing_path(monkeypatch, resolver):
    monkeypatch.setattr(resolver, "_select_space", MagicMock())
    monkeypatch.setattr(resolver, "_ensure_workspace_frame", MagicMock(return_value="frame"))
    enter_mock = MagicMock(side_effect=[True, True])
    monkeypatch.setattr(resolver, "_enter_existing_folder", enter_mock)
    create_mock = MagicMock()
    monkeypatch.setattr(resolver, "_create_folder", create_mock)

    resolver.resolve("企业盘/财务部/报表室", create_missing=False)

    assert enter_mock.call_count == 2
    create_mock.assert_not_called()


def test_resolve_creates_missing(monkeypatch, resolver):
    monkeypatch.setattr(resolver, "_select_space", MagicMock())
    monkeypatch.setattr(resolver, "_ensure_workspace_frame", MagicMock(return_value="frame"))
    enter_mock = MagicMock(return_value=False)
    monkeypatch.setattr(resolver, "_enter_existing_folder", enter_mock)
    create_mock = MagicMock()
    monkeypatch.setattr(resolver, "_create_folder", create_mock)

    resolver.resolve("企业盘/新建目录", create_missing=True)

    create_mock.assert_called_once_with("frame", "新建目录")


def test_resolve_missing_without_create(monkeypatch, resolver):
    monkeypatch.setattr(resolver, "_select_space", MagicMock())
    monkeypatch.setattr(resolver, "_ensure_workspace_frame", MagicMock(return_value="frame"))
    monkeypatch.setattr(resolver, "_enter_existing_folder", MagicMock(return_value=False))

    with pytest.raises(PathNotFoundError):
        resolver.resolve("企业盘/不存在", create_missing=False)


@pytest.mark.parametrize(
    "invalid_path, exc",
    [
        ("", PathValidationError),
        ("企业盘/", PathValidationError),
        ("企业盘//财务", PathValidationError),
        ("企业盘/财务/财务", PathValidationError),
        ("企业盘/财务?", PathValidationError),
    ],
)
def test_invalid_paths_raise(resolver, invalid_path, exc):
    with pytest.raises(exc):
        resolver.resolve(invalid_path)


def test_invalid_space_label(resolver):
    with pytest.raises(PathValidationError):
        resolver.resolve("未知盘/目录")


def test_resolve_create_folder_failure(monkeypatch, resolver):
    monkeypatch.setattr(resolver, "_select_space", MagicMock())
    monkeypatch.setattr(resolver, "_ensure_workspace_frame", MagicMock(return_value="frame"))
    monkeypatch.setattr(resolver, "_enter_existing_folder", MagicMock(return_value=False))

    def fail_create(frame, name):  # noqa: ANN001
        raise PathResolverError("permission denied")

    monkeypatch.setattr(resolver, "_create_folder", fail_create)

    with pytest.raises(PathResolverError):
        resolver.resolve("企业盘/只读", create_missing=True)
