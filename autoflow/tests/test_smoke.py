from __future__ import annotations

from pathlib import Path
import pandas as pd

from autoflow.core.pipeline import Pipeline
from autoflow.core.profiles import load_profiles, ensure_work_dirs, resolve_config_path
from autoflow.services.download.base import ICloudProvider
from autoflow.services.upload.base import IUploader


class FakeDownloadProvider(ICloudProvider):
    def __init__(self):
        self.generated = None

    def download(self, profile, dest_dir: Path, credentials_provider=None):  # type: ignore[override]
        dest_dir.mkdir(parents=True, exist_ok=True)
        p = dest_dir / "fake_input.xlsx"
        df = pd.DataFrame(
            {
                "金额": [100, 200],
                "币种": ["CNY", "CNY"],
                "日期": ["2024-05-10", "2024-05-10"],
                "项目名称": ["Smoke A", "Smoke B"],
            }
        )
        df.to_excel(p, index=False)
        self.generated = p
        return [str(p)]


class FakeUploader(IUploader):
    def upload(self, profile, file_path: Path, shots_dir: Path, credentials_provider=None):  # type: ignore[override]
        # No-op uploader
        return {"status": "ok", "file": str(file_path)}


def test_smoke_pipeline_tmp(tmp_path):
    profiles = load_profiles()
    profile = next(iter(profiles.values()))
    work = ensure_work_dirs()
    fake_dl = FakeDownloadProvider()
    fake_up = FakeUploader()
    pipeline = Pipeline(download_provider=fake_dl, uploader=fake_up)
    res = pipeline.run(profile=profile, out_dir=tmp_path)
    out = Path(res["output_path"])
    assert out.exists()
