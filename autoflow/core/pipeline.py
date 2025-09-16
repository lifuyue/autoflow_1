from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Any
from decimal import Decimal
import shutil

from .logger import get_logger
from .profiles import ensure_work_dirs, resolve_config_path, Profile
from .errors import DownloadError, TransformError, UploadError
from autoflow.services.download.base import provider_from_config, ICloudProvider
from autoflow.services.upload.base import uploader_from_config, IUploader
from autoflow.services.form_processor import FormProcessConfig, process_forms
from autoflow.services.form_processor.providers import StaticRateProvider


ProgressCB = Callable[[str, str], None]


@dataclass
class PipelineResult:
    profile: str
    input_path: str | None
    output_path: str | None
    upload_result: dict[str, Any] | None


class Pipeline:
    """Coordinates Download -> Transform -> Template -> Upload steps."""

    def __init__(
        self,
        logger=None,
        download_provider: ICloudProvider | None = None,
        uploader: IUploader | None = None,
        rate_provider=None,
    ) -> None:
        self.logger = logger or get_logger()
        self.download_provider = download_provider
        self.uploader = uploader
        self.rate_provider = rate_provider or StaticRateProvider()
        self.work_dirs = ensure_work_dirs()

    def run(
        self,
        profile: Profile,
        out_dir: Path | None = None,
        progress_cb: ProgressCB | None = None,
        credentials_provider: Callable[[bool], dict[str, str] | None] | None = None,
        ui_log_writer: Any | None = None,
    ) -> dict[str, Any]:
        def progress(stage: str, detail: str = ""):
            if progress_cb:
                progress_cb(stage, detail)
            self.logger.info("%s - %s", stage, detail)
            if ui_log_writer is not None:
                ui_log_writer.write(f"{stage} - {detail}\n")

        out_dir = out_dir or self.work_dirs["out"]

        # 1. Download
        progress("1/4 下载", "准备下载源文件")
        provider = self.download_provider or provider_from_config(profile.download)
        input_paths = provider.download(
            profile=profile,
            dest_dir=self.work_dirs["inbox"],
            credentials_provider=credentials_provider,
        )
        if not input_paths:
            raise DownloadError("下载模块未获取到任何文件")
        input_path = Path(input_paths[0])
        progress("1/4 下载", f"完成：{input_path.name}")

        # 2. Transform & Template
        progress("2/4 处理", "读取并清洗…")
        mapping_path = resolve_config_path(profile.transform.get("mapping_file", "autoflow/config/mapping.yaml"))
        round_digits = int(profile.transform.get("round_digits", 2))
        confirm_amount = Decimal(str(profile.transform.get("confirm_over_amount_cny", "20000")))
        base_currency = profile.transform.get("base_currency", "CNY")

        form_config = FormProcessConfig(
            mapping_path=str(mapping_path),
            base_currency=base_currency,
            round_digits=round_digits,
            confirm_over_amount_cny=confirm_amount,
        )

        try:
            form_result = process_forms(
                input_paths=[str(input_path)],
                output_dir=str(out_dir),
                config=form_config,
                rate_provider=self.rate_provider,
                non_interactive=True,
            )
        except Exception as e:  # noqa: BLE001
            raise TransformError(str(e)) from e
        output_path = Path(form_result.output_template_path)
        progress("3/4 套模板", f"生成：{output_path.name}")

        # 3. Upload
        progress("4/4 上传", "上传到目标系统…")
        uploader = self.uploader or uploader_from_config(profile.upload)
        try:
            result = uploader.upload(
                profile=profile,
                file_path=Path(output_path),
                shots_dir=self.work_dirs["shot"],
                credentials_provider=credentials_provider,
            )
        except Exception as e:  # noqa: BLE001
            raise UploadError(str(e)) from e
        progress("4/4 上传", f"完成：{result.get('status','ok')}")

        return {
            "profile": profile.name,
            "input_path": str(input_path),
            "output_path": str(output_path),
            "report_path": form_result.report_path,
            "upload": result,
        }
