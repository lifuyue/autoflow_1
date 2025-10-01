import threading
import queue
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText
from pathlib import Path
import time
import os

from autoflow.core.logger import get_logger
from autoflow.core.profiles import load_profiles, Profile
from autoflow.core.pipeline import Pipeline
from autoflow.core.errors import AutoFlowError
from autoflow.services.dingdrive.client import DingDriveClient
from autoflow.services.dingdrive.config import resolve_config
from autoflow.services.dingdrive.uploader import UploadProgress


class TkTextHandler:
    """A simple logging-like handler that writes lines to a Tkinter Text widget."""

    def __init__(self, text_widget: ScrolledText):
        self.text_widget = text_widget

    def write(self, msg: str):
        if not msg:
            return
        self.text_widget.configure(state=tk.NORMAL)
        self.text_widget.insert(tk.END, msg)
        self.text_widget.see(tk.END)
        self.text_widget.configure(state=tk.DISABLED)

    def flush(self):
        pass


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("AutoFlow - XLSX 下载/处理/套模板/上传")
        self.geometry("860x520")

        self.logger = get_logger()
        self.profiles = load_profiles()
        self.selected_profile_name = tk.StringVar()
        try:
            from autoflow.core.profiles import _work_dir
            self.output_dir = tk.StringVar(value=str((_work_dir() / "out").resolve()))
        except Exception:
            self.output_dir = tk.StringVar(value=str(Path("autoflow/work/out").resolve()))
        self.drive_parent = tk.StringVar(value="root")

        self._build_ui()
        self.progress_queue: queue.Queue = queue.Queue()
        self.worker_thread: threading.Thread | None = None
        self.drive_thread: threading.Thread | None = None

    def _build_ui(self):
        # Profile selection
        frm_top = ttk.Frame(self)
        frm_top.pack(fill=tk.X, padx=10, pady=10)

        ttk.Label(frm_top, text="抬头/账号：").pack(side=tk.LEFT)
        profile_keys = list(self.profiles.keys())
        self.cmb_profile = ttk.Combobox(
            frm_top, textvariable=self.selected_profile_name, values=profile_keys, state="readonly", width=40
        )
        if profile_keys:
            self.cmb_profile.current(0)
        self.cmb_profile.pack(side=tk.LEFT, padx=5)

        ttk.Label(frm_top, text="导出目录：").pack(side=tk.LEFT, padx=(15, 0))
        self.ent_outdir = ttk.Entry(frm_top, textvariable=self.output_dir, width=50)
        self.ent_outdir.pack(side=tk.LEFT, padx=5)
        ttk.Button(frm_top, text="浏览...", command=self._choose_outdir).pack(side=tk.LEFT)

        # Buttons
        frm_btn = ttk.Frame(self)
        frm_btn.pack(fill=tk.X, padx=10)
        self.btn_start = ttk.Button(frm_btn, text="开始", command=self._on_start)
        self.btn_start.pack(side=tk.LEFT)
        ttk.Button(frm_btn, text="退出", command=self.destroy).pack(side=tk.RIGHT)

        # Drive upload controls
        frm_drive = ttk.Frame(self)
        frm_drive.pack(fill=tk.X, padx=10, pady=(5, 5))
        ttk.Label(frm_drive, text="Drive 目录：").pack(side=tk.LEFT)
        self.ent_drive_parent = ttk.Entry(frm_drive, textvariable=self.drive_parent, width=40)
        self.ent_drive_parent.pack(side=tk.LEFT, padx=5)
        ttk.Button(frm_drive, text="上传到 Drive…", command=self._on_upload_drive).pack(side=tk.LEFT)

        # Progress
        frm_prog = ttk.Labelframe(self, text="进度")
        frm_prog.pack(fill=tk.X, padx=10, pady=(5, 5))
        self.var_stage = tk.StringVar(value="待开始")
        self.var_detail = tk.StringVar(value="")
        ttk.Label(frm_prog, textvariable=self.var_stage).pack(side=tk.LEFT, padx=(8, 12))
        ttk.Label(frm_prog, textvariable=self.var_detail).pack(side=tk.LEFT)

        # Log area
        frm_log = ttk.Labelframe(self, text="运行日志")
        frm_log.pack(fill=tk.BOTH, expand=True, padx=10, pady=(5, 10))
        self.txt_log = ScrolledText(frm_log, height=18, state=tk.DISABLED)
        self.txt_log.pack(fill=tk.BOTH, expand=True)
        self.text_handler = TkTextHandler(self.txt_log)

        self.after(150, self._poll_progress)

    def _choose_outdir(self):
        d = filedialog.askdirectory(initialdir=self.output_dir.get())
        if d:
            self.output_dir.set(d)

    def _on_upload_drive(self) -> None:
        if self.drive_thread and self.drive_thread.is_alive():
            messagebox.showinfo("提示", "Drive 上传正在进行中，请稍候…")
            return
        selected_file = filedialog.askopenfilename(title="选择要上传的文件")
        if not selected_file:
            return

        file_name = Path(selected_file).name
        parent_hint = self.drive_parent.get().strip() or "root"
        profile_hint = os.getenv("AUTOFLOW_DINGDRIVE_PROFILE")
        current_profile = self.selected_profile_name.get()
        if not profile_hint and current_profile and current_profile in self.profiles:
            meta = self.profiles[current_profile].meta or {}
            profile_hint = meta.get("dingdrive_profile")
        if profile_hint:
            profile_hint = profile_hint.strip() or None

        self.progress_queue.put(("Drive 上传", f"准备上传 {file_name}"))

        def progress_cb(progress: UploadProgress) -> None:
            total = progress.total_bytes or 0
            percent = 0.0 if not total else (progress.uploaded_bytes / total) * 100
            detail = (
                f"{progress.state} {progress.completed_parts}/{progress.total_parts} "
                f"{progress.uploaded_bytes}/{total} ({percent:.1f}%)"
            )
            self.progress_queue.put(("Drive 上传进度", detail))

        def worker() -> None:
            client: DingDriveClient | None = None
            try:
                config = resolve_config(profile_hint)
                client = DingDriveClient(config, logger=self.logger)
                target_parent = parent_hint
                if target_parent.startswith("id:"):
                    target_parent = target_parent[3:]
                elif "/" in target_parent:
                    target_parent = client.ensure_folder(target_parent)
                else:
                    target_parent = target_parent or client.resolve_default_parent()
                file_id = client.upload_file(target_parent, selected_file, progress_cb=progress_cb)
                self.progress_queue.put(("Drive 上传完成", f"文件ID {file_id}"))
            except Exception as exc:  # noqa: BLE001 - surface to UI
                self.logger.exception("Drive upload failed")
                self.progress_queue.put(("Drive 上传失败", str(exc)))
            finally:
                if client is not None:
                    client.close()
                self.drive_thread = None

        self.drive_thread = threading.Thread(target=worker, daemon=True)
        self.drive_thread.start()

    def _on_start(self):
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("提示", "正在运行中，请稍候…")
            return
        sel = self.selected_profile_name.get()
        if not sel:
            messagebox.showwarning("提示", "请先选择一个抬头/账号。")
            return
        profile = self.profiles[sel]
        self._run_profile(profile)

    def _run_profile(self, profile: Profile):
        self.var_stage.set("准备中…")
        self.var_detail.set("")
        self.txt_log.configure(state=tk.NORMAL)
        self.txt_log.delete("1.0", tk.END)
        self.txt_log.configure(state=tk.DISABLED)

        def progress(stage: str, detail: str = ""):
            self.progress_queue.put((stage, detail))

        def ask_credentials(prompt_user=True):
            # Prompt for username/password only when needed
            if not prompt_user:
                return None
            dialog = tk.Toplevel(self)
            dialog.title("登录凭据")
            dialog.grab_set()
            u_var = tk.StringVar()
            p_var = tk.StringVar()
            ttk.Label(dialog, text="用户名：").grid(row=0, column=0, padx=8, pady=6)
            ttk.Entry(dialog, textvariable=u_var, width=32).grid(row=0, column=1, padx=8, pady=6)
            ttk.Label(dialog, text="密码：").grid(row=1, column=0, padx=8, pady=6)
            ttk.Entry(dialog, textvariable=p_var, width=32, show="*").grid(row=1, column=1, padx=8, pady=6)
            result: dict[str, str] = {}

            def ok():
                result["username"] = u_var.get()
                result["password"] = p_var.get()
                dialog.destroy()

            ttk.Button(dialog, text="确定", command=ok).grid(row=2, column=0, columnspan=2, pady=8)
            dialog.wait_window()
            return result if result else None

        def worker():
            try:
                pipeline = Pipeline(logger=self.logger)
                res = pipeline.run(
                    profile=profile,
                    out_dir=Path(self.output_dir.get()),
                    progress_cb=progress,
                    credentials_provider=ask_credentials,
                    ui_log_writer=self.text_handler,
                )
                self.progress_queue.put(
                    (
                        "完成",
                        f"输出：{res.get('output_path','')} 报告：{res.get('report_path','')}",
                    )
                )
            except AutoFlowError as e:
                self.progress_queue.put(("错误", str(e)))
            except Exception as e:  # noqa: BLE001
                self.progress_queue.put(("错误", f"{type(e).__name__}: {e}"))

        self.worker_thread = threading.Thread(target=worker, daemon=True)
        self.worker_thread.start()

    def _poll_progress(self):
        try:
            while True:
                stage, detail = self.progress_queue.get_nowait()
                self.var_stage.set(stage)
                self.var_detail.set(detail)
                ts = time.strftime("%H:%M:%S")
                self.text_handler.write(f"[{ts}] {stage} - {detail}\n")
                if stage == "Drive 上传失败":
                    messagebox.showerror("Drive 上传失败", detail)
                elif stage == "Drive 上传完成":
                    messagebox.showinfo("Drive 上传完成", detail)
        except queue.Empty:
            pass
        finally:
            self.after(200, self._poll_progress)


def main():
    app = App()
    app.mainloop()
