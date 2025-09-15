import threading
import queue
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText
from pathlib import Path
import time

from autoflow.core.logger import get_logger
from autoflow.core.profiles import load_profiles, Profile
from autoflow.core.pipeline import Pipeline
from autoflow.core.errors import AutoFlowError


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

        self._build_ui()
        self.progress_queue: queue.Queue = queue.Queue()
        self.worker_thread: threading.Thread | None = None

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
                self.progress_queue.put(("完成", f"输出：{res.get('output_path','')}"))
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
        except queue.Empty:
            pass
        finally:
            self.after(200, self._poll_progress)


def main():
    app = App()
    app.mainloop()

