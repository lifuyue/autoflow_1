# Agent Guide — AutoFlow

AutoFlow 是一个以 GUI 驱动的自动化小工具（Windows 首发，开发环境可在 macOS/Windows）。核心流程：下载 Excel → 清洗与计算 → 写入模板 → 上传目标系统，并在关键节点产生日志与截图。

本指南面向“工程同学/自动化代理（Agent）”来协作该仓库：如何本地运行、调试、配置、打包分发，以及如何扩展新的下载/上传渠道。

## 快速上手
- 入口：`autoflow/main.py`（Tkinter GUI）
- 主要配置：`autoflow/config/profiles.yaml`、`autoflow/config/mapping.yaml`、`autoflow/config/selectors/*.yaml`
- 模板示例：`autoflow/templates/sample_template.xlsx`
- 运行产物与日志：`autoflow/work/`（`out/`、`logs/`、`logs/shot/`、`tmp/`）

### 开发环境（macOS/Windows）
```bash
# macOS / zsh
python3 -m venv .venv
source .venv/bin/activate
pip install -r autoflow/requirements.txt

# 可选：浏览器自动化
python -m playwright install chromium

# 运行 GUI
python autoflow/main.py
```
```powershell
# Windows / PowerShell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r autoflow\requirements.txt
python autoflow\main.py
```

### VS Code 调试
- 打开“运行和调试”，创建 `launch.json`，`program` 指向 `${workspaceFolder}/autoflow/main.py`
- 常用配置：`console: integratedTerminal`、`justMyCode: true`

## 配置说明
- `profiles.yaml`：多抬头/多账号；每个 profile 的下载/处理/上传参数
- `mapping.yaml`：数据清洗与单元格映射（示例见文件注释）
  - 支持表达式：
    - `today` → 当天日期（YYYY-MM-DD）
    - `sum:Amount`、`first:Column` → 来自源表 DataFrame 的列计算
    - `$profile.company_name` / `$profile.xxx.yyy` → 来自当前 profile 的属性
- `selectors/*.yaml`：浏览器自动化用的选择器，如 `upload_input_selector`、`submit_selector`

路径解析规则：
- 统一使用 `core.profiles.resolve_config_path()` 解析；支持绝对路径与相对路径（可带或不带前缀 `autoflow/`）
- 打包后（PyInstaller）自动兼容 `_MEIPASS` 资源目录

## 运行目录与日志
- 程序启动时确保：`autoflow/work/` 下的 `inbox/`、`out/`、`tmp/`、`logs/`、`logs/shot/`
- 日志：`autoflow/work/logs/app.log`（滚动日志 + 控制台）
- 截图/HTML dump：`autoflow/work/logs/shot/`

## 浏览器自动化（Playwright）
- 默认尝试 `chromium`；若未安装，自动回退到系统 `msedge`/`chrome`
- 首次运行浏览器自动化建议：
```bash
pip install playwright
python -m playwright install chromium
```
- 上传流程：在 `services/upload/*` 内通过 `BrowserRunner` 打开页面、可选登录、设置文件、提交、截图

## 打包分发（Windows EXE）
- 必须在 Windows 上执行打包（跨平台无法生成 exe）
- 执行：
```powershell
# 需先安装依赖与 PyInstaller
pip install -r autoflow\requirements.txt
pip install pyinstaller

# 可选（内置浏览器）：
python -m playwright install chromium

# 打包
autoflow\build_win.bat
```
- 产物：`dist/AutoFlow.exe`（单文件、无控制台）
- 运行产物与日志仍在 exe 同级的 `autoflow/work/**`

### 分发建议
- 必需：`dist/AutoFlow.exe`
- 可选（外置配置，无需重新打包即可改）：`autoflow/config/**`、`autoflow/templates/**`
- 可选（免系统浏览器依赖）：`browsers/**`（拷贝自已安装的 ms-playwright 目录）
- 启动脚本：`autoflow/run_AutoFlow.bat`（支持自动设置 `AUTOFLOW_ROOT` 和 `PLAYWRIGHT_BROWSERS_PATH`）

## 测试
- 冒烟测试（不依赖真实外部系统）：`autoflow/tests/test_smoke.py`
```bash
# 需安装 pytest
echo "pytest==8.3.2" >> requirements-dev.txt  # 若单独管理
pip install -r autoflow/requirements.txt
pytest -q
```

## 目录速览
```
autoflow/
  app_gui/            # Tkinter GUI（`main_gui.py`）
  core/               # 日志/配置/管道/错误（`logger.py`、`profiles.py`、`pipeline.py`）
  services/           # 下载/处理/上传/浏览器（`download/`、`transform/`、`upload/`、`browser/`）
  config/             # 多抬头配置、映射、选择器
  templates/          # Excel 模板（缺失会自动生成示例）
  work/               # 运行时目录（inbox/out/tmp/logs/shot）
  tests/              # 冒烟测试
  README.md           # 使用说明
  build_win.bat       # Windows 打包脚本
  main.py             # 程序入口（GUI）
```

## 扩展开发
- 新增下载渠道：在 `services/download/` 实现 `ICloudProvider` 并在 `provider_from_config` 分派
- 新增上传渠道：在 `services/upload/` 实现 `IUploader` 并在 `uploader_from_config` 分派
- 新增映射规则：在 `services/transform/transformer.py` 的 `_eval_value` 扩展表达式

## 常见问题
- GUI 不弹出：检查 VS Code 启动配置的解释器、`console` 设置为 `integratedTerminal`
- 浏览器无法启动：安装 Chromium 或确保系统已安装 Edge/Chrome；也可随包带 `browsers/`
- 选择器不生效：根据实际页面调整 `autoflow/config/selectors/*.yaml` 并在 `profiles.yaml` 指定路径
- 权限/路径问题：打包后所有可写目录均使用 exe 同级的 `autoflow/work/**`

## 发布清单（建议）
- `AutoFlow.exe`
- `run_AutoFlow.bat`
- （可选）`autoflow/config/**`、`autoflow/templates/**`
- （可选）`browsers/**`
- 简易说明（可新建 `README-Windows.txt`）
