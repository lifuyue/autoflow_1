# AutoFlow (MVP)

面向工厂到跨境电商流程的一键式小工具（Windows 10/11，Python 3.13）。

目标：在 GUI 中选择“抬头/账号”，一键执行固定流程：
下载 Excel → 清洗与计算 → 写入模板 → 上传到目标系统，并在关键节点产生日志与截图。

## 功能概览
- 下载模块（DingTalk Drive / 金山云盘）：直链/API 优先，浏览器自动化回退
- 处理模块（pandas + openpyxl）：读取源表、按映射写入模板（固定单元格/命名区域可扩展）
- 上传模块（金蝶 / 电子税务）：API 优先；无 API 则浏览器自动化上传并截图
- 多抬头/多账号配置：`config/profiles.yaml`；GUI 下拉切换
- 日志追溯：`work/logs/app.log`，截图 `work/logs/shot/`
- 打包：PyInstaller 单文件 EXE（`build_win.bat`）

## 目录结构
```
autoflow/
  app_gui/            # Tk/Tkinter GUI
  core/               # Orchestrator/Profiles/Logger/Errors
  services/           # Download/Form processor/Upload/后续模块
  config/             # 多抬头配置、映射、选择器
  templates/          # Excel 模板（如无则运行时自动生成一个示例）
  work/               # 运行时文件（inbox/out/tmp/logs/shot）
  tests/              # 冒烟测试（使用假 Provider/Uploader，不依赖真实系统）
  README.md
  requirements.txt
  build_win.bat
  main.py             # 程序入口（GUI）
```

## 环境要求
- Windows 10/11，内存 ≥ 8GB（建议）
- 已安装 Edge/Chrome 任一浏览器
- Python 3.13（64-bit）

## 安装
```
# 进入项目根目录（包含 autoflow/ 子目录）
python -m venv .venv
.venv\Scripts\activate  # PowerShell/CMD
pip install -r autoflow/requirements.txt

# 如需浏览器自动化（可选）
python -m playwright install chromium
```

### 安装排错（Windows）
- 检查 Python/pip 指向一致：
  - `python --version`、`where python`
  - `pip --version`、`where pip`
- 先升级安装工具：`python -m pip install -U pip setuptools wheel`
- 重试安装并收集详细日志：
  - `pip -vvv install -r autoflow/requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn`
- 编译相关报错（如 lxml/cryptography）：需要安装 Visual Studio Build Tools 与 Windows SDK。
- Playwright 报错：确认已执行 `python -m playwright install chromium`（或安装 Edge/Chrome）。
- 公司代理/证书：配置 `HTTP_PROXY/HTTPS_PROXY` 环境变量或在 `%APPDATA%\pip\pip.ini` 设置镜像与证书。

## 配置
- profiles：`autoflow/config/profiles.yaml`
  - 每个 profile 样例（见文件内注释）：
    - download：`type`（`dingpan`/`kdocs`），`direct_url`（优先）或 `link_url`+`login`
    - transform：`mapping_file`（默认为 `config/mapping.yaml`），`template_path`
    - upload：`type`（`kingdee`/`tax_ehall`），优先填写 `api.url`，否则 `upload_url`+`selectors_file`+`login`
  - 新增“抬头”：复制一个 profile 段落，改 `display_name`、`company_name`、下载与上传配置即可。

- 映射：`autoflow/config/mapping.yaml`
  - `input_columns`：标准字段 → 多语言/多格式的源表列名列表
  - `computed`：写入到标准化数据框的常量列（如 `base_currency`）
  - `validations`：必填、非负、四舍五入位数等规则
  - `thresholds.confirm_over_amount_cny`：超额提示阈值（CLI 非交互模式下自动标记 `need_confirm=true`）

- 选择器：`autoflow/config/selectors/*.yaml`
  - `upload_input_selector`：文件选择 input
  - `submit_selector`：提交按钮
  - 登录（可在 profiles.yaml 的 `upload.login`/`download.login` 填写选择器）

- 模板：`autoflow/templates/sample_template.xlsx`
  - 若文件不存在，程序运行时会自动生成一个简易模板（包含 B2/B3/B4 三个目标单元格）。

## 运行
```
python autoflow\main.py
```
GUI 说明：
- 下拉选择“抬头/账号”；点“开始”即按 4 步执行（下载/处理/套模板/上传）
- 日志实时输出；遇到验证码或需人工操作，按弹出的浏览器提示完成即可
- 结束后显示输出路径，截图在 `work/logs/shot/`

## CLI
```
python -m autoflow.cli process-forms \
  --input ./work/inbox/*.xlsx \
  --output ./work/out \
  --mapping ./autoflow/config/mapping.yaml \
  --non-interactive
```
说明：
- `--input` 支持 CSV/Excel，可一次性传入多文件（glob 展开或重复传参）
- `--rate USD:CNY=7.12` 可覆盖默认汇率，未填时 `StaticRateProvider` 使用 `default_rate`
- 非交互模式会将超阈值记录写入 `processed_forms_need_confirm.csv`
- 执行结果会输出模板文件与 Markdown 报告路径，便于后续自动化

### 汇率获取模块（USD/CNY）
```
python -m autoflow.cli get-rate --date 2025-01-02 --from USD --to CNY
```
- 输出央行公布的“1美元对人民币 X.XXXX 元”中间价（示例：`7.1879`）。
- 依赖：`requests`、`beautifulsoup4`、`decimal`（随 Python 标准库）。
- 流程：优先抓取“人民币汇率中间价公告”最新文章，若当日公告缺失则回退解析“关键图表-人民币汇率中间价对美元”表格。
- 限制：仅支持 USD/CNY，当日公告缺失时不会自动回退至上一个工作日，央行页面结构调整可能导致需更新解析规则。
- 网络可调：`--connect-timeout`、`--read-timeout`、`--total-deadline` 控制单次请求与整体时长，`--http-debug` 可结合 `build-monthly-rates` 命令输出底层请求日志。

### 月度中间价缓存（USD/CNY）
```
python -m autoflow.cli build-monthly-rates --start 2023-01
python -m autoflow.cli build-monthly-rates --start 2023-01 --output data/rates/monthly_usd_cny.csv
python -m autoflow.cli build-monthly-rates --refresh 2025-09 --refresh 2025-10
python -m autoflow.cli build-monthly-rates --start 2023-01 --rebuild
```
- 构建/补齐“每月首个工作日”USD/CNY 人民币中间价缓存，仅保留月度粒度。
- 缓存 CSV 列顺序固定：`年份,月份,中间价,来源日期`；中间价以四位小数字符串存储，按月升序输出，写入采用临时文件 + 原子替换。
- 增量策略：默认仅补缺月份；`--refresh YYYY-MM` 可对指定月份强制重抓；`--rebuild` 会删除旧缓存后全量重建。
- 失败月份仅记录日志“pending”，流程不终止，可后续通过 `--refresh` 补齐。
- 节假日/调休配置可选：在 `autoflow/config/cn_workdays.yaml` 中维护 `holidays`、`workdays` 列表（示例已提供），用于判定首个工作日与补班日。
- 网络可调：`--connect-timeout`、`--read-timeout`、`--total-deadline` 可细化连接/读取超时与单次抓取总时长；`--http-debug` 可打开底层 HTTP 诊断。

## 打包（Windows）
```
# PowerShell/CMD
autoflow\build_win.bat
```
打包说明：
- 产物位于 `dist/AutoFlow.exe`（单文件、无控制台）
- 首次运行可能触发 Windows SmartScreen/杀软拦截，需手动允许运行
- 若使用浏览器自动化：目标机需安装 Chrome/Edge 或打包后首次执行 `playwright install chromium`

## 常见问题
- 验证码/登录：
  - 首次或会话失效时，会自动打开浏览器登录；请完成验证码后返回 GUI 继续
- 权限不足/网络限制：
  - 直链/API 请求可能被防火墙拦截；可切换到浏览器自动化或让 IT 开白名单
- 文件锁定/Excel 打不开：
  - 关闭占用文件的 Excel 进程，再重试
- Playwright 未安装：
  - `pip install playwright && python -m playwright install chromium`
- 下载/上传选择器不生效：
  - 根据实际页面改 `config/selectors/*.yaml`，检查 `profiles.yaml` 中的 `selectors_file` 路径

## 开发与测试
- 冒烟测试（不依赖真实系统）：
```
pytest -q
```
- 关键模块：
  - `core/pipeline.py`：总控逻辑
  - `services/download/*`：钉盘/金山云盘（直链→浏览器回退）
  - `services/form_processor/*`：映射/清洗/校验/导出与报告
  - `services/upload/*`：金蝶/电子税务（API 优先→浏览器回退）
  - `services/browser/runner.py`：Playwright 封装（open/login/upload/screenshot）

## 凭据策略
- 默认运行时输入（不落盘）
- 若需本地长期保存，可在 `core/profiles.py` 的 `encrypt/decrypt` 中接入 Windows 凭据保存方案（如 DPAPI/Keyring），并在 `profiles.yaml` 中放入加密后的密文（本 MVP 默认未启用）

## API 优先 / 浏览器自动化回退
- 下载：优先 `download.direct_url` 或 `download.api.url`，否则使用 `download.link_url` + 浏览器自动化
- 上传：优先 `upload.api.url`，否则使用 `upload.upload_url` + 浏览器自动化

## 路线图（可选）
- 支持命名区域/表格批量写入
- 支持多文件批量处理
- 更完备的错误提示与截图归档
- 加密凭据与会话复用
