@echo off
setlocal EnableExtensions
REM Launch wrapper for AutoFlow.exe with external config override.
REM If no EXE found, fall back to run from source with a venv.

set SCRIPT_DIR=%~dp0

REM ================= Run packaged EXE if present =================
if exist "%SCRIPT_DIR%\AutoFlow.exe" (
  REM Prefer external config/templates if present next to exe
  if exist "%SCRIPT_DIR%\autoflow\config\profiles.yaml" (
    set AUTOFLOW_ROOT=%SCRIPT_DIR%\
  )
  REM Optional: if you ship browsers under .\browsers, uncomment next line
  REM set PLAYWRIGHT_BROWSERS_PATH=%SCRIPT_DIR%\browsers
  start "" "%SCRIPT_DIR%\AutoFlow.exe"
  endlocal
  goto :eof
)

REM ================= Dev fallback: run from source =================
echo [Info] AutoFlow.exe not found. Running from source (dev mode).
set REPO_ROOT=%SCRIPT_DIR%\..

REM Activate or create venv under repo root
if exist "%REPO_ROOT%\.venv\Scripts\activate" (
  call "%REPO_ROOT%\.venv\Scripts\activate"
) else (
  echo [Info] Creating venv at %REPO_ROOT%\.venv
  py -3.13 -m venv "%REPO_ROOT%\.venv"
  if errorlevel 1 (
    echo [Error] Failed to create virtual environment.
    exit /b 1
  )
  call "%REPO_ROOT%\.venv\Scripts\activate"
)

echo [Info] Python version:
python --version
where python
echo [Info] Pip version:
pip --version

echo [Info] Ensuring dependencies...
pip install -r "%SCRIPT_DIR%\requirements.txt" -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
if errorlevel 1 (
  echo [Error] Failed to install dependencies. Check network/mirror and try again.
  exit /b 1
)

REM Optional: ensure Playwright browser
REM python -m playwright install chromium

python "%REPO_ROOT%\autoflow\main.py"
endlocal
