@echo off
setlocal EnableExtensions EnableDelayedExpansion
REM Build single-file EXE with PyInstaller on Windows
REM Requires Python 3.13 (64-bit) and network access to fetch wheels

set PROJECT_ROOT=%~dp0
cd /d "%PROJECT_ROOT%\"

echo ================= ENV DIAGNOSTICS =================
echo [Env] py launcher path:
where py
echo [Env] Python version:
py -V
echo [Env] Python executable:
py -c "import sys; print(sys.executable)"
echo [Env] Pip version:
py -m pip --version
where pip

echo.
echo [Check] Require Python 3.13...
py -3.13 -V >nul 2>&1
if errorlevel 1 (
  echo [Warn] Python 3.13 not found by 'py -3.13'.
  echo        Please install Python 3.13 (x64) and retry, or edit this script.
)

echo.
echo ================= INSTALL DEPENDENCIES =================
echo [1/4] Upgrade pip/setuptools/wheel
py -3.13 -m pip install -U pip setuptools wheel
if errorlevel 1 (
  echo [Error] Failed to upgrade pip/setuptools/wheel.
  exit /b 1
)

echo [2/4] Install project requirements
py -3.13 -m pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
if errorlevel 1 (
  echo [Error] Dependency install failed.
  echo        Tips:
  echo          - Check network/mirror/SSL or try without the mirror.
  echo          - Ensure Visual Studio Build Tools if native builds are required.
  echo          - Ensure correct Python selected by 'py -3.13'.
  exit /b 1
)

echo [3/4] Ensure Playwright browser (optional)
REM Uncomment to bundle a local Chromium copy on build host
REM py -3.13 -m playwright install chromium

echo.
echo ================= BUILD EXECUTABLE =================
echo [4/4] Building executable with PyInstaller...
py -3.13 -m PyInstaller ^
  --noconfirm --clean --onefile --noconsole ^
  --name AutoFlow ^
  --collect-all playwright ^
  --add-data "config;autoflow\config" ^
  --add-data "templates;autoflow\templates" ^
  main.py
if errorlevel 1 (
  echo [Error] PyInstaller build failed.
  exit /b 1
)

echo Done. Check dist\AutoFlow.exe
endlocal
