@echo off
setlocal
REM Build single-file EXE with PyInstaller on Windows
REM Requires Python 3.11 and dependencies installed

set PROJECT_ROOT=%~dp0
cd /d %PROJECT_ROOT%

echo [1/3] Installing build deps (optional)...
REM If you haven't installed: pip install -r requirements.txt

echo [2/3] Ensuring Playwright browser (optional)...
REM Uncomment if using browser automation in packaged app
REM py -m playwright install chromium

echo [3/3] Building executable...
py -m PyInstaller ^
  --noconfirm --clean --onefile --noconsole ^
  --name AutoFlow ^
  --collect-all playwright ^
  --add-data "config;autoflow\config" ^
  --add-data "templates;autoflow\templates" ^
  main.py

echo Done. Check dist\AutoFlow.exe
endlocal
