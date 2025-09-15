@echo off
setlocal
REM Launch wrapper for AutoFlow.exe with external config override and optional Playwright browsers path
REM Place this .bat next to AutoFlow.exe

set SCRIPT_DIR=%~dp0
REM Prefer external config/templates if present next to exe
if exist "%SCRIPT_DIR%\autoflow\config\profiles.yaml" (
	set AUTOFLOW_ROOT=%SCRIPT_DIR%\
)

REM Optional: if you ship browsers under .\browsers, uncomment next line
REM set PLAYWRIGHT_BROWSERS_PATH=%SCRIPT_DIR%\browsers

start "" "%SCRIPT_DIR%\AutoFlow.exe"
endlocal
