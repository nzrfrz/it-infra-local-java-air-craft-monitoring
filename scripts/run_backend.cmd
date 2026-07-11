@echo off
REM Wrapper cmd.exe -> PowerShell, sama seperti start_infra.cmd
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run_backend.ps1" -Shell cmd %*
