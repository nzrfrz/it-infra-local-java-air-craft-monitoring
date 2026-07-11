@echo off
REM Wrapper supaya start_infra bisa dipanggil dari cmd.exe maupun PowerShell
REM tanpa perlu tahu shell mana yang sedang dipakai (cmd.exe tidak bisa
REM menjalankan .ps1 secara langsung, jadi wrapper ini yang mendelegasikan).
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start_infra.ps1" %*
