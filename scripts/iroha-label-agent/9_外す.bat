@echo off
chcp 932 >nul
powershell -NoProfile -ExecutionPolicy Bypass -File "C:\tools\iroha-label-agent\install.ps1" -Uninstall
echo.
pause
