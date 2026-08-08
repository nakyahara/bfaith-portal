@echo off
REM ============================================================
REM  FBA inbound tracking numbers -> Amazon (runs ON the miniPC)
REM
REM  Reads the Fukuyama Transport shipment CSV (fukutsu_tuiseki.csv)
REM  from the shared Drive folder, matches each waybill number to an
REM  FBA inbound box, and registers it through SP-API.
REM
REM  Scheduled daily at 22:00 JST. The API rejects registration after
REM  the same-day deadline (23:59 JST), so a failure here must be dealt
REM  with the same evening. If it is missed, a human can still type the
REM  numbers into Seller Central the next day.
REM
REM  Manual preview (writes nothing):
REM    scripts\run-fba-tracking.bat
REM  Real run:
REM    scripts\run-fba-tracking.bat --commit
REM
REM  Args are forwarded to node as-is: do not pass untrusted input.
REM  ASCII only + CRLF: a LF-only .bat dies with exit code 255.
REM ============================================================
setlocal
cd /d C:\Users\bfaith\bfaith-portal
if errorlevel 1 (
    echo [ERROR] repository folder not found: C:\Users\bfaith\bfaith-portal
    exit /b 2
)

node scripts\fba-tracking-run.mjs %*
set "RC=%ERRORLEVEL%"

REM Tell the dead-man monitor we ran. Never let the ping change our exit code.
if "%RC%"=="0" (
    set "PSTATUS=ok"
) else (
    set "PSTATUS=fail"
)
powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\bfaith\bfaith-portal\scripts\jobs-monitor\ping.ps1 -Id fba-tracking-input -Status %PSTATUS%

if not "%RC%"=="0" (
    echo.
    echo [ERROR] fba-tracking-run exited with %RC%. Check the GChat message.
)
exit /b %RC%
