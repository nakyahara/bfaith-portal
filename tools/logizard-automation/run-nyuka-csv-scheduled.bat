@echo off
rem Logizard FA04_01 arrival-accepted CSV for the inbound-check iPad app.
rem Called from Task Scheduler (Logizard-NyukaCSV) at 08:40 and 11:45.
rem For a manual dry run:  node auto-nyuka-csv.js --dry
rem
rem Step 2 exports the Logizard product master (FM08_01 / product / default
rem pattern). The iPad app needs its "expiry managed" flag, which the arrival
rem CSV does not carry. It self-limits to once per JST day, so the 11:45 run
rem normally does nothing. Step 2 never changes the exit code of step 1:
rem the arrival CSV is what the floor depends on, and the master has its own
rem dead-man entry (logizard-shohin-csv) in the job registry.
rem
rem Step 1.5 (2026-09-28, master SoR switch 3c-1b-2a) = daily product master import,
rem SHADOW ONLY for now: export the product master, check every row of yesterday's
rem Company DB CSV exists in Logizard, and build the import PREVIEW. It never presses
rem the execute button. It acts only in the 00:15-00:55 window (the 00:20 run) and
rem only once a day; at 08:40 / 11:45 it does nothing. It pings its own job
rem (lz-daily-import-shadow) and never changes the exit code of this bat.
rem Source of truth: bfaith-portal scripts/logizard-import/lz-daily-import.mjs.
cd /d "%~dp0"
if not exist logs mkdir logs
if not exist .env (
    echo [%date% %time%] ERROR .env not found >> logs\scheduled.log
    exit /b 1
)
rem The 08:30 slot collides with the nefuda CSV job, which shares the same
rem Logizard session lock. Wait (max 10 min) until the lock is free instead of
rem failing immediately - node exits at once when the lock is held.
set /a WAITED=0
:waitlock
if not exist logs\logizard-session.lock goto run
if %WAITED% GEQ 20 (
    echo [%date% %time%] WARN lock still held after 10 min, running anyway >> logs\scheduled.log
    goto run
)
set /a WAITED+=1
timeout /t 30 /nobreak > nul
goto waitlock
:run
echo [%date% %time%] ==== nyuka-csv scheduled run ==== >> logs\scheduled.log
node auto-nyuka-csv.js >> logs\scheduled.log 2>&1
set "RC=%ERRORLEVEL%"
if "%RC%"=="0" (powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\bfaith\bfaith-portal\scripts\jobs-monitor\ping.ps1 -Id logizard-nyuka-csv -Status ok >nul 2>&1) else (powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\bfaith\bfaith-portal\scripts\jobs-monitor\ping.ps1 -Id logizard-nyuka-csv -Status fail >nul 2>&1)

echo [%date% %time%] ==== lz-daily-import (shadow, 00:20 run only) ==== >> logs\scheduled.log
rem The shadow step reads yesterday's lz-daily evidence from the portal DATA_DIR (same as daily-sync).
rem This scheduled task has no DATA_DIR of its own (2026-09-29 00:21 the step failed: DATA_DIR missing).
set "DATA_DIR=C:\Users\bfaith\bfaith-portal\data"
node C:\Users\bfaith\bfaith-portal\scripts\logizard-import\lz-daily-import.mjs >> logs\scheduled.log 2>&1

echo [%date% %time%] ==== shohin-csv (product master, once per day) ==== >> logs\scheduled.log
node auto-shohin-csv.js --once-per-day >> logs\scheduled.log 2>&1
set "RC2=%ERRORLEVEL%"
if "%RC2%"=="0" (powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\bfaith\bfaith-portal\scripts\jobs-monitor\ping.ps1 -Id logizard-shohin-csv -Status ok >nul 2>&1) else (powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\bfaith\bfaith-portal\scripts\jobs-monitor\ping.ps1 -Id logizard-shohin-csv -Status fail >nul 2>&1)

exit /b %RC%
