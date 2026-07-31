@echo off
REM ============================================================
REM  au PAY bulk item CSV download (runs ON the miniPC)
REM
REM  Creates the CSV on Wow!manager, waits for it, downloads it,
REM  and overwrites item.csv / stock.csv in the shared Google Drive
REM  folder "AUpay download" via rclone.
REM
REM  Called from the desktop PC over SSH (Stream Deck button).
REM  Run it directly on the miniPC for a manual test.
REM  ASCII only + CRLF: a LF-only .bat dies with exit code 255.
REM ============================================================
setlocal
cd /d C:\Users\bfaith\bfaith-portal
if errorlevel 1 (
    echo [ERROR] repository folder not found: C:\Users\bfaith\bfaith-portal
    exit /b 2
)

REM headless: nobody is looking at the miniPC screen
set HEADLESS=1
REM Playwright browsers live under the bfaith profile, not systemprofile
set PLAYWRIGHT_BROWSERS_PATH=C:\Users\bfaith\AppData\Local\ms-playwright

node scripts\mall-csv-fetcher\aupay-item-csv-download.mjs %*
set RC=%ERRORLEVEL%

if not "%RC%"=="0" (
    echo.
    echo [FAILED] exit code %RC%
    echo   3 = login blocked, needs manual sign-in to Wow!manager
    echo   1 = run failed, see the message above
    echo   2 = bad option
    exit /b %RC%
)
echo.
echo [OK] Finished. See the result above.
exit /b 0
