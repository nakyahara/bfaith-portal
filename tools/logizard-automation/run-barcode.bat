@echo off
cd /d "%~dp0"
echo ============================================
echo  Logizard auto: barcode master sync
echo  (import bc_upload - export master - import shohin)
echo ============================================
if not exist .env (
    echo [ERROR] .env not found. Copy .env.example to .env and set ID/PASSWORD.
    pause
    exit /b 1
)
node auto-barcode.js %*
if errorlevel 1 (
    echo.
    echo [FAILED] Check the message above and error-shots folder, then tell Claude.
    pause
    exit /b 1
)
echo.
echo [OK] Barcode master sync finished.
timeout /t 8
