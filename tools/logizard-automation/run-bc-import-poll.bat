@echo off
rem Logizard: portal button "import to Logizard" (new product barcode CSV).
rem Called from Task Scheduler [Logizard-BcImport] every minute on the miniPC.
rem It asks the portal for a queued job and exits at once when there is none.
rem When there is a job: (1) import the issued logizard_bc_upload.csv, (2) export the barcode master.
rem The runner does not take a job 23:50-01:30 JST (the nightly product master import uses the same account).
rem It does not take a job while another Logizard job holds logs\logizard-session.lock.
rem Source of truth: bfaith-portal tools/logizard-automation/bc-import-runner.js
cd /d "%~dp0"
if not exist logs mkdir logs
if not exist .env (
    echo [%date% %time%] ERROR .env not found >> logs\bc-import.log
    exit /b 1
)
node bc-import-runner.js >> logs\bc-import.log 2>&1
exit /b %ERRORLEVEL%
