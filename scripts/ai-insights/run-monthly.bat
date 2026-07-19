@echo off
rem AI monthly report runner (Task Scheduler entry, daily check)
rem place: C:\tools\ai-insights\run-monthly.bat
cd /d "%~dp0"
node run-monthly-report.js %*
exit /b %ERRORLEVEL%
