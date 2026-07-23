@echo off
rem AI weekly report runner (Task Scheduler entry)
rem place: C:\tools\ai-insights\run-weekly.bat
cd /d "%~dp0"
node run-weekly-report.js %*
exit /b %ERRORLEVEL%
