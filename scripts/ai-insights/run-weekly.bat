@echo off
rem AI経営レポート 週次 runner (Task Scheduler 入口)
rem 配置: C:\tools\ai-insights\run-weekly.bat
cd /d "%~dp0"
node run-weekly-report.js %*
exit /b %ERRORLEVEL%
