@echo off
rem product detail fetch with auto-retry loop (idempotent resume)
rem exit codes from products.js:
rem   0 = finished all      -> ping ok
rem   3 = stopped at time limit, made progress -> ping partial (keeps the daily deadline, not a completion)
rem   4 = ran fine but had NO WORK -> auto-queue the next category (finder --next).
rem       If nothing is left to queue, ping fail: a green light with no work is how
rem       20 days of idling went unnoticed (2026-08-07..27). Never ping ok here.
rem   other = error         -> notify + retry in 5 min
set "SCOUT_KW_WINDOW=1"
set "SCOUT_HOME=C:\Users\bfaith\product-idea-scout"
set "SCOUT_CODE_ROOT=%~dp0"
for %%I in ("%~dp0..\..") do set "SCOUT_PORTAL_ROOT=%%~fI"
set "WAREHOUSE_DB=C:\Users\bfaith\bfaith-portal\data\warehouse.db"
cd /d "%SCOUT_HOME%"
set "PING=%SCOUT_PORTAL_ROOT%\scripts\jobs-monitor\ping.ps1"
:loop
node "%SCOUT_CODE_ROOT%collection-window.cjs"
if errorlevel 3 goto partial
rem --- Step 1: refresh the own-product ledger (what we already launched, and what we withdrew).
rem     It reads warehouse.db, which only lives on the miniPC, so it runs from the portal tree
rem     (that is where better-sqlite3 is). A failure here must not stop the collection.
pushd "%SCOUT_PORTAL_ROOT%"
node scripts\export-own-products.cjs C:\Users\bfaith\product-idea-scout\data\own-products.json >> C:\Users\bfaith\product-idea-scout\data\products.log 2>&1
popd

rem --- Step 2: place our own products on Amazon categories (one-off backfill, then a no-op).
rem     Exit 3 = it fetched something today, so leave products.js for tomorrow. Running both
rem     long batches in one day would blow past the 20h task limit and the tail ping would
rem     never fire -- that is exactly how a truncated batch went silent before.
echo [%date% %time%] own.js start >> data\products.log
node "%SCOUT_CODE_ROOT%own.js" >> data\products.log 2>&1
set ORC=%errorlevel%
if "%ORC%"=="3" goto ownpartial
if not "%ORC%"=="0" goto ownerror

echo [%date% %time%] products.js start >> data\products.log
node "%SCOUT_CODE_ROOT%products.js" >> data\products.log 2>&1
set RC=%errorlevel%
call :publish

if "%RC%"=="0" goto done
if "%RC%"=="3" goto partial
if "%RC%"=="4" goto idle

echo [%date% %time%] products.js exited with error (rc=%RC%) - retry in 5 min >> data\products.log
node notify-crash.js >> data\products.log 2>&1
timeout /t 300 /nobreak >nul
goto loop

:ownpartial
echo [%date% %time%] own.js backfilled today - products.js waits until tomorrow >> data\products.log
call :publish
powershell -NoProfile -ExecutionPolicy Bypass -File "%PING%" -Id product-idea-scout -Status partial -Note "own backfill" >nul 2>&1
goto :eof

:ownerror
echo [%date% %time%] own.js exited with error (rc=%ORC%) - retry in 5 min >> data\products.log
node notify-crash.js >> data\products.log 2>&1
timeout /t 300 /nobreak >nul
goto loop

:done
echo [%date% %time%] products.js finished OK >> data\products.log
powershell -NoProfile -ExecutionPolicy Bypass -File "%PING%" -Id product-idea-scout -Status ok >nul 2>&1
goto :eof

:idle
rem No pending ASIN at all. Before crying for help, try to queue the next category:
rem the 20-day idle run (2026-08-07..27) happened because nobody queued one by hand.
echo [%date% %time%] no work - trying to queue the next category >> data\products.log
node "%SCOUT_CODE_ROOT%collection-window.cjs"
if errorlevel 3 goto partial
node finder.js --next >> data\products.log 2>&1
set FRC=%errorlevel%
if "%FRC%"=="0" goto queued
if "%FRC%"=="3" goto queued
goto nowork

:queued
echo [%date% %time%] queued next category >> data\products.log
set QNOTE=unknown
if exist data\last-queued.txt set /p QNOTE=<data\last-queued.txt
powershell -NoProfile -ExecutionPolicy Bypass -File "%PING%" -Id product-idea-scout -Status partial -Note "queued %QNOTE%" >nul 2>&1
goto :eof

:nowork
rem Nothing left to queue (or finder failed). Ping fail so the dead-man turns this red
rem next day -- a green light with no work is exactly what hid the 20-day idle.
echo [%date% %time%] products.js had no work - IDLE (finder rc=%FRC%) >> data\products.log
set IDLEWHY=unknown
if exist data\last-idle.txt set /p IDLEWHY=<data\last-idle.txt
powershell -NoProfile -ExecutionPolicy Bypass -File "%PING%" -Id product-idea-scout -Status fail -Note "idle: %IDLEWHY%" >nul 2>&1
goto :eof

:partial
rem Do NOT set/read a variable inside an if-block here: %VAR% would expand before the block runs.
echo [%date% %time%] products.js stopped at time limit - partial >> data\products.log
set REMAIN=unknown
if exist data\last-remaining.txt set /p REMAIN=<data\last-remaining.txt
powershell -NoProfile -ExecutionPolicy Bypass -File "%PING%" -Id product-idea-scout -Status partial -Note "remaining %REMAIN%" >nul 2>&1
goto :eof

:publish
rem --- Send the latest themes and own-product ledger to the portal.
rem     Collecting without publishing leaves the screen showing old data and nobody notices;
rem     that is exactly what happened between 2026-08-28 and 09-01.
rem     A failure here must not fail the collection, so the exit code is deliberately ignored
rem     (RC was already captured before this call). push.js logs and exits non-zero on failure.
echo [%date% %time%] publish start >> data\products.log
node "%SCOUT_CODE_ROOT%concepts.js" >> data\products.log 2>&1
node push.js >> data\products.log 2>&1
goto :eof
