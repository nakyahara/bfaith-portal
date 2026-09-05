# run-hourly.ps1 - Logizard stock snapshot hourly pipeline (Task Scheduler entry, miniPC)
#   1) download stock CSV from Logizard (auto-zaiko.js, headless Edge, C:\tools\logizard-automation)
#   2) import into warehouse.db raw_lz_inventory (full replace, single transaction)
#   3) push snapshot to Render mirror_logizard_stock (sync-to-render.js --logizard-only)
# ping semantics: ok = all steps ok / partial = mirror push failed (local data is fresh, next hour retries)
#                 fail = download or import failed (previous snapshot is kept everywhere)
# ASCII only in this file (PS 5.1 reads BOM-less files as CP932; Japanese broke quoting - 2026-08-01 incident)

$ErrorActionPreference = 'Continue'
$repo = 'C:\Users\bfaith\bfaith-portal'
$lzDir = 'C:\tools\logizard-automation'
$logDir = Join-Path $repo 'logs'
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Force $logDir | Out-Null }
$log = Join-Path $logDir 'logizard-stock-hourly.log'

function Write-Log($msg) {
  $stamp = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
  try { Add-Content -Path $log -Value "[$stamp] $msg" } catch {}
}

function Send-Ping($status, $note) {
  $pingArgs = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', (Join-Path $repo 'scripts\jobs-monitor\ping.ps1'), '-Id', 'logizard-stock-hourly', '-Status', $status)
  if ($note) { $pingArgs += @('-Note', $note) }
  powershell @pingArgs
}

Write-Log '==== logizard stock hourly: start ===='

# Resolve CSV output path from logizard-automation .env (last LOGIZARD_ZAIKO_OUT wins, same as loadEnv)
$csv = Join-Path $lzDir 'out\logizard_zaikosu.csv'
$envFile = Join-Path $lzDir '.env'
if (Test-Path $envFile) {
  $line = Get-Content $envFile | Where-Object { $_ -match '^LOGIZARD_ZAIKO_OUT=' } | Select-Object -Last 1
  if ($line) {
    $v = ($line -replace '^LOGIZARD_ZAIKO_OUT=', '').Trim().Trim('"')
    if ($v) { $csv = $v }
  }
}

# Step 1: download from Logizard
Set-Location $lzDir
cmd /c "node auto-zaiko.js >> `"$log`" 2>&1"
$code = $LASTEXITCODE
if ($code -ne 0) {
  Write-Log "step1 download FAILED (exit $code)"
  Send-Ping 'fail' 'download failed'
  exit 1
}

# Step 2: import into warehouse.db (DELETE+INSERT in one transaction, min-rows guard inside)
Set-Location $repo
cmd /c "node apps\warehouse\csv-import.js logizard `"$csv`" >> `"$log`" 2>&1"
$code = $LASTEXITCODE
if ($code -ne 0) {
  Write-Log "step2 import FAILED (exit $code)"
  Send-Ping 'fail' 'import failed'
  exit 1
}

# Step 2b: archive the imported CSV as data\logizard-history\YYYY\MM\zaiko_YYYYMMDD_HHMM.csv.gz
#   (stock history did not exist anywhere before 2026-09-05: raw_lz_inventory is a full replace and the
#    CSV is overwritten every hour). Retention: hourly 90 days, daily last-of-day forever, optional rclone offsite.
#   Failure here never changes the job result (ok/partial); it is written to the log and to the ping note.
$archiveNote = $null
cmd /c "node -r dotenv/config scripts\logizard-stock\archive-snapshot.mjs --csv `"$csv`" >> `"$log`" 2>&1"
$code = $LASTEXITCODE
if ($code -ne 0) {
  Write-Log "step2b archive FAILED (exit $code) - import is ok, history has a gap this hour"
  $archiveNote = 'archive failed'
}

# Step 3: mirror push to Render (freshness-guarded full replace + count verification)
cmd /c "node apps\warehouse\sync-to-render.js --logizard-only >> `"$log`" 2>&1"
$code = $LASTEXITCODE
if ($code -ne 0) {
  Write-Log "step3 mirror push FAILED (exit $code) - local import is ok"
  $note = 'mirror push failed'
  if ($archiveNote) { $note = "$note; $archiveNote" }
  Send-Ping 'partial' $note
  exit 0
}

Write-Log 'all steps ok'
Send-Ping 'ok' $archiveNote
exit 0
