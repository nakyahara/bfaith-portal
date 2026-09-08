# run-expected-profit-nightly.ps1 - Task Scheduler runner for the "expected profit" nightly batch (miniPC).
#
#   powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\bfaith\bfaith-portal\scripts\expected-profit\run-expected-profit-nightly.ps1
#
# What it does: cd to the repo, run `node apps/expected-profit/nightly.js`, keep a rotated log.
#
# Design:
#   - nightly.js sends its OWN jobs-monitor ping (ok only after reading the published pointer back).
#     This runner pings 'fail' ONLY when node exits non-zero, to cover the case where the process dies
#     before it can ping at all (node missing, crash on import, machine trouble). A duplicate fail ping
#     is harmless; a missing one would leave the dead-man alert as the only signal.
#   - Single instance: the scheduler is set to IgnoreNew, but a manual run can still overlap a scheduled
#     one, so a lock file guards it too. A lock older than $LockStaleHours is taken over (a killed run
#     leaves the file behind).
#   - DATA_DIR is set explicitly, exactly like daily-sync.bat, so the batch reads/writes the same
#     warehouse.db / expected-profit.db as everything else on this machine.
#
# IMPORTANT: keep this file ASCII-only.
#   Windows PowerShell 5.1 reads BOM-less files as ANSI (CP932); multi-byte characters can swallow
#   adjacent quotes and silently corrupt parsing (this happened on 2026-08-01 with ping.ps1).
param(
  # Repo checkout to run. Pass a git worktree path to test an unmerged branch.
  [string]$Repo = 'C:\Users\bfaith\bfaith-portal',
  # Print what would run instead of running node (for checking the wiring).
  [switch]$DryRun,
  # Extra arguments for nightly.js, e.g. --skip-publish
  [string[]]$NodeArgs = @()
)
$ErrorActionPreference = 'Continue'

$JobId          = 'expected-profit-nightly'
$Script         = 'apps/expected-profit/nightly.js'
$LogDir         = Join-Path $Repo 'logs'
$LockFile       = Join-Path $Repo 'logs\expected-profit.lock'
$LockStaleHours = 8          # deadline is 06:00 (6.5h after the 23:30 start); anything older is a leftover
$KeepLogDays    = 14
$PingPs1        = Join-Path $Repo 'scripts\jobs-monitor\ping.ps1'

if (-not (Test-Path (Join-Path $Repo $Script))) { Write-Error "not a bfaith-portal checkout: $Repo"; exit 2 }
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$Stamp  = Get-Date -Format 'yyyy-MM-dd_HHmmss'
$Log    = Join-Path $LogDir "expected-profit-$Stamp.log"
$RunLog = Join-Path $LogDir 'expected-profit-runner.log'

function Log([string]$msg) {
  $line = '[' + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + '] ' + $msg
  Write-Output $line
  # PS 5.1 would default to ANSI here
  Add-Content -Path $RunLog -Value $line -Encoding UTF8
}

function Send-FailPing([string]$note) {
  if (-not (Test-Path $PingPs1)) { Log 'ping.ps1 not found - skipped'; return }
  $safe = ($note -replace '["\r\n\t]', ' ') -replace '\s+', ' '
  if ($safe.Length -gt 180) { $safe = $safe.Substring(0, 180) }
  # ping.ps1 has its own timeout and always exits 0 by design
  try { & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $PingPs1 -Id $JobId -Status 'fail' -Note $safe }
  catch { Log ('ping error: ' + $_.Exception.Message) }
}

# --- single instance -------------------------------------------------------
if (Test-Path $LockFile) {
  $age = (Get-Date) - (Get-Item $LockFile).LastWriteTime
  if ($age.TotalHours -lt $LockStaleHours) {
    Log ('another run is in progress (lock age ' + [int]$age.TotalMinutes + ' min) - exiting')
    exit 0
  }
  Log ('stale lock (' + [int]$age.TotalHours + 'h) - taking it over')
  Remove-Item -Force $LockFile
}
Set-Content -Path $LockFile -Value ("pid=" + $PID + " started=" + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss')) -Encoding ASCII

$code = 1
try {
  # --- log rotation (best effort) -----------------------------------------
  try {
    Get-ChildItem -Path $LogDir -Filter 'expected-profit-*.log' -ErrorAction SilentlyContinue |
      Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-$KeepLogDays) } |
      Remove-Item -Force -ErrorAction SilentlyContinue
  } catch { }

  $env:DATA_DIR = Join-Path $Repo 'data'
  Set-Location $Repo
  $argLine = @($Script) + $NodeArgs
  Log ('start : node ' + ($argLine -join ' ') + '  (DATA_DIR=' + $env:DATA_DIR + ')')
  Log ('log   : ' + $Log)

  if ($DryRun) {
    Log 'dry run - node was not started'
    $code = 0
  } else {
    # 2>&1 keeps stderr in the same file, in order
    & node.exe @argLine *>&1 | Tee-Object -FilePath $Log
    $code = $LASTEXITCODE
    if ($null -eq $code) { $code = 1 }
    Log ('node exit code: ' + $code)
    if ($code -ne 0) {
      # nightly.js pings 'fail' itself on the paths it knows about; this covers the ones it cannot reach
      Send-FailPing ('nightly.js exit ' + $code + ' (see ' + $Log + ')')
    }
  }
} catch {
  Log ('runner error: ' + $_.Exception.Message)
  Send-FailPing ('runner error: ' + $_.Exception.Message)
  $code = 1
} finally {
  Remove-Item -Force $LockFile -ErrorAction SilentlyContinue
}
exit $code
