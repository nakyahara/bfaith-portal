# jobs-monitor success ping. Called from the tail of each runner (bat/ps1) on miniPC.
#
#   powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\bfaith\bfaith-portal\scripts\jobs-monitor\ping.ps1 -Id warehouse-daily-sync -Status ok
#
# Design:
#   - NEVER fail the caller job (always exit 0). Monitoring is secondary to the job itself.
#   - Token and URL come from bfaith-portal\.env (JOBS_MONITOR_TOKEN / JOBS_MONITOR_URL).
#     If unset, silently do nothing (safe to wire into runners before rollout).
#
# IMPORTANT: keep this file ASCII-only.
#   Windows PowerShell 5.1 reads BOM-less files as ANSI (CP932). Multi-byte characters
#   can swallow adjacent quotes and silently corrupt parsing -- this actually happened
#   on 2026-08-01 (Japanese comment ate a closing quote, Authorization header broke,
#   every ping got 401 while the same code pasted inline worked fine).
param(
  [Parameter(Mandatory = $true)][string]$Id,
  [ValidateSet('ok', 'fail', 'start')][string]$Status = 'ok'
)

$ErrorActionPreference = 'SilentlyContinue'

# Best-effort local log (to tell "ping was not sent" from "ping was sent but not received"
# after a dead-man alert). Rewrites itself past 1MB. Logging failures are ignored too.
$logFile = Join-Path $env:TEMP 'jobs-monitor-ping.log'
function Write-PingLog([string]$msg) {
  try {
    if ((Test-Path $logFile) -and (Get-Item $logFile).Length -gt 1MB) { Remove-Item $logFile -Force }
    ('[' + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + '] ' + $Id + ' ' + $Status + ' ' + $msg) | Out-File $logFile -Append -Encoding utf8
  } catch { }
}

try {
  $envFile = 'C:\Users\bfaith\bfaith-portal\.env'
  if (-not (Test-Path $envFile)) { Write-PingLog 'skip: no .env'; exit 0 }
  $token = (Select-String -Path $envFile -Pattern '^JOBS_MONITOR_TOKEN=' | Select-Object -First 1).Line -replace '^[^=]+=', ''
  if (-not $token) { Write-PingLog 'skip: JOBS_MONITOR_TOKEN not set'; exit 0 }
  $base = (Select-String -Path $envFile -Pattern '^JOBS_MONITOR_URL=' | Select-Object -First 1).Line -replace '^[^=]+=', ''
  if (-not $base) { $base = 'https://bfaith-portal.onrender.com' }
  $uri = $base.TrimEnd('/') + '/apps/jobs-monitor/ping/' + $Id + '?status=' + $Status
  Invoke-RestMethod -Uri $uri -Method Post -Headers @{ Authorization = 'Bearer ' + $token } -TimeoutSec 15 | Out-Null
  Write-PingLog 'sent'
} catch {
  # Never take the caller down with us.
  Write-PingLog ('error: ' + $_.Exception.Message)
}
exit 0
