# install.ps1 - register the "expected profit" nightly task on the miniPC (idempotent).
#
#   powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\bfaith\bfaith-portal\scripts\expected-profit\install.ps1
#
# Registers Task Scheduler entry [ExpectedProfitNightly]:
#   daily 23:30 -> scripts\expected-profit\run-expected-profit-nightly.ps1 -> node apps/expected-profit/nightly.js
#
# Why 23:30 and a 7h limit: the batch spends 50-100 min fetching mall prices and must be finished before
# daily-sync (07:00, P1) starts. nightly.js stops by itself at the 06:00 deadline; the task limit of 7h
# (= 06:30) is the outer bound, matching grace_hours in config/jobs-registry.mjs.
#
# Principal: bfaith / Interactive / Limited - the same pattern as PhGenerateNightly and MallCsvFetchAll
# (no stored password; requires bfaith to stay logged on at the console).
#
# IMPORTANT: keep this file ASCII-only (PS 5.1 reads BOM-less files as CP932).
param(
  [string]$Repo = 'C:\Users\bfaith\bfaith-portal',
  [string]$TaskName = 'ExpectedProfitNightly',
  # Print the plan and the environment check without touching Task Scheduler.
  [switch]$DryRun
)
$ErrorActionPreference = 'Stop'

$Me     = 'bfaith'
$At     = '23:30'
$Runner = Join-Path $Repo 'scripts\expected-profit\run-expected-profit-nightly.ps1'
$Entry  = Join-Path $Repo 'apps\expected-profit\nightly.js'
$EnvFile = Join-Path $Repo '.env'

if (-not (Test-Path $Entry))  { throw "not a bfaith-portal checkout (nightly.js not found): $Repo" }
if (-not (Test-Path $Runner)) { throw "runner not found: $Runner  (git pull?)" }

# --- environment check -----------------------------------------------------
# The batch needs these. 2026-09-08 lesson (design doc 16-15): the first three publish attempts all failed
# on configuration assumptions, so say out loud what is missing instead of finding out at 23:30.
$required = @(
  'SP_API_SELLER_ID', 'SP_API_REFRESH_TOKEN', 'SP_API_CLIENT_ID', 'SP_API_CLIENT_SECRET',
  'RAKUTEN_SERVICE_SECRET', 'RAKUTEN_LICENSE_KEY',
  'RENDER_MIRROR_URL', 'MIRROR_SYNC_KEY',
  'JOBS_MONITOR_URL', 'JOBS_MONITOR_TOKEN'
)
$envText = if (Test-Path $EnvFile) { Get-Content -LiteralPath $EnvFile -Raw } else { '' }
$missing = @()
foreach ($k in $required) {
  $inFile = $envText -match ('(?m)^\s*' + [regex]::Escape($k) + '\s*=\s*\S')
  $inEnv  = [bool](Get-Item -Path ('Env:' + $k) -ErrorAction SilentlyContinue)
  if (-not ($inFile -or $inEnv)) { $missing += $k }
}

$node = (Get-Command node.exe -ErrorAction SilentlyContinue)
if (-not $node) { throw 'node.exe not found on PATH (the task would fail every night)' }

Write-Output 'plan:'
Write-Output ('  task    : ' + $TaskName + '  daily ' + $At + '  as ' + $Me + ' (Interactive, Limited)')
Write-Output ('  runs    : ' + $Runner)
Write-Output ('  which   : node ' + $Entry)
Write-Output ('  node    : ' + $node.Source)
Write-Output ('  logs    : ' + (Join-Path $Repo 'logs') + '\expected-profit-*.log')
if ($missing.Count -gt 0) {
  Write-Output ('  MISSING env (' + $EnvFile + '): ' + ($missing -join ', '))
} else {
  Write-Output '  env     : all required keys present'
}

if ($DryRun) { Write-Output ''; Write-Output 'dry run - nothing was registered'; exit 0 }

# --- register --------------------------------------------------------------
$action = New-ScheduledTaskAction -Execute 'powershell.exe' `
            -Argument ('-NoProfile -ExecutionPolicy Bypass -File "' + $Runner + '" -Repo "' + $Repo + '"')
$trigger = New-ScheduledTaskTrigger -Daily -At $At
$principal = New-ScheduledTaskPrincipal -UserId $Me -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable `
              -ExecutionTimeLimit (New-TimeSpan -Hours 7) `
              -MultipleInstances IgnoreNew `
              -RunOnlyIfNetworkAvailable
Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
  -Principal $principal -Settings $settings -Force | Out-Null

# --- verify what actually landed (not what we asked for) -------------------
$t = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if (-not $t) { throw "task $TaskName was not registered" }
$info = Get-ScheduledTaskInfo -TaskName $TaskName
$gotArg = $t.Actions[0].Arguments
if ($gotArg -notlike ('*' + $Runner + '*')) { throw "registered action does not point at the runner: $gotArg" }

Write-Output ''
Write-Output 'registered:'
Write-Output ('  ' + $TaskName + '  state=' + $t.State + '  next=' + $info.NextRunTime)
Write-Output ''
Write-Output 'next steps:'
Write-Output ('  1. git pull in ' + $Repo + '  (the new shipping columns come from the merged PR)')
if ($missing.Count -gt 0) {
  Write-Output ('  2. fill in the missing env keys: ' + ($missing -join ', '))
}
Write-Output ('  3. test run  : powershell -NoProfile -ExecutionPolicy Bypass -File "' + $Runner + '" --% ')
Write-Output ('     or without publishing: -NodeArgs --skip-publish')
Write-Output ('  4. watch     : ' + (Join-Path $Repo 'logs') + '\expected-profit-runner.log')
Write-Output ('  5. the morning after, jobs-monitor should show ' + 'expected-profit-nightly' + ' = ok')
