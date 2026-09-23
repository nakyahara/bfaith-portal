# ASCII only: runs with Windows PowerShell 5.1. Config has paths and attestations, no secrets.
param([string]$Config = 'C:\tmp\product-scout-ai-work\kw-runtime\daily-config.json', [switch]$Probe)
$ErrorActionPreference = 'Stop'
$OutputEncoding = [Console]::OutputEncoding = [Text.UTF8Encoding]::new()
$cfg = Get-Content -LiteralPath $Config -Raw -Encoding UTF8
$obj = $cfg | ConvertFrom-Json
if (-not (Test-Path -LiteralPath $obj.state_dir)) { New-Item -ItemType Directory -Path $obj.state_dir | Out-Null }
$log = Join-Path $obj.state_dir 'daily.log'
if ((Test-Path -LiteralPath $log) -and (Get-Item -LiteralPath $log).Length -gt 5MB) {
  Get-Content -LiteralPath $log -Tail 500 -Encoding UTF8 | Set-Content -LiteralPath ($log + '.previous') -Encoding UTF8
  Clear-Content -LiteralPath $log
}
# Never let logging stop a failure report: another run may hold daily.log open (node output redirection)
function Write-Log([string]$msg) {
  try { Add-Content -LiteralPath $log -Value ('[' + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + '] run-keywords: ' + $msg) -Encoding UTF8 -ErrorAction Stop } catch { }
}
function Send-FailPing([string]$note) {
  if ($obj.ping_script -and [IO.Path]::IsPathRooted([string]$obj.ping_script) -and (Test-Path -LiteralPath $obj.ping_script)) {
    try { & powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File $obj.ping_script -Id 'product-kw-scout' -Status 'fail' -Note $note | Out-Null } catch { }
  }
}

# One Claude at a time on this machine (PR3-0, scripts\claude-guard\ClaudeGuard.ps1). PhGenerateNightly (02:30)
# uses the same subscription OAuth. This process joins a KILL_ON_JOB_CLOSE job first, so node and the claude it
# starts die with it; the lock is held open until this script exits (the OS releases it if this process dies).
. (Join-Path $PSScriptRoot '..\..\claude-guard\ClaudeGuard.ps1')
if (-not (Enable-KillOnCloseJob)) { Write-Log 'could not create the kill-on-close job object'; Send-FailPing 'claude guard: job object failed'; exit 1 }
$waitMin = if ($Probe) { 1 } else { 30 }
$deadline = (Get-Date).ToUniversalTime().AddMinutes($waitMin)
if (-not (Enter-ClaudeLock -DeadlineUtc $deadline)) {
  Send-FailPing 'claude guard: another Claude job held the lock'
  Write-Log ('another Claude job held C:\tools\claude-lock\claude.lock for ' + $waitMin + ' min')
  exit 1
}
try {
  if (-not (Wait-NoClaudeResidue -DeadlineUtc $deadline)) {
    Write-Log ('Claude or an AI runner is still running: ' + ((Get-ClaudeResidue | ForEach-Object { $_.Name + ':' + $_.Pid }) -join ' '))
    Send-FailPing 'claude guard: Claude still running'
    exit 1
  }
  Push-Location $PSScriptRoot
  try {
    $entry = if ($Probe) { 'kw-preflight.cjs' } else { 'kw-publish.cjs' }
    $cfg | & node (Join-Path $PSScriptRoot $entry) >> $log 2>&1
    $result = $LASTEXITCODE
  } finally { Pop-Location }
} finally { Exit-ClaudeLock }
exit $result
