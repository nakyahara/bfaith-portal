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
Push-Location $PSScriptRoot
try {
  $entry = if ($Probe) { 'kw-preflight.cjs' } else { 'kw-publish.cjs' }
  $cfg | & node (Join-Path $PSScriptRoot $entry) >> $log 2>&1
  $result = $LASTEXITCODE
} finally { Pop-Location }
exit $result
