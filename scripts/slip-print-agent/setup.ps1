# Slip print agent - one shot installer for the shipping PC.
#
# What this does:
#   1. copies the agent files into C:\tools\slip-print-agent
#   2. asks for the device token issued by the portal and writes config.json
#   3. runs one poll to prove the server accepts this PC
#   4. registers the scheduled task (SYSTEM, runs without anybody signed in)
#   5. turns sleep off (a sleeping PC prints nothing)
#
# Output: RESULT_setup.txt next to this script (send it back if something fails).
# ASCII only (PowerShell 5.1 / Task Scheduler safety). Japanese instructions: see the tejun README next to this file.
[CmdletBinding()]
param(
  [string] $Token = '',
  [string] $Dest = 'C:\tools\slip-print-agent'
)

$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [Text.Encoding]::UTF8
$out = Join-Path $PSScriptRoot 'RESULT_setup.txt'
if (Test-Path $out) { Remove-Item $out -Force }
function W([string]$s) { Add-Content -Path $out -Value $s -Encoding UTF8; Write-Host $s }
function Fail([string]$s) { W ("NG: " + $s); W ""; W "----- stopped -----"; exit 1 }

W ("=== slip print agent setup on {0} ({1}) ===" -f $env:COMPUTERNAME, (Get-Date -Format 'yyyy-MM-dd HH:mm'))

# --- 0. administrator ---------------------------------------------------------
$id = [Security.Principal.WindowsIdentity]::GetCurrent()
$pr = New-Object Security.Principal.WindowsPrincipal($id)
if (-not $pr.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
  Fail 'Run this as Administrator (right click 1_setup.bat -> run as administrator).'
}
W "admin       : OK"

# --- 1. copy files ------------------------------------------------------------
$need = @('agent.ps1', 'install.ps1', 'config.example.json', 'SumatraPDF.exe')
foreach ($f in $need) {
  $src = Join-Path $PSScriptRoot $f
  if (-not (Test-Path $src)) { Fail ("missing file in this folder: " + $f) }
  # A truncated file (e.g. a bad sync) would be copied over the working one and the task would
  # then start an empty script every 10 minutes. Refuse early instead.
  if ((Get-Item $src).Length -lt 1KB) { Fail ("file is empty or truncated - restore it from the G drive version history first: " + $f) }
}
New-Item -ItemType Directory -Force -Path $Dest | Out-Null
foreach ($f in $need) { Copy-Item (Join-Path $PSScriptRoot $f) (Join-Path $Dest $f) -Force }
if (Test-Path (Join-Path $PSScriptRoot 'README.md')) {
  Copy-Item (Join-Path $PSScriptRoot 'README.md') (Join-Path $Dest 'README.md') -Force
}
W ("copied to   : " + $Dest)

# --- 2. token / config.json ---------------------------------------------------
$cfgPath = Join-Path $Dest 'config.json'
if (-not $Token) {
  if (Test-Path $cfgPath) {
    $existing = (Get-Content $cfgPath -Raw | ConvertFrom-Json).token
    if ($existing -and $existing -ne 'PASTE_DEVICE_TOKEN_HERE') {
      Write-Host ''
      Write-Host 'config.json already has a token. Press Enter to keep it, or paste a new one.'
      $Token = Read-Host 'token (Enter = keep current)'
      if (-not $Token) { $Token = $existing }
    }
  }
}
if (-not $Token) {
  Write-Host ''
  Write-Host 'Paste the token from the portal:'
  Write-Host '  https://picking.bfaith-wh.uk/apps/packing/admin/devices'
  Write-Host '  -> "shipping PC print agent" -> issue token (shown only once)'
  Write-Host ''
  $Token = Read-Host 'token'
}
$Token = $Token.Trim()
if (-not $Token -or $Token -eq 'PASTE_DEVICE_TOKEN_HERE') { Fail 'no token given.' }
if ($Token.Length -lt 20) { Fail 'that does not look like a token (too short).' }

$cfg = Get-Content (Join-Path $Dest 'config.example.json') -Raw | ConvertFrom-Json
$cfg.token = $Token
$cfg.sumatraPath = (Join-Path $Dest 'SumatraPDF.exe')
$cfg.workDir = (Join-Path $Dest 'work')
if ($cfg.PSObject.Properties.Name -contains '_comment') { $cfg.PSObject.Properties.Remove('_comment') }
# UTF-8 without BOM (PowerShell 5.1 Out-File would add one and ConvertFrom-Json chokes on it)
[IO.File]::WriteAllText($cfgPath, ($cfg | ConvertTo-Json), (New-Object Text.UTF8Encoding($false)))
W ("config.json : written (token " + $Token.Substring(0, 4) + "... , " + $Token.Length + " chars)")

# --- 2b. stop a resident agent ------------------------------------------------
# An agent already running (the scheduled task from an earlier setup) holds the single-instance
# mutex, so the test poll below could not even start - and it would grab the test job anyway.
# Stop it here; step 4 starts a fresh one from the files copied above.
if (Get-ScheduledTask -TaskName 'BFaith-SlipPrintAgent' -ErrorAction SilentlyContinue) {
  Stop-ScheduledTask -TaskName 'BFaith-SlipPrintAgent' -ErrorAction SilentlyContinue
}
$agentPath = Join-Path $Dest 'agent.ps1'
$stale = @(Get-CimInstance Win32_Process -Filter "Name = 'powershell.exe'" |
  Where-Object { $_.CommandLine -like ('*' + $agentPath + '*') })
foreach ($p in $stale) { Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue }
if ($stale.Count -gt 0) { Start-Sleep -Seconds 2 }
W ("old agent   : stopped " + $stale.Count + " running instance(s)")

# --- 3. one poll (proves the server accepts this PC) --------------------------
W ""
W "--- test poll (agent.ps1 -Once) ---"
# The child's stderr must be collected, not fatal: with $ErrorActionPreference = Stop every
# stderr line of a native command becomes a terminating NativeCommandError and hides the output.
$ErrorActionPreference = 'Continue'
$log = & powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $Dest 'agent.ps1') -ConfigPath (Join-Path $Dest 'config.json') -Once 2>&1
$code = $LASTEXITCODE
$ErrorActionPreference = 'Stop'
foreach ($line in $log) { W ("  " + [string]$line) }
if ($code -ne 0) {
  W ""
  W "The test poll failed. Common causes:"
  W "  not authorised (401)        -> token is wrong or was revoked; issue a new one"
  W "  server refused to hand out  -> no printers registered for this PC in the portal"
  Fail ("agent.ps1 -Once exited with " + $code)
}
W "test poll   : OK"

# --- 4. scheduled task --------------------------------------------------------
W ""
W "--- register scheduled task ---"
$ErrorActionPreference = 'Continue'
$log2 = & powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $Dest 'install.ps1') 2>&1
$code2 = $LASTEXITCODE
$ErrorActionPreference = 'Stop'
foreach ($line in $log2) { W ("  " + [string]$line) }
if ($code2 -ne 0) { Fail ("install.ps1 exited with " + $code2) }

$task = Get-ScheduledTask -TaskName 'BFaith-SlipPrintAgent' -ErrorAction SilentlyContinue
if (-not $task) { Fail 'scheduled task was not registered.' }
W ("task state  : " + $task.State)

# --- 5. no sleep --------------------------------------------------------------
& powercfg /change standby-timeout-ac 0 | Out-Null
& powercfg /change standby-timeout-dc 0 | Out-Null
& powercfg /change hibernate-timeout-ac 0 | Out-Null
W "sleep       : disabled (standby / hibernate = never)"

W ""
W "----- done -----"
W "The agent is running. Press the reprint button on the packing iPad and the slip"
W "should come out of the printer registered for that shipping class."
W ""
W ("log file    : " + (Join-Path $Dest 'work\agent.log'))
