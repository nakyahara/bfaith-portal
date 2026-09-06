# Iroha storage-box label agent - one shot installer for the Iroha PC (Brother QL-800).
#
# What this does:
#   1. checks that b-PAC (bpac.Document) is installed - stops with instructions if not
#   2. copies the agent files + template copies into C:\tools\iroha-label-agent
#      (the G: drive is a per-user mount and is NOT visible to the SYSTEM task, so everything
#       the agent needs at run time must live on C:)
#   3. asks for the device token issued by the portal and writes config.json
#   4. runs one poll to prove the server accepts this PC
#   5. registers the scheduled task (SYSTEM, runs without anybody signed in)
#   6. turns sleep off (a sleeping PC prints nothing - this PC slept after 15 min)
#
# Output: RESULT_setup.txt next to this script (send it back if something fails).
# ASCII only (PowerShell 5.1 / Task Scheduler safety). Japanese instructions: README.md next to this file.
[CmdletBinding()]
param(
  [string] $Token = '',
  [string] $Dest = 'C:\tools\iroha-label-agent'
)

$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [Text.Encoding]::UTF8
$ScriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$out = Join-Path $ScriptDir 'RESULT_setup.txt'
if (Test-Path $out) { Remove-Item $out -Force }
function W([string]$s) { Add-Content -Path $out -Value $s -Encoding UTF8; Write-Host $s }
function Fail([string]$s) { W ("NG: " + $s); W ""; W "----- stopped -----"; exit 1 }

W ("=== iroha label agent setup on {0} ({1}) ===" -f $env:COMPUTERNAME, (Get-Date -Format 'yyyy-MM-dd HH:mm'))

# --- 0. administrator ---------------------------------------------------------
$id = [Security.Principal.WindowsIdentity]::GetCurrent()
$pr = New-Object Security.Principal.WindowsPrincipal($id)
if (-not $pr.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
  Fail 'Run this as Administrator (right click 1_setup.bat -> run as administrator).'
}
W "admin       : OK"

# --- 1. b-PAC present? ----------------------------------------------------------
$probe = '$ErrorActionPreference = ''Stop''; try { $d = New-Object -ComObject bpac.Document; exit 0 } catch { exit 3 }'
$bpacOk = @()
foreach ($c in @((Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'),
                 (Join-Path $env:SystemRoot 'SysWOW64\WindowsPowerShell\v1.0\powershell.exe'))) {
  if (-not (Test-Path $c)) { continue }
  & $c -NoProfile -NonInteractive -Command $probe | Out-Null
  if ($LASTEXITCODE -eq 0) { $bpacOk += $c }
}
if ($bpacOk.Count -eq 0) {
  W "b-PAC       : NOT INSTALLED"
  W ""
  W "Install the Brother b-PAC Client Component first (free, needs a Brother developer account):"
  W "  https://support.brother.co.jp/j/s/es/dev/ja/bpac/download/index.html"
  W "  -> b-PAC 3.4 'Client Component' (64-bit for 64-bit Windows; the 32-bit one also works, the task"
  W "     will then be registered with the 32-bit PowerShell automatically)"
  W "Then run this setup again."
  Fail 'b-PAC missing'
}
W ("b-PAC       : OK via " + ($bpacOk -join ' ; '))

# --- 2. copy files ------------------------------------------------------------
$need = @('agent.ps1', 'print-label.ps1', 'install.ps1', 'config.example.json')
foreach ($f in $need) {
  $src = Join-Path $ScriptDir $f
  if (-not (Test-Path $src)) { Fail ("missing file in this folder: " + $f) }
  # A truncated file (e.g. a bad sync) would be copied over the working one and the task would
  # then start an empty script every 10 minutes. Refuse early instead.
  if ((Get-Item $src).Length -lt 100) { Fail ("file is empty or truncated - restore it from the G drive version history first: " + $f) }
}
$tplSrc = Join-Path $ScriptDir 'templates'
foreach ($t in @('hakolabel_auto_JAN.lbx', 'hakolabel_auto_FNSKU.lbx')) {
  $p = Join-Path $tplSrc $t
  if (-not (Test-Path $p)) { Fail ("template copy missing: " + $p + "  (run make-auto-lbx.ps1 first)") }
  if ((Get-Item $p).Length -lt 500) { Fail ("template copy looks truncated: " + $p) }
}
New-Item -ItemType Directory -Force -Path $Dest | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $Dest 'templates') | Out-Null
foreach ($f in $need) { Copy-Item (Join-Path $ScriptDir $f) (Join-Path $Dest $f) -Force }
Copy-Item (Join-Path $tplSrc '*.lbx') (Join-Path $Dest 'templates') -Force
if (Test-Path (Join-Path $ScriptDir 'README.md')) {
  Copy-Item (Join-Path $ScriptDir 'README.md') (Join-Path $Dest 'README.md') -Force
}
W ("copied to   : " + $Dest)

# --- 3. token / config.json ---------------------------------------------------
$cfgPath = Join-Path $Dest 'config.json'
if (-not $Token) {
  if (Test-Path $cfgPath) {
    $existing = (Get-Content $cfgPath -Raw -Encoding UTF8 | ConvertFrom-Json).token
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
  Write-Host '  https://bfaith-portal.onrender.com/apps/iroha-work/admin'
  Write-Host '  -> section "storage-box label (Iroha PC)" -> register the print agent -> token (shown only once)'
  Write-Host ''
  $Token = Read-Host 'token'
}
$Token = $Token.Trim()
if (-not $Token -or $Token -eq 'PASTE_DEVICE_TOKEN_HERE') { Fail 'no token given.' }
if ($Token.Length -lt 20) { Fail 'that does not look like a token (too short).' }

$cfg = Get-Content (Join-Path $Dest 'config.example.json') -Raw -Encoding UTF8 | ConvertFrom-Json
$cfg.token = $Token
$cfg.workDir = (Join-Path $Dest 'work')
$cfg.templates.jan   = (Join-Path $Dest 'templates\hakolabel_auto_JAN.lbx')
$cfg.templates.fnsku = (Join-Path $Dest 'templates\hakolabel_auto_FNSKU.lbx')
if ($cfg.PSObject.Properties.Name -contains '_comment') { $cfg.PSObject.Properties.Remove('_comment') }
# UTF-8 without BOM (PowerShell 5.1 Out-File would add one and ConvertFrom-Json chokes on it)
[IO.File]::WriteAllText($cfgPath, ($cfg | ConvertTo-Json -Depth 5), (New-Object Text.UTF8Encoding($false)))
W ("config.json : written (token " + $Token.Substring(0, 4) + "... , " + $Token.Length + " chars)")

# --- 3b. stop a resident agent ------------------------------------------------
# An agent already running (the scheduled task from an earlier setup) holds the single-instance
# mutex, so the test poll below could not even start - and it would grab the test job anyway.
# Stop it here; step 5 starts a fresh one from the files copied above.
if (Get-ScheduledTask -TaskName 'BFaith-IrohaLabelAgent' -ErrorAction SilentlyContinue) {
  Stop-ScheduledTask -TaskName 'BFaith-IrohaLabelAgent' -ErrorAction SilentlyContinue
}
$agentPath = Join-Path $Dest 'agent.ps1'
$stale = @(Get-CimInstance Win32_Process -Filter "Name = 'powershell.exe'" |
  Where-Object { $_.CommandLine -like ('*' + $agentPath + '*') })
foreach ($p in $stale) { Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue }
if ($stale.Count -gt 0) { Start-Sleep -Seconds 2 }
W ("old agent   : stopped " + $stale.Count + " running instance(s)")

# --- 4. one poll (proves the server accepts this PC) --------------------------
W ""
W "--- test poll (agent.ps1 -Once) ---"
# Use the PowerShell that can see b-PAC (the same one install.ps1 will register).
$psExe = $bpacOk[0]
# The child's stderr must be collected, not fatal: with $ErrorActionPreference = Stop every
# stderr line of a native command becomes a terminating NativeCommandError and hides the output.
$ErrorActionPreference = 'Continue'
$log = & $psExe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $Dest 'agent.ps1') -ConfigPath (Join-Path $Dest 'config.json') -Once 2>&1
$code = $LASTEXITCODE
$ErrorActionPreference = 'Stop'
foreach ($line in $log) { W ("  " + [string]$line) }
if ($code -ne 0) {
  W ""
  W "The test poll failed. Common causes:"
  W "  not authorised (401)        -> token is wrong or was revoked; issue a new one"
  W "  server refused to hand out  -> no printer registered for this PC in the portal"
  W "  HTTP 404 from /print/next   -> the portal does not have the print queue yet (server side PR not deployed)"
  Fail ("agent.ps1 -Once exited with " + $code)
}
W "test poll   : OK"

# --- 5. scheduled task --------------------------------------------------------
W ""
W "--- register scheduled task ---"
$ErrorActionPreference = 'Continue'
$log2 = & powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $Dest 'install.ps1') 2>&1
$code2 = $LASTEXITCODE
$ErrorActionPreference = 'Stop'
foreach ($line in $log2) { W ("  " + [string]$line) }
if ($code2 -ne 0) { Fail ("install.ps1 exited with " + $code2) }

$task = Get-ScheduledTask -TaskName 'BFaith-IrohaLabelAgent' -ErrorAction SilentlyContinue
if (-not $task) { Fail 'scheduled task was not registered.' }
W ("task state  : " + $task.State)

# --- 6. no sleep --------------------------------------------------------------
& powercfg /change standby-timeout-ac 0 | Out-Null
& powercfg /change standby-timeout-dc 0 | Out-Null
& powercfg /change hibernate-timeout-ac 0 | Out-Null
W "sleep       : disabled (standby / hibernate = never)"

W ""
W "----- done -----"
W "The agent is running. Press the box-label button on the iroha-work iPad (card detail) and"
W "the label should come out of the Brother QL-800."
W ""
W ("log file    : " + (Join-Path $Dest 'work\agent.log'))
