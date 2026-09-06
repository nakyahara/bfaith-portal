#Requires -Version 5.1
<#
  Register the Iroha storage-box label agent as a scheduled task on the Iroha PC.

  Run this from an ADMINISTRATOR PowerShell window.

  The task is deliberately configured so that nobody has to be signed in:
    - trigger  : at system startup (plus a repeat, so a crashed run comes back)
    - account  : SYSTEM  (the slip print agent proved SYSTEM can use a USB printer;
                 b-PAC under SYSTEM is a first-time check - see README "verify on the machine")
    - battery  : do NOT stop on battery (the default "start only on AC" silently kills it)
    - wake     : allowed to wake the machine

  b-PAC bitness: the component must match the PowerShell that runs the agent. This script
  probes 64-bit powershell.exe first, then the 32-bit one (SysWOW64), and registers the task
  with whichever can create the bpac.Document COM object. If neither can, it stops.

  ASCII only - Task Scheduler + PowerShell 5.1 mangles non-ASCII .ps1 files.
  Japanese instructions: README.md next to this file.
#>
[CmdletBinding()]
param(
  [string] $TaskName = 'BFaith-IrohaLabelAgent',
  [switch] $Uninstall
)

$ErrorActionPreference = 'Stop'
$ScriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }

function Assert-Admin {
  $id = [Security.Principal.WindowsIdentity]::GetCurrent()
  $pr = New-Object Security.Principal.WindowsPrincipal($id)
  if (-not $pr.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'Please run this from an Administrator PowerShell window.'
  }
}

Assert-Admin

if ($Uninstall) {
  if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed scheduled task: $TaskName"
  } else {
    Write-Host "Nothing to remove: $TaskName"
  }
  return
}

$agent  = Join-Path $ScriptDir 'agent.ps1'
$helper = Join-Path $ScriptDir 'print-label.ps1'
$config = Join-Path $ScriptDir 'config.json'
foreach ($f in @($agent, $helper, $config)) {
  if (-not (Test-Path $f)) { throw "Missing file: $f" }
}

# Fail before registering anything if the config is obviously not filled in - a task that
# starts and immediately dies on every boot is worse than no task.
$cfg = Get-Content $config -Raw -Encoding UTF8 | ConvertFrom-Json
if (-not $cfg.token -or $cfg.token -eq 'PASTE_DEVICE_TOKEN_HERE') {
  throw "config.json still has the placeholder token. Register this PC in the portal admin screen first."
}
if (-not $cfg.baseUrl) { throw 'config.json has no baseUrl.' }
foreach ($t in @('jan', 'fnsku')) {
  $p = [string]$cfg.templates.$t
  if (-not $p) { throw "config.json has no templates.$t" }
  if (-not (Test-Path $p)) { throw "template not found: $p (run make-auto-lbx.ps1 and copy the templates folder)" }
}

# --- which powershell.exe can talk to b-PAC? ------------------------------------
# NOTE: the double quotes around Stop would be eaten by the child's command-line parsing
# (seen 2026-09-05: "The term 'Stop' is not recognized"), so use doubled single quotes.
$probe = '$ErrorActionPreference = ''Stop''; try { $d = New-Object -ComObject bpac.Document; exit 0 } catch { exit 3 }'
$candidates = @(
  (Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'),
  (Join-Path $env:SystemRoot 'SysWOW64\WindowsPowerShell\v1.0\powershell.exe')
)
$psExe = $null
foreach ($c in $candidates) {
  if (-not (Test-Path $c)) { continue }
  & $c -NoProfile -NonInteractive -Command $probe | Out-Null
  if ($LASTEXITCODE -eq 0) { $psExe = $c; break }
  Write-Host ("b-PAC not available via {0}" -f $c)
}
if (-not $psExe) {
  throw 'b-PAC (bpac.Document) is not installed for 64-bit or 32-bit PowerShell. Install the b-PAC Client Component first (see README), then run this again.'
}
Write-Host ("b-PAC found via {0}" -f $psExe)

$action = New-ScheduledTaskAction -Execute $psExe `
  -Argument ('-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "{0}"' -f $agent)

# At startup, and a repeating trigger as a safety net so a crash does not need a reboot.
$atStartup = New-ScheduledTaskTrigger -AtStartup
$repeat    = New-ScheduledTaskTrigger -Once -At (Get-Date).Date.AddMinutes(1) `
  -RepetitionInterval (New-TimeSpan -Minutes 10)

$principal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest

$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
  -WakeToRun `
  -StartWhenAvailable `
  -MultipleInstances IgnoreNew `
  -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) `
  -ExecutionTimeLimit ([TimeSpan]::Zero)

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
  # Stop the running agent first, otherwise the OLD agent.ps1 keeps running and the new
  # task never starts a fresh one (MultipleInstances = IgnoreNew).
  Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
  Get-CimInstance Win32_Process -Filter "Name = 'powershell.exe'" |
    Where-Object { $_.CommandLine -like ('*' + $agent + '*') } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
  Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}
Register-ScheduledTask -TaskName $TaskName -Action $action `
  -Trigger @($atStartup, $repeat) -Principal $principal -Settings $settings `
  -Description 'B-Faith iroha storage-box label agent - pulls label jobs from bfaith-portal (iroha-work) and prints on the QL-800 via b-PAC' | Out-Null

Start-ScheduledTask -TaskName $TaskName
Start-Sleep -Seconds 3
$t = Get-ScheduledTask -TaskName $TaskName
$i = Get-ScheduledTaskInfo -TaskName $TaskName

Write-Host ''
Write-Host "Registered and started: $TaskName"
Write-Host ("  PowerShell   : {0}" -f $psExe)
Write-Host ("  State        : {0}" -f $t.State)
Write-Host ("  Last run     : {0}" -f $i.LastRunTime)
Write-Host ("  Last result  : {0}" -f $i.LastTaskResult)
Write-Host ''
Write-Host 'Check the log after a few seconds:'
Write-Host ("  Get-Content '{0}' -Tail 20" -f (Join-Path $ScriptDir 'work\agent.log'))
Write-Host ''
Write-Host 'IMPORTANT: also set the power plan so the PC never sleeps, otherwise nothing prints:'
Write-Host '  powercfg /change standby-timeout-ac 0'
Write-Host '  powercfg /change standby-timeout-dc 0'
