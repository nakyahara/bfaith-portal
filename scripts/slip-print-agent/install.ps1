#Requires -Version 5.1
<#
  Register the slip print agent as a scheduled task on the shipping PC.

  Run this from an ADMINISTRATOR PowerShell window.

  The task is deliberately configured so that nobody has to be signed in:
    - trigger  : at system startup (plus a repeat, so a crashed run comes back)
    - account  : SYSTEM  (proved on 2026-08-27 that SYSTEM can see and use the USB printer)
    - battery  : do NOT stop on battery (the default "start only on AC" silently kills it)
    - wake     : allowed to wake the machine

  ASCII only - Task Scheduler + PowerShell 5.1 mangles non-ASCII .ps1 files.
  Japanese instructions: README.md next to this file.
#>
[CmdletBinding()]
param(
  [string] $TaskName = 'BFaith-SlipPrintAgent',
  [switch] $Uninstall
)

$ErrorActionPreference = 'Stop'

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
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed scheduled task: $TaskName"
  } else {
    Write-Host "Nothing to remove: $TaskName"
  }
  return
}

$agent  = Join-Path $PSScriptRoot 'agent.ps1'
$config = Join-Path $PSScriptRoot 'config.json'
foreach ($f in @($agent, $config)) {
  if (-not (Test-Path $f)) { throw "Missing file: $f" }
}

# Fail before registering anything if the config is obviously not filled in - a task that
# starts and immediately dies on every boot is worse than no task.
$cfg = Get-Content $config -Raw -Encoding UTF8 | ConvertFrom-Json
if (-not $cfg.token -or $cfg.token -eq 'PASTE_DEVICE_TOKEN_HERE') {
  throw "config.json still has the placeholder token. Register the shipping PC in the admin screen first."
}
if (-not $cfg.baseUrl) { throw 'config.json has no baseUrl.' }

$sumatra = if ($cfg.sumatraPath) { $cfg.sumatraPath } else { Join-Path $PSScriptRoot 'SumatraPDF.exe' }
if (-not (Test-Path $sumatra)) {
  throw "SumatraPDF.exe not found at $sumatra - copy it next to agent.ps1 or set sumatraPath in config.json."
}

$action = New-ScheduledTaskAction -Execute 'powershell.exe' `
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
  -Description 'B-Faith slip print agent (pulls print jobs from the packing server)' | Out-Null

Start-ScheduledTask -TaskName $TaskName
Start-Sleep -Seconds 3
$t = Get-ScheduledTask -TaskName $TaskName
$i = Get-ScheduledTaskInfo -TaskName $TaskName

Write-Host ''
Write-Host "Registered and started: $TaskName"
Write-Host ("  State        : {0}" -f $t.State)
Write-Host ("  Last run     : {0}" -f $i.LastRunTime)
Write-Host ("  Last result  : {0}" -f $i.LastTaskResult)
Write-Host ''
Write-Host 'Check the log after a few seconds:'
Write-Host ("  Get-Content '{0}' -Tail 20" -f (Join-Path $PSScriptRoot 'work\agent.log'))
Write-Host ''
Write-Host 'IMPORTANT: also set the power plan so the PC never sleeps, otherwise nothing prints:'
Write-Host '  powercfg /change standby-timeout-ac 0'
Write-Host '  powercfg /change standby-timeout-dc 0'
