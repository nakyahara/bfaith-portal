# ASCII only. Prepare tests subscription login under S4U without calling any AI model.
param([ValidateSet('Prepare','Enable','Disable')][string]$Mode = 'Prepare', [string]$Config = 'C:\tmp\product-scout-ai-work\kw-runtime\daily-config.json')
$ErrorActionPreference = 'Stop'
$taskName = 'ProductKWScout'
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..\..'))
$registry = Join-Path $root 'config\jobs-registry.mjs'
if (-not (Select-String -LiteralPath $registry -Pattern "id: 'product-kw-scout'" -Quiet)) { throw 'Registry entry missing' }
$cfg = Get-Content -LiteralPath $Config -Encoding UTF8 -Raw | ConvertFrom-Json
$runner = Join-Path $PSScriptRoot 'run-keywords.ps1'
$baseArgs = '-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File "' + $runner + '" -Config "' + $Config + '"'
$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existing -and $existing.Actions.Arguments -notlike ('*' + $runner + '*')) { throw 'Task name belongs to another runner' }
if ($Mode -eq 'Disable') { Disable-ScheduledTask -TaskName $taskName | Out-Null; exit 0 }
if ($Mode -eq 'Prepare') {
  if ($existing -and $existing.State -eq 'Running') { throw 'Task is running' }
  $principal = New-ScheduledTaskPrincipal -UserId 'bfaith' -LogonType S4U -RunLevel Limited
  $settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 90) -MultipleInstances IgnoreNew
  $probeAction = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument ($baseArgs + ' -Probe') -WorkingDirectory $PSScriptRoot
  $started = Get-Date
  Register-ScheduledTask -TaskName $taskName -Action $probeAction -Settings $settings -Principal $principal -Force | Out-Null
  try {
    Start-ScheduledTask -TaskName $taskName
    for ($n=0; $n -lt 90; $n++) {
      Start-Sleep -Seconds 1
      $info = Get-ScheduledTaskInfo -TaskName $taskName
      if ($info.LastRunTime -ge $started.AddSeconds(-2) -and (Get-ScheduledTask -TaskName $taskName).State -ne 'Running') { break }
    }
    $resultFile = Join-Path $cfg.state_dir 'task-preflight.json'
    if (-not (Test-Path -LiteralPath $resultFile) -or (Get-Item -LiteralPath $resultFile).LastWriteTime -lt $started) { throw 'S4U probe did not finish' }
    $result = Get-Content -LiteralPath $resultFile -Raw -Encoding UTF8 | ConvertFrom-Json
    if (-not $result.ok) { throw 'Subscription login unavailable under S4U' }
    Write-Output 'S4U subscription login verified; no model invoked'
  } finally {
    Disable-ScheduledTask -TaskName $taskName | Out-Null
  }
  $action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument $baseArgs -WorkingDirectory $PSScriptRoot
  Set-ScheduledTask -TaskName $taskName -Action $action -Trigger (New-ScheduledTaskTrigger -Daily -At '05:00') | Out-Null
  Disable-ScheduledTask -TaskName $taskName | Out-Null
  Write-Output 'Prepared daily task is DISABLED until portal deployment and Enable'
  exit 0
}
# Enable only after an actual publication/readback and a successful S4U probe.
$probe = Get-Content -LiteralPath (Join-Path $cfg.state_dir 'task-preflight.json') -Raw -Encoding UTF8 | ConvertFrom-Json
$published = Get-Content -LiteralPath (Join-Path $cfg.state_dir 'last-published.json') -Raw -Encoding UTF8 | ConvertFrom-Json
if (-not $probe.ok -or -not $published.body_hash) { throw 'Verified preflight/publication required' }
if (-not $existing) { throw 'Run Prepare first' }
$collector = Get-ScheduledTask -TaskName 'ProductIdeaScout'
if ($collector.Actions.Execute -notlike '*product-scout*\scripts\product-idea-scout\run-products.bat') { throw 'Unexpected collector action' }
$backup = Join-Path $cfg.state_dir 'collector-task-before-keywords.xml'
if (-not (Test-Path -LiteralPath $backup)) { Export-ScheduledTask -TaskName 'ProductIdeaScout' | Set-Content -LiteralPath $backup -Encoding UTF8 }
$collectorRunner = Join-Path $root 'scripts\product-idea-scout\run-products.bat'
$collectorAction = New-ScheduledTaskAction -Execute $collectorRunner -WorkingDirectory $collector.Actions.WorkingDirectory
Set-ScheduledTask -TaskName 'ProductIdeaScout' -Action $collectorAction | Out-Null
Enable-ScheduledTask -TaskName $taskName | Out-Null
Write-Output 'Enabled 05:00 KW run and 04:15 collector cutoff'
