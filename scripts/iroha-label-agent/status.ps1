# Show the current state of the iroha storage-box label agent (for troubleshooting).
# Output: RESULT_status.txt next to this script.
# ASCII only (PowerShell 5.1 safety).
$ErrorActionPreference = 'Continue'
[Console]::OutputEncoding = [Text.Encoding]::UTF8
$ScriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$dest = 'C:\tools\iroha-label-agent'
$out = Join-Path $ScriptDir 'RESULT_status.txt'
if (Test-Path $out) { Remove-Item $out -Force }
function W([string]$s) { Add-Content -Path $out -Value $s -Encoding UTF8; Write-Host $s }

W ("=== iroha label agent status on {0} ({1}) ===" -f $env:COMPUTERNAME, (Get-Date -Format 'yyyy-MM-dd HH:mm'))

$task = Get-ScheduledTask -TaskName 'BFaith-IrohaLabelAgent' -ErrorAction SilentlyContinue
if ($task) {
  $info = Get-ScheduledTaskInfo -TaskName 'BFaith-IrohaLabelAgent'
  W ("task        : " + $task.State)
  W ("  last run  : " + $info.LastRunTime + "  (result " + $info.LastTaskResult + ")")
  W ("  next run  : " + $info.NextRunTime)
  W ("  runs with : " + $task.Actions[0].Execute)
} else {
  W "task        : NOT REGISTERED (run 1_setup.bat)"
}

$cfg = Join-Path $dest 'config.json'
W ("config.json : " + $(if (Test-Path $cfg) { 'present' } else { 'MISSING' }))
foreach ($t in @('hakolabel_auto_JAN.lbx', 'hakolabel_auto_FNSKU.lbx')) {
  $p = Join-Path $dest ('templates\' + $t)
  W (("template    : {0} " -f $t) + $(if (Test-Path $p) { 'present' } else { 'MISSING' }))
}

W ""
W "--- b-PAC ---"
$probe = '$ErrorActionPreference = "Stop"; try { $d = New-Object -ComObject bpac.Document; exit 0 } catch { exit 3 }'
foreach ($c in @((Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'),
                 (Join-Path $env:SystemRoot 'SysWOW64\WindowsPowerShell\v1.0\powershell.exe'))) {
  if (-not (Test-Path $c)) { continue }
  & $c -NoProfile -NonInteractive -Command $probe | Out-Null
  $bits = if ($c -like '*SysWOW64*') { '32-bit' } else { '64-bit' }
  W ("  " + $bits + " PowerShell : " + $(if ($LASTEXITCODE -eq 0) { 'bpac.Document OK' } else { 'NOT available' }))
}

W ""
W "--- printers on this PC ---"
foreach ($p in (Get-Printer -ErrorAction SilentlyContinue | Sort-Object Name)) {
  W ("  " + $p.Name + "   [" + $p.PortName + "]  " + $p.PrinterStatus)
}
$ql = Get-Printer -Name 'Brother QL-800' -ErrorAction SilentlyContinue
if ($ql) {
  $q = @(Get-PrintJob -PrinterName 'Brother QL-800' -ErrorAction SilentlyContinue)
  W ("  QL-800 queue: " + $q.Count + " job(s)")
  foreach ($j in $q) { W ("    #" + $j.Id + "  " + $j.DocumentName + "  " + $j.JobStatus) }
}

W ""
W "--- power ---"
$ac = (powercfg /q SCHEME_CURRENT SUB_SLEEP STANDBYIDLE | Select-String 'AC' | Select-Object -Last 1)
if ($ac) { W ("  " + $ac.Line.Trim() + "   (0x0 = never sleeps)") }

W ""
W "--- agent.log (last 25 lines) ---"
$log = Join-Path $dest 'work\agent.log'
if (Test-Path $log) {
  foreach ($line in (Get-Content $log -Tail 25 -Encoding UTF8)) { W ("  " + $line) }
} else {
  W "  (no log yet - the agent has not run)"
}

W ""
W "--- ledger (jobs this PC has handled) ---"
$ledger = Join-Path $dest 'work\ledger'
if (Test-Path $ledger) {
  $files = @(Get-ChildItem $ledger -Filter *.json -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending)
  W ("  " + $files.Count + " job(s)")
  foreach ($f in ($files | Select-Object -First 8)) {
    $j = Get-Content $f.FullName -Raw -Encoding UTF8 | ConvertFrom-Json
    W ("  " + $f.BaseName + " : " + $j.stage + " / " + $j.result + "  " + $f.LastWriteTime)
  }
} else {
  W "  (none yet)"
}
