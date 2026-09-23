# test-claude-guard.ps1 - real-process tests for scripts\claude-guard\ClaudeGuard.ps1 (ASCII only).
# Run: powershell -NoProfile -ExecutionPolicy Bypass -File scripts\test-claude-guard.ps1
# Uses a temp lock path and a temp USERPROFILE; never touches C:\tools or the real ~\.claude.
$ErrorActionPreference = 'Stop'
$Guard = Join-Path $PSScriptRoot 'claude-guard\ClaudeGuard.ps1'
. $Guard
# ASCII-only path: the child scripts are run by PS 5.1 and must not depend on the user's (non-ASCII) TEMP path
$tmp = Join-Path 'C:\tmp' ('claude-guard-test-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
New-Item -ItemType Directory -Force -Path $tmp | Out-Null
$lock = Join-Path $tmp 'claude.lock'
$pass = 0; $fail = 0
function Ok([bool]$c, [string]$label) { if ($c) { $script:pass++; Write-Output ('  ok  ' + $label) } else { $script:fail++; Write-Output ('  NG  ' + $label) } }
function DeadlineIn([int]$sec) { return (Get-Date).ToUniversalTime().AddSeconds($sec) }
function Start-Ps([string]$code) {
  $f = Join-Path $tmp ('child-' + [guid]::NewGuid().ToString('N').Substring(0, 6) + '.ps1')
  Set-Content -LiteralPath $f -Value $code -Encoding UTF8
  return Start-Process -FilePath 'powershell.exe' -ArgumentList @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $f) -WindowStyle Hidden -PassThru
}
function Wait-File([string]$f, [int]$sec) { for ($i = 0; $i -lt $sec * 10; $i++) { if (Test-Path -LiteralPath $f) { return $true }; Start-Sleep -Milliseconds 100 }; return $false }
function Alive([int]$id) { return [bool](Get-Process -Id $id -ErrorAction SilentlyContinue) }

try {
  Write-Output '[1] lock: exclusive while the holder lives, released by the OS when the holder dies'
  $held = Join-Path $tmp 'held.txt'
  $holder = Start-Ps (". '$Guard'`r`nif (Enter-ClaudeLock -DeadlineUtc (Get-Date).ToUniversalTime().AddSeconds(5) -PollSec 1 -Path '$lock') { Set-Content -LiteralPath '$held' -Value 'x' }`r`nStart-Sleep -Seconds 60")
  Ok (Wait-File $held 15) 'child took the lock'
  Ok (-not (Enter-ClaudeLock -DeadlineUtc (DeadlineIn 3) -PollSec 1 -Path $lock)) 'second taker waits and gives up at the deadline (lock not broken)'
  Stop-Process -Id $holder.Id -Force
  Ok (Enter-ClaudeLock -DeadlineUtc (DeadlineIn 10) -PollSec 1 -Path $lock) 'after the holder is killed, the lock is free (no stale file to reclaim)'
  Exit-ClaudeLock

  Write-Output '[2] KILL_ON_JOB_CLOSE: killing only the parent also kills its children (orphan cannot start Claude later)'
  foreach ($useJob in @($false, $true)) {
    $pidFile = Join-Path $tmp ('grandchild-' + $useJob + '.txt')
    $enable = if ($useJob) { "if (-not (Enable-KillOnCloseJob)) { exit 9 }" } else { '' }
    $parent = Start-Ps (". '$Guard'`r`n$enable`r`n`$c = Start-Process -FilePath 'powershell.exe' -ArgumentList @('-NoProfile','-Command','Start-Sleep -Seconds 90 # guard-test-grandchild') -WindowStyle Hidden -PassThru`r`nSet-Content -LiteralPath '$pidFile' -Value `$c.Id`r`nStart-Sleep -Seconds 90")
    Ok (Wait-File $pidFile 15) ('parent started a child (job=' + $useJob + ')')
    Start-Sleep -Milliseconds 300
    $gc = [int](Get-Content -LiteralPath $pidFile -Raw).Trim()
    Stop-Process -Id $parent.Id -Force
    Start-Sleep -Seconds 3
    if ($useJob) { Ok (-not (Alive $gc)) 'with the job: the child died with the parent' }
    else { Ok (Alive $gc) 'control without the job: the child survives the parent (the test can tell the difference)'; Stop-Process -Id $gc -Force -ErrorAction SilentlyContinue }
  }

  Write-Output '[3] residue: an AI runner or Claude left running blocks the next start'
  Ok ((Get-ClaudeResidue).Count -eq 0 -or $true) '(baseline read works)'
  $marker = Start-Process -FilePath 'powershell.exe' -ArgumentList @('-NoProfile', '-Command', 'Start-Sleep -Seconds 60 # C:\x\bin\ad-kw-ai.mjs') -WindowStyle Hidden -PassThru
  Start-Sleep -Seconds 1
  $r = Get-ClaudeResidue
  Ok ([bool]($r | Where-Object { $_.Pid -eq $marker.Id })) 'a process whose command line runs ad-kw-ai.mjs is residue'
  Ok (-not (Wait-NoClaudeResidue -DeadlineUtc (DeadlineIn 3) -PollSec 1)) 'Wait-NoClaudeResidue gives up while it lives'
  Stop-Process -Id $marker.Id -Force
  Start-Sleep -Seconds 1
  Ok (-not [bool]((Get-ClaudeResidue) | Where-Object { $_.Pid -eq $marker.Id })) 'gone after it ends'
  Ok ([bool]('C:\Users\b\AppData\Roaming\npm\node_modules\@anthropic-ai\claude-code\cli.js -p' -match $ClaudeResiduePattern)) 'pattern: claude-code cli.js (node)'
  Ok ([bool]('node kw-publish.cjs' -match $ClaudeResiduePattern) -and [bool]('powershell -File C:\tools\ph-nightly\bin\run-ph-generate.ps1' -match $ClaudeResiduePattern)) 'pattern: product-scout runner / ph-nightly runner'
  Ok (-not [bool]('node C:\tools\ph-nightly\bin\phq.mjs queue' -match $ClaudeResiduePattern) -and -not [bool]('codex exec --sandbox read-only' -match $ClaudeResiduePattern)) 'pattern: phq / codex are not Claude'

  Write-Output '[4] oauth_refresh.lock is removed only while holding the lock and with no Claude alive'
  # On a dev PC the Claude Code session running this test is itself residue -> exclude what was alive before the test
  $baseline = @((Get-ClaudeResidue) | ForEach-Object { $_.Pid })
  if ($baseline.Count) { Write-Output ('  (baseline residue excluded: ' + ($baseline -join ',') + ')') }
  $savedProfile = $env:USERPROFILE
  $env:USERPROFILE = $tmp
  New-Item -ItemType Directory -Force -Path (Join-Path $tmp '.claude') | Out-Null
  $oauth = Join-Path $tmp '.claude\.oauth_refresh.lock'
  Set-Content -LiteralPath $oauth -Value 'x'
  Ok ((Remove-OauthLockIfSafe -ExcludePid $baseline) -eq 'kept-not-holding' -and (Test-Path $oauth)) 'not holding the lock -> kept'
  Ok (Enter-ClaudeLock -DeadlineUtc (DeadlineIn 5) -PollSec 1 -Path $lock) '(took the lock)'
  $marker2 = Start-Process -FilePath 'powershell.exe' -ArgumentList @('-NoProfile', '-Command', 'Start-Sleep -Seconds 60 # kw-publish.cjs') -WindowStyle Hidden -PassThru
  Start-Sleep -Seconds 1
  Ok ((Remove-OauthLockIfSafe -ExcludePid $baseline) -eq 'kept-claude-running' -and (Test-Path $oauth)) 'a Claude runner alive -> kept'
  Stop-Process -Id $marker2.Id -Force
  Start-Sleep -Seconds 1
  Ok ((Remove-OauthLockIfSafe -ExcludePid $baseline) -eq 'removed' -and -not (Test-Path $oauth)) 'holding + nothing alive -> removed'
  Ok ((Remove-OauthLockIfSafe -ExcludePid $baseline) -eq 'absent') 'absent -> nothing to do'
  Exit-ClaudeLock
  $env:USERPROFILE = $savedProfile

  Write-Output '[5] Stop-ProcessTree kills descendants (Process.Kill alone leaves them)'
  $pidFile2 = Join-Path $tmp 'tree.txt'
  $top = Start-Ps ("`$c = Start-Process -FilePath 'powershell.exe' -ArgumentList @('-NoProfile','-Command','Start-Sleep -Seconds 90') -WindowStyle Hidden -PassThru`r`nSet-Content -LiteralPath '$pidFile2' -Value `$c.Id`r`nStart-Sleep -Seconds 90")
  Ok (Wait-File $pidFile2 15) '(tree started)'
  Start-Sleep -Milliseconds 300
  $leaf = [int](Get-Content -LiteralPath $pidFile2 -Raw).Trim()
  Stop-ProcessTree $top.Id
  Start-Sleep -Seconds 2
  Ok (-not (Alive $top.Id) -and -not (Alive $leaf)) 'top and leaf are gone'
  Write-Output '[6] the real run-keywords.ps1 (product-scout) with a fake kw-publish.cjs'
  # tree: <tmp>\rk\scripts\product-idea-scout\ai\{run-keywords.ps1, kw-publish.cjs, kw-preflight.cjs} + <tmp>\rk\scripts\claude-guard\ClaudeGuard.ps1
  $rk = Join-Path $tmp 'rk\scripts'
  $ai = Join-Path $rk 'product-idea-scout\ai'
  New-Item -ItemType Directory -Force -Path $ai, (Join-Path $rk 'claude-guard') | Out-Null
  Copy-Item (Join-Path $PSScriptRoot 'product-idea-scout\ai\run-keywords.ps1') $ai
  Copy-Item $Guard (Join-Path $rk 'claude-guard')
  $nodePid = Join-Path $tmp 'node-pid.txt'
  $fake = "require('fs').writeFileSync(process.argv[2]||'" + ($nodePid -replace '\\', '\\\\') + "', String(process.pid)); setTimeout(() => {}, 90000);"
  Set-Content -LiteralPath (Join-Path $ai 'kw-publish.cjs') -Value $fake -Encoding ASCII
  Set-Content -LiteralPath (Join-Path $ai 'kw-preflight.cjs') -Value 'process.exit(0)' -Encoding ASCII
  $state = Join-Path $tmp 'state'
  $pingLog = Join-Path $tmp 'ping.log'
  $fakePing = Join-Path $tmp 'ping.ps1'
  Set-Content -LiteralPath $fakePing -Value ("param([string]`$Id,[string]`$Status,[string]`$Note)`r`nAdd-Content -LiteralPath '" + $pingLog + "' -Value (`$Id + ' ' + `$Status + ' ' + `$Note)") -Encoding ASCII
  $cfgFile = Join-Path $tmp 'daily-config.json'
  Set-Content -LiteralPath $cfgFile -Value (@{ state_dir = $state; ping_script = $fakePing } | ConvertTo-Json) -Encoding UTF8
  $env:CLAUDE_GUARD_LOCK = Join-Path $tmp 'rk.lock'
  $env:CLAUDE_GUARD_TEST_EXCLUDE = ($baseline -join ',')   # the Claude Code session running this test (dev PC)
  $run1 = Start-Process -FilePath 'powershell.exe' -ArgumentList @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', (Join-Path $ai 'run-keywords.ps1'), '-Config', $cfgFile) -WindowStyle Hidden -PassThru
  Ok (Wait-File $nodePid 20) 'run 1 took the lock and started node (kw-publish)'
  $np = [int](Get-Content -LiteralPath $nodePid -Raw).Trim()
  $t0 = Get-Date
  $run2 = Start-Process -FilePath 'powershell.exe' -ArgumentList @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', (Join-Path $ai 'run-keywords.ps1'), '-Config', $cfgFile, '-Probe') -WindowStyle Hidden -PassThru
  $run2.WaitForExit(120000) | Out-Null
  Ok ($run2.ExitCode -eq 1 -and ((Get-Date) - $t0).TotalSeconds -ge 40) ('run 2 (probe) waited ~1 min for the lock and failed (exit ' + $run2.ExitCode + ')')
  Ok ((Test-Path $pingLog) -and ((Get-Content -LiteralPath $pingLog -Raw) -match 'product-kw-scout fail claude guard')) 'run 2 sent a fail ping'
  # run 1 keeps daily.log open (node output redirection), so run 2 cannot always log there - the fail ping is what counts
  Stop-Process -Id $run1.Id -Force   # only the parent PowerShell (e.g. the task host was killed)
  Start-Sleep -Seconds 3
  Ok (-not (Alive $np)) 'killing only run 1 (PowerShell) also killed its node (the job)'
  Remove-Item -LiteralPath $nodePid -Force -ErrorAction SilentlyContinue
  $run3 = Start-Process -FilePath 'powershell.exe' -ArgumentList @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', (Join-Path $ai 'run-keywords.ps1'), '-Config', $cfgFile) -WindowStyle Hidden -PassThru
  Ok (Wait-File $nodePid 20) 'run 3 gets the lock right away (the OS released it)'
  Stop-ProcessTree $run3.Id
  Remove-Item Env:\CLAUDE_GUARD_LOCK
  Remove-Item Env:\CLAUDE_GUARD_TEST_EXCLUDE
} finally {
  Exit-ClaudeLock
  Remove-Item -LiteralPath $tmp -Recurse -Force -ErrorAction SilentlyContinue
}
Write-Output ''
Write-Output ("$pass PASS / $fail FAIL")
if ($fail -gt 0) { exit 1 }
