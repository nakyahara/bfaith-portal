# install.ps1 - set up / update the nightly runner on miniPC (run as bfaith, idempotent, never deletes user data). ASCII only.
#   powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\bfaith\bfaith-portal\scripts\ph-nightly\install.ps1
# Layout (Codex R2 critical 2: code must not be writable by the session that runs it):
#   C:\tools\ph-nightly\bin\   phq.mjs, copy_lint.py           <- ACL: bfaith denied write
#   C:\tools\ph-nightly\work\  Claude's cwd; ./phq ./phreview   <- shims ACL-denied; generated files writable
#   C:\tools\ph-nightly\work\.claude\settings.json               <- ACL: bfaith denied write
#   C:\tools\ph-nightly\work\.claude\skills -> <repo>\.claude\skills (junction)
# Does NOT log in to Claude Code / Codex (interactive, once, by a person) and does NOT place the service token.
param(
  # Repo checkout to wire in. Default = production clone. Pass a git worktree path to test an unmerged branch.
  [string]$Repo = 'C:\Users\bfaith\bfaith-portal'
)
$ErrorActionPreference = 'Stop'
$Src   = Join-Path $Repo 'scripts\ph-nightly'
$Root  = 'C:\tools\ph-nightly'
$Bin   = Join-Path $Root 'bin'
$Work  = Join-Path $Root 'work'
$SkillsTarget = Join-Path $Repo '.claude\skills'
$Me = 'bfaith'

if (-not (Test-Path (Join-Path $Src 'phq.mjs'))) { throw "not a ph-nightly source dir: $Src" }
if (-not (Test-Path (Join-Path $SkillsTarget 'ph-generate\SKILL.md'))) { throw "skills not found under $SkillsTarget" }

# ACL helpers. Deny ACEs on the running user beat any group allow (bfaith may be an Administrator).
# Claude Code's Write/Edit run as bfaith -> EACCES. install.ps1 lifts the deny, copies, and re-applies it.
function Unprotect([string]$p) { if (Test-Path $p) { icacls $p /remove:d $Me | Out-Null } }
function Protect([string]$p, [bool]$isDir) {
  if ($isDir) { icacls $p /deny ($Me + ':(OI)(CI)(W,D,DC,WDAC,WO)') | Out-Null }
  else        { icacls $p /deny ($Me + ':(W,D,DC,WDAC,WO)') | Out-Null }
  if ($LASTEXITCODE -ne 0) { throw "icacls deny failed on $p (exit $LASTEXITCODE)" }
}

New-Item -ItemType Directory -Force -Path $Bin, $Work, (Join-Path $Work '.claude'), (Join-Path $Root 'logs') | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $env:USERPROFILE '.claude\secrets') | Out-Null

# 1) code (bin/) + shims (work/) + settings: lift deny, copy, re-deny
Unprotect $Bin
Unprotect (Join-Path $Work 'phq'); Unprotect (Join-Path $Work 'phreview'); Unprotect (Join-Path $Work '.claude')
Copy-Item -Force (Join-Path $Src 'phq.mjs')       (Join-Path $Bin 'phq.mjs')
Copy-Item -Force (Join-Path $Src 'copy_lint.py')  (Join-Path $Bin 'copy_lint.py')   # canonical = AI_reference (miniPC has no G:)
Copy-Item -Force (Join-Path $Src 'phq')           (Join-Path $Work 'phq')
Copy-Item -Force (Join-Path $Src 'phreview')      (Join-Path $Work 'phreview')
Copy-Item -Force (Join-Path $Src 'settings.json') (Join-Path $Work '.claude\settings.json')
foreach ($f in @('phq', 'phreview')) {
  $bytes = [IO.File]::ReadAllBytes((Join-Path $Work $f))
  if (@($bytes | Where-Object { $_ -eq 13 }).Count -gt 0) { throw "$f has CRLF line endings (bash shim needs LF; check .gitattributes)" }
}

# 2) skills: junction to the repo checkout so `git pull` updates the skill (single source of truth).
#    Only a *junction* is accepted at the path (Codex R2 medium 7). Anything else: stop and tell a person.
$skills = Join-Path $Work '.claude\skills'
$needLink = $true
if (Test-Path $skills) {
  $item = Get-Item $skills -Force
  if ($item.LinkType -ne 'Junction') { throw "$skills exists and is not a junction (LinkType=$($item.LinkType)). Move it away by hand, then re-run install.ps1" }
  $current = ($item.Target | Select-Object -First 1)
  if ($current -and ($current.TrimEnd('\') -ieq $SkillsTarget.TrimEnd('\'))) { $needLink = $false }
  else {
    cmd /c rmdir "$skills"     # removes the link only, never the target contents
    if ($LASTEXITCODE -ne 0) { throw "could not remove old junction $skills (exit $LASTEXITCODE)" }
  }
}
if ($needLink) {
  cmd /c mklink /J "$skills" "$SkillsTarget" | Out-Null
  if ($LASTEXITCODE -ne 0) { throw "mklink failed (exit $LASTEXITCODE)" }
}
if (-not (Test-Path (Join-Path $skills 'ph-generate\SKILL.md'))) { throw "junction does not resolve to the skill: $skills" }

# 3) re-apply write denies (after the junction exists; the junction itself is under work\.claude)
Protect $Bin $true
Protect (Join-Path $Work 'phq') $false
Protect (Join-Path $Work 'phreview') $false
Protect (Join-Path $Work '.claude\settings.json') $false

# 4) scheduled task: bfaith / Interactive (no stored password - same pattern as MallCsvFetchAll).
#    Requires bfaith to stay logged on (console). SYSTEM cannot read the subscription OAuth.
$taskName = 'PhGenerateNightly'
$action   = New-ScheduledTaskAction -Execute 'powershell.exe' `
              -Argument ('-NoProfile -ExecutionPolicy Bypass -File "' + (Join-Path $Src 'run-ph-generate.ps1') + '"')
$trigger  = New-ScheduledTaskTrigger -Daily -At 02:30
$principal = New-ScheduledTaskPrincipal -UserId $Me -LogonType Interactive -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
              -MultipleInstances IgnoreNew -RunOnlyIfNetworkAvailable
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
$t = Get-ScheduledTask -TaskName $taskName
if (-not $t) { throw "task $taskName was not registered" }

Write-Output "installed:"
Write-Output ("  bin      : " + $Bin + " (phq.mjs, copy_lint.py) [write denied for " + $Me + "]")
Write-Output ("  work     : " + $Work + " (./phq ./phreview [write denied], generated files writable)")
Write-Output ("  settings : " + (Join-Path $Work '.claude\settings.json') + " [write denied]")
Write-Output ("  skills   : " + $skills + " -> " + $SkillsTarget + " (junction)")
Write-Output ("  task     : " + $taskName + " daily 02:30 as " + $Me + " (Interactive), state=" + $t.State)
Write-Output ""
Write-Output "remaining manual steps (once, by a person):"
Write-Output ("  1. service token : " + (Join-Path $env:USERPROFILE '.claude\secrets\ph-service-token.txt') + "  (= Render PH_SERVICE_TOKEN)")
Write-Output ("  2. claude login  : cd " + $Work + " ; claude  ->  /login (subscription account)")
Write-Output "  3. codex login   : codex login (ChatGPT subscription)"
Write-Output ("  4. first run     : powershell -NoProfile -ExecutionPolicy Bypass -File " + (Join-Path $Src 'run-ph-generate.ps1'))
