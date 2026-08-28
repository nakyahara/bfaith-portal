# install.ps1 - set up / update the nightly runner on miniPC (run as bfaith, idempotent, never destructive). ASCII only.
#   powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\bfaith\bfaith-portal\scripts\ph-nightly\install.ps1
# Does NOT log in to Claude Code / Codex (interactive, once, by a person) and does NOT place the service token.
param(
  # Repo checkout to wire in. Default = production clone. Pass a git worktree path to test an unmerged branch
  # (skills junction + task action then point at that worktree; re-run without -Repo after merge).
  [string]$Repo = 'C:\Users\bfaith\bfaith-portal'
)
$ErrorActionPreference = 'Stop'
$Src     = Join-Path $Repo 'scripts\ph-nightly'
$WorkDir = 'C:\tools\ph-nightly'
$SkillsTarget = Join-Path $Repo '.claude\skills'

if (-not (Test-Path (Join-Path $Src 'phq.mjs'))) { throw "not a ph-nightly source dir: $Src" }
if (-not (Test-Path (Join-Path $SkillsTarget 'ph-generate\SKILL.md'))) { throw "skills not found under $SkillsTarget" }

New-Item -ItemType Directory -Force -Path (Join-Path $WorkDir '.claude') | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $WorkDir 'logs') | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $env:USERPROFILE '.claude\secrets') | Out-Null

# 1) permissions + the fixed-function CLI (the only things Claude may execute)
Copy-Item -Force (Join-Path $Src 'settings.json') (Join-Path $WorkDir '.claude\settings.json')
Copy-Item -Force (Join-Path $Src 'phq.mjs')       (Join-Path $WorkDir 'phq.mjs')
Copy-Item -Force (Join-Path $Src 'phq')           (Join-Path $WorkDir 'phq')
Copy-Item -Force (Join-Path $Src 'copy_lint.py')  (Join-Path $WorkDir 'copy_lint.py')   # canonical = AI_reference (miniPC has no G:)

# 2) skills: junction to the repo checkout so `git pull` updates the skill (single source of truth).
#    Never delete a real directory here (Codex R1 high): if something unexpected sits at the path, stop and tell a person.
$skills = Join-Path $WorkDir '.claude\skills'
$needLink = $true
if (Test-Path $skills) {
  $item = Get-Item $skills -Force
  $isLink = [bool]($item.Attributes -band [IO.FileAttributes]::ReparsePoint)
  if (-not $isLink) {
    throw "$skills is a real directory (expected a junction). Move it away by hand, then re-run install.ps1"
  }
  $current = ($item.Target | Select-Object -First 1)
  if ($current -and ($current.TrimEnd('\') -ieq $SkillsTarget.TrimEnd('\'))) {
    $needLink = $false
  } else {
    cmd /c rmdir "$skills"     # removes the link only, never the target contents
    if ($LASTEXITCODE -ne 0) { throw "could not remove old junction $skills (exit $LASTEXITCODE)" }
  }
}
if ($needLink) {
  cmd /c mklink /J "$skills" "$SkillsTarget" | Out-Null
  if ($LASTEXITCODE -ne 0) { throw "mklink failed (exit $LASTEXITCODE)" }
}
if (-not (Test-Path (Join-Path $skills 'ph-generate\SKILL.md'))) { throw "junction does not resolve to the skill: $skills" }

# 3) scheduled task: bfaith / Interactive (no stored password - same pattern as MallCsvFetchAll).
#    Requires bfaith to stay logged on (console). SYSTEM cannot read the subscription OAuth.
$taskName = 'PhGenerateNightly'
$action   = New-ScheduledTaskAction -Execute 'powershell.exe' `
              -Argument ('-NoProfile -ExecutionPolicy Bypass -File "' + (Join-Path $Src 'run-ph-generate.ps1') + '"')
$trigger  = New-ScheduledTaskTrigger -Daily -At 02:30
$principal = New-ScheduledTaskPrincipal -UserId 'bfaith' -LogonType Interactive -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
              -MultipleInstances IgnoreNew -RunOnlyIfNetworkAvailable
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
$t = Get-ScheduledTask -TaskName $taskName
if (-not $t) { throw "task $taskName was not registered" }

Write-Output "installed:"
Write-Output ("  settings : " + (Join-Path $WorkDir '.claude\settings.json'))
Write-Output ("  phq      : " + (Join-Path $WorkDir 'phq.mjs') + " (+ shim ./phq, copy_lint.py)")
Write-Output ("  skills   : " + $skills + " -> " + $SkillsTarget + " (junction)")
Write-Output ("  task     : " + $taskName + " daily 02:30 as bfaith (Interactive), state=" + $t.State)
Write-Output ""
Write-Output "remaining manual steps (once, by a person):"
Write-Output ("  1. service token : " + (Join-Path $env:USERPROFILE '.claude\secrets\ph-service-token.txt') + "  (= Render PH_SERVICE_TOKEN)")
Write-Output "  2. claude login  : cd C:\tools\ph-nightly ; claude  ->  /login (subscription account)"
Write-Output "  3. codex login   : codex login (ChatGPT subscription)"
Write-Output ("  4. first run     : powershell -NoProfile -ExecutionPolicy Bypass -File " + (Join-Path $Src 'run-ph-generate.ps1'))
