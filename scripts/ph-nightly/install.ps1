# install.ps1 - set up / update the nightly runner on miniPC (run as bfaith, idempotent). ASCII only.
#   powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\bfaith\bfaith-portal\scripts\ph-nightly\install.ps1
# Layout (Codex R2-R4: nothing the session executes, or reads as instructions, may be writable by that session):
#   C:\tools\ph-nightly\bin\    phq.mjs, copy_lint.py, run-ph-generate.ps1, ping.ps1   <- ACL: bfaith denied write (inherited)
#   C:\tools\ph-nightly\work\   Claude's cwd; generated files                             <- writable
#   C:\tools\ph-nightly\work\phq, phreview                                               <- ACL: bfaith denied write
#   C:\tools\ph-nightly\work\.claude\   settings.json + skills\ (COPIED)                  <- ACL: bfaith denied write (inherited)
# Everything the scheduled task executes is a protected COPY (bin\run-ph-generate.ps1), never the repo checkout:
# with -Repo <worktree> the checkout is not deny-listed and the session could edit a runner there (Codex R4 high 2).
# Skills are copied, not junctioned, for the same reason (R3 critical 1). Re-run this script after `git pull`.
# Does NOT log in to Claude Code / Codex (interactive, once, by a person) and does NOT place the service token.
#
# PS 5.1 trap (found on the miniPC 2026-08-28): a helper named `Icacls` calling `& icacls` recurses into itself
# (function names are case-insensitive and shadow executables) -> CallDepthOverflow. Always call icacls.exe.
param(
  # Repo checkout to install FROM. Default = production clone. Pass a git worktree path to test an unmerged branch.
  [string]$Repo = 'C:\Users\bfaith\bfaith-portal'
)
$ErrorActionPreference = 'Stop'
$Src   = Join-Path $Repo 'scripts\ph-nightly'
$Root  = 'C:\tools\ph-nightly'
$Bin   = Join-Path $Root 'bin'
$Work  = Join-Path $Root 'work'
$Cfg   = Join-Path $Work '.claude'
$SkillsSrc = Join-Path $Repo '.claude\skills\ph-generate'
$Me = 'bfaith'

if (-not (Test-Path (Join-Path $Src 'phq.mjs'))) { throw "not a ph-nightly source dir: $Src" }
if (-not (Test-Path (Join-Path $SkillsSrc 'SKILL.md'))) { throw "skill not found: $SkillsSrc" }
if (-not (Test-Path (Join-Path $Repo 'scripts\jobs-monitor\ping.ps1'))) { throw "ping.ps1 not found under $Repo" }

# ACL helpers. Explicit deny ACEs on the running user beat any group allow. The task runs with RunLevel Limited,
# so even though bfaith is an Administrator the session only has the filtered token and cannot take ownership /
# rewrite ACLs without UAC (Codex R3 high 1). Claude Code's Write/Edit -> EACCES.
function Invoke-Icacls([string[]]$argv) {
  & icacls.exe @argv | Out-Null
  if ($LASTEXITCODE -ne 0) { throw ("icacls failed (exit " + $LASTEXITCODE + "): icacls " + ($argv -join ' ')) }
}
function Unprotect([string]$p) { if (Test-Path $p) { Invoke-Icacls @($p, '/remove:d', $Me) } }
function Protect([string]$p, [bool]$isDir) {
  if ($isDir) { Invoke-Icacls @($p, '/deny', ($Me + ':(OI)(CI)(W,D,DC,WDAC,WO)')) }
  else        { Invoke-Icacls @($p, '/deny', ($Me + ':(W,D,DC,WDAC,WO)')) }
}
# Verify one ACE: our user, DENY, the five rights, and (OI)(CI) on directories (Codex R4 medium 2)
function Assert-Denied([string]$p, [bool]$isDir) {
  $lines = & icacls.exe $p
  if ($LASTEXITCODE -ne 0) { throw "icacls query failed on $p" }
  $inh = ''
  if ($isDir) { $inh = '\(OI\)\(CI\)' }
  $re = [regex]::Escape($Me) + ':' + $inh + '(\(I\))?\(DENY\)\((?=[^)]*\bW\b)(?=[^)]*\bD\b)(?=[^)]*\bDC\b)(?=[^)]*\bWDAC\b)(?=[^)]*\bWO\b)[^)]*\)'
  if (-not (($lines -join "`n") -match $re)) { throw "deny ACE missing or incomplete on $p" }
}

# All protected targets are registered BEFORE any deny is lifted, and the finally re-protects each one
# independently (Codex R4 high 1). The original error and any re-protect errors are reported together (R5 medium).
$targets = @(
  @{ p = $Bin;                          d = $true  },
  @{ p = $Cfg;                          d = $true  },
  @{ p = (Join-Path $Work 'phq');       d = $false },
  @{ p = (Join-Path $Work 'phreview');  d = $false }
)
$updateError = $null
$reprotectErrors = @()
try {
  try {
    New-Item -ItemType Directory -Force -Path $Bin, $Work, $Cfg, (Join-Path $Root 'logs') | Out-Null
    New-Item -ItemType Directory -Force -Path (Join-Path $env:USERPROFILE '.claude\secrets') | Out-Null

    # 1) lift denies
    foreach ($t in $targets) { Unprotect $t.p }

    # 2) executable code + shims + settings (all as protected copies)
    Copy-Item -Force (Join-Path $Src 'phq.mjs')              (Join-Path $Bin 'phq.mjs')
    Copy-Item -Force (Join-Path $Src 'copy_lint.py')         (Join-Path $Bin 'copy_lint.py')   # canonical = AI_reference (miniPC has no G:)
    Copy-Item -Force (Join-Path $Src 'run-ph-generate.ps1')  (Join-Path $Bin 'run-ph-generate.ps1')
    Copy-Item -Force (Join-Path $Repo 'scripts\jobs-monitor\ping.ps1') (Join-Path $Bin 'ping.ps1')
    Copy-Item -Force (Join-Path $Src 'phq')                  (Join-Path $Work 'phq')
    Copy-Item -Force (Join-Path $Src 'phreview')             (Join-Path $Work 'phreview')
    Copy-Item -Force (Join-Path $Src 'settings.json')        (Join-Path $Cfg 'settings.json')
    foreach ($f in @('phq', 'phreview')) {
      $bytes = [IO.File]::ReadAllBytes((Join-Path $Work $f))
      if (@($bytes | Where-Object { $_ -eq 13 }).Count -gt 0) { throw "$f has CRLF line endings (bash shim needs LF; check .gitattributes)" }
    }

    # 3) skills: copy into the protected config dir. An old junction at the path is unlinked (link only).
    $skills = Join-Path $Cfg 'skills'
    if (Test-Path $skills) {
      $item = Get-Item $skills -Force
      if ($item.LinkType) {
        cmd /c rmdir "$skills"
        if ($LASTEXITCODE -ne 0) { throw "could not remove old junction $skills (exit $LASTEXITCODE)" }
      } else {
        Remove-Item -Recurse -Force $skills   # install-owned directory, only ever populated by this script
      }
    }
    New-Item -ItemType Directory -Force -Path (Join-Path $skills 'ph-generate') | Out-Null
    Copy-Item -Force -Recurse (Join-Path $SkillsSrc '*') (Join-Path $skills 'ph-generate')
    if (-not (Test-Path (Join-Path $skills 'ph-generate\SKILL.md'))) { throw "skill copy failed: $skills" }
  } catch {
    $updateError = $_.Exception.Message
  }
} finally {
  # 4) re-apply write denies no matter what happened above; never stop at the first failure
  foreach ($t in $targets) {
    if (Test-Path $t.p) {
      try { Protect $t.p $t.d } catch { $reprotectErrors += ("re-protect failed: " + $t.p + " : " + $_.Exception.Message) }
    }
  }
}
if ($updateError -or $reprotectErrors.Count -gt 0) {
  $msg = @()
  if ($updateError) { $msg += ("update failed: " + $updateError) }
  $msg += $reprotectErrors
  throw ($msg -join "`n")
}
foreach ($t in $targets) { Assert-Denied $t.p $t.d }
Assert-Denied (Join-Path $Bin 'phq.mjs') $false                       # inheritance reached files under bin
Assert-Denied (Join-Path $Bin 'run-ph-generate.ps1') $false
Assert-Denied (Join-Path $Cfg 'settings.json') $false
Assert-Denied (Join-Path $Cfg 'skills\ph-generate\SKILL.md') $false

# 5) scheduled task runs the PROTECTED copy of the runner. bfaith / Interactive (no stored password - same
#    pattern as MallCsvFetchAll) / RunLevel Limited. Requires bfaith to stay logged on (console).
$taskName = 'PhGenerateNightly'
$action   = New-ScheduledTaskAction -Execute 'powershell.exe' `
              -Argument ('-NoProfile -ExecutionPolicy Bypass -File "' + (Join-Path $Bin 'run-ph-generate.ps1') + '"')
$trigger  = New-ScheduledTaskTrigger -Daily -At 02:30
$principal = New-ScheduledTaskPrincipal -UserId $Me -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
              -MultipleInstances IgnoreNew -RunOnlyIfNetworkAvailable
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
$t = Get-ScheduledTask -TaskName $taskName
if (-not $t) { throw "task $taskName was not registered" }

Write-Output "installed:"
Write-Output ("  bin      : " + $Bin + " (phq.mjs, copy_lint.py, run-ph-generate.ps1, ping.ps1) [write denied for " + $Me + "]")
Write-Output ("  work     : " + $Work + " (./phq ./phreview [write denied], generated files writable)")
Write-Output ("  config   : " + $Cfg + " (settings.json + skills copy) [write denied]")
Write-Output ("  task     : " + $taskName + " daily 02:30 as " + $Me + " (Interactive, Limited) -> bin\run-ph-generate.ps1, state=" + $t.State)
Write-Output ("  source   : " + $Repo)
Write-Output ""
Write-Output "remaining manual steps (once, by a person):"
Write-Output ("  1. service token : " + (Join-Path $env:USERPROFILE '.claude\secrets\ph-service-token.txt') + "  (= Render PH_SERVICE_TOKEN)")
Write-Output ("  2. claude login  : cd " + $Work + " ; claude  ->  /login (subscription account)")
Write-Output "  3. codex login   : codex login (ChatGPT subscription)"
Write-Output ("  4. first run     : powershell -NoProfile -ExecutionPolicy Bypass -File " + (Join-Path $Bin 'run-ph-generate.ps1'))
