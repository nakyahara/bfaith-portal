# run-ph-generate.ps1 - nightly runner for product-hub "AI info pending" queue (Task Scheduler entry).
# Runs Claude Code headless (subscription auth of the logged-on user) inside C:\tools\ph-nightly.
#
# Success is decided by the SERVER queue, never by what Claude reports.
# Durable progress = drafts that left the queue for good (written back or blocked for a human):
#   pending = claimable + leased   (a draft that is merely leased has NOT been processed - Codex R1 high)
#   done    = pending_before - pending_after
# Verdict (timeout beats everything):
#   timeout                                   -> fail    (killed run; leases expire by themselves in 30 min)
#   claimable_after == 0 and leased_after == 0 -> ok
#   done > 0                                  -> partial (next night continues)
#   otherwise                                 -> fail    (auth / tool denied / site layout changed)
# "blocked" (needs-human) never counts as failure: those drafts are shown to people on the board.
#
# Keep this file ASCII only (PS 5.1 reads BOM-less files as CP932). The prompt is English for the same reason
# and must not contain quotes or cmd metacharacters (it is passed through claude.cmd = cmd.exe).
$ErrorActionPreference = 'Continue'

$WorkDir    = 'C:\tools\ph-nightly'
# Repo = the checkout this script lives in (production clone, or a worktree when testing an unmerged branch)
$Repo       = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$Claude     = Join-Path $env:APPDATA 'npm\claude.cmd'
$PingPs1    = Join-Path $Repo 'scripts\jobs-monitor\ping.ps1'
$TokenFile  = Join-Path $env:USERPROFILE '.claude\secrets\ph-service-token.txt'
$Base       = 'https://bfaith-portal.onrender.com/apps/product-hub/service-api'
$TimeoutMin = 100     # 1 draft per claim, ~6-8 min each incl. Codex review; up to 15 drafts per night
$MaxDrafts  = 15
$PingId     = 'ph-generate-nightly'
$Stamp      = Get-Date -Format 'yyyyMMdd-HHmmss'
$LogDir     = Join-Path $WorkDir 'logs'
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$OutLog = Join-Path $LogDir "$Stamp.out.log"
$ErrLog = Join-Path $LogDir "$Stamp.err.log"
$RunLog = Join-Path $LogDir 'runner.log'
$script:pingFailed = $false

function Log([string]$msg) {
  $line = '[' + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + '] ' + $msg
  Write-Output $line
  Add-Content -Path $RunLog -Value $line
}
function Send-Ping([string]$status, [string]$note) {
  Log ("ping " + $status + " : " + $note)
  if (-not (Test-Path $PingPs1)) { Log 'ping.ps1 not found - skipped'; $script:pingFailed = $true; return }
  try {
    $pp = Start-Process -FilePath 'powershell.exe' -PassThru -NoNewWindow -Wait:$false `
            -ArgumentList @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $PingPs1, '-Id', $PingId, '-Status', $status, '-Note', ('"' + $note + '"'))
    if (-not $pp.WaitForExit(60000)) { try { $pp.Kill() } catch { }; Log 'ping timed out (60s)'; $script:pingFailed = $true; return }
    if ($pp.ExitCode -ne 0) { Log ('ping exit ' + $pp.ExitCode); $script:pingFailed = $true }
  } catch { Log ('ping error: ' + $_.Exception.Message); $script:pingFailed = $true }
}
function Finish([int]$code) { if ($script:pingFailed -and $code -eq 0) { exit 3 }; exit $code }
function Get-Queue {
  $tok = (Get-Content -LiteralPath $TokenFile -Raw).Trim()
  $r = Invoke-RestMethod -Uri "$Base/generation-queue" -Headers @{ Authorization = "Bearer $tok" } -TimeoutSec 60
  return $r.queue
}

# --- preflight ------------------------------------------------------------------
if (-not (Test-Path $Claude)) { Send-Ping 'fail' 'claude.cmd not found (npm install -g @anthropic-ai/claude-code)'; Finish 1 }
if (-not (Test-Path $TokenFile)) { Send-Ping 'fail' 'ph-service-token.txt missing'; Finish 1 }
if (-not (Test-Path (Join-Path $WorkDir 'phq.mjs'))) { Send-Ping 'fail' 'phq.mjs missing in workdir (run install.ps1)'; Finish 1 }

# Auth check every night, even when there is nothing to generate: subscription OAuth can expire silently
# and a quiet week would otherwise hide it until a busy night.
$authJson = ''
try { $authJson = (& $Claude auth status 2>$null | Out-String) } catch { $authJson = '' }
if ($authJson -notmatch '"loggedIn"\s*:\s*true') {
  Send-Ping 'fail' 'claude not logged in on miniPC (as bfaith: cd C:\tools\ph-nightly ; claude ; /login)'
  Finish 1
}

try { $before = Get-Queue } catch { Send-Ping 'fail' ('queue pre-check failed: ' + $_.Exception.Message); Finish 1 }
$pendingBefore = [int]$before.claimable + [int]$before.leased
Log ("before: claimable=" + $before.claimable + " leased=" + $before.leased + " blocked=" + $before.blocked)
if ([int]$before.claimable -eq 0 -and [int]$before.leased -eq 0) {
  Send-Ping 'ok' ('nothing to generate (blocked=' + $before.blocked + ')')
  Finish 0
}

# --- run Claude Code headless ----------------------------------------------------
# Permissions come from C:\tools\ph-nightly\.claude\settings.json (defaultMode dontAsk; only ./phq and codex exec).
# ASCII, no quotes, no cmd metacharacters (& | < > ^ %). One draft at a time: lease is 30 min.
$prompt = 'Process the product-hub generation queue. Follow the ph-generate skill in this workspace exactly: claim ONE draft at a time with ./phq, verify identity, generate, lint, Codex review, then submit or block. Every claimed draft must end as done, blocked, or released. Stop when ./phq claim returns no drafts or after ' + $MaxDrafts + ' drafts. Never read the service token and never touch files outside this workspace. Finish with one line: done=N blocked=N released=N'
$timedOut = $false
$claudeExit = -1
try {
  $p = Start-Process -FilePath $Claude -WorkingDirectory $WorkDir -NoNewWindow -PassThru `
         -RedirectStandardOutput $OutLog -RedirectStandardError $ErrLog `
         -ArgumentList @('-p', ('"' + $prompt + '"'), '--output-format', 'json')
  if (-not $p.WaitForExit($TimeoutMin * 60 * 1000)) {
    $timedOut = $true
    try { $p.Kill() } catch { }
    try { $p.WaitForExit(30000) | Out-Null } catch { }
    Log ("timeout after " + $TimeoutMin + " min - killed")
  }
  try { $claudeExit = $p.ExitCode } catch { $claudeExit = -1 }
} catch {
  Log ('failed to start claude: ' + $_.Exception.Message)
  Send-Ping 'fail' ('failed to start claude: ' + $_.Exception.Message)
  Finish 1
}
Log ("claude exit=" + $claudeExit + " timedOut=" + $timedOut)

# --- verify against the server ---------------------------------------------------
Start-Sleep -Seconds 5
try { $after = Get-Queue } catch { Send-Ping 'fail' ('queue post-check failed: ' + $_.Exception.Message); Finish 1 }
$pendingAfter = [int]$after.claimable + [int]$after.leased
$done = $pendingBefore - $pendingAfter
if ($done -lt 0) { $done = 0 }   # new drafts entered the queue during the run
$note = 'done=' + $done + ' remaining=' + $after.claimable + ' leased=' + $after.leased + ' blocked=' + $after.blocked + ' exit=' + $claudeExit
Log ("after: " + $note)

if ($timedOut) { Send-Ping 'fail' ('timeout: ' + $note + ' (see ' + $ErrLog + ')'); Finish 1 }
if ([int]$after.claimable -eq 0 -and [int]$after.leased -eq 0) { Send-Ping 'ok' $note; Finish 0 }
if ($done -gt 0) { Send-Ping 'partial' $note; Finish 0 }
Send-Ping 'fail' ('no progress: ' + $note + ' (see ' + $ErrLog + ')')
Finish 1
