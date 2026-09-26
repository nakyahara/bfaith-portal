# run-ph-generate.ps1 - nightly runner for product-hub (Task Scheduler entry PhGenerateNightly, 02:30).
# Two queues, one Claude at a time, inside the 2 h task window:
#   1. manuscripts  : Claude Code headless with the ph-generate skill (ping 'ph-generate-nightly')
#   2. SP-ad keyword AI (PR3b 2026-09-23): bin\ad-kw-ai.mjs = tool-less AI, reserved per job on Render
#      (ping 'ph-adkw-ai-nightly'). A manuscript failure / timeout never hides or blocks the ad result.
#
# Manuscript success is decided by the SERVER queue, never by what Claude reports.
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
# The task runs this with no arguments (= production defaults). The parameters exist only for
# scripts\test-ph-nightly-runner.ps1 (a temp root, a local service-api, a fake Claude under a temp APPDATA).
param(
  [string]$Root = 'C:\tools\ph-nightly',
  [string]$Base = 'https://bfaith-portal.onrender.com/apps/product-hub/service-api',
  [string]$TokenFile = ''
)
$ErrorActionPreference = 'Continue'

$WorkDir    = Join-Path $Root 'work'      # Claude's cwd (generated files); code lives in bin\ (write-denied)
# This script runs from its protected copy in bin\ (install.ps1 puts ping.ps1 next to it). The repo checkout is
# never executed by the task, so a writable worktree cannot become a persistence path (Codex R4 high 2).
$Claude     = Join-Path $env:APPDATA 'npm\claude.cmd'
$PingPs1    = Join-Path $PSScriptRoot 'ping.ps1'
$AdRunner   = Join-Path $PSScriptRoot 'ad-kw-ai.mjs'
if (-not $TokenFile) { $TokenFile = Join-Path $env:USERPROFILE '.claude\secrets\ph-service-token.txt' }
$TimeoutMin = 80      # manuscripts: 1 draft per claim, ~6-8 min each incl. Codex review (was 100 before the ad queue)
$MaxDrafts  = 15
$PingId     = 'ph-generate-nightly'
$AdPingId   = 'ph-adkw-ai-nightly'
$AdMaxMin   = 25      # ad keyword AI: at most 25 min per night (1 call per job, <= 10 min each)
$AdMinMin   = 10      # below this the ad runner could not even reserve (8 min CLI minimum + margin)
$AdStaleMin = 36 * 60 # oldest ad request waiting longer than this = stuck (partial ping)
$Stamp      = Get-Date -Format 'yyyyMMdd-HHmmss'
$LogDir     = Join-Path $Root 'logs'
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$OutLog = Join-Path $LogDir "$Stamp.out.log"
$ErrLog = Join-Path $LogDir "$Stamp.err.log"
$AdOutLog = Join-Path $LogDir "$Stamp.adkw.out.log"
$AdErrLog = Join-Path $LogDir "$Stamp.adkw.err.log"
$RunLog = Join-Path $LogDir 'runner.log'
$script:pingFailed = $false

# Write-Host (not Write-Output): functions below return values, and Write-Output would leak into them
function Log([string]$msg) {
  $line = '[' + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + '] ' + $msg
  Write-Host $line
  Add-Content -Path $RunLog -Value $line -Encoding UTF8   # PS 5.1 default would be ANSI (Codex R3)
}
function Send-Ping([string]$status, [string]$note, [string]$id = $PingId) {
  # note goes through cmd-style argument parsing: strip quotes / control chars, cap length (Codex R2 medium 9)
  $safe = ($note -replace '["\r\n\t]', ' ') -replace '\s+', ' '
  if ($safe.Length -gt 180) { $safe = $safe.Substring(0, 180) }
  Log ("ping " + $id + " " + $status + " : " + $safe)
  if (-not (Test-Path $PingPs1)) { Log 'ping.ps1 not found - skipped'; $script:pingFailed = $true; return }
  # ping.ps1 has its own 15s HTTP timeout and always exits 0 by design; call it synchronously.
  try {
    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $PingPs1 -Id $id -Status $status -Note $safe | Out-Null
    if ($LASTEXITCODE -ne 0) { Log ('ping exit ' + $LASTEXITCODE); $script:pingFailed = $true }
  } catch { Log ('ping error: ' + $_.Exception.Message); $script:pingFailed = $true }
}
# Claude could not be used at all (guard / lock / auth): both jobs failed tonight
# (the ad ping also carries what stage 0 resent, so a Claude failure never hides it)
function Send-BothFail([string]$note) { Send-Ping 'fail' $note $PingId; Send-Ping 'fail' ($note + $script:resendNote) $AdPingId }
$script:resend = $null
$script:resendNote = ''
function Finish([int]$code) { Exit-ClaudeLock; if ($script:pingFailed -and $code -eq 0) { exit 3 }; exit $code }
function Get-Token { return (Get-Content -LiteralPath $TokenFile -Raw).Trim() }
function Get-Queue {
  $r = Invoke-RestMethod -Uri "$Base/generation-queue" -Headers @{ Authorization = ('Bearer ' + (Get-Token)) } -TimeoutSec 60
  return $r.queue
}
function Get-AdQueue {
  $r = Invoke-RestMethod -Uri "$Base/ad-kw-ai/queue" -Headers @{ Authorization = ('Bearer ' + (Get-Token)) } -TimeoutSec 60
  return $r.queue
}
# PR3c: enqueue own-brand products for the automatic ad keyword run (server keeps the daily cap; a resend only fills
# what is left of today's cap). Returns @{ Status = ok|fail; Note }. 503 ai_disabled (flag off) is ok, not a failure.
function Invoke-AutoEnqueue {
  try {
    $r = Invoke-RestMethod -Method Post -Uri "$Base/ad-kw-ai/auto-enqueue" -Headers @{ Authorization = ('Bearer ' + (Get-Token)) } -ContentType 'application/json' -Body '{}' -TimeoutSec 90
    $n = @($r.enqueued).Count
    $note = 'auto=+' + $n + ' (' + $r.today + '/' + $r.cap + ')'
    Log $note
    return @{ Status = 'ok'; Note = $note }
  } catch {
    $code = $null
    try { $code = [int]$_.Exception.Response.StatusCode } catch { $code = $null }
    $body = ''
    try { $body = [string]$_.ErrorDetails.Message } catch { $body = '' }
    if ($code -eq 503 -and $body -match 'ai_disabled') { return @{ Status = 'ok'; Note = 'auto=off' } }
    return @{ Status = 'fail'; Note = ('auto-enqueue failed (HTTP ' + $code + '): ' + $_.Exception.Message) }
  }
}

# --- one Claude at a time (PR3-0, scripts\claude-guard\ClaudeGuard.ps1 copied next to this file) ---------------
# ProductKWScout (05:00, S4U) uses the same subscription OAuth, so this is NOT the only claude user any more.
# 1) this process joins a KILL_ON_JOB_CLOSE job: if it dies, claude / node and their children die with it
# 2) a lock file held open (FileShare.None) for the whole run; the OS releases it if this process dies
# 3) before any claude call: no claude / AI runner left over from another run
$Guard = Join-Path $PSScriptRoot 'ClaudeGuard.ps1'
if (-not (Test-Path $Guard)) { Send-BothFail 'ClaudeGuard.ps1 not installed (run install.ps1)'; exit 1 }
. $Guard
if (-not (Enable-KillOnCloseJob)) { Send-BothFail 'could not create the kill-on-close job object'; exit 1 }
$LockWaitMin = 20
# The task is killed by Task Scheduler at 2 h (install.ps1). Waiting for the lock uses that time, so each queue gets
# only what is left after keeping $EndSlackMin for the post-checks and pings (Codex #1427 R1 #3).
$RunStartUtc  = (Get-Date).ToUniversalTime()
$TaskLimitMin = 120
$EndSlackMin  = 10
$MinClaudeMin = 15
function MinutesLeft { return (Get-RunMinutesLeft -StartedUtc $RunStartUtc -TaskLimitMin $TaskLimitMin -EndSlackMin $EndSlackMin) }

# A leftover ~\.claude\.oauth_refresh.lock kills claude -p at startup ("another Claude Code process is
# refreshing it or exited mid-refresh") before any work is done. Seen 2026-09-02: a lock created at 02:30:06
# was never cleaned up and the run died in 6 seconds with done=0 remaining=12.
# Removed only while this run holds the Claude lock AND no Claude process is alive (never by age alone).
# Returns $true when claude may start now (Codex #1427 R2: anything but absent / removed must stop the start).
function Test-ClaudeStartable {
  $r = Test-ReadyToStartClaude
  if ($r.Status -ne 'absent') { Log ('oauth_refresh.lock: ' + $r.Status) }
  return [bool]$r.Ok
}

# --- 0. resend unsent SP-ad results (PR3b, Codex #1431 R2) ---------------------------------------------------
# Results generated on an earlier night but not delivered (Render down, 401 ...) are sent FIRST and on their own:
# no Claude, no lock, no auth, no manuscript - only the service token and the network. A Claude outage must not
# keep already-generated results from reaching Render. The outcome is carried into the final ad ping.
function Invoke-AdResend {
  $pendingDir = Join-Path $Root 'ad-kw-ai-data\pending'
  $pending = @(Get-ChildItem -LiteralPath $pendingDir -Filter '*.json' -File -ErrorAction SilentlyContinue).Count
  if ($pending -eq 0) { return $null }
  if (-not (Test-Path $AdRunner)) { return @{ Status = 'fail'; Note = ('resend: ' + $pending + ' unsent but ad-kw-ai.mjs not installed') } }
  $env:AD_KW_AI_BASE = $Base
  $out = Join-Path $LogDir "$Stamp.adkw-resend.out.log"
  $err = Join-Path $LogDir "$Stamp.adkw-resend.err.log"
  $deadline = (Get-Date).ToUniversalTime().AddMinutes(5).ToString('o')
  try {
    $p = Start-Process -FilePath 'node' -WorkingDirectory $PSScriptRoot -NoNewWindow -PassThru -RedirectStandardOutput $out -RedirectStandardError $err `
           -ArgumentList @($AdRunner, '--deadline', $deadline, '--run-id', ('adkw-resend-' + $Stamp), '--root', $Root, '--resend-only')
    $null = $p.Handle   # PS 5.1: keep ExitCode readable
    if (-not $p.WaitForExit(7 * 60 * 1000)) { Stop-ProcessTree $p.Id; return @{ Status = 'fail'; Note = 'resend: runner did not stop - killed' } }
    $exit = $p.ExitCode
  } catch { return @{ Status = 'fail'; Note = ('resend: failed to start: ' + $_.Exception.Message) } }
  $s = $null
  try { $s = ((Get-Content -LiteralPath $out -Encoding UTF8 | Where-Object { $_ -match '^\{' } | Select-Object -Last 1) | ConvertFrom-Json) } catch { $s = $null }
  $note = if ($s) { 'resend: resent=' + $s.resent + ' rejected_results=' + $s.rejected_results + ' failed=' + $s.failed + ' pending=' + $s.pending_left + ' stopped=' + $s.stopped + ' exit=' + $exit } else { 'resend: no summary exit=' + $exit }
  Log $note
  $status = if ($exit -eq 0) { 'ok' } elseif ($exit -eq 2) { 'partial' } else { 'fail' }
  return @{ Status = $status; Note = $note }
}
$script:resend = Invoke-AdResend
# no '|' in the separator: the note goes through a command line (ping.ps1 -Note)
if ($script:resend) { $script:resendNote = ' // ' + $script:resend.Note }

# --- preflight ------------------------------------------------------------------
if (-not (Test-Path $Claude)) { Send-BothFail 'claude.cmd not found (npm install -g @anthropic-ai/claude-code)'; Finish 1 }
if (-not (Test-Path $TokenFile)) { Send-BothFail 'ph-service-token.txt missing'; Finish 1 }
if (-not (Test-Path (Join-Path $Root 'bin\phq.mjs')) -or -not (Test-Path (Join-Path $WorkDir 'phq'))) { Send-BothFail 'phq not installed (run install.ps1)'; Finish 1 }

# Take the Claude lock before the first claude call (auth status refreshes OAuth too). Held until Finish.
$lockDeadline = (Get-Date).ToUniversalTime().AddMinutes($LockWaitMin)
if (-not (Enter-ClaudeLock -DeadlineUtc $lockDeadline)) {
  Send-BothFail ('another Claude job held the lock for ' + $LockWaitMin + ' min (C:\tools\claude-lock\claude.lock)')
  Finish 1
}
$residue = Wait-NoClaudeResidue -DeadlineUtc $lockDeadline
if ($residue -ne 'clean') {
  # 'unknown' = the process list could not be read: never treated as "nothing is running"
  Send-BothFail ('claude guard (' + $residue + '): ' + (Format-ClaudeResidue))
  Finish 1
}

# Auth check every night, even when there is nothing to generate: subscription OAuth can expire silently
# and a quiet week would otherwise hide it until a busy night.
if (-not (Test-ClaudeStartable)) { Send-BothFail ('claude not started: oauth_refresh.lock kept (' + (Format-ClaudeResidue) + ')'); Finish 1 }
$authJson = ''
try { $authJson = (& $Claude auth status 2>$null | Out-String) } catch { $authJson = '' }
if ($authJson -notmatch '"loggedIn"\s*:\s*true') {
  Send-BothFail 'claude not logged in on miniPC (as bfaith: cd C:\tools\ph-nightly ; claude ; /login)'
  Finish 1
}

# --- 1. manuscripts ---------------------------------------------------------------
# Returns @{ Status = ok|partial|fail; Note; Clean = claude tree confirmed gone (the ad queue may start Claude) }
function Invoke-Manuscripts {
  try { $before = Get-Queue } catch { return @{ Status = 'fail'; Note = ('queue pre-check failed: ' + $_.Exception.Message); Clean = $true } }
  $pendingBefore = [int]$before.claimable + [int]$before.leased
  Log ("before: claimable=" + $before.claimable + " leased=" + $before.leased + " blocked=" + $before.blocked)
  if ([int]$before.claimable -eq 0 -and [int]$before.leased -eq 0) {
    return @{ Status = 'ok'; Note = ('nothing to generate (blocked=' + $before.blocked + ')'); Clean = $true }
  }
  # Permissions come from work\.claude\settings.json (defaultMode dontAsk; only ./phq and ./phreview).
  # ASCII, no quotes, no cmd metacharacters (& | < > ^ %). One draft at a time: lease is 30 min.
  $prompt = 'Process the product-hub generation queue. Follow the ph-generate skill in this workspace exactly: claim ONE draft at a time with ./phq, verify identity, generate, lint, review with ./phreview, then submit or block. Every claimed draft must end as done, blocked, or released. Stop when ./phq claim returns no drafts or after ' + $MaxDrafts + ' drafts. Never read the service token and never touch files outside this workspace. Finish with one line: done=N blocked=N released=N'
  # One retry, only for the transient OAuth-refresh failure: claude then dies within seconds having done
  # zero work, and its own error text says "retry in a minute". Nothing else is retried - a second full run
  # after a real (partial) failure could burn claims twice.
  $timedOut = $false
  $claudeExit = -1
  for ($attempt = 1; $attempt -le 2; $attempt++) {
    if (-not (Test-ClaudeStartable)) { return @{ Status = 'fail'; Note = ('claude not started: oauth_refresh.lock kept (' + (Format-ClaudeResidue) + ')'); Clean = $false } }
    # Claude gets at most $TimeoutMin, and never more than the task has left minus the end slack
    $claudeMin = [Math]::Min($TimeoutMin, (MinutesLeft))
    if ($claudeMin -lt $MinClaudeMin) { return @{ Status = 'fail'; Note = ('not enough time left for claude: ' + $claudeMin + ' min (lock wait / retry used the task window)'); Clean = $true } }
    try {
      $p = Start-Process -FilePath $Claude -WorkingDirectory $WorkDir -NoNewWindow -PassThru `
             -RedirectStandardOutput $OutLog -RedirectStandardError $ErrLog `
             -ArgumentList @('-p', ('"' + $prompt + '"'), '--output-format', 'json')
      # PS 5.1: with -NoNewWindow + redirection, ExitCode stays empty unless the handle is taken right away (PR3b e2e test)
      $null = $p.Handle
      if (-not $p.WaitForExit($claudeMin * 60 * 1000)) {
        $timedOut = $true
        # claude.cmd -> cmd -> node: Process.Kill() would kill only cmd and leave node running (PR3-0)
        Stop-ProcessTree $p.Id
        try { $p.WaitForExit(30000) | Out-Null } catch { }
        Log ("timeout after " + $claudeMin + " min - killed the process tree; left: " + (Format-ClaudeResidue))
      }
      try { $claudeExit = $p.ExitCode } catch { $claudeExit = -1 }
    } catch {
      return @{ Status = 'fail'; Note = ('failed to start claude: ' + $_.Exception.Message); Clean = $true }
    }
    Log ("claude exit=" + $claudeExit + " timedOut=" + $timedOut + " attempt=" + $attempt)
    if ($timedOut -or $attempt -ge 2) { break }
    $outText = ''
    try { $outText = [System.IO.File]::ReadAllText($OutLog) } catch { $outText = '' }
    if ($outText -notmatch 'Failed to refresh OAuth token') { break }
    Log 'transient OAuth refresh failure - keeping attempt-1 logs and retrying once in 60s'
    try { Copy-Item $OutLog ($OutLog + '.attempt1') -Force } catch { }
    try { Copy-Item $ErrLog ($ErrLog + '.attempt1') -Force } catch { }
    Start-Sleep -Seconds 60
  }
  # after a timeout the tree was killed: the ad queue may start Claude only when nothing is left (Codex plan R2 P1 #4)
  $clean = (Wait-NoClaudeResidue -DeadlineUtc (Get-Date).ToUniversalTime().AddMinutes(2) -PollSec 5) -eq 'clean'
  Start-Sleep -Seconds 5
  try { $after = Get-Queue } catch { return @{ Status = 'fail'; Note = ('queue post-check failed: ' + $_.Exception.Message); Clean = $clean } }
  $pendingAfter = [int]$after.claimable + [int]$after.leased
  $done = $pendingBefore - $pendingAfter
  if ($done -lt 0) { $done = 0 }   # new drafts entered the queue during the run
  $note = 'done=' + $done + ' remaining=' + $after.claimable + ' leased=' + $after.leased + ' blocked=' + $after.blocked + ' exit=' + $claudeExit
  Log ("after: " + $note)
  if ($timedOut) { return @{ Status = 'fail'; Note = ('timeout: ' + $note + ' (see ' + $ErrLog + ')'); Clean = $clean } }
  if ([int]$after.claimable -eq 0 -and [int]$after.leased -eq 0) { return @{ Status = 'ok'; Note = $note; Clean = $clean } }
  if ($done -gt 0) { return @{ Status = 'partial'; Note = $note; Clean = $clean } }
  return @{ Status = 'fail'; Note = ('no progress: ' + $note + ' (see ' + $ErrLog + ')'); Clean = $clean }
}

# --- 2. SP-ad keyword AI (PR3b) ----------------------------------------------------
# Returns @{ Status = ok|partial|fail; Note }. Never started when the manuscript Claude tree is still alive.
function Invoke-AdKeywords([bool]$manuscriptClean) {
  # PR3c: own-brand products are enqueued automatically BEFORE the queue is read (an empty queue must not skip the
  # first auto job - Codex PR3c R1 #1). 503 ai_disabled = flag off (normal). Any other failure is a failure, not "0 jobs".
  $enq = Invoke-AutoEnqueue
  if ($enq.Status -eq 'fail') { return @{ Status = 'fail'; Note = $enq.Note } }
  try { $aq = Get-AdQueue } catch { return @{ Status = 'fail'; Note = ('ad queue check failed: ' + $_.Exception.Message) } }   # never "0 jobs"
  # pings are cut at 180 chars: keep the counters short (input = needs_input, failed = failed and not yet reviewed)
  $qnote = $enq.Note + ' claimable=' + $aq.claimable + ' retry_wait=' + $aq.retry_wait + ' review=' + $aq.needs_review + ' input=' + $aq.needs_input + ' failed=' + $aq.failed_unreviewed + ' oldest_min=' + $aq.oldest_wait_min
  # unsent results saved by an earlier night are resent whatever the queue / flag says (Codex #1431 R1 #1)
  $pendingDir = Join-Path $Root 'ad-kw-ai-data\pending'
  $pending = @(Get-ChildItem -LiteralPath $pendingDir -Filter '*.json' -File -ErrorAction SilentlyContinue).Count
  Log ('ad before: ' + $qnote + ' enabled=' + $aq.enabled + ' pending=' + $pending)
  $work = ([bool]$aq.enabled -and [int]$aq.claimable -gt 0)
  if (-not $work -and $pending -eq 0) {
    if (-not $aq.enabled) { return @{ Status = 'ok'; Note = 'disabled on Render (AD_KW_AI_ENABLED off)' } }
    $stuck = ($aq.oldest_wait_min -ne $null -and [int]$aq.oldest_wait_min -gt $AdStaleMin)
    if ($stuck) { return @{ Status = 'partial'; Note = ('nothing claimable but a request waits too long: ' + $qnote) } }
    if ([int]$aq.needs_review -gt 0) { return @{ Status = 'partial'; Note = ('needs_review waits for a person: ' + $qnote) } }
    # a failed job stays visible until a person marks it reviewed on the screen (then it is no longer counted)
    if ([int]$aq.failed_unreviewed -gt 0) { return @{ Status = 'partial'; Note = ('failed job(s) wait for a person: ' + $qnote) } }
    if ([int]$aq.retry_wait -gt 0) { return @{ Status = 'partial'; Note = ('retry_wait (next night): ' + $qnote) } }
    return @{ Status = 'ok'; Note = ('nothing to do ' + $qnote) }
  }
  $resendOnly = -not $work   # only unsent results (flag off, or no new request)
  if (-not $manuscriptClean) { return @{ Status = 'fail'; Note = ('not started: the manuscript Claude tree may still be alive ' + (Format-ClaudeResidue)) } }
  if (-not (Test-Path $AdRunner)) { return @{ Status = 'fail'; Note = 'ad-kw-ai.mjs not installed (run install.ps1)' } }
  $adMin = [Math]::Min($AdMaxMin, (MinutesLeft))
  $needMin = if ($resendOnly) { 2 } else { $AdMinMin }
  if ($adMin -lt $needMin) { return @{ Status = 'fail'; Note = ('no time left for the ad queue: ' + $adMin + ' min (manuscripts used the window) ' + $qnote) } }
  if (-not $resendOnly -and -not (Test-ClaudeStartable)) { return @{ Status = 'fail'; Note = ('claude not started: oauth_refresh.lock kept (' + (Format-ClaudeResidue) + ')') } }
  $deadline = (Get-Date).ToUniversalTime().AddMinutes($adMin).ToString('o')
  $runId = 'adkw-' + $Stamp
  $env:AD_KW_AI_BASE = $Base   # the runner reads the service token itself (never passed on a command line)
  $adArgs = @($AdRunner, '--deadline', $deadline, '--run-id', $runId, '--root', $Root)
  if ($resendOnly) { $adArgs += '--resend-only' }
  try {
    $p = Start-Process -FilePath 'node' -WorkingDirectory $PSScriptRoot -NoNewWindow -PassThru `
           -RedirectStandardOutput $AdOutLog -RedirectStandardError $AdErrLog -ArgumentList $adArgs
    $null = $p.Handle   # PS 5.1: keep ExitCode readable (see above)
    # the runner keeps its own deadline; +2 min grace, then the whole tree is killed
    if (-not $p.WaitForExit(($adMin + 2) * 60 * 1000)) {
      Stop-ProcessTree $p.Id
      try { $p.WaitForExit(30000) | Out-Null } catch { }
      return @{ Status = 'fail'; Note = ('ad runner did not stop by its deadline - killed (see ' + $AdErrLog + ')') }
    }
    $exit = $p.ExitCode
  } catch { return @{ Status = 'fail'; Note = ('failed to start ad-kw-ai.mjs: ' + $_.Exception.Message) } }
  $summary = $null
  try { $summary = ((Get-Content -LiteralPath $AdOutLog -Encoding UTF8 | Where-Object { $_ -match '^\{' } | Select-Object -Last 1) | ConvertFrom-Json) } catch { $summary = $null }
  $snote = if ($summary) { 'claimed=' + $summary.claimed + ' submitted=' + $summary.submitted + ' accepted=' + $summary.accepted + ' rejected_results=' + $summary.rejected_results + ' failed=' + $summary.failed + ' resent=' + $summary.resent + ' pending=' + $summary.pending_left + ' stopped=' + $summary.stopped } else { 'no summary' }
  # the post-check must succeed: a failed read is never "ok" (Codex #1431 R1 #6)
  try { $aq2 = Get-AdQueue } catch { return @{ Status = 'fail'; Note = ($snote + ' exit=' + $exit + ' ad queue post-check failed: ' + $_.Exception.Message) } }
  $note = $enq.Note + ' ' + $snote + ' exit=' + $exit + ' after: claimable=' + $aq2.claimable + ' retry_wait=' + $aq2.retry_wait + ' review=' + $aq2.needs_review + ' input=' + $aq2.needs_input + ' failed=' + $aq2.failed_unreviewed + ' oldest_min=' + $aq2.oldest_wait_min
  Log ('ad after: ' + $note)
  # an unreadable exit code is never "success" (0 / 2 are the only non-failure codes of ad-kw-ai.mjs)
  if ($exit -ne 0 -and $exit -ne 2) { return @{ Status = 'fail'; Note = ($note + ' (see ' + $AdErrLog + ')') } }
  if (-not $summary) { return @{ Status = 'fail'; Note = ($note + ' (see ' + $AdErrLog + ')') } }
  if ($work -and [int]$summary.claimed -eq 0 -and [int]$summary.resent -eq 0 -and @('daily_cap', 'deadline') -notcontains [string]$summary.stopped) { return @{ Status = 'fail'; Note = ('no progress: ' + $note) } }
  if ($exit -eq 2) { return @{ Status = 'partial'; Note = $note } }
  $stuck2 = ($aq2.oldest_wait_min -ne $null -and [int]$aq2.oldest_wait_min -gt $AdStaleMin)
  if ([int]$aq2.needs_review -gt 0 -or [int]$aq2.failed_unreviewed -gt 0 -or [int]$aq2.retry_wait -gt 0 -or $stuck2 -or [int]$summary.pending_left -gt 0) { return @{ Status = 'partial'; Note = $note } }
  return @{ Status = 'ok'; Note = $note }
}

$ms = Invoke-Manuscripts
Send-Ping $ms.Status $ms.Note $PingId
$ad = Invoke-AdKeywords ([bool]$ms.Clean)
# stage 0 (resend) never makes the night look better than it was: fail > partial > ok
if ($script:resend) {
  $rank = @{ ok = 0; partial = 1; fail = 2 }
  if ($rank[$script:resend.Status] -gt $rank[$ad.Status]) { $ad.Status = $script:resend.Status }
  $ad.Note = $ad.Note + $script:resendNote
}
Send-Ping $ad.Status $ad.Note $AdPingId
if ($ms.Status -eq 'fail' -or $ad.Status -eq 'fail') { Finish 1 }
Finish 0
