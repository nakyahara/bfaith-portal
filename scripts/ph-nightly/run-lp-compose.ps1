# run-lp-compose.ps1 - LP compose runner for product-hub (Task Scheduler entry PhLpComposeMinutely, every 1 min).
# Spec = AI_reference 'Shohin-Hub_LP-Kousei-AI_Stage1_Design_20260930.md' (Japanese file name in the doc store).
#
# Stage 1 is a MEASUREMENT, not an unattended feature: a person presses a button on the product detail page and
# watches the screen. This runner only picks that request up quickly.
#
# Why every minute is cheap:
#   the poll is ONE http call (bin\phlp.mjs queue). Claude is started only when claimable > 0.
#   So a minute with no work costs one request and no Claude session, no lock, no residue check.
#
# One Claude at a time (PR3-0): the SAME subscription OAuth is used by PhGenerateNightly (02:30) and
# ProductKWScout (05:00). This runner must therefore take the shared lock before starting Claude -
# but with a SHORT deadline: at one run per minute it must never queue up behind a 2 h nightly job.
# If the lock is busy, this run exits quietly and the next minute tries again.
#
# Success is decided by the SERVER queue, never by what Claude reports:
#   claimable_after < claimable_before -> ok (the request left the queue for good)
#   needs_review went up              -> partial (a person has to look; the board shows it)
#   nothing moved                     -> fail (auth / tool denied / spec missing)
#
# Keep this file ASCII only (PS 5.1 reads BOM-less files as CP932). The prompt is English for the same reason
# and must not contain quotes or cmd metacharacters (it is passed through claude.cmd = cmd.exe).
param(
  [string]$Root = 'C:\tools\ph-nightly',
  [string]$Base = 'https://bfaith-portal.onrender.com/apps/product-hub/service-api',
  [string]$TokenFile = ''
)
$ErrorActionPreference = 'Continue'

$WorkDir   = Join-Path $Root 'work'
$Claude    = Join-Path $env:APPDATA 'npm\claude.cmd'
$PingPs1   = Join-Path $PSScriptRoot 'ping.ps1'
$Phlp      = Join-Path $Root 'bin\phlp.mjs'
if (-not $TokenFile) { $TokenFile = Join-Path $env:USERPROFILE '.claude\secrets\ph-service-token.txt' }
$PingId      = 'ph-lp-compose'
$TimeoutMin  = 12     # one request: read spec + look at images + write + lint + Codex review (<= 2 rounds)
$LockWaitSec = 5      # SHORT: at 1 run/min we must not pile up behind the nightly jobs
$Stamp     = Get-Date -Format 'yyyyMMdd-HHmmss'
$LogDir    = Join-Path $Root 'logs'
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$OutLog = Join-Path $LogDir "$Stamp.lp.out.log"
$ErrLog = Join-Path $LogDir "$Stamp.lp.err.log"
$RunLog = Join-Path $LogDir 'lp-compose.log'

function Log([string]$msg) {
  $line = '[' + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + '] ' + $msg
  Write-Host $line
  try { Add-Content -Path $RunLog -Value $line -Encoding utf8 } catch { }
}

function Send-Ping([string]$status, [string]$note) {
  if (-not (Test-Path $PingPs1)) { return }
  try { & $PingPs1 -Id $PingId -Status $status -Note $note | Out-Null } catch { Log ('ping failed: ' + $_.Exception.Message) }
}

# bin\phlp.mjs queue -> @{ enabled; claimable; running; needs_review; ... }
function Get-Queue {
  $env:PH_SERVICE_TOKEN = (Get-Content -Raw -LiteralPath $TokenFile).Trim()
  $env:PH_LP_BASE = $Base
  try {
    $json = & node $Phlp queue 2>&1 | Out-String
    return ($json | ConvertFrom-Json).queue
  } finally {
    Remove-Item Env:PH_SERVICE_TOKEN -ErrorAction SilentlyContinue
  }
}

if (-not (Test-Path $Phlp) -or -not (Test-Path (Join-Path $WorkDir 'phlp'))) {
  Log 'phlp not installed (run install.ps1)'
  Send-Ping 'fail' 'phlp not installed'
  exit 1
}
if (-not (Test-Path $TokenFile)) { Log 'service token file missing'; Send-Ping 'fail' 'service token missing'; exit 1 }

# --- 1. cheap poll: is there anything to do? --------------------------------------
# No Claude, no lock, no residue check when the queue is empty. This is the common case (most minutes).
$before = $null
try { $before = Get-Queue } catch { Log ('queue check failed: ' + $_.Exception.Message); Send-Ping 'fail' 'queue check failed'; exit 1 }
if (-not $before) { Log 'queue check returned nothing'; Send-Ping 'fail' 'queue check returned nothing'; exit 1 }
if (-not $before.enabled) { Log 'PH_LP_COMPOSE_ENABLED is off on the server - nothing to do'; exit 0 }
if ([int]$before.claimable -eq 0) {
  # A quiet minute is normal and must not be noisy. Ping ok so the dead-man monitor sees the runner alive.
  Send-Ping 'ok' ('idle (running=' + $before.running + ' needs_review=' + $before.needs_review + ')')
  exit 0
}
Log ('work found: claimable=' + $before.claimable + ' running=' + $before.running + ' needs_review=' + $before.needs_review)

# --- 2. one Claude at a time ------------------------------------------------------
$Guard = Join-Path $PSScriptRoot 'ClaudeGuard.ps1'
if (-not (Test-Path $Guard)) { Log 'ClaudeGuard.ps1 not installed (run install.ps1)'; Send-Ping 'fail' 'ClaudeGuard missing'; exit 1 }
. $Guard
if (-not (Enable-KillOnCloseJob)) { Log 'could not create the kill-on-close job object'; Send-Ping 'fail' 'job object failed'; exit 1 }

# SHORT deadline on purpose: if a nightly job holds the lock, skip this minute instead of waiting.
$lockDeadline = (Get-Date).ToUniversalTime().AddSeconds($LockWaitSec)
if (-not (Enter-ClaudeLock -DeadlineUtc $lockDeadline -PollSec 1)) {
  Log 'another Claude job holds the lock - skipping this minute'
  Send-Ping 'ok' 'skipped (another Claude job holds the lock)'
  exit 0
}
try {
  $residue = Wait-NoClaudeResidue -DeadlineUtc ((Get-Date).ToUniversalTime().AddSeconds(10)) -PollSec 2
  if ($residue -ne 'clean') {
    # 'unknown' = the process list could not be read: never treated as "nothing is running"
    Log ('claude guard (' + $residue + '): ' + (Format-ClaudeResidue))
    Send-Ping 'fail' ('claude guard: ' + $residue)
    exit 1
  }
  if (-not (Test-ClaudeStartable)) {
    Log ('claude not started: oauth_refresh.lock kept (' + (Format-ClaudeResidue) + ')')
    Send-Ping 'fail' 'oauth_refresh.lock kept'
    exit 1
  }

  # --- 3. one request ------------------------------------------------------------
  # Permissions come from work\.claude\settings.json (defaultMode dontAsk; only ./phlp and ./phlpreview).
  # ASCII, no quotes, no cmd metacharacters (& | < > ^ %). ONE request per run: the person is watching.
  $prompt = 'Process ONE LP compose request. Follow the ph-lp-compose skill in this workspace exactly: claim one request with ./phlp, download and LOOK AT the product images, read the spec file, reserve BEFORE writing, write the section 7 output, lint it, review with ./phlpreview, then send the result with ./phlp result and clean up. After reserve, a failure must be reported as result --rejected, never as fail. Never read the service token and never touch files outside this workspace. Finish with one line: job=N status=done or rejected or failed'
  $timedOut = $false
  $claudeExit = -1
  try {
    $p = Start-Process -FilePath $Claude -WorkingDirectory $WorkDir -NoNewWindow -PassThru `
           -RedirectStandardOutput $OutLog -RedirectStandardError $ErrLog `
           -ArgumentList @('-p', ('"' + $prompt + '"'), '--output-format', 'json')
    # PS 5.1: with -NoNewWindow + redirection, ExitCode stays empty unless the handle is taken right away
    $null = $p.Handle
    if (-not $p.WaitForExit($TimeoutMin * 60 * 1000)) {
      $timedOut = $true
      # claude.cmd -> cmd -> node: Process.Kill() would kill only cmd and leave node running (PR3-0)
      Stop-ProcessTree $p.Id
      try { $p.WaitForExit(30000) | Out-Null } catch { }
      Log ('timeout after ' + $TimeoutMin + ' min - killed the process tree; left: ' + (Format-ClaudeResidue))
    }
    try { $claudeExit = $p.ExitCode } catch { $claudeExit = -1 }
  } catch {
    Log ('failed to start claude: ' + $_.Exception.Message)
    Send-Ping 'fail' 'failed to start claude'
    exit 1
  }
  Log ('claude exit=' + $claudeExit + ' timedOut=' + $timedOut)
  $null = Wait-NoClaudeResidue -DeadlineUtc ((Get-Date).ToUniversalTime().AddMinutes(2)) -PollSec 5
} finally {
  Exit-ClaudeLock
}

# --- 4. the SERVER decides whether anything happened ------------------------------
Start-Sleep -Seconds 3
$after = $null
try { $after = Get-Queue } catch { Log ('queue post-check failed: ' + $_.Exception.Message); Send-Ping 'fail' 'queue post-check failed'; exit 1 }
$moved = [int]$before.claimable - [int]$after.claimable
$needsReviewUp = [int]$after.needs_review - [int]$before.needs_review
$note = 'moved=' + $moved + ' claimable=' + $after.claimable + ' needs_review=' + $after.needs_review + ' exit=' + $claudeExit
Log ('after: ' + $note)

if ($timedOut) { Send-Ping 'fail' ('timeout; ' + $note); exit 1 }
if ($needsReviewUp -gt 0) {
  # reserved but no result came back = outcome unknown. A person decides; we never retry it automatically.
  Send-Ping 'partial' ('needs_review +' + $needsReviewUp + '; ' + $note)
  exit 0
}
if ($moved -gt 0) { Send-Ping 'ok' $note; exit 0 }
Send-Ping 'fail' ('nothing moved; ' + $note)
exit 1
