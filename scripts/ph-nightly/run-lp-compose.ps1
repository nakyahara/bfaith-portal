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
#   pending = claimable + running. A merely claimed (leased) request is NOT progress.
#   pending_after < pending_before     -> ok (the request left the queue for good)
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
$TimeoutMin  = 20     # one request: up to 16 images + write + lint + Codex review (<= 2 rounds). 2026-10-04: two runs took 13.5 min
$LockWaitSec = 5      # SHORT: at 1 run/min we must not pile up behind the nightly jobs
$Stamp     = Get-Date -Format 'yyyyMMdd-HHmmss'
$LogDir    = Join-Path $Root 'logs'
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$OutLog = Join-Path $LogDir "$Stamp.lp.out.log"
$ErrLog = Join-Path $LogDir "$Stamp.lp.err.log"
$RunLog = Join-Path $LogDir 'lp-compose.log'
# Outside work\ (Claude may read and write work\). install.ps1 creates it; settings.json denies it to Claude.
$StateDir = Join-Path $Root 'state'
New-Item -ItemType Directory -Force -Path $StateDir | Out-Null

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

# Which model wrote the MAIN answer (codex exec review #1591 High). Read from claude --output-format stream-json:
# assistant events with no parent_tool_use_id (= not a sub-agent). modelUsage is NOT evidence - it also lists
# auxiliary calls (Haiku). Same rule as product-idea-scout/ai/cli.cjs parseResponse.
# Returns @{ Models = @(...); Unreadable = $bool; IsError = $bool; ErrorText = '...'; HasResult = $bool }
function Read-ClaudeStream([string]$path) {
  $r = @{ Models = @(); Unreadable = $false; IsError = $false; ErrorText = ''; HasResult = $false }
  if (-not (Test-Path -LiteralPath $path)) { $r.Unreadable = $true; return $r }
  foreach ($line in Get-Content -LiteralPath $path -Encoding UTF8) {
    # only the two event types we need (tool results can be large; do not parse them)
    # (inside a JSON string the quotes are escaped, so this matches real keys only; key order is not assumed)
    if ($line -notmatch '"type":"(assistant|result)"') { continue }
    try { $e = $line | ConvertFrom-Json } catch { $r.Unreadable = $true; continue }
    if ($e.type -eq 'assistant' -and -not $e.parent_tool_use_id) {
      $m = [string]$e.message.model
      # \z, not $ (see above). A model string of another shape is "cannot verify", never "match".
      if ($m -cmatch '^claude-[a-z0-9]+(-[a-z0-9]+){1,6}\z') { if ($r.Models -notcontains $m) { $r.Models += $m } }
      else { $r.Unreadable = $true }
    } elseif ($e.type -eq 'result') {
      $r.HasResult = $true
      if ($e.is_error) { $r.IsError = $true; $r.ErrorText = [string]$e.result }
    }
  }
  return $r
}

# Attach the main-answer model to this run's generation on the server. ONLY this runner calls it: ./phlp has no
# command for it and Claude may only run ./phlp and ./phlpreview, so Claude cannot reach it. The server decides
# match / mismatch / unknown and never overwrites an earlier check.
function New-ModelCheckBody([string]$runId, [string[]]$models) {
  # models are already shape-checked in Read-ClaudeStream (no quotes or backslashes can reach the JSON)
  $arr = if (@($models).Count -gt 0) { '["' + (@($models) -join '","') + '"]' } else { '[]' }
  return '{"runner_run_id":"' + $runId + '","actual_models":' + $arr + '}'
}
function Send-ModelCheckBody([string]$body) {
  $tok = (Get-Content -Raw -LiteralPath $TokenFile).Trim()
  return Invoke-RestMethod -Method Post -Uri ($Base + '/lp-compose/model-check') -Headers @{ Authorization = ('Bearer ' + $tok) } `
    -ContentType 'application/json' -Body $body -TimeoutSec 60
}
# A check that could not be sent is kept in state\ (outside work\, Claude cannot read or write it) and re-sent
# at the start of every run - also when the queue is empty - until the server answers (codex #1591 R2 Medium).
# The server returns the stored check for a re-send, and closes a generation as "unknown" after 15 min anyway.
function Send-PendingModelChecks {
  foreach ($f in @(Get-ChildItem -LiteralPath $StateDir -Filter 'lp-model-check-*.json' -ErrorAction SilentlyContinue)) {
    if ($f.LastWriteTime -lt (Get-Date).AddDays(-1)) {
      Log ('model check dropped after 1 day (the server has closed it as unknown): ' + $f.Name)
      Remove-Item -LiteralPath $f.FullName -Force -ErrorAction SilentlyContinue
      continue
    }
    try {
      $r = Send-ModelCheckBody (Get-Content -Raw -LiteralPath $f.FullName)
      Log ('model check re-sent: ' + $f.Name + ' -> ' + ((@($r.checks) | ForEach-Object { [string]$_.model_check }) -join ','))
      Remove-Item -LiteralPath $f.FullName -Force -ErrorAction SilentlyContinue
    } catch { Log ('model check re-send failed (kept): ' + $f.Name + ': ' + $_.Exception.Message) }
  }
}

if (-not (Test-Path $Phlp) -or -not (Test-Path (Join-Path $WorkDir 'phlp'))) {
  Log 'phlp not installed (run install.ps1)'
  Send-Ping 'fail' 'phlp not installed'
  exit 1
}
if (-not (Test-Path $TokenFile)) { Log 'service token file missing'; Send-Ping 'fail' 'service token missing'; exit 1 }

# --- 0. model checks that earlier runs could not send (cheap: usually no file) ----
Send-PendingModelChecks

# --- 1. cheap poll: is there anything to do? --------------------------------------
# No Claude, no lock, no residue check when the queue is empty. This is the common case (most minutes).
$before = $null
try { $before = Get-Queue } catch { Log ('queue check failed: ' + $_.Exception.Message); Send-Ping 'fail' 'queue check failed'; exit 1 }
if (-not $before) { Log 'queue check returned nothing'; Send-Ping 'fail' 'queue check returned nothing'; exit 1 }
if (-not $before.enabled) {
  # The registry entry is a heartbeat (max_age 1 h): a deliberately disabled feature must still ping,
  # otherwise the monitor reports the runner dead after an hour (Codex review P2).
  Log 'PH_LP_COMPOSE_ENABLED is off on the server - nothing to do'
  Send-Ping 'ok' 'disabled on the server (PH_LP_COMPOSE_ENABLED)'
  exit 0
}
if ([int]$before.claimable -eq 0) {
  # A quiet minute is normal and must not be noisy. Ping ok so the dead-man monitor sees the runner alive.
  Send-Ping 'ok' ('idle (running=' + $before.running + ' needs_review=' + $before.needs_review + ')')
  exit 0
}
# The model is decided on the SERVER (Render PH_LP_COMPOSE_MODEL, also shown on the button) - never by Claude.
# The same value goes to claude --model and, via PH_LP_MODEL, to ./phlp reserve; the server refuses any other.
# \z, not $: .NET $ also matches before a trailing newline.
$Model = [string]$before.model
if ($Model -cnotmatch '^claude-[a-z0-9]+(-[a-z0-9]+){1,6}(\[1m\])?\z') {
  Log ('server sent no usable model: [' + $Model + '] (server older than this runner?)')
  Send-Ping 'fail' 'server sent no usable model'
  exit 1
}
Log ('work found: claimable=' + $before.claimable + ' running=' + $before.running + ' needs_review=' + $before.needs_review + ' model=' + $Model)

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
  # Test-ReadyToStartClaude comes from ClaudeGuard.ps1. (Test-ClaudeStartable is a LOCAL helper of
  # run-ph-generate.ps1: calling it here was CommandNotFound, which left the try via finally and skipped
  # Claude every minute while the request sat in the queue - 2026-10-02. test-runner-commands.ps1 guards this.)
  $ready = Test-ReadyToStartClaude
  if (-not $ready.Ok) {
    Log ('claude not started: oauth_refresh.lock ' + $ready.Status + ' (' + (Format-ClaudeResidue) + ')')
    Send-Ping 'fail' ('oauth_refresh.lock ' + $ready.Status)
    exit 1
  }

  # --- 3. one request ------------------------------------------------------------
  # Permissions come from work\.claude\settings.json (defaultMode dontAsk; only ./phlp and ./phlpreview).
  # ASCII, no quotes, no cmd metacharacters (& | < > ^ %). ONE request per run: the person is watching.
  # The skill is the authority; this prompt only has to get the session into it without a false start.
  # It names seen-<ID>.md and ./phlp lint on purpose: ./phlpreview refuses to run without the
  # seen file, and the server lint is what decides whether an accepted result is taken.
  $prompt = 'Process ONE LP compose request. Follow the ph-lp-compose skill in this workspace exactly: claim one request with ./phlp, download all images (product and material) and LOOK AT every one, write what you saw into seen-<ID>.md, follow the image_guide that came with the claim, read the spec file, reserve BEFORE writing, write the section 7 output following the instruction that came with the claim, check it with ./phlp lint until it passes, review with ./phlpreview, then send the result with ./phlp result and clean up. After reserve, a failure must be reported as result --rejected, never as fail. Never read the service token and never touch files outside this workspace. Finish with one line: job=N status=done or rejected or failed'
  $timedOut = $false
  $claudeExit = -1
  # This run's id. ./phlp claim puts it on the job (whatever --run Claude writes), reserve copies it to the
  # generation, and the model check (section 4) finds the generation by it.
  $RunId = 'lpr-' + $Stamp + '-' + [guid]::NewGuid().ToString('N').Substring(0, 6)
  $env:PH_LP_MODEL = $Model   # inherited by claude -> ./phlp (reserve records the REQUESTED model)
  $env:PH_LP_RUN_ID = $RunId
  # Same conditions every run (2026-10-04, checked on the miniPC with a secret word in MEMORY.md):
  # - no auto memory: notes from earlier runs (shared work dir with the manuscript job) must not change this run
  # - no claude.ai connectors / MCP servers (Gmail, Drive ... were attached to the session)
  # Only for this process tree; the nightly manuscript runner is not affected.
  $env:CLAUDE_CODE_DISABLE_AUTO_MEMORY = '1'
  $env:ENABLE_CLAUDEAI_MCP_SERVERS = 'false'
  try {
    $p = Start-Process -FilePath $Claude -WorkingDirectory $WorkDir -NoNewWindow -PassThru `
           -RedirectStandardOutput $OutLog -RedirectStandardError $ErrLog `
           -ArgumentList @('-p', ('"' + $prompt + '"'), '--model', $Model, '--output-format', 'stream-json', '--verbose', '--strict-mcp-config')
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
  Remove-Item Env:PH_LP_MODEL -ErrorAction SilentlyContinue
  Remove-Item Env:PH_LP_RUN_ID -ErrorAction SilentlyContinue
  Remove-Item Env:CLAUDE_CODE_DISABLE_AUTO_MEMORY -ErrorAction SilentlyContinue
  Remove-Item Env:ENABLE_CLAUDEAI_MCP_SERVERS -ErrorAction SilentlyContinue
  Log ('claude exit=' + $claudeExit + ' timedOut=' + $timedOut + ' run=' + $RunId)
  # Without this a refused model or a login problem looks like "nothing moved"
  # (2026-10-02: Claude Code 2.1.252 refused Opus 5.5 with API 400 - needs 2.1.280+).
  $stream = Read-ClaudeStream $OutLog
  Log ('main model(s)=' + ($stream.Models -join ',') + ' unreadable=' + $stream.Unreadable + ' result=' + $stream.HasResult + ' is_error=' + $stream.IsError)
  if ($stream.IsError) { Log ('claude reported an error: ' + $stream.ErrorText.Substring(0, [Math]::Min(300, $stream.ErrorText.Length))) }
  $null = Wait-NoClaudeResidue -DeadlineUtc ((Get-Date).ToUniversalTime().AddMinutes(2)) -PollSec 5
} finally {
  Exit-ClaudeLock
}

# --- 4. attach the main-answer model, then let the SERVER decide what happened -----
# FIRST, before the queue post-check (a failed post-check must not lose the model check - codex #1591 R2 Medium).
# Always sent, also after an error or a timeout: the server records "unknown" when no main model could be read,
# never "match", and moves a done request to needs_review unless it is a match (the text is then not shown).
# A model string of another shape is not sent as evidence (Unreadable -> the list is sent empty = unknown).
# The body is written to state\ first and removed only after the server answered; otherwise the next runs re-send it.
$checkNote = 'not sent'
$modelVerified = $true
$pendingFile = Join-Path $StateDir ('lp-model-check-' + $RunId + '.json')
try {
  $send = if ($stream.Unreadable) { @() } else { $stream.Models }
  $mcBody = New-ModelCheckBody $RunId $send
  [IO.File]::WriteAllText($pendingFile, $mcBody, (New-Object Text.UTF8Encoding($false)))
  $mc = Send-ModelCheckBody $mcBody
  Remove-Item -LiteralPath $pendingFile -Force -ErrorAction SilentlyContinue
  $checks = @($mc.checks | ForEach-Object { [string]$_.model_check })
  $checkNote = if ($checks.Count) { $checks -join ',' } else { 'no generation' }
  if ($checks | Where-Object { $_ -ne 'match' }) { $modelVerified = $false }
} catch {
  $checkNote = 'send failed (kept for re-send): ' + $_.Exception.Message
  $modelVerified = $false
}

Start-Sleep -Seconds 3
$after = $null
try { $after = Get-Queue } catch { Log ('queue post-check failed: ' + $_.Exception.Message + ' check=' + $checkNote); Send-Ping 'fail' 'queue post-check failed'; exit 1 }
# A merely CLAIMED request is NOT progress: claimable goes down while running goes up, and the request
# is lost (it only turns into failed when the lease expires 40 min later). Same lesson as the manuscript
# runner: pending = claimable + leased (Codex review P1).
$pendingBefore = [int]$before.claimable + [int]$before.running
$pendingAfter  = [int]$after.claimable + [int]$after.running
$moved = $pendingBefore - $pendingAfter
$needsReviewUp = [int]$after.needs_review - [int]$before.needs_review
$note = 'moved=' + $moved + ' claimable=' + $after.claimable + ' running=' + $after.running + ' needs_review=' + $after.needs_review + ' exit=' + $claudeExit + ' model=' + $Model + ' main=' + ($stream.Models -join ',') + ' check=' + $checkNote
Log ('after: ' + $note)

if ($timedOut) { Send-Ping 'fail' ('timeout; ' + $note); exit 1 }
# A result whose model is not verified (another model, unreadable, or the check not stored) must not pass as ok:
# the measurement compares ONE model. The screen shows the same check next to the result.
if (-not $modelVerified -and ($moved -gt 0 -or $needsReviewUp -gt 0)) { Send-Ping 'partial' ('model not verified; ' + $note); exit 0 }
if ($needsReviewUp -gt 0) {
  # reserved but no result came back = outcome unknown. A person decides; we never retry it automatically.
  Send-Ping 'partial' ('needs_review +' + $needsReviewUp + '; ' + $note)
  exit 0
}
if ($moved -gt 0) { Send-Ping 'ok' $note; exit 0 }
if ([int]$after.running -gt [int]$before.running) {
  # claimed but never finished: the lease will expire into failed. Say so now, do not call it ok.
  Send-Ping 'fail' ('claimed but not finished; ' + $note)
  exit 1
}
Send-Ping 'fail' ('nothing moved; ' + $note)
exit 1
