# run-expected-profit-nightly.ps1 - Task Scheduler runner for the "expected profit" nightly batch (miniPC).
#
#   powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\bfaith\bfaith-portal\scripts\expected-profit\run-expected-profit-nightly.ps1
#
# What it does: cd to the repo, run `node apps/expected-profit/nightly.js`, keep a rotated log.
#
# Design (Codex review 2026-09-09):
#   - The runner STOPS THE CHILD ITSELF at $StopAtHhmm (06:15 JST by default), before the task's own
#     ExecutionTimeLimit (07:00 after a 23:30 start = 06:30). nightly.js only checks its 06:00 deadline
#     BETWEEN steps, so a single long step can run past it; if the Scheduler killed the whole PowerShell
#     process, no ping would be sent and the lock would be left behind. Stopping the child from here keeps
#     the report and the cleanup in our hands. The task limit stays as the outer backstop.
#   - Ping ownership is decided by the exit code (see nightly.js header):
#       0 = ok, reported      5 = ok but COULD NOT report  -> the runner sends the ok ping
#       3 = failed, reported  1 = failed and not reported  -> the runner sends the fail ping
#     If the runner has to stop the child at the deadline it sends 'fail' (this job has no
#     partial_max_days in the registry, and a stopped night leaves the screen on yesterday's numbers).
#     jobs-monitor keeps only the LAST ping (store.js recordPing is an upsert), so a second ping from here
#     would overwrite nightly's specific reason ("could not publish: ...") with a generic one.
#   - Single instance: the scheduler is set to IgnoreNew, but a manual run can still overlap a scheduled
#     one, so a lock file guards it too. A lock older than $LockStaleHours is taken over (a killed run
#     leaves the file behind).
#   - DATA_DIR is set explicitly, exactly like daily-sync.bat, so the batch reads/writes the same
#     warehouse.db / expected-profit.db as everything else on this machine.
#   - node is started with Start-Process and its own stdout/stderr files. Piping a native command through
#     Tee-Object in PS 5.1 wraps every stderr line in a NativeCommandError record, which makes the log
#     unreadable (and $? unreliable) even on a clean exit.
#
# IMPORTANT: keep this file ASCII-only.
#   Windows PowerShell 5.1 reads BOM-less files as ANSI (CP932); multi-byte characters can swallow
#   adjacent quotes and silently corrupt parsing (this happened on 2026-08-01 with ping.ps1).
param(
  # Repo checkout to run. Pass a git worktree path to test an unmerged branch.
  [string]$Repo = 'C:\Users\bfaith\bfaith-portal',
  # Wall clock at which the child is stopped, HH:mm. Must be BEFORE the task's ExecutionTimeLimit (06:30).
  [string]$StopAtHhmm = '06:15',
  # Print what would run instead of running node (for checking the wiring).
  [switch]$DryRun,
  # Extra arguments for nightly.js, e.g. --skip-publish
  [string[]]$NodeArgs = @(),
  # Entry point to run. Overridable so the runner itself can be exercised (exit codes, deadline stop).
  [string]$Entry = 'apps/expected-profit/nightly.js'
)
$ErrorActionPreference = 'Continue'

$JobId          = 'expected-profit-nightly'
$Script         = $Entry
$LogDir         = Join-Path $Repo 'logs'
$LockFile       = Join-Path $Repo 'logs\expected-profit.lock'
$LockStaleHours = 8          # the run itself is bounded by $StopAtHhmm; anything older is a leftover
$KeepLogDays    = 14
$PingPs1        = Join-Path $Repo 'scripts\jobs-monitor\ping.ps1'

if (-not (Test-Path (Join-Path $Repo $Script))) { Write-Error "not a bfaith-portal checkout: $Repo"; exit 2 }
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$Stamp  = Get-Date -Format 'yyyy-MM-dd_HHmmss'
$OutLog = Join-Path $LogDir "expected-profit-$Stamp.out.log"
$ErrLog = Join-Path $LogDir "expected-profit-$Stamp.err.log"
$RunLog = Join-Path $LogDir 'expected-profit-runner.log'

function Log([string]$msg) {
  $line = '[' + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + '] ' + $msg
  Write-Output $line
  # PS 5.1 would default to ANSI here
  Add-Content -Path $RunLog -Value $line -Encoding UTF8
}

# ping.ps1 always exits 0 by design (monitoring must never fail the job), so a failed ping cannot be
# detected from its exit code. Its own log is $env:TEMP\jobs-monitor-ping.log; note here that we tried.
function Send-Ping([string]$status, [string]$note) {
  if (-not (Test-Path $PingPs1)) { Log 'ping.ps1 not found - NOT reported'; return }
  $safe = ($note -replace '["\r\n\t]', ' ') -replace '\s+', ' '
  if ($safe.Length -gt 180) { $safe = $safe.Substring(0, 180) }
  Log ('ping ' + $status + ' : ' + $safe)
  try { & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $PingPs1 -Id $JobId -Status $status -Note $safe }
  catch { Log ('ping error: ' + $_.Exception.Message) }
}

# The stop time is the next occurrence of $StopAtHhmm (the batch starts at 23:30 and crosses midnight).
function Get-StopTime([string]$hhmm) {
  $t = [datetime]::ParseExact($hhmm, 'HH:mm', $null)
  $stop = (Get-Date).Date.AddHours($t.Hour).AddMinutes($t.Minute)
  if ($stop -le (Get-Date)) { $stop = $stop.AddDays(1) }
  return $stop
}

# --- single instance -------------------------------------------------------
if (Test-Path $LockFile) {
  $age = (Get-Date) - (Get-Item $LockFile).LastWriteTime
  if ($age.TotalHours -lt $LockStaleHours) {
    Log ('another run is in progress (lock age ' + [int]$age.TotalMinutes + ' min) - exiting')
    exit 0
  }
  Log ('stale lock (' + [int]$age.TotalHours + 'h) - taking it over')
  Remove-Item -Force $LockFile
}
Set-Content -Path $LockFile -Value ("pid=" + $PID + " started=" + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss')) -Encoding ASCII

$code = 1
try {
  # --- log rotation (best effort) -----------------------------------------
  try {
    Get-ChildItem -Path $LogDir -Filter 'expected-profit-*.log' -ErrorAction SilentlyContinue |
      Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-$KeepLogDays) } |
      Remove-Item -Force -ErrorAction SilentlyContinue
  } catch { }

  $env:DATA_DIR = Join-Path $Repo 'data'
  $stopAt = Get-StopTime $StopAtHhmm
  $argLine = @($Script) + $NodeArgs
  Log ('start : node ' + ($argLine -join ' ') + '  (DATA_DIR=' + $env:DATA_DIR + ')')
  Log ('stop  : ' + $stopAt.ToString('yyyy-MM-dd HH:mm') + '  log: ' + $OutLog)

  if ($DryRun) {
    Log 'dry run - node was not started'
    $code = 0
  } else {
    $p = Start-Process -FilePath 'node.exe' -ArgumentList $argLine -WorkingDirectory $Repo `
           -RedirectStandardOutput $OutLog -RedirectStandardError $ErrLog -NoNewWindow -PassThru
    # PS 5.1 quirk: with -PassThru (and no -Wait) the object does not keep the process handle, so
    # $p.ExitCode stays $null even after the process has exited. Touching .Handle caches it.
    # Without this every successful night reads as "exit 1" and this runner would send a bogus fail
    # ping over nightly.js's ok (found while testing the runner, 2026-09-09).
    $null = $p.Handle
    $killed = $false
    while (-not $p.HasExited) {
      if ((Get-Date) -ge $stopAt) {
        Log ('deadline ' + $StopAtHhmm + ' reached - stopping node (pid ' + $p.Id + ')')
        try { Stop-Process -Id $p.Id -Force -ErrorAction Stop } catch { Log ('stop failed: ' + $_.Exception.Message) }
        $killed = $true
        break
      }
      Start-Sleep -Seconds 20
    }
    # give the process a moment to actually die, then read the code
    try { $p.WaitForExit(30000) | Out-Null } catch { }
    $code = if ($killed) { 4 } elseif ($null -ne $p.ExitCode) { $p.ExitCode } else { 1 }
    Log ('node exit code: ' + $code)

    if ($killed) {
      # 'fail', not 'partial': jobs-monitor only treats partial as "it ran" for jobs that declare
      # partial_max_days (evaluate.js), and this job does not. More importantly, a stopped night means
      # the screen still shows the previous generation - that is a failure to report, not a half-success.
      # The note says it was stopped on purpose; the generation may already be built and can be published
      # next night (Codex 4th round).
      Send-Ping 'fail' ('stopped at ' + $StopAtHhmm + ' before the task limit - numbers were not updated (see ' + $OutLog + ')')
    } elseif ($code -eq 0) {
      # nightly.js reported the success itself
    } elseif ($code -eq 5) {
      # the job DID succeed; only the report failed (401 / timeout). Without this the monitor would
      # keep showing yesterday until the dead-man alert fires (Codex 3rd round).
      Send-Ping 'ok' 'published, but nightly.js could not reach jobs-monitor'
      $code = 0
    } elseif ($code -eq 3) {
      Log 'nightly.js already reported the failure - not pinging again'
    } else {
      Send-Ping 'fail' ('nightly.js exit ' + $code + ' without reporting (see ' + $ErrLog + ')')
    }
  }
} catch {
  Log ('runner error: ' + $_.Exception.Message)
  Send-Ping 'fail' ('runner error: ' + $_.Exception.Message)
  $code = 1
} finally {
  Remove-Item -Force $LockFile -ErrorAction SilentlyContinue
}
exit $code
