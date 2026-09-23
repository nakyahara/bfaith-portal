# ClaudeGuard.ps1 - one Claude Code at a time on the miniPC (dot-source this file).
# ASCII only (PS 5.1 reads BOM-less files as CP932).
#
# Several scheduled runners share the same subscription OAuth in ~\.claude (bfaith):
#   PhGenerateNightly (02:30, Interactive)  - product-hub manuscripts (+ ad keywords, PR3b)
#   ProductKWScout    (05:00, S4U)          - product-scout keyword stage
# Two Claude processes refreshing the same OAuth token at once break each other, and the old runner deleted
# ~\.claude\.oauth_refresh.lock unconditionally. Design = SP-ad KW design doc, PR3 plan v2.2 (Codex plan review R3):
#   1. Lock = a file opened with FileShare.None and kept open (Enter-ClaudeLock). The OS releases it when the
#      holder dies. Works across logon sessions (Interactive / S4U) without SeCreateGlobalPrivilege.
#   2. Enable-KillOnCloseJob puts THIS PowerShell process into a new Job Object with KILL_ON_JOB_CLOSE.
#      Children started afterwards (node, claude) are in the job automatically, so if this parent dies the OS
#      kills them too: an orphaned runner cannot start Claude later (e.g. one waiting on HTTP).
#   3. Before starting Claude: Wait-NoClaudeResidue (no claude / claude-code node / known AI runner left over).
#      Safety net for anything the job did not catch.
#   4. ~\.claude\.oauth_refresh.lock is removed only while holding the lock AND with no Claude process alive
#      (Remove-OauthLockIfSafe). Never by age alone.

$script:ClaudeLockStream = $null
$script:ClaudeJobHandle = [IntPtr]::Zero
# CLAUDE_GUARD_LOCK overrides the path for tests only (scripts\test-claude-guard.ps1); runners never set it.
$ClaudeLockPath = if ($env:CLAUDE_GUARD_LOCK) { $env:CLAUDE_GUARD_LOCK } else { 'C:\tools\claude-lock\claude.lock' }

if (-not ('BFaith.KillOnCloseJob' -as [type])) {
  Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
namespace BFaith {
  public static class KillOnCloseJob {
    [StructLayout(LayoutKind.Sequential)]
    struct BASIC { public long PerProcessUserTimeLimit; public long PerJobUserTimeLimit; public uint LimitFlags;
      public UIntPtr MinimumWorkingSetSize; public UIntPtr MaximumWorkingSetSize; public uint ActiveProcessLimit;
      public UIntPtr Affinity; public uint PriorityClass; public uint SchedulingClass; }
    [StructLayout(LayoutKind.Sequential)]
    struct IO { public ulong a, b, c, d, e, f; }
    [StructLayout(LayoutKind.Sequential)]
    struct EXTENDED { public BASIC Basic; public IO Io; public UIntPtr ProcessMemoryLimit; public UIntPtr JobMemoryLimit;
      public UIntPtr PeakProcessMemoryUsed; public UIntPtr PeakJobMemoryUsed; }
    [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
    static extern IntPtr CreateJobObject(IntPtr attrs, string name);
    [DllImport("kernel32.dll", SetLastError = true)]
    static extern bool SetInformationJobObject(IntPtr job, int cls, ref EXTENDED info, uint len);
    [DllImport("kernel32.dll", SetLastError = true)]
    static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);
    [DllImport("kernel32.dll")]
    static extern IntPtr GetCurrentProcess();
    const uint KILL_ON_JOB_CLOSE = 0x2000;
    // Unnamed job, NULL security attributes = the handle is not inheritable (children cannot keep the job open).
    public static IntPtr EnableForCurrentProcess() {
      IntPtr job = CreateJobObject(IntPtr.Zero, null);
      if (job == IntPtr.Zero) throw new System.ComponentModel.Win32Exception(Marshal.GetLastWin32Error());
      EXTENDED info = new EXTENDED();
      info.Basic.LimitFlags = KILL_ON_JOB_CLOSE;
      if (!SetInformationJobObject(job, 9, ref info, (uint)Marshal.SizeOf(typeof(EXTENDED))))
        throw new System.ComponentModel.Win32Exception(Marshal.GetLastWin32Error());
      if (!AssignProcessToJobObject(job, GetCurrentProcess()))
        throw new System.ComponentModel.Win32Exception(Marshal.GetLastWin32Error());
      return job;
    }
  }
}
'@
}

# Put this process (and every child started after this call) into a KILL_ON_JOB_CLOSE job. Call first thing.
# Returns $true on success. The handle is kept in this process only; the job dies with it.
function Enable-KillOnCloseJob {
  if ($script:ClaudeJobHandle -ne [IntPtr]::Zero) { return $true }
  try { $script:ClaudeJobHandle = [BFaith.KillOnCloseJob]::EnableForCurrentProcess(); return $true }
  catch { return $false }
}

# Take the lock, waiting until $DeadlineUtc. Returns $true when held. A lock held by a live runner is never
# broken; a dead holder releases it by itself (OS closes the handle).
function Enter-ClaudeLock([datetime]$DeadlineUtc, [int]$PollSec = 15, [string]$Path = $ClaudeLockPath) {
  if ($script:ClaudeLockStream) { return $true }
  $dir = Split-Path -Parent $Path
  if (-not (Test-Path -LiteralPath $dir)) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
  while ($true) {
    try {
      $script:ClaudeLockStream = [System.IO.File]::Open($Path, [System.IO.FileMode]::OpenOrCreate,
        [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
      return $true
    } catch [System.IO.IOException] {
      if ((Get-Date).ToUniversalTime().AddSeconds($PollSec) -gt $DeadlineUtc) { return $false }
      Start-Sleep -Seconds $PollSec
    }
  }
}
function Exit-ClaudeLock {
  if ($script:ClaudeLockStream) { try { $script:ClaudeLockStream.Dispose() } catch { }; $script:ClaudeLockStream = $null }
}

# Processes that ARE Claude Code or an AI worker that starts it (node), other than this process.
# The PowerShell runners themselves are NOT residue (Codex #1427 R1 #1): a guarded runner that is only waiting for
# the lock has started nothing, and counting it made two runners wait for each other until both deadlines.
# A guarded runner starts node / claude only after taking the lock, and its job kills them if it dies.
# (An unguarded OLD runner is still caught once it starts node / claude; install both runners together.)
$ClaudeResiduePattern = '(?i)(@anthropic-ai[\\/]claude-code|claude-code[\\/](bin|cli\.js)|\bkw-publish\.cjs\b|\bkw-preflight\.cjs\b|\bad-kw-ai\.mjs\b)'
# Returns @{ Ok = $true; Items = @(...) } or @{ Ok = $false; Error = '...' } when the process list could not be read.
# A failed listing is NEVER "nothing is running" (Codex #1427 R1 #2): callers must not start Claude or delete the OAuth lock.
# CommandLine is readable for this user's own processes (the runners and Claude all run as the same user);
# an unreadable command line belongs to another user (e.g. a SYSTEM service's node), which cannot use this OAuth.
function Get-ClaudeResidue([int[]]$ExcludePid = @()) {
  $skip = @($PID) + $ExcludePid
  # CLAUDE_GUARD_TEST_EXCLUDE (comma-separated pids) is for tests on a dev PC where a Claude Code session is running
  if ($env:CLAUDE_GUARD_TEST_EXCLUDE) { $skip += @($env:CLAUDE_GUARD_TEST_EXCLUDE -split ',' | Where-Object { $_ -match '^\d+$' } | ForEach-Object { [int]$_ }) }
  try { $all = @(Get-CimInstance Win32_Process -ErrorAction Stop) }
  catch { return [pscustomobject]@{ Ok = $false; Items = @(); Error = ('process list failed: ' + $_.Exception.Message) } }
  if ($all.Count -eq 0) { return [pscustomobject]@{ Ok = $false; Items = @(); Error = 'process list was empty' } }
  $found = @()
  foreach ($p in $all) {
    if ($skip -contains [int]$p.ProcessId) { continue }
    $name = [string]$p.Name
    $cmd = [string]$p.CommandLine
    if ($name -ieq 'claude.exe' -or ($cmd -and $cmd -match $ClaudeResiduePattern)) {
      $found += [pscustomobject]@{ Pid = [int]$p.ProcessId; Name = $name; CommandLine = $(if ($cmd.Length -gt 160) { $cmd.Substring(0, 160) } else { $cmd }) }
    }
  }
  return [pscustomobject]@{ Ok = $true; Items = $found; Error = $null }
}
# Wait until no residue is left (or the deadline). Returns 'clean' | 'residue' (still running at the deadline) | 'unknown' (listing failed).
function Wait-NoClaudeResidue([datetime]$DeadlineUtc, [int]$PollSec = 10, [int[]]$ExcludePid = @()) {
  while ($true) {
    $r = Get-ClaudeResidue -ExcludePid $ExcludePid
    if (-not $r.Ok) { return 'unknown' }
    if (@($r.Items).Count -eq 0) { return 'clean' }
    if ((Get-Date).ToUniversalTime().AddSeconds($PollSec) -gt $DeadlineUtc) { return 'residue' }
    Start-Sleep -Seconds $PollSec
  }
}
# One line for logs / pings: "claude.exe:123 node.exe:456" or the listing error
function Format-ClaudeResidue([int[]]$ExcludePid = @()) {
  $r = Get-ClaudeResidue -ExcludePid $ExcludePid
  if (-not $r.Ok) { return $r.Error }
  return ((@($r.Items) | ForEach-Object { $_.Name + ':' + $_.Pid }) -join ' ')
}

# Remove ~\.claude\.oauth_refresh.lock only when it is provably stale: we hold the lock and no Claude runs.
# Returns 'absent' | 'removed' | 'kept-not-holding' | 'kept-claude-running' | 'kept-unknown' | 'remove-failed'.
function Remove-OauthLockIfSafe([int[]]$ExcludePid = @()) {
  $oauth = Join-Path $env:USERPROFILE '.claude\.oauth_refresh.lock'
  if (-not (Test-Path -LiteralPath $oauth)) { return 'absent' }
  if (-not $script:ClaudeLockStream) { return 'kept-not-holding' }
  $r = Get-ClaudeResidue -ExcludePid $ExcludePid
  if (-not $r.Ok) { return 'kept-unknown' }
  if (@($r.Items).Count -gt 0) { return 'kept-claude-running' }
  try { Remove-Item -LiteralPath $oauth -Recurse -Force; return 'removed' } catch { return 'remove-failed' }
}

# Call right before EVERY claude start (auth status, each attempt). Ok=$true only when the OAuth lock is absent or was
# removed safely; kept-unknown / kept-claude-running / kept-not-holding / remove-failed mean "do not start Claude now"
# (Codex #1427 R2: the result must stop the runner, not only be logged).
function Test-ReadyToStartClaude([int[]]$ExcludePid = @()) {
  $s = Remove-OauthLockIfSafe -ExcludePid $ExcludePid
  return [pscustomobject]@{ Ok = ($s -eq 'absent' -or $s -eq 'removed'); Status = $s }
}

# Minutes left before the Task Scheduler kills this run, minus the time kept for ending cleanly (Codex #1427 R1 #3).
function Get-RunMinutesLeft([datetime]$StartedUtc, [int]$TaskLimitMin, [int]$EndSlackMin) {
  return [int][Math]::Floor($TaskLimitMin - $EndSlackMin - ((Get-Date).ToUniversalTime() - $StartedUtc).TotalMinutes)
}

# Kill a process and all of its descendants (claude.cmd -> cmd -> node). Process.Kill() alone kills only the top.
function Stop-ProcessTree([int]$RootPid) {
  try { & taskkill.exe /PID $RootPid /T /F 2>$null | Out-Null } catch { }
}
