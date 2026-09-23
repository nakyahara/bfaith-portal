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

# Processes that are (or are about to start) Claude Code, other than this process. Command lines are read via CIM.
$ClaudeResiduePattern = '(?i)(@anthropic-ai[\\/]claude-code|claude-code[\\/](bin|cli\.js)|\bkw-publish\.cjs\b|\bkw-preflight\.cjs\b|\bad-kw-ai\.mjs\b|\brun-ph-generate\.ps1\b|\brun-keywords\.ps1\b)'
function Get-ClaudeResidue([int[]]$ExcludePid = @()) {
  $skip = @($PID) + $ExcludePid
  # CLAUDE_GUARD_TEST_EXCLUDE (comma-separated pids) is for tests on a dev PC where a Claude Code session is running
  if ($env:CLAUDE_GUARD_TEST_EXCLUDE) { $skip += @($env:CLAUDE_GUARD_TEST_EXCLUDE -split ',' | Where-Object { $_ -match '^\d+$' } | ForEach-Object { [int]$_ }) }
  $found = @()
  foreach ($p in (Get-CimInstance Win32_Process -ErrorAction SilentlyContinue)) {
    if ($skip -contains [int]$p.ProcessId) { continue }
    $name = [string]$p.Name
    $cmd = [string]$p.CommandLine
    if ($name -ieq 'claude.exe' -or ($cmd -and $cmd -match $ClaudeResiduePattern)) {
      $found += [pscustomobject]@{ Pid = [int]$p.ProcessId; Name = $name; CommandLine = $(if ($cmd.Length -gt 160) { $cmd.Substring(0, 160) } else { $cmd }) }
    }
  }
  return ,$found
}
# Wait until no residue is left (or the deadline). Returns $true when clean.
function Wait-NoClaudeResidue([datetime]$DeadlineUtc, [int]$PollSec = 10, [int[]]$ExcludePid = @()) {
  while ($true) {
    $r = Get-ClaudeResidue -ExcludePid $ExcludePid
    if ($r.Count -eq 0) { return $true }
    if ((Get-Date).ToUniversalTime().AddSeconds($PollSec) -gt $DeadlineUtc) { return $false }
    Start-Sleep -Seconds $PollSec
  }
}

# Remove ~\.claude\.oauth_refresh.lock only when it is provably stale: we hold the lock and no Claude runs.
# Returns 'absent' | 'removed' | 'kept-not-holding' | 'kept-claude-running' | 'remove-failed'.
function Remove-OauthLockIfSafe([int[]]$ExcludePid = @()) {
  $oauth = Join-Path $env:USERPROFILE '.claude\.oauth_refresh.lock'
  if (-not (Test-Path -LiteralPath $oauth)) { return 'absent' }
  if (-not $script:ClaudeLockStream) { return 'kept-not-holding' }
  if ((Get-ClaudeResidue -ExcludePid $ExcludePid).Count -gt 0) { return 'kept-claude-running' }
  try { Remove-Item -LiteralPath $oauth -Recurse -Force; return 'removed' } catch { return 'remove-failed' }
}

# Kill a process and all of its descendants (claude.cmd -> cmd -> node). Process.Kill() alone kills only the top.
function Stop-ProcessTree([int]$RootPid) {
  try { & taskkill.exe /PID $RootPid /T /F 2>$null | Out-Null } catch { }
}
