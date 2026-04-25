# run-rankcheck-safe.ps1
#
# Rakuten Ranking Checker Runner (miniPC) - Task Scheduler wrapper.
# Following 2026-04-17 safe-debug philosophy:
#   - Single instance: lockfile prevents concurrent runs
#   - Log rotation: per-day file, exit code + start/end timestamps
#   - Low priority: BelowNormal, yields to warehouse / other services
#   - Exit notification: GChat webhook on failure (optional)
#
# NOTE: This file is intentionally ASCII-only.
# Windows PowerShell 5.1 cannot parse multi-byte chars without BOM.
# Adding Japanese strings here will break the script when invoked via Task Scheduler.
#
# Install location: C:\tools\rankcheck-runner\run-rankcheck-safe.ps1
#   - C:\Users\bfaith\bfaith-portal           : git clone (shared with WarehouseServer)
#                                                or set RANKCHECK_REPO_DIR env to override
#   - C:\tools\rankcheck-runner\data          : ranking-checker.db location
#   - C:\tools\rankcheck-runner\logs          : execution logs
#   - C:\tools\rankcheck-runner\.env          : runner env (RAKUTEN_APP_ID etc)
#
# Task Scheduler example:
#   Program:   powershell.exe
#   Arguments: -NoProfile -ExecutionPolicy Bypass -File "C:\tools\rankcheck-runner\run-rankcheck-safe.ps1"
#   Trigger:   Daily 13:00 JST
#
# Manual test:
#   powershell -NoProfile -ExecutionPolicy Bypass -File .\run-rankcheck-safe.ps1 -Force

param(
    [switch]$Force
)

$ErrorActionPreference = 'Stop'

# --- Paths ---
$root    = 'C:\tools\rankcheck-runner'
$repo    = if ($env:RANKCHECK_REPO_DIR) { $env:RANKCHECK_REPO_DIR } else { 'C:\Users\bfaith\bfaith-portal' }
$dataDir = Join-Path $root 'data'
$logDir  = Join-Path $root 'logs'
$lockFile = Join-Path $root 'runner.lock'
$envFile = Join-Path $root '.env'

foreach ($d in @($dataDir, $logDir)) {
    if (!(Test-Path $d)) { New-Item -ItemType Directory -Path $d | Out-Null }
}

$ts       = Get-Date -Format 'yyyy-MM-dd_HHmmss'
$dateTag  = Get-Date -Format 'yyyy-MM-dd'
$logFile  = Join-Path $logDir "runner-$dateTag.log"

function Write-Log {
    param([string]$Message)
    $line = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Message"
    Write-Host $line
    Add-Content -Path $logFile -Value $line -Encoding utf8
}

# --- Single-instance lock ---
if (Test-Path $lockFile) {
    $existingPid = (Get-Content $lockFile -ErrorAction SilentlyContinue | Select-Object -First 1)
    if ($existingPid -and (Get-Process -Id $existingPid -ErrorAction SilentlyContinue)) {
        Write-Log "[ERROR] Runner already running (PID $existingPid). Aborting."
        exit 73
    } else {
        Write-Log "[WARN] stale lockfile detected. PID $existingPid not alive. Removing."
        Remove-Item $lockFile -Force
    }
}
"$PID" | Out-File -FilePath $lockFile -Encoding ascii

try {
    Write-Log "[START] rankcheck-runner pid=$PID force=$($Force.IsPresent)"
    Write-Log "repo=$repo data=$dataDir log=$logFile"

    if (!(Test-Path $repo)) {
        throw "repository not found: $repo"
    }

    # --- Environment ---
    $env:DATA_DIR = $dataDir
    $env:RANKCHECK_AUTO_ENABLED = 'false'   # prevent in-proc node-cron from also firing
    if (Test-Path $envFile) {
        Write-Log ".env loading: $envFile"
        Get-Content $envFile | Where-Object { $_ -and $_ -notmatch '^\s*#' } | ForEach-Object {
            $kv = $_ -split '=', 2
            if ($kv.Count -eq 2) {
                $key = $kv[0].Trim()
                $val = $kv[1].Trim().Trim('"').Trim("'")
                Set-Item -Path "env:$key" -Value $val
            }
        }
    } else {
        Write-Log "[WARN] .env not found: $envFile"
    }

    # --- Launch runner ---
    $args = @('apps/ranking-checker-runner/runner.js')
    if ($Force) { $args += '--force' }

    Push-Location $repo
    try {
        # Order matters: Start-Process without -Wait, then set PriorityClass, then WaitForExit().
        # If -Wait is used, PriorityClass setting comes after exit (no effect).
        $proc = Start-Process -FilePath 'node.exe' -ArgumentList $args `
            -PassThru -NoNewWindow `
            -RedirectStandardOutput (Join-Path $logDir "runner-$ts.stdout.log") `
            -RedirectStandardError  (Join-Path $logDir "runner-$ts.stderr.log")
        try { $proc.PriorityClass = 'BelowNormal' } catch { Write-Log "[WARN] PriorityClass set failed: $($_.Exception.Message)" }
        $proc.WaitForExit()
        $exitCode = $proc.ExitCode
        Write-Log "[END] exit=$exitCode"

        # Merge stdout/stderr into the daily log, then remove the per-run files
        $stdoutFile = Join-Path $logDir "runner-$ts.stdout.log"
        $stderrFile = Join-Path $logDir "runner-$ts.stderr.log"
        foreach ($f in @($stdoutFile, $stderrFile)) {
            if (Test-Path $f) {
                $content = Get-Content $f -Raw
                if ($content) { Add-Content -Path $logFile -Value "--- $(Split-Path -Leaf $f) ---`n$content" -Encoding utf8 }
                Remove-Item $f -Force
            }
        }

        # --- Failure notification (optional) ---
        if ($exitCode -ne 0 -and $env:GCHAT_WEBHOOK_URL) {
            $body = @{
                text = "[rankcheck-runner] abnormal exit=$exitCode time=$ts host=$env:COMPUTERNAME"
            } | ConvertTo-Json -Compress
            try {
                Invoke-RestMethod -Uri $env:GCHAT_WEBHOOK_URL -Method Post -Body $body -ContentType 'application/json' | Out-Null
                Write-Log "GChat notification sent"
            } catch {
                Write-Log "[WARN] GChat notification failed: $($_.Exception.Message)"
            }
        }

        exit $exitCode
    } finally {
        Pop-Location
    }
} catch {
    Write-Log "[FATAL] $($_.Exception.Message)"
    if ($env:GCHAT_WEBHOOK_URL) {
        try {
            $body = @{ text = "[rankcheck-runner] FATAL: $($_.Exception.Message) host=$env:COMPUTERNAME" } | ConvertTo-Json -Compress
            Invoke-RestMethod -Uri $env:GCHAT_WEBHOOK_URL -Method Post -Body $body -ContentType 'application/json' | Out-Null
        } catch {}
    }
    exit 1
} finally {
    if (Test-Path $lockFile) { Remove-Item $lockFile -Force -ErrorAction SilentlyContinue }
}
