# run-rankcheck-safe.ps1
#
# 楽天順位チェッカー Runner (miniPC版) の Task Scheduler ラッパー。
# 2026-04-17 safe-debug 思想に沿って、以下の安全装置を提供する:
#   - 単一実行: 既に別インスタンスが動いていたら即失敗 (lockfile方式)
#   - ログローテ: 日付ごとにファイル分割、終了コードと開始/終了時刻を記録
#   - 低優先度: BelowNormal で warehouse / 他サービスに負ける
#   - 終了通知: 失敗時は GChat webhook (任意)
#
# 配置先 (想定): C:\tools\rankcheck-runner\run-rankcheck-safe.ps1
#   - C:\tools\rankcheck-runner\bfaith-portal  : git clone
#   - C:\tools\rankcheck-runner\data           : ranking-checker.db 置き場
#   - C:\tools\rankcheck-runner\logs           : 実行ログ
#   - C:\tools\rankcheck-runner\.env           : Runner用env (RAKUTEN_APP_ID 等)
#
# Task Scheduler 登録例:
#   Program:   powershell.exe
#   Arguments: -NoProfile -ExecutionPolicy Bypass -File "C:\tools\rankcheck-runner\run-rankcheck-safe.ps1"
#   Trigger:   Daily 13:00 JST
#   Settings: "Run whether user is logged on or not" + "Do not start a new instance"
#
# 手動テスト:
#   powershell -NoProfile -ExecutionPolicy Bypass -File .\run-rankcheck-safe.ps1 -Force

param(
    [switch]$Force
)

$ErrorActionPreference = 'Stop'

# --- パス構成 ---
$root    = 'C:\tools\rankcheck-runner'
$repo    = Join-Path $root 'bfaith-portal'
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

# --- 単一実行ガード (lockfile) ---
if (Test-Path $lockFile) {
    $existingPid = (Get-Content $lockFile -ErrorAction SilentlyContinue | Select-Object -First 1)
    if ($existingPid -and (Get-Process -Id $existingPid -ErrorAction SilentlyContinue)) {
        Write-Log "[ERROR] 既に Runner が実行中 (PID $existingPid). 中断します."
        exit 73
    } else {
        Write-Log "[WARN] 古い lockfile を検出。PID $existingPid は存在しないので削除します."
        Remove-Item $lockFile -Force
    }
}
"$PID" | Out-File -FilePath $lockFile -Encoding ascii

try {
    Write-Log "[START] rankcheck-runner pid=$PID force=$($Force.IsPresent)"
    Write-Log "repo=$repo data=$dataDir log=$logFile"

    if (!(Test-Path $repo)) {
        throw "リポジトリが見つかりません: $repo"
    }

    # --- 環境変数 ---
    $env:DATA_DIR = $dataDir
    $env:RANKCHECK_AUTO_ENABLED = 'false'   # node-cron 二重起動防止
    if (Test-Path $envFile) {
        Write-Log ".env を読み込み中: $envFile"
        Get-Content $envFile | Where-Object { $_ -and $_ -notmatch '^\s*#' } | ForEach-Object {
            $kv = $_ -split '=', 2
            if ($kv.Count -eq 2) {
                $key = $kv[0].Trim()
                $val = $kv[1].Trim().Trim('"').Trim("'")
                Set-Item -Path "env:$key" -Value $val
            }
        }
    } else {
        Write-Log "[WARN] .env が見つかりません: $envFile"
    }

    # --- Runner 起動 ---
    $args = @('apps/ranking-checker-runner/runner.js')
    if ($Force) { $args += '--force' }

    Push-Location $repo
    try {
        # -Wait すると PriorityClass の設定タイミングが遅すぎるので、
        # 非Wait で起動 → PriorityClass 設定 → WaitForExit() の順。
        $proc = Start-Process -FilePath 'node.exe' -ArgumentList $args `
            -PassThru -NoNewWindow `
            -RedirectStandardOutput (Join-Path $logDir "runner-$ts.stdout.log") `
            -RedirectStandardError  (Join-Path $logDir "runner-$ts.stderr.log")
        try { $proc.PriorityClass = 'BelowNormal' } catch { Write-Log "[WARN] PriorityClass 設定失敗: $($_.Exception.Message)" }
        $proc.WaitForExit()
        $exitCode = $proc.ExitCode
        Write-Log "[END] exit=$exitCode"

        # stdout/stderr を日次ログにマージして小さいファイルを削除
        $stdoutFile = Join-Path $logDir "runner-$ts.stdout.log"
        $stderrFile = Join-Path $logDir "runner-$ts.stderr.log"
        foreach ($f in @($stdoutFile, $stderrFile)) {
            if (Test-Path $f) {
                $content = Get-Content $f -Raw
                if ($content) { Add-Content -Path $logFile -Value "--- $(Split-Path -Leaf $f) ---`n$content" -Encoding utf8 }
                Remove-Item $f -Force
            }
        }

        # --- 失敗通知 (任意) ---
        if ($exitCode -ne 0 -and $env:GCHAT_WEBHOOK_URL) {
            $body = @{
                text = "[rankcheck-runner] 異常終了 exit=$exitCode time=$ts host=$env:COMPUTERNAME"
            } | ConvertTo-Json -Compress
            try {
                Invoke-RestMethod -Uri $env:GCHAT_WEBHOOK_URL -Method Post -Body $body -ContentType 'application/json' | Out-Null
                Write-Log "GChat 通知送信完了"
            } catch {
                Write-Log "[WARN] GChat 通知失敗: $($_.Exception.Message)"
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
