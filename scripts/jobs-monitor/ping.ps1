# jobs-monitor への ping (成功報告)。miniPC の各ランナー (bat/ps1) の末尾から呼ぶ。
#
#   powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\bfaith\bfaith-portal\scripts\jobs-monitor\ping.ps1 -Id warehouse-daily-sync -Status ok
#
# 設計:
#   - ⭐ping の失敗でジョブ本体を絶対に失敗させない (常に exit 0)。
#     監視はおまけであり、本体の仕事より優先されてはならない
#   - トークンとURLは bfaith-portal\.env から読む (JOBS_MONITOR_TOKEN / JOBS_MONITOR_URL)。
#     未設定なら黙って何もしない (導入前のランナーに先に仕込んでも無害)
param(
  [Parameter(Mandatory = $true)][string]$Id,
  [ValidateSet('ok', 'fail', 'start')][string]$Status = 'ok'
)

$ErrorActionPreference = 'SilentlyContinue'

# best-effort のローカルログ (dead-man発報後に「pingが出なかったのか届かなかったのか」を切り分ける材料。
# 1MBを超えたら捨てて書き直す。ログ失敗も無視 = 本体を絶対に巻き込まない)
$logFile = Join-Path $env:TEMP 'jobs-monitor-ping.log'
function Write-PingLog([string]$msg) {
  try {
    if ((Test-Path $logFile) -and (Get-Item $logFile).Length -gt 1MB) { Remove-Item $logFile -Force }
    ('[' + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + '] ' + $Id + ' ' + $Status + ' ' + $msg) | Out-File $logFile -Append -Encoding utf8
  } catch { }
}

try {
  $envFile = 'C:\Users\bfaith\bfaith-portal\.env'
  if (-not (Test-Path $envFile)) { Write-PingLog 'skip: .env なし'; exit 0 }
  $token = (Select-String -Path $envFile -Pattern '^JOBS_MONITOR_TOKEN=' | Select-Object -First 1).Line -replace '^[^=]+=', ''
  if (-not $token) { Write-PingLog 'skip: JOBS_MONITOR_TOKEN 未設定'; exit 0 }
  $base = (Select-String -Path $envFile -Pattern '^JOBS_MONITOR_URL=' | Select-Object -First 1).Line -replace '^[^=]+=', ''
  if (-not $base) { $base = 'https://bfaith-portal.onrender.com' }
  $uri = $base.TrimEnd('/') + '/apps/jobs-monitor/ping/' + $Id + '?status=' + $Status
  Invoke-RestMethod -Uri $uri -Method Post -Headers @{ Authorization = 'Bearer ' + $token } -TimeoutSec 15 | Out-Null
  Write-PingLog 'sent'
} catch {
  # 何があっても本体を巻き込まない
  Write-PingLog ('error: ' + $_.Exception.Message)
}
exit 0
