# run-yahoo-review-send.ps1 - Yahoo review follow/coupon mail sender (Task Scheduler entry, PR-Y-C5)
# Runs daily at 12:20 JST. Deliberately 15 min after RakutenReviewMailSend (12:05):
#   warehouse.db must not take concurrent writers, and vendor "rakuraku follow" also sends around 12:45.
#   1. coupon : issue this month's 5% coupon if the ledger has none (idempotent; exits before launching
#               the browser when status is already 'issued', so the daily cost is one SQLite read).
#               Skipped entirely while the cutover stage is 'shadow' - the vendor still owns every order,
#               so a coupon issued now would sit unused in the store.
#               Failure does NOT stop the job: follow mails need no coupon, and the sender gates coupon
#               mails on the ledger anyway (no_monthly_coupon = fail-closed). Yahoo coupons run from the
#               1st to the end of the NEXT month, so two months overlap and the sender falls back to any
#               coupon that is valid right now - one failed day at the start of a month is not urgent.
#   2. plan   : promote actions scheduled for today's 12:00 to ready (daily-sync at 07:00 cannot).
#               Failure DOES stop the job: send must see today's reviews / suppressions / ownership.
#   3. send   : at-most-once Gmail send of all ready+eligible actions (0 before cutover).
#               Refuses to send at all unless the From address was verified (verify-from, 90 days).
# Exit code = worst (max) step code. jobs-monitor ping at the end (ok / fail). Keep this file ASCII only.
$ErrorActionPreference = 'Continue'
Set-Location 'C:\Users\bfaith\bfaith-portal'
$env:DATA_DIR = 'C:\Users\bfaith\bfaith-portal\data'
$env:WAREHOUSE_DATA_DIR = 'C:\Users\bfaith\bfaith-portal\data'
# Playwright must be told where the browsers live when the task runs as SYSTEM
$env:PLAYWRIGHT_BROWSERS_PATH = 'C:\Users\bfaith\AppData\Local\ms-playwright'
$env:HEADLESS = '1'
$worst = 0

# Do not create coupons while every order still belongs to the vendor (stage = shadow):
# there is nothing to send, so issuing one would just leave an unused coupon in the store every month.
$stage = (node apps\warehouse\plan-rakuten-review-campaigns.js cutover-stage --mall yahoo | Select-Object -Last 1)
if ($LASTEXITCODE -ne 0) {
  Write-Output "[run] cannot read cutover stage (exit $LASTEXITCODE) - coupon step skipped"
  if ($LASTEXITCODE -gt $worst) { $worst = $LASTEXITCODE }
} elseif ($stage -eq 'shadow') {
  Write-Output "[run] cutover stage is shadow - coupon step skipped (nothing to send yet)"
} else {
  $month = Get-Date -Format 'yyyy-MM'
  node scripts\mall-csv-fetcher\yahoo-review-coupon-issue.mjs --month $month --live
  if ($LASTEXITCODE -ne 0) {
    Write-Output "[run] coupon step failed (exit $LASTEXITCODE) - continuing (follow mails do not need it)"
    if ($LASTEXITCODE -gt $worst) { $worst = $LASTEXITCODE }
  }
}

node apps\warehouse\plan-rakuten-review-campaigns.js plan --mall yahoo
$planExit = $LASTEXITCODE
if ($planExit -gt $worst) { $worst = $planExit }

if ($planExit -eq 0) {
  node apps\warehouse\send-yahoo-review-mails.js send --limit 1500 --notify
  if ($LASTEXITCODE -gt $worst) { $worst = $LASTEXITCODE }
} else {
  Write-Output "[run] plan failed (exit $planExit) - send skipped (stale state must not be sent)"
}

$status = 'ok'
if ($worst -ne 0) { $status = 'fail' }
powershell -NoProfile -ExecutionPolicy Bypass -File 'C:\Users\bfaith\bfaith-portal\scripts\jobs-monitor\ping.ps1' -Id yahoo-review-mail-send -Status $status
exit $worst
