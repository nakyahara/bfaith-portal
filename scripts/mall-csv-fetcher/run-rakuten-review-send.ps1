# run-rakuten-review-send.ps1 - Rakuten review follow/coupon mail sender (Task Scheduler entry, PR-C5)
# Runs daily at 12:05 JST (vendor "rakuraku coupon" sent at 12:00; review posting peaks right after).
#   1. ensure-monthly : issue this month's (and from the 25th, next month's) 5% coupon if missing (idempotent).
#                       Failure does NOT stop the job: follow mails do not need the coupon, and the sender
#                       gates coupon mails on the registry anyway (no_monthly_coupon = fail-closed).
#   2. plan           : promote actions scheduled for today's 12:00 to ready (daily-sync at 07:00 cannot).
#                       Failure DOES stop the job: send must see today's reviews / suppressions / ownership.
#   3. send           : at-most-once SMTP send of all ready+eligible actions (0 before cutover)
# Exit code = worst (max) step code. jobs-monitor ping at the end (ok / fail). Keep this file ASCII only.
$ErrorActionPreference = 'Continue'
Set-Location 'C:\Users\bfaith\bfaith-portal'
$env:DATA_DIR = 'C:\Users\bfaith\bfaith-portal\data'
$worst = 0

node apps\warehouse\manage-rakuten-review-coupon.js ensure-monthly --live
if ($LASTEXITCODE -gt $worst) { $worst = $LASTEXITCODE }

node apps\warehouse\plan-rakuten-review-campaigns.js plan
$planExit = $LASTEXITCODE
if ($planExit -gt $worst) { $worst = $planExit }

if ($planExit -eq 0) {
  node apps\warehouse\send-rakuten-review-mails.js send --limit 1500 --notify
  if ($LASTEXITCODE -gt $worst) { $worst = $LASTEXITCODE }
} else {
  Write-Output "[run] plan failed (exit $planExit) - send skipped (stale state must not be sent)"
}

$status = 'ok'
if ($worst -ne 0) { $status = 'fail' }
powershell -NoProfile -ExecutionPolicy Bypass -File 'C:\Users\bfaith\bfaith-portal\scripts\jobs-monitor\ping.ps1' -Id rakuten-review-mail-send -Status $status
exit $worst
