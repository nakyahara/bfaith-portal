# run-yahoo-review-send.ps1 - Yahoo review follow/coupon mail sender (Task Scheduler entry, PR-Y-C5)
# Runs daily at 12:20 JST. Deliberately 15 min after RakutenReviewMailSend (12:05):
#   warehouse.db must not take concurrent writers, and vendor "rakuraku follow" also sends around 12:45.
#   1. plan   : promote actions scheduled for today's 12:00 to ready (daily-sync at 07:00 cannot).
#               Failure DOES stop the job: send must see today's reviews / suppressions / ownership.
#   2. send   : at-most-once Gmail send of all ready+eligible actions (0 before cutover).
#               Refuses to send at all unless the From address was verified (verify-from, 90 days).
#
# This task runs as SYSTEM, so it must never touch Playwright: opening the persistent Yahoo profile
# as SYSTEM destroys the store creator session (cookies are encrypted per Windows user - 2026-08-28
# incident). The monthly coupon is issued by run-yahoo-review-coupon.ps1, which runs as bfaith at 10:10.
# A missing coupon only blocks coupon mails (no_monthly_coupon = fail-closed); follow mails still go out.
#
# Exit code = worst (max) step code. jobs-monitor ping at the end (ok / fail). Keep this file ASCII only.
$ErrorActionPreference = 'Continue'
Set-Location 'C:\Users\bfaith\bfaith-portal'
$env:DATA_DIR = 'C:\Users\bfaith\bfaith-portal\data'
$worst = 0

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
