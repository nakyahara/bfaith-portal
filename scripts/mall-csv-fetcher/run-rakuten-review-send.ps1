# run-rakuten-review-send.ps1 - Rakuten review follow/coupon mail sender (Task Scheduler entry, PR-C5)
# Runs daily at 12:05 JST (vendor "rakuraku coupon" sent at 12:00; review posting peaks right after).
#   1. ensure-monthly : issue this month's (and from the 25th, next month's) 5% coupon if missing (idempotent)
#   2. plan           : promote actions scheduled for today's 12:00 to ready (daily-sync at 07:00 cannot)
#   3. send           : at-most-once SMTP send of all ready+eligible actions (0 before cutover)
# Exit code = worst step. jobs-monitor ping at the end (ok / fail). Keep this file ASCII only.
$ErrorActionPreference = 'Continue'
Set-Location 'C:\Users\bfaith\bfaith-portal'
$env:DATA_DIR = 'C:\Users\bfaith\bfaith-portal\data'
$worst = 0

node apps\warehouse\manage-rakuten-review-coupon.js ensure-monthly --live
if ($LASTEXITCODE -ne 0) { $worst = $LASTEXITCODE }

node apps\warehouse\plan-rakuten-review-campaigns.js plan
if ($LASTEXITCODE -ne 0) { $worst = $LASTEXITCODE }

node apps\warehouse\send-rakuten-review-mails.js send --limit 1500 --notify
if ($LASTEXITCODE -ne 0) { $worst = $LASTEXITCODE }

$status = 'ok'
if ($worst -ne 0) { $status = 'fail' }
powershell -NoProfile -ExecutionPolicy Bypass -File 'C:\Users\bfaith\bfaith-portal\scripts\jobs-monitor\ping.ps1' -Id rakuten-review-mail-send -Status $status
exit $worst
