# run-rakuten-point-campaign.ps1 - Rakuten per-item point campaign monthly setter (Task Scheduler entry)
# Daily run. On day 1 (JST) writes this month's point campaign (12:00 - end of month 23:59:59)
# for items listed in the Google Sheet; days 2-3 are silent recovery. Other days: no-op, exit 0.
# Always pings jobs-monitor (ok/fail) so the dead-man watch sees liveness even on no-op days.
Set-Location 'C:\Users\bfaith\bfaith-portal'
node apps\warehouse\manage-rakuten-point-campaign.js apply --live
$code = $LASTEXITCODE
$status = 'fail'
if ($code -eq 0) { $status = 'ok' }
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\jobs-monitor\ping.ps1 -Id rakuten-point-campaign -Status $status
exit $code
