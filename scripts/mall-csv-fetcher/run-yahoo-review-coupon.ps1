# run-yahoo-review-coupon.ps1 - monthly 5% review coupon for Yahoo (Task Scheduler entry, PR-Y-C5)
#
# Runs daily at 10:10 JST as **bfaith (Interactive)**, NOT as SYSTEM.
#   Playwright reuses the persistent profile scripts\mall-csv-fetcher\.profile-yahoo, and Chromium
#   encrypts its cookies with a key tied to the Windows user (DPAPI). A SYSTEM task cannot decrypt
#   them, decides it is logged out, and REWRITES the cookie store - which destroys the store creator
#   session and needs a 2FA re-login on the machine itself (2026-08-28 incident).
#   Keep every Playwright step out of the SYSTEM job (YahooReviewMailSend at 12:20 does plan+send only).
#
# 10:10 also keeps the profile to ourselves: MallCsvFetchAll uses it at 05:30 and
# YahooCouponRotate at 09:30. Two Chromium processes on one profile directory cannot both open it.
#
# The coupon must exist before the 12:20 send job, but there is slack: Yahoo coupons run from the 1st
# to the end of the NEXT month, so two months overlap and the sender falls back to any coupon valid now.
# Skipped entirely while the cutover stage is 'shadow' (the vendor still owns every order).
# Keep this file ASCII only.
$ErrorActionPreference = 'Continue'
# Hard guard: refuse to run as SYSTEM even if someone re-registers this task with the wrong principal.
# (lib-yahoo-login.mjs refuses too - that one also covers manual runs - but stop before we get there.)
$me = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
if ($me -eq 'NT AUTHORITY\SYSTEM') {
  Write-Output "[run] refusing to run as $me - opening the Yahoo profile as SYSTEM destroys the login session"
  powershell -NoProfile -ExecutionPolicy Bypass -File 'C:\Users\bfaith\bfaith-portal\scripts\jobs-monitor\ping.ps1' -Id yahoo-review-coupon-issue -Status fail
  exit 2
}
Set-Location 'C:\Users\bfaith\bfaith-portal'
$env:DATA_DIR = 'C:\Users\bfaith\bfaith-portal\data'
$env:WAREHOUSE_DATA_DIR = 'C:\Users\bfaith\bfaith-portal\data'
$env:PLAYWRIGHT_BROWSERS_PATH = 'C:\Users\bfaith\AppData\Local\ms-playwright'
$env:HEADLESS = '1'
$rc = 0

$stage = (node apps\warehouse\plan-rakuten-review-campaigns.js cutover-stage --mall yahoo | Select-Object -Last 1)
if ($LASTEXITCODE -ne 0) {
  Write-Output "[run] cannot read cutover stage (exit $LASTEXITCODE) - coupon step skipped"
  $rc = $LASTEXITCODE
} elseif ($stage -eq 'shadow') {
  Write-Output "[run] cutover stage is shadow - nothing to send yet, no coupon needed"
} else {
  $month = Get-Date -Format 'yyyy-MM'
  node scripts\mall-csv-fetcher\yahoo-review-coupon-issue.mjs --month $month --live
  $rc = $LASTEXITCODE
}

$status = 'ok'
if ($rc -ne 0) { $status = 'fail' }
powershell -NoProfile -ExecutionPolicy Bypass -File 'C:\Users\bfaith\bfaith-portal\scripts\jobs-monitor\ping.ps1' -Id yahoo-review-coupon-issue -Status $status
exit $rc
