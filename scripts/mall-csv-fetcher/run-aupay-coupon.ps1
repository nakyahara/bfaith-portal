# run-aupay-coupon.ps1 - au PAY store coupon rotation (Task Scheduler entry)
# Daily run. Creates next month's 4 coupons only on/after ACOUPON_CREATE_DAY (default 25),
# and only if that month's coupons do not exist yet. Other days it does nothing and stays silent.
$env:HEADLESS = '1'
$env:PLAYWRIGHT_BROWSERS_PATH = 'C:\Users\bfaith\AppData\Local\ms-playwright'
Set-Location 'C:\Users\bfaith\bfaith-portal'
node scripts\mall-csv-fetcher\aupay-coupon-rotate.mjs --live
exit $LASTEXITCODE
