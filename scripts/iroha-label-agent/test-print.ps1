#Requires -Version 5.1
<#
  Hands-on test of the b-PAC printing path for the Iroha storage-box label (Brother QL-800).

  No server, no token, no scheduled task, nothing copied to C:\ - this only proves that THIS
  PC can turn a template + real product data into a label on the Brother QL-800, and it
  answers the "verify on the machine" items of README.md while somebody stands at the
  printer:
    1. is b-PAC installed? (re-launches itself under the other PowerShell bitness when the
       component was installed for that one only)
    2. is the printer there and online, and does the QL-800 driver have the registered
       62 x 67.2mm paper format? b-PAC ignores the size stored in the template and uses a
       driver paper format (measured 2026-09-05), so a missing format stops the test here
       with the registration steps.
    3. are the two template copies there?
    4. take a row of the real barcode master CSV (falls back to sample data)
    5. export a BMP preview, report its size, open it - compare with a hand-printed label
    6. ask, then print ONE label (b-PAC renders, GDI prints - see print-label.ps1) and watch
       the spooler: does the document name carry our marker (hakolabel-test-*)? did it leave?
    7. ask, then print THREE (three pages in one spool job) - cut between labels?

  Run it from 3_test.bat (double click). Screen text lives in messages.json (UTF-8) because
  this file must stay ASCII (Windows PowerShell 5.1 mis-reads non-ASCII .ps1 files).
  Output: RESULT_test.txt next to this script - send it back when something looks wrong.
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [Text.Encoding]::UTF8
$ScriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }

$msgPath = Join-Path $ScriptDir 'messages.json'
if (-not (Test-Path $msgPath)) { throw "messages.json not found next to this script: $msgPath" }
$M = Get-Content $msgPath -Raw -Encoding UTF8 | ConvertFrom-Json

# b-PAC helpers: Test-BpacAvailable / Get-BpacMediaInfo / Invoke-BpacPrint / Get-PrintConfig / Get-LabelArgs / ...
. (Join-Path $ScriptDir 'print-label.ps1') -Library

# --- 1. b-PAC (before any output, so a re-launch does not print things twice) --------
$bp = Test-BpacAvailable
if (-not $bp.Ok -and -not $env:IROHA_TEST_RELAUNCHED) {
  # The component may be installed for the other bitness only. Try once from there.
  # From a 64-bit process SysWOW64 is the 32-bit PowerShell; from a 32-bit process
  # Sysnative is the real System32 (64-bit).
  $alt = if ([IntPtr]::Size -eq 8) { Join-Path $env:SystemRoot 'SysWOW64\WindowsPowerShell\v1.0\powershell.exe' }
         else                      { Join-Path $env:SystemRoot 'Sysnative\WindowsPowerShell\v1.0\powershell.exe' }
  if (Test-Path $alt) {
    $env:IROHA_TEST_RELAUNCHED = '1'
    & $alt -NoProfile -ExecutionPolicy Bypass -File $PSCommandPath
    exit $LASTEXITCODE
  }
}

$out = Join-Path $ScriptDir 'RESULT_test.txt'
if (Test-Path $out) { Remove-Item $out -Force }
function W([string]$s) { Add-Content -Path $out -Value $s -Encoding UTF8; Write-Host $s }
function WL($lines) { foreach ($l in @($lines)) { W ([string]$l) } }
function Stop-Test([int]$code) { W ''; W $M.stopped; exit $code }

W ($M.title -f $env:COMPUTERNAME, (Get-Date -Format 'yyyy-MM-dd HH:mm'))
W ''
if (-not $bp.Ok) {
  W $M.bpacMissingTitle
  WL $M.bpacMissing
  W ("  ({0})" -f $bp.Detail)
  Stop-Test 2
}
W ($M.bpacOk -f $bp.Bits)

# --- 2. printer present + online, and the registered paper format for this label ----------
#     b-PAC ignores the size stored in the .lbx and uses one of the DRIVER's paper formats,
#     so the 62 x 67.2mm format must be registered once in the QL-700 driver (see messages).
$cfg = Get-PrintConfig (Join-Path $ScriptDir 'config.example.json')
$printer = [string]$cfg.printerName
$label   = Get-LabelArgs $cfg

$printers = @(Get-Printer -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Name)
if ($printers -notcontains $printer) { W ($M.printerMissing -f $printer, ($printers -join ' / ')); Stop-Test 1 }
$info = $null
try { $info = Get-BpacMediaInfo -PrinterName $printer -FormatName $label.LabelMediaName } catch { W ($M.printerOffline -f $printer); W ("  ({0})" -f $_.Exception.Message); Stop-Test 1 }
if (-not $info.Online) { W ($M.printerOffline -f $printer); Stop-Test 1 }
# MEASURED 2026-09-06 (QL-800): a registered long tape format does NOT show up in
# GetSupportedMediaNames, so a miss here is only a warning. Step 5 renders the label and
# measures it - that is what actually decides whether the paper format is usable.
if (-not $info.HasFormat) {
  W ($M.formatNotListed -f $label.LabelMediaName)
} else {
  W ($M.formatOk -f $label.LabelMediaName, $info.ReportedName)
}

# --- 3. templates (used straight from this folder - the test runs as the signed-in user) --
foreach ($t in @('jan', 'fnsku')) {
  $name = if ($t -eq 'jan') { 'hakolabel_auto_JAN.lbx' } else { 'hakolabel_auto_FNSKU.lbx' }
  $p = Join-Path $ScriptDir "templates\$name"
  if (-not (Test-Path $p)) { W ($M.templateMissing -f $p); Stop-Test 1 }
  $cfg.templates.$t = $p
}
W $M.templatesOk

# --- 4. sample data: a row of the real barcode master CSV (product id, name, search name, barcode, expiry flag 01/02) ---
function Get-SampleRow {
  param([string] $CsvPath)
  if (-not $CsvPath -or -not (Test-Path $CsvPath)) { return $null }
  try {
    Add-Type -AssemblyName Microsoft.VisualBasic
    # Shift_JIS, RFC4180 quotes - same file the hand-operated template merges from
    $p = New-Object Microsoft.VisualBasic.FileIO.TextFieldParser($CsvPath, [Text.Encoding]::GetEncoding(932))
    try {
      $p.TextFieldType = [Microsoft.VisualBasic.FileIO.FieldType]::Delimited
      $p.SetDelimiters(',')
      $p.HasFieldsEnclosedInQuotes = $true
      $header = $null
      while (-not $p.EndOfData) {
        $f = $p.ReadFields()
        if (-not $header) { $header = $f; continue }
        if ($f.Length -lt 5) { continue }
        $bc = ([string]$f[3]).Trim()
        if (-not $bc) { continue }
        $type = $(if ($bc -match '^[0-9]+$') { 'jan' } elseif ($bc -match '^[A-Za-z0-9]+$') { 'fnsku' } else { '' })
        if (-not $type) { continue }
        # expiry flag: 01 = no expiry management, 02 = managed (measured on the master: 4823 / 239)
        $flag = ([string]$f[4]).Trim()
        return @{
          code    = ([string]$f[0]).Trim()
          name    = [string]$f[1]
          barcode = $bc
          type    = $type
          packQty = [string]$M.samplePackQty
          expiry  = $(if ($flag -eq '02') { [string]$M.sampleExpiry } else { '' })
        }
      }
    } finally { $p.Close(); $p.Dispose() }
  } catch { return $null }
  return $null
}

$paths = Get-Content (Join-Path $ScriptDir 'templates.json') -Raw -Encoding UTF8 | ConvertFrom-Json
$row = Get-SampleRow ([string]$paths.sampleCsv)
if ($row) { W ($M.sampleFromCsv -f $row.code) } else { $row = $M.sample; W $M.sampleFallback }
W ("      {0}" -f $row.name)
W ("      {0} : {1} ({2})   {3} : {4}   {5} : {6}" -f $M.wordBarcode, $row.barcode, $row.type.ToUpper(), $M.wordPackQty, $row.packQty, $M.wordExpiry, $row.expiry)

# --- 5. preview (no paper) -----------------------------------------------------------------
$fields  = New-LabelFields $cfg $row.type $row.barcode $row.name $row.packQty ([string]$row.expiry)
$tplPath = Resolve-TemplatePath $cfg $row.type
$bmp = Join-Path $env:TEMP 'hakolabel_preview.bmp'
$r = $null
try {
  $r = Invoke-BpacPrint -TemplatePath $tplPath -Fields $fields -PrinterName $printer -ExportBmp $bmp @label
} catch {
  W ($M.previewFailed -f $_.Exception.Message)
  # The paper format is the usual cause and step 2 only warns now, so show the steps here.
  WL ($M.formatMissing | ForEach-Object { $_ -f $label.LabelMediaName, (($info.Formats | Select-Object -First 14) -join ' / ') })
  Stop-Test 1
}
$wmm = 0; $hmm = 0
try {
  Add-Type -AssemblyName System.Drawing
  $img = [System.Drawing.Image]::FromFile($bmp)
  $wmm = [Math]::Round($img.Width / 300 * 25.4, 1); $hmm = [Math]::Round($img.Height / 300 * 25.4, 1)
  $img.Dispose()
} catch { }
# The image is the label as printed: long side = tape length (67.2), short side = tape width (62).
# Only the tape width has to match - Invoke-BpacRender already worked out which side that is.
# The other side is the label length, and on a continuous roll b-PAC sets it from the content
# (QL-800), so it can be longer OR shorter than the design: printing always happens on a
# LabelLengthMm page, which clips a long canvas and leaves blank tape after a short one.
$long = $r.LengthMm; $short = $r.WidthMm
if ([Math]::Abs($short - $label.LabelWidthMm) -gt $label.ToleranceMm) {
  W ($M.previewWrongSize -f $wmm, $hmm, $label.LabelLengthMm, $label.LabelWidthMm, $label.LabelMediaName)
  try { Start-Process $bmp } catch { }
  Stop-Test 1
}
W ($M.previewOk -f $r.MediaName, $r.WidthMm, $r.LengthMm, $wmm, $hmm, $bmp)
if ([Math]::Abs($long - $label.LabelLengthMm) -gt $label.ToleranceMm) {
  WL ($M.previewLonger | ForEach-Object { $_ -f $long, $label.LabelLengthMm, $r.MediaName })
}
try { Start-Process $bmp } catch { }
W ''
WL $M.previewCheck

# --- 6. print one, watch the spooler ------------------------------------------------------
function Invoke-TestPrint([int] $Copies) {
  $docName = 'hakolabel-test-' + (Get-Date -Format 'HHmmss')
  $before = @(Get-PrintJob -PrinterName $printer -ErrorAction SilentlyContinue | ForEach-Object { [int]$_.Id })
  W ($M.printing -f $Copies, $printer, $docName)
  try {
    [void](Invoke-BpacPrint -TemplatePath $tplPath -Fields $fields -PrinterName $printer `
      -Copies $Copies -DocName $docName -RenderDir $env:TEMP @label)
  } catch { W ($M.printFailed -f $_.Exception.Message); return $false }

  # Which spool jobs appeared, what were they called, did they leave? This is exactly what
  # agent.ps1 relies on (README "verify on the machine" #2 / #3).
  $seen = @{}
  $deadline = (Get-Date).AddSeconds(8)
  while ((Get-Date) -lt $deadline) {
    foreach ($j in @(Get-PrintJob -PrinterName $printer -ErrorAction SilentlyContinue)) {
      if ($before -notcontains [int]$j.Id) { $seen[[int]$j.Id] = [string]$j.DocumentName }
    }
    Start-Sleep -Milliseconds 200
  }
  $names = @($seen.Values | Select-Object -Unique)
  W ($M.spoolSeen -f $seen.Count, ($names -join ' / '))
  $mine = @($seen.Keys | Where-Object { $seen[$_] -like "*$docName*" })
  if ($mine.Count -ge 1) { W ($M.spoolNameOk -f $mine.Count) } else { W $M.spoolNameMissing }
  $left = @(Get-PrintJob -PrinterName $printer -ErrorAction SilentlyContinue | Where-Object { $mine -contains [int]$_.Id })
  if ($left.Count -eq 0) { W $M.spoolGone } else { W ($M.spoolStuck -f (($left | ForEach-Object { [string]$_.JobStatus }) -join ' / ')) }
  return $true
}

W ''
$ans = Read-Host $M.askPrint
if ($ans -ne 'y' -and $ans -ne 'Y') { W $M.skippedPrint; W ''; W ($M.resultFile -f $out); exit 0 }
if (-not (Invoke-TestPrint 1)) { Stop-Test 1 }
W ''
WL $M.afterFirst

# --- 7. three in a row: one spool job or three? cut between labels? ------------------------
W ''
$ans2 = Read-Host $M.askThree
if ($ans2 -eq 'y' -or $ans2 -eq 'Y') {
  [void](Invoke-TestPrint 3)
  W ''
  WL $M.afterThree
}

W ''
W $M.done
W ($M.resultFile -f $out)
