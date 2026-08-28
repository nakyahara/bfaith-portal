#Requires -Version 5.1
<#
  Slip print agent (shipping PC) - pulls print jobs from the packing server and prints them.

  Design notes (why it is written this way):

  * PULL ONLY. The server never connects to this PC, so no fixed IP and no inbound port.

  * NEVER PRINT THE SAME SLIP TWICE. The server already stops re-distributing a job once the
    PDF has been handed over. This agent adds the second half of that guarantee with a
    persistent ledger on disk:

        leased -> downloaded -> submitting -> submitted -> done

    A job that already reached "downloaded" is never downloaded or printed again, even after
    a crash or a restart. If we crashed while "submitting" we cannot know whether paper came
    out, so we report a failure to the server and let a human check the physical slip.
    Missing one slip is recoverable; printing someone else's slip twice is not.

  * THE SERVER DECIDES THE PRINTER. printerName comes from the job payload and is used
    verbatim. There is no local printer setting and no fallback to the default printer:
    the whole point is that a mis-set PC must not print a slip on the wrong device.

  * ASCII ONLY in this file. Task Scheduler + Windows PowerShell 5.1 mangles non-ASCII .ps1
    content (this bit us twice already). Japanese printer names still work fine because they
    arrive over HTTP as UTF-8 JSON and are passed through as native strings.

  Japanese documentation: README.md next to this file.
  Requirement doc: AI_reference "sekkei" folder, "okurijo jidou insatsu youken teigi 20260827"
  (see README.md for the exact Japanese path - this file must stay ASCII)
#>
[CmdletBinding()]
param(
  [string] $ConfigPath = (Join-Path $PSScriptRoot 'config.json'),
  # Run a single poll and exit. Used by the smoke test and by install.ps1 verification.
  [switch] $Once
)

$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [Text.Encoding]::UTF8

# ---------------------------------------------------------------- configuration

if (-not (Test-Path $ConfigPath)) {
  throw "Config not found: $ConfigPath (copy config.example.json and fill in the token)"
}
$cfg = Get-Content $ConfigPath -Raw -Encoding UTF8 | ConvertFrom-Json
foreach ($k in @('baseUrl', 'token')) {
  if (-not $cfg.$k) { throw "Config is missing '$k': $ConfigPath" }
}
$BaseUrl      = $cfg.baseUrl.TrimEnd('/')
$Token        = $cfg.token
$PollSec      = if ($cfg.pollSec)   { [int]$cfg.pollSec }   else { 4 }
$HeartbeatSec = if ($cfg.heartbeatSec) { [int]$cfg.heartbeatSec } else { 45 }
$Sumatra      = if ($cfg.sumatraPath) { $cfg.sumatraPath } else { Join-Path $PSScriptRoot 'SumatraPDF.exe' }
$WorkDir      = if ($cfg.workDir)   { $cfg.workDir }   else { Join-Path $PSScriptRoot 'work' }
$LedgerDir    = Join-Path $WorkDir 'ledger'
$PdfDir       = Join-Path $WorkDir 'pdf'
$LogPath      = Join-Path $WorkDir 'agent.log'
# The whole point of this agent is unattended printing, so a backoff that grows without
# bound would silently stop the line. Cap it so we always come back.
$MaxBackoffSec = 60

foreach ($d in @($WorkDir, $LedgerDir, $PdfDir)) {
  if (-not (Test-Path $d)) { New-Item -ItemType Directory -Path $d -Force | Out-Null }
}


function Write-Log {
  param([string] $Level, [string] $Message)
  $line = '{0} [{1}] {2}' -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Level, $Message
  Write-Host $line
  try { Add-Content -Path $LogPath -Value $line -Encoding UTF8 } catch { }
  # Keep the log from growing forever on a machine nobody logs into.
  try {
    if ((Get-Item $LogPath -ErrorAction SilentlyContinue).Length -gt 5MB) {
      Move-Item $LogPath "$LogPath.1" -Force
    }
  } catch { }
}

# Only one agent per machine. Two of them would hand jobs to the same printer at the same
# time and confuse the "which spool job was mine" bookkeeping. The scheduled task itself is
# set to IgnoreNew, but that does not stop someone running this by hand while it is resident.
$script:Mutex = New-Object System.Threading.Mutex($false, 'Global\BFaith-SlipPrintAgent')
if (-not $script:Mutex.WaitOne(0)) {
  Write-Log 'INFO' 'another slip print agent is already running on this PC - exiting'
  exit 0
}

# ---------------------------------------------------------------- HTTP

function Invoke-Api {
  param(
    [string] $Method,
    [string] $Path,
    $Body = $null,
    [string] $OutFile = $null
  )
  # NOTE: do not call this hashtable $args - that is an automatic variable in PowerShell
  # and overwriting it breaks the splat below (the request silently never goes out).
  $params = @{
    Method          = $Method
    Uri             = "$BaseUrl$Path"
    Headers         = @{ Authorization = "Bearer $Token" }
    TimeoutSec      = 20
    ErrorAction     = 'Stop'
    UseBasicParsing = $true
  }
  if ($Body) {
    $params.Body        = ($Body | ConvertTo-Json -Compress)
    $params.ContentType = 'application/json; charset=utf-8'
  }
  if ($OutFile) { $params.OutFile = $OutFile }
  # PS 5.1 throws on any non-2xx, so 204/401/409 all land in catch. The caller needs the
  # status code to tell "nothing to print" from "something is wrong".
  try {
    $res = Invoke-WebRequest @params
    return @{ Status = [int]$res.StatusCode; Content = $res.Content }
  } catch [System.Net.WebException] {
    $resp = $_.Exception.Response
    if ($resp) {
      $code = [int]$resp.StatusCode
      $text = ''
      try {
        $sr = New-Object System.IO.StreamReader($resp.GetResponseStream())
        $text = $sr.ReadToEnd()
      } catch { }
      return @{ Status = $code; Content = $text }
    }
    throw
  }
}

# ---------------------------------------------------------------- ledger

function Get-LedgerPath { param([int] $JobId) Join-Path $LedgerDir "$JobId.json" }

<#
  Returns the ledger entry, or $null when there is none.
  A ledger file that exists but cannot be read is NOT the same as "no ledger": it may be the
  record of a slip we already printed. Return a poisoned entry so the caller refuses to print.
#>
function Get-Ledger {
  param([int] $JobId)
  $p = Get-LedgerPath $JobId
  if (-not (Test-Path $p)) { return $null }
  try {
    $obj = Get-Content $p -Raw -Encoding UTF8 | ConvertFrom-Json
    if (-not $obj -or -not $obj.stage) { throw 'ledger has no stage' }
    if ($obj.jobId -and [int]$obj.jobId -ne $JobId) { throw 'ledger jobId does not match' }
    return $obj
  } catch {
    Write-Log 'ERROR' "ledger for job $JobId is unreadable ($($_.Exception.Message)) - refusing to print it"
    return [pscustomobject]@{ jobId = $JobId; stage = 'unreadable' }
  }
}

function Set-Ledger {
  param([int] $JobId, [hashtable] $Fields)
  $cur = Get-Ledger $JobId
  $obj = @{ jobId = $JobId }
  if ($cur) { foreach ($p in $cur.PSObject.Properties) { $obj[$p.Name] = $p.Value } }
  foreach ($k in $Fields.Keys) { $obj[$k] = $Fields[$k] }
  $obj['updatedAt'] = (Get-Date -Format 'o')
  $tmp = (Get-LedgerPath $JobId) + '.tmp'
  # Write to a temp file first: a half-written ledger entry would lose the "already printed"
  # fact, which is exactly what must never be lost.
  ($obj | ConvertTo-Json -Depth 5) | Set-Content -Path $tmp -Encoding UTF8
  Move-Item $tmp (Get-LedgerPath $JobId) -Force
}

# ---------------------------------------------------------------- printing

function Get-SpoolJobIds {
  param([string] $PrinterName)
  try { return @(Get-PrintJob -PrinterName $PrinterName -ErrorAction Stop | ForEach-Object { [int]$_.Id }) }
  catch { return $null }   # $null = could not look (different from "queue is empty")
}

<#
  Pick OUR spool job out of the ones that appeared since we started printing.

  Someone printing something else on the same printer at the same moment would otherwise be
  followed instead of us, and when THEIR job finished we would announce our slip as printed.
  The document name carries our unique file name (slip-<jobId>.pdf), so use that first.
  When it cannot be decided, say so - the caller turns that into "please check the slip".
#>
function Select-OwnSpoolJob {
  param($NewJobs, [string] $DocHint)
  # ONLY a document-name match counts. "there is exactly one new job" looks tempting, but
  # our own slip can pass through too fast to observe and be replaced in the queue by
  # somebody else's job a moment later - we would then follow theirs and call OUR slip
  # printed when THEIRS finished. Guessing is how a missing slip goes unnoticed.
  $named = @($NewJobs | Where-Object { [string]$_.DocumentName -like "*$DocHint*" })
  if ($named.Count -eq 1) { return @{ JobId = [int]$named[0].Id } }
  if ($named.Count -gt 1) { return @{ Ambiguous = "several spool jobs are named like $DocHint" } }
  $names = (@($NewJobs | ForEach-Object { [string]$_.DocumentName }) -join ' / ')
  return @{ Ambiguous = "no spool job named like $DocHint (queue shows: $names)" }
}

<#
  Wait for the job we just spooled and say what actually happened to it.

  Exit code 0 from the print program only means "handed to the spooler". If we reported
  "printed" on that alone, a USB cable pulled out, an empty paper roll or a spooler error
  would still be announced as a success and nobody would notice the slip never came out.

  Returns one of:
    printed   - the job left the queue with no error: paper came out
    error     - the spooler is reporting a problem with it (paper out, offline, blocked...)
    unknown   - we could not follow it (spooler not queryable, still stuck after the wait)

  $Before is the list of job ids that were already in that queue before we printed, so we
  only ever look at OUR job.
#>
function Wait-PrintResult {
  param([string] $PrinterName, $Before, [string] $DocHint, [int] $AppearSec = 5, [int] $FinishSec = 90)

  $mine = $null
  $deadline = (Get-Date).AddSeconds($AppearSec)
  while ((Get-Date) -lt $deadline -and -not $mine) {
    $all = $null
    try { $all = @(Get-PrintJob -PrinterName $PrinterName -ErrorAction Stop) } catch {
      return @{ Result = 'unknown'; Detail = 'the print queue could not be read' }
    }
    $new = @($all | Where-Object { $Before -notcontains [int]$_.Id })
    if ($new.Count -ge 1) {
      $pick = Select-OwnSpoolJob $new $DocHint
      if ($pick.Ambiguous) { return @{ Result = 'unknown'; Detail = $pick.Ambiguous } }
      $mine = $pick.JobId
    } else {
      Start-Sleep -Milliseconds 100   # a small label can pass through very quickly
    }
  }

  if (-not $mine) {
    # We never saw our job in the queue. It may have printed too fast to observe, or the
    # spooler may have refused it - we cannot tell the two apart, so we must not claim it
    # printed. "unknown" asks a person to look at the actual slip, which is safe: it never
    # tells anyone to print a second copy.
    return @{ Result = 'unknown'; Detail = 'the spool job was never seen in the queue'; JobId = $null }
  }

  $bad = @('Error', 'Offline', 'PaperOut', 'Blocked', 'UserIntervention', 'Paused', 'Deleted')
  $deadline = (Get-Date).AddSeconds($FinishSec)
  while ((Get-Date) -lt $deadline) {
    # "our job is no longer in the queue" (= it printed) and "we could not ask the spooler"
    # look identical if both end up as $null. They are not the same thing: a stopped spooler
    # or a permission error must never be reported as a successful print.
    $gone = $false
    $j = $null
    try {
      $all = @(Get-PrintJob -PrinterName $PrinterName -ErrorAction Stop)
      $j = $all | Where-Object { [int]$_.Id -eq $mine } | Select-Object -First 1
      if (-not $j) { $gone = $true }
    } catch {
      return @{ Result = 'unknown'; Detail = "the print queue stopped answering: $($_.Exception.Message)"; JobId = $mine }
    }
    if ($gone) { return @{ Result = 'printed'; Detail = 'left the queue'; JobId = $mine } }
    $status = [string]$j.JobStatus
    foreach ($b in $bad) {
      if ($status -like "*$b*") { return @{ Result = 'error'; Detail = "spooler says: $status"; JobId = $mine } }
    }
    Start-Sleep -Milliseconds 500
  }
  return @{ Result = 'unknown'; Detail = "still in the queue after ${FinishSec}s"; JobId = $mine }
}

<#
  Is there a printer on THIS PC with exactly this name?
  Never rely on the print program to fail cleanly on an unknown name - some versions fall
  back to the default printer, which is how a slip ends up on the wrong device.
  Checked before we write the "submitting" ledger stage, so a miss is a clean failure.
#>
function Test-PrinterExists {
  param([string] $PrinterName)
  try {
    $all = Get-Printer -ErrorAction Stop | Select-Object -ExpandProperty Name
  } catch {
    Write-Log 'WARN' "could not list printers: $($_.Exception.Message)"
    return $false
  }
  # Case-insensitive (that is how Windows treats printer names) but no trimming and no
  # partial match: " Nekopos" and "Nekopos 2" must not be accepted for "Nekopos".
  foreach ($n in $all) { if ($n -eq $PrinterName) { return $true } }
  return $false
}

function Invoke-Print {
  param([string] $PrinterName, [string] $FilePath)
  if (-not (Test-Path $Sumatra)) { throw "SumatraPDF not found: $Sumatra" }
  # -print-settings noscale is mandatory: a few percent of shrink makes the barcode
  # unreadable on a thermal label.
  #
  # MEASURED 2026-08-28: Start-Process -ArgumentList SPLITS a printer name that contains a
  # space, even when the arguments are passed as an array:
  #   -ArgumentList @('-print-to','Munbyn ITPP941(300DPI)',...)
  #     -> the exe receives [-print-to] [Munbyn] [ITPP941(300DPI)] ...
  # That would print to a printer that does not exist, or to the wrong one. The call
  # operator with a splatted array keeps each element as exactly one argument, which is
  # what we need for "Munbyn ITPP941(300DPI)" and for the Japanese names.
  $argv = @('-print-to', $PrinterName, '-print-settings', 'noscale', '-silent', $FilePath)
  & $Sumatra @argv 2>&1 | Out-Null
  return $LASTEXITCODE
}

# ---------------------------------------------------------------- job handling

<#
  Report a job that did not print.

  $Uncertain is the important flag.
    $false : we are sure no paper came out (bad printer name, download failed, ...).
             The server tells the office "print it by hand" - which is correct.
    $true  : we handed it to the spooler (or might have) and then lost track of it.
             The server must NOT say "print it by hand", because if paper did come out the
             office would print a second copy and it could end up on the wrong parcel.
             It says "check the actual slip" instead and does not reprint automatically.

  Only mark the ledger as finished when the server actually accepted the report (HTTP 200).
  Otherwise keep the current stage so we never re-print, and let the server time it out.
#>
function Report-NotPrinted {
  param([int] $JobId, [string] $Lease, [string] $Reason, [bool] $Uncertain = $false)
  $body = @{ lease = $Lease; ok = $false; error = $Reason }
  if ($Uncertain) { $body.uncertain = $true }
  try {
    $r = Invoke-Api -Method POST -Path "/print/$JobId/completed" -Body $body
    if ($r.Status -eq 200) {
      Set-Ledger $JobId @{ stage = 'done'; result = if ($Uncertain) { 'unknown' } else { 'failed' }; reason = $Reason }
      Write-Log 'WARN' "job $JobId reported as not printed ($Reason, uncertain=$Uncertain)"
    } else {
      Write-Log 'ERROR' "job $JobId report rejected: HTTP $($r.Status) $($r.Content)"
    }
  } catch {
    Write-Log 'ERROR' "job $JobId could not report: $($_.Exception.Message)"
  }
}

<#
  A job we have already taken past "downloaded" must never be downloaded or printed again.
  Decide what to tell the server instead.
#>
function Resolve-KnownJob {
  param($Job, $Ledger)
  $id    = [int]$Job.id
  $lease = [string]$Job.leaseToken
  $stage = [string]$Ledger.stage
  if ($stage -eq 'submitted') {
    # We spooled it. Close it out with the result we actually observed - never assume it
    # printed just because we got as far as this stage.
    $pr = [string]$Ledger.printResult
    if ($pr -and $pr -ne 'printed') {
      Report-NotPrinted $id $lease "printing did not complete ($($Ledger.printDetail))" $true
      return
    }
    if (-not $pr) {
      # Spooled, but we never managed to observe the outcome (crashed while watching).
      Report-NotPrinted $id $lease 'agent lost track after handing the slip to the printer' $true
      return
    }
    try {
      # Resend /submitted first. The original one may never have reached the server, in
      # which case the job is still 'dispatched' there and a completion report would be
      # rejected - and the slip we actually printed would end up announced as "unknown".
      # The server accepts a repeat of the same report as a success (replayed).
      $sub = Invoke-Api -Method POST -Path "/print/$id/submitted" `
        -Body @{ lease = $lease; spool_job_id = $Ledger.spoolJobId }
      if ($sub.Status -ne 200 -and $sub.Status -ne 409) {
        Write-Log 'ERROR' "job $id re-submitted report rejected: HTTP $($sub.Status)"
      }
      $r = Invoke-Api -Method POST -Path "/print/$id/completed" -Body @{ lease = $lease; ok = $true }
      if ($r.Status -eq 200) {
        Set-Ledger $id @{ stage = 'done'; result = 'completed' }
        Write-Log 'INFO' "job $id was already printed; reported completed"
      } else {
        Write-Log 'ERROR' "job $id completion report rejected: HTTP $($r.Status)"
      }
    } catch {
      Write-Log 'ERROR' "job $id completion report failed: $($_.Exception.Message)"
    }
  } elseif ($stage -eq 'submitting' -or $stage -eq 'unreadable') {
    # We died between "about to print" and "printed", or the ledger is damaged. Either way we
    # cannot claim nothing came out - do not let the office be told to print it by hand.
    Report-NotPrinted $id $lease 'agent lost track while printing - please check the actual slip' $true
  } else {
    # downloaded but never printed: safe to say nothing came out.
    Report-NotPrinted $id $lease 'agent restarted before printing' $false
  }
}

function Invoke-Job {
  param($Job)
  $id      = [int]$Job.id
  $lease   = [string]$Job.leaseToken
  $printer = [string]$Job.printerName
  $slip    = [string]$Job.neSlipNo

  if (-not $printer) { Report-NotPrinted $id $lease 'server did not say which printer to use' $false; return }

  $ledger = Get-Ledger $id
  if ($ledger -and $ledger.stage -and $ledger.stage -ne 'leased') {
    Write-Log 'WARN' "job $id is already at stage '$($ledger.stage)' - will not print it again"
    Resolve-KnownJob $Job $ledger
    return
  }
  Set-Ledger $id @{ stage = 'leased'; lease = $lease; printer = $printer; neSlipNo = $slip }

  # 1. download. The file name doubles as the marker we look for in the print queue,
  #    so keep it unique per job and ASCII.
  $docName = "slip-$id.pdf"
  $pdf = Join-Path $PdfDir $docName
  $res = Invoke-Api -Method GET -Path "/print/$id/pdf?lease=$lease" -OutFile $pdf
  if ($res.Status -ne 200) {
    Write-Log 'ERROR' "job $id pdf download failed: HTTP $($res.Status) $($res.Content)"
    # The server already marks the job failed for 404/409/410, so do not report again.
    Set-Ledger $id @{ stage = 'done'; result = 'failed'; reason = "pdf HTTP $($res.Status)" }
    return
  }
  if ($Job.pdfSha256) {
    $sha = (Get-FileHash -Path $pdf -Algorithm SHA256).Hash.ToLower()
    if ($sha -ne ([string]$Job.pdfSha256).ToLower()) {
      Report-NotPrinted $id $lease 'downloaded PDF does not match the expected checksum' $false
      return
    }
  }
  Set-Ledger $id @{ stage = 'downloaded'; pdf = $pdf }

  # 2. Make sure the printer this job is for actually exists on this PC, BEFORE we write
  #    "submitting". A missing name here is a clean "nothing came out" failure; if we only
  #    found out after handing it to the print program we could not say that any more.
  if (-not (Test-PrinterExists $printer)) {
    Report-NotPrinted $id $lease "this PC has no printer named '$printer'" $false
    return
  }

  # 3. print. Record the intent BEFORE handing anything to the spooler, so a crash here
  #    is recognisable as "we do not know whether paper came out".
  #    Snapshot the queue first so we can tell OUR spool job from anything already in it.
  $before = Get-SpoolJobIds $printer
  if ($null -eq $before) {
    # We could not read the queue. If we printed now we would have no way to tell our own
    # spool job from one that was already there, and could call someone else's job "ours".
    # Nothing has been handed to the spooler yet, so this is a clean "did not print".
    Report-NotPrinted $id $lease 'the print queue could not be read before printing' $false
    return
  }
  Set-Ledger $id @{ stage = 'submitting' }
  Write-Log 'INFO' "printing slip $slip (job $id) on '$printer'"
  $exit = -1
  try { $exit = Invoke-Print $printer $pdf } catch {
    # We are past "submitting": the print program may have reached the spooler before dying,
    # so we must not tell the office to print it by hand.
    Report-NotPrinted $id $lease "print command failed: $($_.Exception.Message)" $true
    return
  }
  if ($exit -ne 0) {
    Report-NotPrinted $id $lease "print command exited with $exit" $true
    return
  }

  # 4. tell the server it is in the spooler, then FOLLOW THE ACTUAL SPOOL JOB.
  #    Exit code 0 only proves the hand-off - a pulled USB cable or an empty roll would
  #    otherwise be announced as "printed" and nobody would notice the missing slip.
  $outcome = Wait-PrintResult $printer $before $docName
  $spool = $outcome.JobId
  # Record WHAT HAPPENED, not just "we got as far as submitting". If the report below fails
  # and we restart, the recovery path must resend this same result - otherwise a job that
  # did not print cleanly would be closed out as "printed".
  Set-Ledger $id @{
    stage = 'submitted'; spoolJobId = $spool
    printResult = [string]$outcome.Result; printDetail = [string]$outcome.Detail
  }
  $r = Invoke-Api -Method POST -Path "/print/$id/submitted" -Body @{ lease = $lease; spool_job_id = $spool }
  if ($r.Status -ne 200) {
    Write-Log 'ERROR' "job $id submitted-report rejected: HTTP $($r.Status) $($r.Content)"
    return   # leave the ledger at 'submitted' - we will close it out on the next poll
  }

  if ($outcome.Result -ne 'printed') {
    # 'error'   : the spooler is unhappy (paper out, offline, blocked...)
    # 'unknown' : we could not follow it
    # Neither can be called a clean failure, because part of the job may already be on
    # paper - so report it as uncertain and let a person look at the actual slip.
    Report-NotPrinted $id $lease "printing did not complete ($($outcome.Detail))" $true
    Write-Log 'WARN' "slip $slip (job $id) did not print cleanly: $($outcome.Detail)"
    return
  }

  $r = Invoke-Api -Method POST -Path "/print/$id/completed" -Body @{ lease = $lease; ok = $true }
  if ($r.Status -eq 200) {
    Set-Ledger $id @{ stage = 'done'; result = 'completed' }
    Write-Log 'INFO' "slip $slip (job $id) printed on '$printer' ($($outcome.Detail))"
  } else {
    Write-Log 'ERROR' "job $id completed-report rejected: HTTP $($r.Status) $($r.Content)"
  }
}

# ---------------------------------------------------------------- housekeeping

function Remove-OldLedgerEntries {
  # The server keeps the extracted PDFs for 7 days; keep our own traces a bit longer so a
  # replayed job id can still be recognised, then drop them.
  $limit = (Get-Date).AddDays(-14)
  foreach ($f in Get-ChildItem $LedgerDir -Filter '*.json' -ErrorAction SilentlyContinue) {
    if ($f.LastWriteTime -ge $limit) { continue }
    # Only drop entries we finished cleanly. A damaged or half-finished entry is evidence -
    # keep it so somebody can work out what happened to that slip.
    $id = 0
    if (-not [int]::TryParse([IO.Path]::GetFileNameWithoutExtension($f.Name), [ref]$id)) { continue }
    $led = Get-Ledger $id
    if ($led -and $led.stage -eq 'done') { Remove-Item $f.FullName -Force -ErrorAction SilentlyContinue }
  }
  foreach ($f in Get-ChildItem $PdfDir -Filter '*.pdf' -ErrorAction SilentlyContinue) {
    if ($f.LastWriteTime -lt $limit) { Remove-Item $f.FullName -Force -ErrorAction SilentlyContinue }
  }
}

<#
  On start-up, close out anything the previous run left open.

  The server never hands the same job to /print/next again once the PDF has been downloaded,
  so a job we died on would otherwise sit here until the server's own timeout. Ask the server
  what it thinks and report accordingly - but never re-print.
#>
function Resolve-UnfinishedLedger {
  foreach ($f in Get-ChildItem $LedgerDir -Filter '*.json' -ErrorAction SilentlyContinue) {
    $id = 0
    if (-not [int]::TryParse([IO.Path]::GetFileNameWithoutExtension($f.Name), [ref]$id)) { continue }
    $led = Get-Ledger $id
    if (-not $led -or $led.stage -eq 'done') { continue }
    $lease = [string]$led.lease
    if (-not $lease) { continue }
    $r = $null
    try { $r = Invoke-Api -Method GET -Path "/print/$id/status" } catch {
      Write-Log 'WARN' "job $id status check failed: $($_.Exception.Message)"
      continue
    }
    if ($r.Status -ne 200) { continue }
    $state = ($r.Content | ConvertFrom-Json).job.state
    # Already settled on the server side (someone printed it by hand, it timed out, ...).
    if ($state -in @('completed', 'failed', 'unknown', 'manual')) {
      Set-Ledger $id @{ stage = 'done'; result = "server:$state" }
      continue
    }
    Write-Log 'WARN' "job $id was left at stage '$($led.stage)' by a previous run (server says '$state')"
    Resolve-KnownJob ([pscustomobject]@{ id = $id; leaseToken = $lease }) $led
  }
}

# ---------------------------------------------------------------- main loop

Write-Log 'INFO' "agent starting (server=$BaseUrl poll=${PollSec}s once=$Once)"
try { Resolve-UnfinishedLedger } catch { Write-Log 'ERROR' "start-up ledger check failed: $($_.Exception.Message)" }
$lastHeartbeat = [DateTime]::MinValue
$lastCleanup   = [DateTime]::MinValue
$backoff       = 0

while ($true) {
  try {
    if (((Get-Date) - $lastHeartbeat).TotalSeconds -ge $HeartbeatSec) {
      $note = 'ready'
      try {
        $printers = (Get-Printer -ErrorAction Stop | Select-Object -ExpandProperty Name) -join ' / '
        if ($printers) { $note = "printers: $printers" }
      } catch { $note = 'printer list unavailable' }
      $hb = Invoke-Api -Method POST -Path '/print/heartbeat' -Body @{ note = $note }
      if ($hb.Status -eq 401) { throw 'heartbeat rejected (401) - check the token in config.json' }
      $lastHeartbeat = Get-Date
    }
    if (((Get-Date) - $lastCleanup).TotalHours -ge 6) { Remove-OldLedgerEntries; $lastCleanup = Get-Date }

    # NOTE: break/continue inside a PowerShell switch applies to the SWITCH, not to this
    # loop, so the flow below is written with if/elseif on purpose.
    $res = Invoke-Api -Method GET -Path '/print/next'
    $sleepSec = $PollSec
    if ($res.Status -eq 200) {
      $job = ($res.Content | ConvertFrom-Json).job
      if ($job) { Invoke-Job $job }
      $backoff = 0
      $sleepSec = 0        # something to do: come straight back for the next one
    } elseif ($res.Status -eq 204) {
      $backoff = 0
    } elseif ($res.Status -eq 401) {
      throw 'not authorised (401) - the device token is wrong or was revoked'
    } elseif ($res.Status -eq 409) {
      # No printer registered for this device on the server side. Nothing we can fix here.
      Write-Log 'WARN' "server refused to hand out work: $($res.Content)"
      $sleepSec = 30
    } else {
      Write-Log 'WARN' "unexpected response from /print/next: HTTP $($res.Status)"
    }
  } catch {
    $backoff = [Math]::Min($MaxBackoffSec, [Math]::Max(2, $backoff * 2))
    Write-Log 'ERROR' "$($_.Exception.Message) (retrying in ${backoff}s)"
    $sleepSec = $backoff
  }
  if ($Once) { break }
  if ($sleepSec -gt 0) { Start-Sleep -Seconds $sleepSec }
}

Write-Log 'INFO' 'agent stopped'
