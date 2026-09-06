#Requires -Version 5.1
<#
  Iroha storage-box label print agent - Iroha PC, Brother QL-800.
  Pulls label print jobs from the bfaith-portal iroha-work app and prints them with
  b-PAC from the P-touch template copies (see make-auto-lbx.ps1 / print-label.ps1).
  Derived 2026-09-06 from the warehouse PC nefuda (price tag) agent: same design and the
  same server contract, plus an optional "expiry" text object; the box label has a single
  CODE128 object that takes both JAN and FNSKU (config.objects.jan / fnsku name the same object).

  This is the slip print agent (scripts/slip-print-agent, Codex-reviewed 6 rounds) with the
  PDF/SumatraPDF part replaced by "b-PAC renders the template to a 300 dpi bitmap, GDI
  prints the bitmap with an explicit 62mm x 67.2mm paper" (print-label.ps1 explains why
  b-PAC's own PrintOut cannot be used on this PC). Everything about "never print twice" is kept.

  Design notes (why it is written this way):

  * PULL ONLY. The server never connects to this PC, so no fixed IP and no inbound port.

  * NEVER PRINT THE SAME JOB TWICE. The server stops re-distributing a job once it has been
    handed out. This agent adds the second half of that guarantee with a persistent ledger:

        leased -> received -> submitting -> submitted -> done

    A job that reached "received" is never taken again, even after a crash or a restart.
    If we crashed while "submitting" we cannot know whether labels came out, so we report
    "uncertain" and let a person look at the printer. A missing label costs one tap on the
    iPad; a duplicate label on a carton costs a mis-shelved product.

  * THE SERVER DECIDES THE PRINTER. printerName comes from the job and is used verbatim.
    No local printer setting, no fallback to the default printer.

  * THE JOB CARRIES THE DATA. There is no file to download: productName / barcode /
    barcodeType / packQty / expiry / copies arrive as JSON and are pushed into the template objects.
    The JAN template copy is used for digit-only barcodes, the FNSKU copy for Amazon codes.

  * ASCII ONLY in this file. Task Scheduler + Windows PowerShell 5.1 mangles non-ASCII .ps1
    content. Japanese object names / product names arrive over HTTP or from config.json as
    UTF-8 and are passed through as native strings.

  Japanese documentation: README.md next to this file.
#>
[CmdletBinding()]
param(
  [string] $ConfigPath = '',   # default resolved below (see $ScriptDir)
  # Run a single poll and exit. Used by setup.ps1 verification.
  [switch] $Once
)

$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [Text.Encoding]::UTF8

$AgentVersion = '2026-09-06.1-iroha'

# Windows PowerShell 5.1 leaves $PSScriptRoot EMPTY while param() defaults are evaluated when the
# script is started with "powershell -File" (which is how setup.ps1 and the scheduled task run
# it), so the config default is resolved here. $MyInvocation is the fallback for the same reason.
$ScriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
if (-not $ConfigPath) { $ConfigPath = Join-Path $ScriptDir 'config.json' }

# ---------------------------------------------------------------- configuration

if (-not (Test-Path $ConfigPath)) {
  throw "Config not found: $ConfigPath (copy config.example.json and fill in the token)"
}
$cfg = Get-Content $ConfigPath -Raw -Encoding UTF8 | ConvertFrom-Json
foreach ($k in @('baseUrl', 'token', 'templates', 'objects')) {
  if (-not $cfg.$k) { throw "Config is missing '$k': $ConfigPath" }
}
$BaseUrl      = $cfg.baseUrl.TrimEnd('/')
$Token        = $cfg.token
$PollSec      = if ($cfg.pollSec)      { [int]$cfg.pollSec }      else { 4 }
$HeartbeatSec = if ($cfg.heartbeatSec) { [int]$cfg.heartbeatSec } else { 45 }
$WorkDir      = if ($cfg.workDir)      { $cfg.workDir }           else { Join-Path $ScriptDir 'work' }
$MaxCopies    = if ($cfg.maxCopies)    { [int]$cfg.maxCopies }    else { 50 }
if (-not $cfg.label) { throw "Config is missing 'label' (mediaName / widthMm / lengthMm / paperRawKind): $ConfigPath" }
$LedgerDir    = Join-Path $WorkDir 'ledger'
$JobDir       = Join-Path $WorkDir 'jobs'
$RenderDir    = Join-Path $WorkDir 'render'    # BMP rendered by b-PAC for each job (kept 14 days as evidence)
$LogPath      = Join-Path $WorkDir 'agent.log'
# The whole point of this agent is unattended printing, so a backoff that grows without
# bound would silently stop the line. Cap it so we always come back.
$MaxBackoffSec = 60

foreach ($d in @($WorkDir, $LedgerDir, $JobDir, $RenderDir)) {
  if (-not (Test-Path $d)) { New-Item -ItemType Directory -Path $d -Force | Out-Null }
}

# b-PAC helpers (Invoke-BpacPrint / Test-BpacAvailable / Get-BpacMediaInfo / New-LabelFields / ...)
. (Join-Path $ScriptDir 'print-label.ps1') -Library
# The driver paper format the label is printed on (b-PAC ignores the size in the .lbx and
# takes a driver format instead - see print-label.ps1 header). Registered once in the
# QL-800 printing preferences; its name/size live in config.json "label".
$LabelArgs = Get-LabelArgs $cfg

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
$script:Mutex = $null
$haveMutex = $false
try {
  $script:Mutex = New-Object System.Threading.Mutex($false, 'Global\BFaith-IrohaLabelAgent')
  $haveMutex = $script:Mutex.WaitOne(0)
} catch {
  # "Access denied" here means the mutex exists but belongs to another account - typically the
  # SYSTEM scheduled task while somebody runs this by hand. That still means "already running".
  $haveMutex = $false
}
if (-not $haveMutex) {
  Write-Log 'INFO' 'another iroha label agent is already running on this PC (scheduled task BFaith-IrohaLabelAgent?) - exiting'
  if ($Once) {
    Write-Log 'ERROR' 'stop it first (as Administrator): Stop-ScheduledTask -TaskName BFaith-IrohaLabelAgent'
    exit 1
  }
  exit 0
}

# ---------------------------------------------------------------- HTTP

function Invoke-Api {
  param(
    [string] $Method,
    [string] $Path,
    $Body = $null
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
    # Product names are Japanese: send explicit UTF-8 bytes, not a .NET string that
    # Invoke-WebRequest would encode with the default (ANSI) code page.
    $params.Body        = [Text.Encoding]::UTF8.GetBytes(($Body | ConvertTo-Json -Compress -Depth 5))
    $params.ContentType = 'application/json; charset=utf-8'
  }
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
  record of a label we already printed. Return a poisoned entry so the caller refuses to print.
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
  [IO.File]::WriteAllText($tmp, ($obj | ConvertTo-Json -Depth 5), (New-Object Text.UTF8Encoding($false)))
  Move-Item $tmp (Get-LedgerPath $JobId) -Force
}

# ---------------------------------------------------------------- printing

<#
  Read the spool queue of one printer. Returns an array (possibly empty) of objects with
  Id / DocumentName / JobStatus, or $null when the queue could not be read at all
  (different from "queue is empty").

  ROOT CAUSE OF THE FIRST REAL JOB FAILING (2026-09-05 19:09, "the print queue could not be read
  before printing"): the old Get-SpoolJobIds did "return @(...)". PowerShell UNROLLS a returned
  array, so an EMPTY queue (the normal case!) arrived at the caller as $null = "could not read".
  Every return of an array from here is therefore written as ",@(...)" (unary comma keeps it an
  array). Get-PrintJob (PrintManagement / MSFT_PrintJob) is tried first; if it throws, fall back
  to the classic WMI class Win32_PrintJob (Name = "<printer>, <jobid>") and LOG WHY.
#>
function Get-SpoolJobs {
  param([string] $PrinterName)
  try {
    return ,@(Get-PrintJob -PrinterName $PrinterName -ErrorAction Stop | ForEach-Object {
      [pscustomobject]@{ Id = [int]$_.Id; DocumentName = [string]$_.DocumentName; JobStatus = [string]$_.JobStatus }
    })
  } catch {
    $why1 = $_.Exception.Message
    try {
      $prefix = $PrinterName + ', '
      $all = @(Get-CimInstance -ClassName Win32_PrintJob -ErrorAction Stop |
        Where-Object { ([string]$_.Name).StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase) })
      if (-not $script:WarnedGetPrintJob) {
        Write-Log 'WARN' "Get-PrintJob failed ($why1) - reading the queue through Win32_PrintJob instead"
        $script:WarnedGetPrintJob = $true
      }
      return ,@($all | ForEach-Object {
        [pscustomobject]@{
          Id = [int]$_.JobId; DocumentName = [string]$_.Document
          JobStatus = (([string]$_.JobStatus) + ' ' + ([string]$_.Status)).Trim()
        }
      })
    } catch {
      Write-Log 'ERROR' "the print queue of '$PrinterName' could not be read: Get-PrintJob: $why1 / Win32_PrintJob: $($_.Exception.Message)"
      return $null
    }
  }
}

function Get-SpoolJobIds {
  param([string] $PrinterName)
  $jobs = Get-SpoolJobs $PrinterName
  if ($null -eq $jobs) { return $null }   # $null = could not look (different from "queue is empty")
  return ,@($jobs | ForEach-Object { [int]$_.Id })   # unary comma: an empty queue must stay @(), not become $null
}

<#
  Pick OUR spool job(s) out of the ones that appeared since we started printing.

  Someone printing something else on the same printer at the same moment would otherwise be
  followed instead of us, and when THEIR job finished we would announce our label as printed.
  The document name we give b-PAC (hakolabel-<jobId>) is the only thing that counts. With
  several copies b-PAC may spool one job or one per copy - either way every one of them
  carries our name, so we follow all of them.
  When it cannot be decided, say so - the caller turns that into "please check the printer".
#>
function Select-OwnSpoolJobs {
  param($NewJobs, [string] $DocHint)
  # ONLY a document-name match counts. "there is exactly one new job" looks tempting, but
  # our own label can pass through too fast to observe and be replaced in the queue by
  # somebody else's job a moment later - we would then follow theirs and call OUR label
  # printed when THEIRS finished. Guessing is how a missing label goes unnoticed.
  $named = @($NewJobs | Where-Object { [string]$_.DocumentName -like "*$DocHint*" })
  if ($named.Count -ge 1) { return @{ JobIds = @($named | ForEach-Object { [int]$_.Id }) } }
  $names = (@($NewJobs | ForEach-Object { [string]$_.DocumentName }) -join ' / ')
  return @{ Ambiguous = "no spool job named like $DocHint (queue shows: $names)" }
}

<#
  Wait for the job(s) we just spooled and say what actually happened.

  A clean return from b-PAC only means "handed to the spooler". If we reported "printed" on
  that alone, a USB cable pulled out, an empty tape roll or a spooler error would still be
  announced as a success and nobody would notice the label never came out.

  Returns one of:
    printed   - every one of our jobs left the queue with no error: labels came out
    error     - the spooler is reporting a problem (tape out, offline, blocked...)
    unknown   - we could not follow it (spooler not queryable, still stuck after the wait)

  $Before is the list of job ids that were already in that queue before we printed, so we
  only ever look at OUR jobs.
#>
function Wait-PrintResult {
  param([string] $PrinterName, $Before, [string] $DocHint, [int] $AppearSec = 5, [int] $FinishSec = 120)

  $mine = @()
  $deadline = (Get-Date).AddSeconds($AppearSec)
  while ((Get-Date) -lt $deadline -and $mine.Count -eq 0) {
    $all = Get-SpoolJobs $PrinterName
    if ($null -eq $all) {
      return @{ Result = 'unknown'; Detail = 'the print queue could not be read' }
    }
    $new = @($all | Where-Object { $Before -notcontains [int]$_.Id })
    if ($new.Count -ge 1) {
      $pick = Select-OwnSpoolJobs $new $DocHint
      if ($pick.Ambiguous) { return @{ Result = 'unknown'; Detail = $pick.Ambiguous } }
      $mine = @($pick.JobIds)
    } else {
      Start-Sleep -Milliseconds 100   # a small label can pass through very quickly
    }
  }

  if ($mine.Count -eq 0) {
    # We never saw our job in the queue. It may have printed too fast to observe, or the
    # spooler may have refused it - we cannot tell the two apart, so we must not claim it
    # printed. "unknown" asks a person to look at the printer, which is safe: it never
    # tells anyone to print a second copy.
    return @{ Result = 'unknown'; Detail = 'the spool job was never seen in the queue'; JobId = $null }
  }

  $bad = @('Error', 'Offline', 'PaperOut', 'Blocked', 'UserIntervention', 'Paused', 'Deleted')
  $deadline = (Get-Date).AddSeconds($FinishSec)
  $first = $mine[0]
  while ((Get-Date) -lt $deadline) {
    # "our jobs are no longer in the queue" (= they printed) and "we could not ask the
    # spooler" look identical if both end up empty. They are not the same thing: a stopped
    # spooler or a permission error must never be reported as a successful print.
    $left = @()
    $all = Get-SpoolJobs $PrinterName
    if ($null -eq $all) {
      return @{ Result = 'unknown'; Detail = 'the print queue stopped answering (see agent.log)'; JobId = $first }
    }
    $left = @($all | Where-Object { $mine -contains [int]$_.Id })
    if ($left.Count -eq 0) { return @{ Result = 'printed'; Detail = "left the queue ($($mine.Count) spool job(s))"; JobId = $first } }
    foreach ($j in $left) {
      $status = [string]$j.JobStatus
      foreach ($b in $bad) {
        if ($status -like "*$b*") { return @{ Result = 'error'; Detail = "spooler says: $status"; JobId = $first } }
      }
    }
    Start-Sleep -Milliseconds 500
  }
  return @{ Result = 'unknown'; Detail = "still in the queue after ${FinishSec}s"; JobId = $first }
}

<#
  Is there a printer on THIS PC with exactly this name?
  b-PAC's SetPrinter would refuse an unknown name, but we check first so that a miss is a
  clean failure before we write the "submitting" ledger stage.
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
  # partial match: " QL-700" and "QL-700 2" must not be accepted for "Brother QL-700".
  foreach ($n in $all) { if ($n -eq $PrinterName) { return $true } }
  return $false
}

# ---------------------------------------------------------------- job handling

<#
  Report a job that did not print.

  $Uncertain is the important flag.
    $false : we are sure no label came out (bad printer name, bad job data, ...).
             The server tells the iPad "not printed - try again / print by hand".
    $true  : we handed it to b-PAC (or might have) and then lost track of it.
             The server must NOT say "print it again", because if labels did come out the
             next attempt would produce duplicates. It says "check the printer" instead.

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
      Set-Ledger $JobId @{ stage = 'done'; result = $(if ($Uncertain) { 'unknown' } else { 'failed' }); reason = $Reason }
      Write-Log 'WARN' "job $JobId reported as not printed ($Reason, uncertain=$Uncertain)"
    } else {
      Write-Log 'ERROR' "job $JobId report rejected: HTTP $($r.Status) $($r.Content)"
    }
  } catch {
    Write-Log 'ERROR' "job $JobId could not report: $($_.Exception.Message)"
  }
}

<#
  A job we have already taken past "received" must never be printed again.
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
      Report-NotPrinted $id $lease 'agent lost track after handing the label to the printer' $true
      return
    }
    try {
      # Resend /submitted first. The original one may never have reached the server, in
      # which case the job is still 'dispatched' there and a completion report would be
      # rejected. The server accepts a repeat of the same report as a success (replayed).
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
    # cannot claim nothing came out - do not let the iPad be told to print it again.
    Report-NotPrinted $id $lease 'agent lost track while printing - please check the printer' $true
  } else {
    # received but never printed: safe to say nothing came out.
    Report-NotPrinted $id $lease 'agent restarted before printing' $false
  }
}

<#
  Check the job data BEFORE anything touches the printer. Every problem here is a clean
  "nothing came out" failure. Returns $null when the job is fine, otherwise the reason.
#>
function Test-JobData {
  param($Job)
  if (-not $Job.printerName) { return 'server did not say which printer to use' }
  $type = ([string]$Job.barcodeType).ToLower()
  if ($type -ne 'jan' -and $type -ne 'fnsku') { return "unknown barcodeType '$($Job.barcodeType)' (jan / fnsku)" }
  $bc = ([string]$Job.barcode).Trim()
  if (-not $bc) { return 'job has no barcode' }
  if ($type -eq 'jan'   -and $bc -notmatch '^[0-9]+$')        { return "JAN barcode must be digits only (got '$bc')" }
  if ($type -eq 'fnsku' -and $bc -notmatch '^[A-Za-z0-9]+$')  { return "FNSKU barcode must be letters and digits only (got '$bc')" }
  if (-not ([string]$Job.productName).Trim()) { return 'job has no product name' }
  $extra = ([string]$Job.extraPackQty).Trim()
  if ($extra -and $extra -notmatch '^[0-9]+$') { return "extraPackQty is not a whole number ('$extra')" }
  $copies = 0
  if (-not [int]::TryParse([string]$Job.copies, [ref]$copies)) { return "copies is not a number ('$($Job.copies)')" }
  if ($copies -lt 1 -or $copies -gt $MaxCopies) { return "copies out of range: $copies (allowed 1..$MaxCopies)" }
  return $null
}

function Invoke-Job {
  param($Job)
  $id      = [int]$Job.id
  $lease   = [string]$Job.leaseToken
  $printer = [string]$Job.printerName
  $code    = [string]$Job.productCode

  $ledger = Get-Ledger $id
  if ($ledger -and $ledger.stage -and $ledger.stage -ne 'leased') {
    Write-Log 'WARN' "job $id is already at stage '$($ledger.stage)' - will not print it again"
    Resolve-KnownJob $Job $ledger
    return
  }
  Set-Ledger $id @{ stage = 'leased'; lease = $lease; printer = $printer; productCode = $code }

  # 1. validate + keep the job data on disk (durable; replaces the PDF download stage)
  $bad = Test-JobData $Job
  if ($bad) { Report-NotPrinted $id $lease $bad $false; return }
  $type   = ([string]$Job.barcodeType).ToLower()
  $bc     = ([string]$Job.barcode).Trim()
  $copies = [int]$Job.copies
  $jobPath = Join-Path $JobDir "$id.json"
  [IO.File]::WriteAllText($jobPath, ($Job | ConvertTo-Json -Depth 5), (New-Object Text.UTF8Encoding($false)))
  Set-Ledger $id @{ stage = 'received'; job = $jobPath; barcodeType = $type; copies = $copies }

  # 2. Make sure the printer this job is for actually exists on this PC, BEFORE we write
  #    "submitting". A missing name here is a clean "nothing came out" failure.
  if (-not (Test-PrinterExists $printer)) {
    Report-NotPrinted $id $lease "this PC has no printer named '$printer'" $false
    return
  }
  $template = $null
  try { $template = Resolve-TemplatePath $cfg $type } catch { Report-NotPrinted $id $lease $_.Exception.Message $false; return }
  if (-not (Test-Path $template)) { Report-NotPrinted $id $lease "template file is missing on this PC: $template" $false; return }
  $fields = New-LabelFields $cfg $type $bc ([string]$Job.productName) ([string]$Job.packQty) ([string]$Job.expiry)
  # The last carton can hold fewer pieces than the full ones, so the job may ask for ONE extra
  # label carrying that quantity (Nakahara 2026-09-06: 6 cartons = 70 x 5 + 10 -> 5 labels of
  # 70 and 1 of 10). Empty = no extra label, which is exactly what older jobs send.
  $extraQty = ([string]$Job.extraPackQty).Trim()

  # 3. print. Record the intent BEFORE handing anything to b-PAC, so a crash here
  #    is recognisable as "we do not know whether labels came out".
  #    Snapshot the queue first so we can tell OUR spool job from anything already in it.
  $before = Get-SpoolJobIds $printer
  if ($null -eq $before) {
    # We could not read the queue. If we printed now we would have no way to tell our own
    # spool job from one that was already there, and could call someone else's job "ours".
    # Nothing has been handed to the spooler yet, so this is a clean "did not print".
    Report-NotPrinted $id $lease 'the print queue could not be read before printing' $false
    return
  }
  $docName = "hakolabel-$id"
  Set-Ledger $id @{ stage = 'submitting'; docName = $docName }
  $extraNote = if ($extraQty) { " +1 label of $extraQty" } else { '' }
  Write-Log 'INFO' "printing $code ($type $bc x$copies$extraNote, job $id) on '$printer'"
  try {
    [void](Invoke-BpacPrint -TemplatePath $template -Fields $fields -PrinterName $printer `
      -Copies $copies -DocName $docName -RenderDir $RenderDir @LabelArgs)
    # The odd carton: one more label with its own quantity. SAME document name, so the spool
    # tracking below follows both jobs as ours; only the BMP kept as evidence gets its own name.
    if ($extraQty) {
      $fields2 = New-LabelFields $cfg $type $bc ([string]$Job.productName) $extraQty ([string]$Job.expiry)
      [void](Invoke-BpacPrint -TemplatePath $template -Fields $fields2 -PrinterName $printer `
        -Copies 1 -DocName $docName -BmpName "$docName-r" -RenderDir $RenderDir @LabelArgs)
    }
  } catch {
    $msg = $_.Exception.Message
    # print-label.ps1 prefixes the message with the stage it reached. Up to "[rendering]"
    # ("[opening]" / "[selecting printer]" / "[checking media]" = paper format missing or
    # wrong size / "[filling]") nothing has been handed to the spooler, so that is a clean
    # failure. From "[printing]" on, labels may already be coming out - report uncertain,
    # never "print again".
    $uncertain = ($msg -like '`[printing`]*')
    Report-NotPrinted $id $lease "b-PAC failed: $msg" $uncertain
    return
  }

  # 4. tell the server it is in the spooler, then FOLLOW THE ACTUAL SPOOL JOB.
  #    A clean return from b-PAC only proves the hand-off - a pulled USB cable or an empty
  #    roll would otherwise be announced as "printed" and nobody would notice.
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
    # 'error'   : the spooler is unhappy (tape out, offline, blocked...)
    # 'unknown' : we could not follow it
    # Neither can be called a clean failure, because part of the job may already be on
    # paper - so report it as uncertain and let a person look at the printer.
    Report-NotPrinted $id $lease "printing did not complete ($($outcome.Detail))" $true
    Write-Log 'WARN' "$code (job $id) did not print cleanly: $($outcome.Detail)"
    return
  }

  $r = Invoke-Api -Method POST -Path "/print/$id/completed" -Body @{ lease = $lease; ok = $true }
  if ($r.Status -eq 200) {
    Set-Ledger $id @{ stage = 'done'; result = 'completed' }
    Write-Log 'INFO' "$code (job $id) printed x$copies on '$printer' ($($outcome.Detail))"
  } else {
    Write-Log 'ERROR' "job $id completed-report rejected: HTTP $($r.Status) $($r.Content)"
  }
}

# ---------------------------------------------------------------- housekeeping

function Remove-OldLedgerEntries {
  $limit = (Get-Date).AddDays(-14)
  foreach ($f in Get-ChildItem $LedgerDir -Filter '*.json' -ErrorAction SilentlyContinue) {
    if ($f.LastWriteTime -ge $limit) { continue }
    # Only drop entries we finished cleanly. A damaged or half-finished entry is evidence -
    # keep it so somebody can work out what happened to that label.
    $id = 0
    if (-not [int]::TryParse([IO.Path]::GetFileNameWithoutExtension($f.Name), [ref]$id)) { continue }
    $led = Get-Ledger $id
    if ($led -and $led.stage -eq 'done') { Remove-Item $f.FullName -Force -ErrorAction SilentlyContinue }
  }
  foreach ($f in Get-ChildItem $JobDir -Filter '*.json' -ErrorAction SilentlyContinue) {
    if ($f.LastWriteTime -lt $limit) { Remove-Item $f.FullName -Force -ErrorAction SilentlyContinue }
  }
  foreach ($f in Get-ChildItem $RenderDir -Filter '*.bmp' -ErrorAction SilentlyContinue) {
    if ($f.LastWriteTime -lt $limit) { Remove-Item $f.FullName -Force -ErrorAction SilentlyContinue }
  }
}

<#
  On start-up, close out anything the previous run left open.

  The server never hands the same job to /print/next again once it has been handed out,
  so a job we died on would otherwise sit here until the server's own timeout. Ask the
  server what it thinks and report accordingly - but never re-print.
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

$bpac = Test-BpacAvailable
Write-Log 'INFO' "agent $AgentVersion starting (server=$BaseUrl poll=${PollSec}s once=$Once) - $($bpac.Detail)"
if (-not $bpac.Ok) {
  # Keep polling so the heartbeat tells the server (and the admin screen) what is wrong,
  # but never take a job we cannot print: /print/next is skipped below.
  Write-Log 'ERROR' 'b-PAC is missing for this PowerShell - jobs will NOT be taken until it is installed (see README)'
}
try { Resolve-UnfinishedLedger } catch { Write-Log 'ERROR' "start-up ledger check failed: $($_.Exception.Message)" }
$lastHeartbeat = [DateTime]::MinValue
$script:LastMediaProbe = [DateTime]::MinValue   # see the heartbeat below (QL-800 paper format probe)
$script:MediaProbe     = $null
$lastCleanup   = [DateTime]::MinValue
$backoff       = 0
$onceFailed    = $false   # -Once: report a failed poll through the exit code (setup.ps1 relies on it)

while ($true) {
  try {
    if (((Get-Date) - $lastHeartbeat).TotalSeconds -ge $HeartbeatSec) {
      $note = 'ready'
      $printers = ''
      try {
        $printers = (Get-Printer -ErrorAction Stop | Select-Object -ExpandProperty Name) -join ' / '
        if ($printers) { $note = "printers: $printers" }
      } catch { $note = 'printer list unavailable' }
      if (-not $bpac.Ok) { $note = "b-PAC MISSING ($($bpac.Bits)-bit) - " + $note }
      # Is the registered paper format still there, is the printer online, what does it
      # report? Lets the admin screen explain a stuck queue before anybody taps the button.
      # Best effort: the printer may be off. (The reported media is informational only -
      # with a compatible roll it just echoes the driver default.)
      $formatOk = $null; $reported = ''
      if ($bpac.Ok -and $cfg.printerName) {
        try {
          $m = Get-BpacMediaInfo -PrinterName ([string]$cfg.printerName) -FormatName $LabelArgs.LabelMediaName
          $formatOk = [bool]$m.HasFormat
          $reported = if ($m.Online) { "$($m.ReportedId) $($m.ReportedName)" } else { 'offline' }
          # MEASURED 2026-09-06 (QL-800): a long tape format that IS registered never appears in
          # the driver list, so "not listed" is not proof and would show a false "paper missing"
          # warning on the iPad forever. Ask the document instead - select the format and measure
          # it - and cache the answer for 10 minutes (it opens a COM object). If the probe cannot
          # run at all we report $null (unknown) rather than crying wolf.
          if (-not $formatOk) {
            if (((Get-Date) - $script:LastMediaProbe).TotalMinutes -ge 10) {
              $script:LastMediaProbe = Get-Date
              $script:MediaProbe = $null
              try {
                $script:MediaProbe = Test-LabelMediaUsable -TemplatePath (Resolve-TemplatePath $cfg 'jan') `
                  -PrinterName ([string]$cfg.printerName) @LabelArgs
              } catch { Write-Log 'WARN' "paper format probe failed: $($_.Exception.Message)" }
            }
            if ($script:MediaProbe) {
              $formatOk = [bool]$script:MediaProbe.Ok
              $reported = "$reported / media: $($script:MediaProbe.Detail)"
            } else {
              $formatOk = $null
            }
          }
        } catch { $reported = "unreadable: $($_.Exception.Message)" }
        if ($formatOk -eq $false) { $note = "LABEL PAPER '$($LabelArgs.LabelMediaName)' NOT USABLE - " + $note }
        $note = "$note / printer reports: $reported"
      }
      $hb = Invoke-Api -Method POST -Path '/print/heartbeat' -Body @{
        note = $note; version = $AgentVersion; bpac = $bpac.Ok; host = $env:COMPUTERNAME
        paperFormat = $LabelArgs.LabelMediaName; paperFormatOk = $formatOk; printerReports = $reported
      }
      if ($hb.Status -eq 401) { throw 'heartbeat rejected (401) - check the token in config.json' }
      if ($hb.Status -ne 200) {
        # 404 here = the portal does not have the print queue endpoints yet (server PR not deployed).
        Write-Log 'WARN' "heartbeat got HTTP $($hb.Status) from $BaseUrl/print/heartbeat"
        $onceFailed = $true
      }
      $lastHeartbeat = Get-Date
    }
    if (((Get-Date) - $lastCleanup).TotalHours -ge 6) { Remove-OldLedgerEntries; $lastCleanup = Get-Date }

    $sleepSec = $PollSec
    if ($bpac.Ok) {
      # NOTE: break/continue inside a PowerShell switch applies to the SWITCH, not to this
      # loop, so the flow below is written with if/elseif on purpose.
      $res = Invoke-Api -Method GET -Path '/print/next'
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
        $onceFailed = $true
        $sleepSec = 30
      } else {
        Write-Log 'WARN' "unexpected response from /print/next: HTTP $($res.Status)"
        $onceFailed = $true
        $sleepSec = 30
      }
    } else {
      $onceFailed = $true
      $sleepSec = 30
    }
  } catch {
    $backoff = [Math]::Min($MaxBackoffSec, [Math]::Max(2, $backoff * 2))
    Write-Log 'ERROR' "$($_.Exception.Message) (retrying in ${backoff}s)"
    $onceFailed = $true
    $sleepSec = $backoff
  }
  if ($Once) { break }
  if ($sleepSec -gt 0) { Start-Sleep -Seconds $sleepSec }
}

Write-Log 'INFO' 'agent stopped'
if ($Once -and $onceFailed) { exit 1 }
