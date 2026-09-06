#Requires -Version 5.1
<#
  Print helper for the Iroha storage-box label - Brother QL-800 on the Iroha PC.
  (Derived from the warehouse PC nefuda helper, 2026-09-06. The measurements quoted below
  were taken on the QL-700 with driver 6.5; the QL-800 uses the same driver family and the
  same paper-format registration. The box label adds an optional "expiry" text object.)

  HOW A LABEL GETS PRINTED (decided 2026-09-05 after a day of measurements, see below):
    1. b-PAC (Brother's COM component, ProgID "bpac.Document") opens the .lbx template copy,
       selects the driver paper format registered for this label ("hakolabel 62x67" = 62mm
       continuous tape, 67.2mm long), fills the objects by name and RENDERS the label to a
       300 dpi monochrome BMP (Export). b-PAC is used as a renderer only.
    2. The BMP is printed through the normal Windows GDI path (System.Drawing.Printing)
       with an explicit paper: driver form 259 ("62mm" continuous) x 67.2mm, landscape,
       drawn from the physical page corner so the layout lands exactly where the template
       puts it. The spool document name is the caller's DocName (the agent finds its own
       spool job by it). Cut behaviour comes from the driver's default settings
       ("cut after every label" was on in the QL-700 preferences).

  WHY NOT b-PAC's OWN PrintOut: on this PC it fails with ErrorCode 11 ("the currently
  selected printer is not supported") for EVERY media, including the one the printer
  itself reports, while StartPrint succeeds and Export works. IPrinter.IsPrinterSupported
  is checked at start-up and logged; do not spend time on PrintOut again.

  MEASURED 2026-09-05 on this PC (QL-700 driver 6.5, compatible 62mm continuous roll):
    * b-PAC ignores the label size stored in the .lbx: any template opens on the printer's
      current default paper (user PrintTicket/DEVMODE). IDocument.Length setter is ignored
      (PowerShell, VBScript, IDispatch). What works: SetMediaByName/SetMediaById picks a
      DRIVER paper format and the document takes that format's width/length. The 62x67.2
      format is registered once in the QL-800 printing preferences (Advanced tab > long
      tape format [Settings...] > New, name = config label.mediaName).
    * The physical roll is a compatible (non-DK) roll: the printer/status monitor reports
      274 "62mm x 29mm" whatever is loaded. Informational only, never used as a gate.
    * Hard margins reported by the driver for this paper: 3.0mm along the tape length,
      1.5mm across - identical to the template's own margins, so drawing the full-size
      BMP at (-HardMarginX, -HardMarginY) reproduces the template exactly.
    * PowerShell's COM adapter does not see b-PAC's type library: $doc.Printer reads as
      $null and $doc.Length/Width read garbage. Methods work; properties are read through
      IDispatch (InvokeMember) - see Get-BpacDocProp / Get-BpacPrinterObject.

  Two ways to use this file:
    1. dot-sourced by agent.ps1 / test-print.ps1:   . .\print-label.ps1 -Library
    2. by hand (no server needed):
       powershell -ExecutionPolicy Bypass -File print-label.ps1 -BarcodeType jan -Barcode 4573473360422 `
         -ProductName "test" -PackQty 24 -Expiry "2027-03" -ExportBmp C:\tools\iroha-label-agent\work\preview.bmp
       powershell -ExecutionPolicy Bypass -File print-label.ps1 -BarcodeType jan -Barcode 4573473360422 `
         -ProductName "test" -PackQty 24 -Expiry "2027-03" -Copies 1 -Print

  Object names and the paper format name live in config.json (they are Japanese; this file
  must stay ASCII - Task Scheduler + PowerShell 5.1 mangles non-ASCII .ps1 files).

  Constants (verified against the SDK reference bPAC34.chm):
    ExportType bexBmp = 4; IDocument.Length/Width are in 1/1440 inch (mm = v * 25.4 / 1440)
    ErrorCode 11 = "not supported by the currently selected printer"
#>
[CmdletBinding()]
param(
  [switch] $Library,
  [string] $ConfigPath = '',
  [ValidateSet('', 'jan', 'fnsku')]
  [string] $BarcodeType = '',
  [string] $Barcode = '',
  [string] $ProductName = '',
  [string] $PackQty = '',
  [string] $Expiry = '',
  [int]    $Copies = 1,
  [string] $Printer = '',
  [string] $DocName = '',
  [string] $ExportBmp = '',
  [switch] $Print
)

$ErrorActionPreference = 'Stop'
$script:PrintLabelDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }

$BPAC_PROGID     = 'bpac.Document'
$BPAC_EXPORT_BMP = 4          # bexBmp
$RENDER_DPI      = 300        # the QL-800 (like the QL-700) prints at 300 dpi: render 1:1

Add-Type -AssemblyName System.Drawing

function Release-Com {
  param($Obj)
  if ($null -ne $Obj) { try { [void][Runtime.InteropServices.Marshal]::ReleaseComObject($Obj) } catch { } }
}

function ConvertTo-Mm { param([int] $Units1440) return [Math]::Round($Units1440 * 25.4 / 1440, 1) }

<#
  Can this PowerShell create the b-PAC COM object?
  Returns a hashtable: Ok (bool), Detail (string), Bits (32/64 of THIS process).
#>
function Test-BpacAvailable {
  $bits = [IntPtr]::Size * 8
  $doc = $null
  try {
    $doc = New-Object -ComObject $BPAC_PROGID
    return @{ Ok = $true; Detail = "b-PAC available ($bits-bit)"; Bits = $bits }
  } catch {
    return @{ Ok = $false; Detail = "b-PAC not available for $bits-bit PowerShell: $($_.Exception.Message)"; Bits = $bits }
  } finally { Release-Com $doc }
}

# IDispatch property read - PowerShell's own adapter returns nothing useful for this component.
function Get-BpacDocProp {
  param([Parameter(Mandatory = $true)] $Doc, [Parameter(Mandatory = $true)] [string] $Name)
  return [System.__ComObject].InvokeMember($Name, [Reflection.BindingFlags]::GetProperty, $null, $Doc, $null)
}

# IDocument property setter through IDispatch (same reason as the getter above).
# ALWAYS read the value back: some properties are silently ignored (see the header).
function Set-BpacDocProp {
  param([Parameter(Mandatory = $true)] $Doc, [Parameter(Mandatory = $true)] [string] $Name, [Parameter(Mandatory = $true)] $Value)
  [void][System.__ComObject].InvokeMember($Name, [Reflection.BindingFlags]::SetProperty, $null, $Doc, @($Value))
}

# IDocument.Printer through IDispatch. Caller releases the returned object.
function Get-BpacPrinterObject {
  param([Parameter(Mandatory = $true)] $Doc)
  $pr = Get-BpacDocProp $Doc 'Printer'
  if ($null -eq $pr) { throw 'b-PAC did not return its IPrinter object' }
  return $pr
}

<#
  What the driver offers and what the printer reports, in one call:
    Formats      - paper format names the QL-700 driver has (SetMediaByName accepts these)
    HasFormat    - is $FormatName among them
    Online       - IPrinter.IsPrinterOnline
    Supported    - IPrinter.IsPrinterSupported (b-PAC's own opinion; printing does not use b-PAC anyway)
    ReportedId   - IPrinter.GetMediaId  (compatible roll: just echoes 274 "62mm x 29mm")
    ReportedName - IPrinter.GetMediaName
  Uses its own bpac.Document when none is given.
#>
function Get-BpacMediaInfo {
  param([Parameter(Mandatory = $true)] [string] $PrinterName, [string] $FormatName = '', $Doc = $null)
  $own = $false
  if ($null -eq $Doc) { $Doc = New-Object -ComObject $BPAC_PROGID; $own = $true }
  $pr = $null
  try {
    if (-not $Doc.SetPrinter($PrinterName, $false)) { throw "b-PAC refused printer '$PrinterName' (ErrorCode $(Get-BpacDocProp $Doc 'ErrorCode'))" }
    $pr = Get-BpacPrinterObject $Doc
    $formats = @($pr.GetSupportedMediaNames() | ForEach-Object { [string]$_ })
    $online = [bool]$pr.IsPrinterOnline($PrinterName)
    $supported = $null
    try { $supported = [bool]$pr.IsPrinterSupported($PrinterName) } catch { }
    $rid = 0; $rname = ''
    if ($online) { try { $rid = [int]$pr.GetMediaId(); $rname = [string]$pr.GetMediaName() } catch { } }
    return @{
      Formats = $formats; HasFormat = ($FormatName -and ($formats -contains $FormatName))
      Online = $online; Supported = $supported; ReportedId = $rid; ReportedName = $rname
    }
  } finally {
    Release-Com $pr
    if ($own) { try { $Doc.Close() } catch { }; Release-Com $Doc }
  }
}

<#
  Can this printer actually produce the label geometry? The driver's format list is NOT
  reliable (the QL-800 hides registered long tape formats, see the header), so ask the
  document itself: select the format by name and measure what comes out. Opens and closes one
  b-PAC document; prints nothing and exports nothing. Never throws - a failure is an answer.
  Accepts the same splat as Invoke-BpacPrint (@LabelArgs), PaperRawKind is ignored here.
  Returns @{ Ok; Detail }
#>
function Test-LabelMediaUsable {
  param(
    [Parameter(Mandatory = $true)] [string] $TemplatePath,
    [Parameter(Mandatory = $true)] [string] $PrinterName,
    [string] $LabelMediaName = '',
    [string] $FallbackMediaName = '',
    [double] $LabelWidthMm = 0,
    [double] $LabelLengthMm = 0,
    [double] $ToleranceMm = 1.0,
    [int] $PaperRawKind = 0
  )
  if (-not (Test-Path $TemplatePath)) { return @{ Ok = $false; Detail = "template not found: $TemplatePath" } }
  $doc = $null
  try {
    $doc = New-Object -ComObject $BPAC_PROGID
    if (-not $doc.Open($TemplatePath)) { return @{ Ok = $false; Detail = 'template could not be opened' } }
    if (-not $doc.SetPrinter($PrinterName, $false)) { return @{ Ok = $false; Detail = "printer refused: $PrinterName" } }
    foreach ($cand in @($LabelMediaName, $FallbackMediaName)) {
      if (-not $cand) { continue }
      if (-not $doc.SetMediaByName([string]$cand, $false)) { continue }
      if ([string]$doc.GetMediaName() -eq [string]$cand) { break }
    }
    $paper = [string]$doc.GetMediaName()
    $lenMm = ConvertTo-Mm ([int](Get-BpacDocProp $doc 'Length'))
    $widMm = ConvertTo-Mm ([int](Get-BpacDocProp $doc 'Width'))
    if ($LabelWidthMm -gt 0 -and [Math]::Abs($widMm - $LabelWidthMm) -gt $ToleranceMm) { return @{ Ok = $false; Detail = "'$paper' width ${widMm}mm, needs ${LabelWidthMm}mm" } }
    # Length 0 = continuous roll, auto length: fine. A FIXED canvas may be longer than the design
    # (the blank tail is clipped when printing) but never shorter.
    if ($LabelLengthMm -gt 0 -and $lenMm -gt 0 -and $lenMm -lt ($LabelLengthMm - $ToleranceMm)) { return @{ Ok = $false; Detail = "'$paper' length ${lenMm}mm, needs at least ${LabelLengthMm}mm" } }
    $lenText = if ($lenMm -gt 0) { "${lenMm}" } else { 'auto' }
    return @{ Ok = $true; Detail = "'$paper' $lenText x ${widMm} mm" }
  } catch {
    return @{ Ok = $false; Detail = $_.Exception.Message }
  } finally {
    if ($doc) { try { $doc.Close() } catch { } ; Release-Com $doc }
  }
}

<#
  Step 1 - render one label with b-PAC to a 300 dpi BMP. Throws with a "[stage]" prefix.
  Returns @{ Path; MediaName; WidthMm; LengthMm }
#>
function Invoke-BpacRender {
  param(
    [Parameter(Mandatory = $true)] [string] $TemplatePath,
    [Parameter(Mandatory = $true)] [hashtable] $Fields,
    [Parameter(Mandatory = $true)] [string] $PrinterName,
    [Parameter(Mandatory = $true)] [string] $BmpPath,
    [string] $LabelMediaName = '',
    [string] $FallbackMediaName = '',
    [double] $LabelWidthMm = 0,
    [double] $LabelLengthMm = 0,
    [double] $ToleranceMm = 1.0
  )
  if (-not (Test-Path $TemplatePath)) { throw "[opening] template not found: $TemplatePath" }
  $doc = $null
  try { $doc = New-Object -ComObject $BPAC_PROGID } catch {
    throw "[opening] b-PAC is not installed for this $([IntPtr]::Size * 8)-bit PowerShell: $($_.Exception.Message)"
  }
  $stage = 'opening'
  try {
    if (-not $doc.Open($TemplatePath)) { throw "b-PAC could not open the template (ErrorCode $(Get-BpacDocProp $doc 'ErrorCode')): $TemplatePath" }

    $stage = 'selecting printer'
    # The paper format list belongs to a printer driver, so the printer must be selected first.
    # fitPage = false: never let b-PAC rescale the objects to some other paper.
    if (-not $doc.SetPrinter($PrinterName, $false)) { throw "b-PAC refused printer '$PrinterName' (ErrorCode $(Get-BpacDocProp $doc 'ErrorCode'))" }

    $stage = 'checking media'
    $hint = ''
    $mediaUsed = ''
    if ($LabelMediaName -or $FallbackMediaName) {
      # b-PAC opens the template on the printer's CURRENT DEFAULT paper (see header), so the
      # document keeps the driver's paper unless we change it here.
      # MEASURED 2026-09-05 (QL-700): the registered long tape format was listed AND selectable,
      # but SetMediaByName also returns TRUE for names the driver does not have - a TRUE alone
      # proves nothing.
      # MEASURED 2026-09-06 (QL-800, Iroha PC): a registered long tape format is NEITHER listed
      # by GetSupportedMediaNames NOR selectable - SetMediaByName fails with ErrorCode 17367041.
      # Only the driver's own names work ('62mm' continuous, '62mm x 29mm', ...).
      # So: try the design format, then the fallback continuous format, and otherwise keep
      # whatever paper the driver is on. THE PROOF is the measured size checked below.
      $info = Get-BpacMediaInfo -PrinterName $PrinterName -FormatName $LabelMediaName -Doc $doc
      foreach ($cand in @($LabelMediaName, $FallbackMediaName)) {
        if (-not $cand) { continue }
        if (-not $doc.SetMediaByName([string]$cand, $false)) { continue }
        if ([string]$doc.GetMediaName() -eq [string]$cand) { $mediaUsed = [string]$cand; break }
      }
      $seen = if ($info.HasFormat) { 'listed' } else { 'NOT listed' }
      $hint = "b-PAC is on paper '$([string]$doc.GetMediaName())' (asked for '$LabelMediaName', $seen by the driver; fallback '$FallbackMediaName'). Fix: make sure a 62mm continuous roll is loaded, or set the $PrinterName default paper size to '$LabelMediaName' on the [Basic] tab of the printing preferences. Names b-PAC accepts here: $($info.Formats -join ' / ')"
    }
    $lenMm = ConvertTo-Mm ([int](Get-BpacDocProp $doc 'Length'))
    $widMm = ConvertTo-Mm ([int](Get-BpacDocProp $doc 'Width'))
    # The tape width is physical: it has to match.
    if ($LabelWidthMm -gt 0 -and [Math]::Abs($widMm - $LabelWidthMm) -gt $ToleranceMm) {
      throw "label width is ${widMm}mm but the design needs ${LabelWidthMm}mm (wrong tape or wrong paper). $hint"
    }
    # MEASURED 2026-09-06 (QL-800): on a CONTINUOUS format ('62mm') the document reports
    # Length = 0 - a continuous roll has no fixed length, so b-PAC sizes the label to its
    # content. Try to pin it to the design length anyway (the setter is ignored on die-cut
    # media - see the header - but it costs nothing to try here and would give an exact canvas).
    if ($lenMm -le 0 -and $LabelLengthMm -gt 0) {
      try { Set-BpacDocProp $doc 'Length' ([int][Math]::Round($LabelLengthMm * 1440 / 25.4)) } catch { }
      $lenMm = ConvertTo-Mm ([int](Get-BpacDocProp $doc 'Length'))
    }
    # Still 0 = auto length. That is fine: the objects keep their absolute positions from the
    # origin and Invoke-GdiPrint always prints on a ${LabelLengthMm}mm page, so a longer canvas
    # is clipped and a shorter one just leaves blank tape at the end.
    $autoLength = ($lenMm -le 0)
    # A FIXED canvas may be longer than the design but never shorter - that would cut the
    # barcode off before it reaches the paper.
    if (-not $autoLength -and $LabelLengthMm -gt 0 -and $lenMm -lt ($LabelLengthMm - $ToleranceMm)) {
      throw "label length is ${lenMm}mm but the design needs at least ${LabelLengthMm}mm. $hint"
    }
    $mediaName = [string]$doc.GetMediaName()

    $stage = 'filling'
    foreach ($name in $Fields.Keys) {
      $obj = $doc.GetObject([string]$name)
      if ($null -eq $obj) { throw "template has no object named '$name': $TemplatePath" }
      try { $obj.Text = [string]$Fields[$name] }
      finally { Release-Com $obj }
    }

    $stage = 'rendering'
    $dir = Split-Path -Parent $BmpPath
    if ($dir -and -not (Test-Path $dir)) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
    if (Test-Path $BmpPath) { [IO.File]::Delete($BmpPath) }
    if (-not $doc.Export($BPAC_EXPORT_BMP, $BmpPath, $RENDER_DPI)) { throw "export failed (ErrorCode $(Get-BpacDocProp $doc 'ErrorCode'))" }
    if (-not (Test-Path $BmpPath)) { throw "export reported success but no file appeared: $BmpPath" }
    # The BMP must measure what the document says, otherwise something rescaled it. With an
    # auto length canvas there is no length to compare against, so find the side that IS the
    # tape width and let the other side be whatever the content needed. Do not use max/min:
    # an auto length label can come out shorter than the 62mm tape is wide.
    $img = [System.Drawing.Image]::FromFile($BmpPath)
    try {
      $imgW = [Math]::Round($img.Width / $RENDER_DPI * 25.4, 1)
      $imgH = [Math]::Round($img.Height / $RENDER_DPI * 25.4, 1)
    } finally { $img.Dispose() }
    $wRef = if ($widMm -gt 0) { $widMm } else { $LabelWidthMm }
    if ($wRef -gt 0 -and [Math]::Abs($imgH - $wRef) -le $ToleranceMm) { $imgShort = $imgH; $imgLong = $imgW }
    elseif ($wRef -gt 0 -and [Math]::Abs($imgW - $wRef) -le $ToleranceMm) { $imgShort = $imgW; $imgLong = $imgH }
    else { throw "rendered image is ${imgW} x ${imgH} mm - neither side is the ${wRef}mm tape width" }
    if (-not $autoLength -and [Math]::Abs($imgLong - $lenMm) -gt $ToleranceMm) {
      throw "rendered image is ${imgLong} x ${imgShort} mm but the document is ${lenMm} x ${widMm} mm"
    }
    return @{ Path = $BmpPath; MediaName = $mediaName; WidthMm = $imgShort; LengthMm = $imgLong; AutoLength = $autoLength }
  } catch {
    $m = $_.Exception.Message
    if ($m -notmatch '^\[') { $m = "[{0}] {1}" -f $stage, $m }
    throw (New-Object System.Exception($m, $_.Exception))
  } finally {
    if ($doc) { try { $doc.Close() } catch { } }
    Release-Com $doc
  }
}

<#
  Step 2 - print a rendered label BMP through GDI with an explicit paper.
  The image is placed at the physical page corner (minus the driver's hard margins), so a
  BMP rendered by b-PAC for the same paper lands exactly where the template put things.
  Throws with "[printing]" prefix (from here on paper may come out).
  Returns @{ Pages; PageInfo }
#>
function Invoke-GdiPrint {
  param(
    [Parameter(Mandatory = $true)] [string] $BmpPath,
    [Parameter(Mandatory = $true)] [string] $PrinterName,
    [int] $Copies = 1,
    [string] $DocName = 'hakolabel',
    [int] $PaperRawKind = 259,
    [double] $WidthMm = 62,
    [double] $LengthMm = 67.2
  )
  $img = [System.Drawing.Image]::FromFile($BmpPath)
  $pd = New-Object System.Drawing.Printing.PrintDocument
  try {
    $pd.PrinterSettings.PrinterName = $PrinterName
    if (-not $pd.PrinterSettings.IsValid) { throw "[printing] GDI cannot use printer '$PrinterName'" }
    $pd.DocumentName = $DocName
    # PaperSize is in 1/100 inch. RawKind = the driver's dmPaperSize for "62mm" continuous;
    # the height is the label length the driver will feed and cut.
    $w100 = [int][Math]::Round($WidthMm / 25.4 * 100)
    $l100 = [int][Math]::Round($LengthMm / 25.4 * 100)
    $ps = New-Object System.Drawing.Printing.PaperSize('label', $w100, $l100)
    $ps.RawKind = $PaperRawKind
    $pd.DefaultPageSettings.PaperSize = $ps
    $pd.DefaultPageSettings.Landscape = $true      # template is landscape: length runs horizontally
    $pd.DefaultPageSettings.Margins = New-Object System.Drawing.Printing.Margins(0, 0, 0, 0)
    $pd.OriginAtMargins = $false
    $pd.PrintController = New-Object System.Drawing.Printing.StandardPrintController   # no progress dialog
    # The PrintPage handler runs as a delegate: hand it what it needs through script scope.
    $script:GdiImage = $img
    $script:GdiPagesLeft = $Copies
    $script:GdiPageInfo = ''
    $pd.add_PrintPage({
      param($sender, $e)
      $g = $e.Graphics
      $g.PageUnit = [System.Drawing.GraphicsUnit]::Display   # 1/100 inch
      $g.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::NearestNeighbor
      $im = $script:GdiImage
      $w = [single]($im.Width / $im.HorizontalResolution * 100.0)
      $h = [single]($im.Height / $im.VerticalResolution * 100.0)
      # (0,0) of the Graphics is the printable area's corner; shift back by the hard margins
      # so the image's own margins (same as the template's) line up with the physical label.
      $x = [single](-$e.PageSettings.HardMarginX)
      $y = [single](-$e.PageSettings.HardMarginY)
      $g.DrawImage($im, (New-Object System.Drawing.RectangleF($x, $y, $w, $h)))
      if (-not $script:GdiPageInfo) {
        $pb = $e.PageBounds
        $script:GdiPageInfo = "page {0}x{1} (1/100in) hardMargin {2},{3} image {4}x{5}px" -f $pb.Width, $pb.Height, $e.PageSettings.HardMarginX, $e.PageSettings.HardMarginY, $im.Width, $im.Height
      }
      $script:GdiPagesLeft--
      $e.HasMorePages = ($script:GdiPagesLeft -gt 0)
    })
    try { $pd.Print() } catch { throw "[printing] GDI print failed: $($_.Exception.Message)" }
    return @{ Pages = $Copies; PageInfo = $script:GdiPageInfo }
  } finally {
    try { $pd.Dispose() } catch { }
    try { $img.Dispose() } catch { }
    $script:GdiImage = $null
  }
}

<#
  Render or print ONE label design (n copies) from a template.

    -TemplatePath    .lbx copy WITHOUT database link (see make-auto-lbx.ps1)
    -Fields          hashtable  objectName -> text   (every object must exist in the template)
    -PrinterName     Windows printer name, used verbatim (no fallback to the default printer)
    -Copies          1..n identical labels (pages of one spool job)
    -DocName         spool document name; the agent uses it to find its own spool job
    -LabelMediaName  driver paper format to select for rendering (the registered 62 x 67.2mm
                     "long tape format"). Required for correct output - see header.
    -LabelWidthMm / -LabelLengthMm / -ToleranceMm
                     what the label must measure; refuse if the format does not
    -PaperRawKind    driver paper id used for the GDI print (259 = "62mm" continuous)
    -ExportBmp       when given, only render to this BMP (preview) - nothing is printed
    -RenderDir       where the BMP for printing is written (default: %TEMP%)

  Throws on any failure, with the stage in square brackets at the front of the message.
  Stages before "[printing]" mean nothing was handed to the spooler (clean failure);
  "[printing]" means labels may already be coming out (report uncertain, never re-print).
  Returns @{ Ok = $true; Stage = 'exported'|'printed'; MediaName; WidthMm; LengthMm; ... }
#>
function Invoke-BpacPrint {
  param(
    [Parameter(Mandatory = $true)] [string] $TemplatePath,
    [Parameter(Mandatory = $true)] [hashtable] $Fields,
    [string] $PrinterName = '',
    [int] $Copies = 1,
    [string] $DocName = 'hakolabel',
    [string] $LabelMediaName = '',
    [double] $LabelWidthMm = 0,
    [double] $LabelLengthMm = 0,
    [double] $ToleranceMm = 1.0,
    [int] $PaperRawKind = 259,
    [string] $FallbackMediaName = '',
    # Name for the evidence BMP when it must differ from DocName (two prints in one job:
    # the full cartons and the odd one share a document name so the spooler tracking sees both).
    [string] $BmpName = '',
    [string] $ExportBmp = '',
    [string] $RenderDir = ''
  )
  if ($Copies -lt 1) { throw "[opening] copies must be 1 or more (got $Copies)" }
  if (-not $PrinterName) { throw '[opening] no printer name given (the paper format lives in the printer driver)' }

  $bmp = $ExportBmp
  if (-not $bmp) {
    if (-not $RenderDir) { $RenderDir = $env:TEMP }
    $base = if ($BmpName) { $BmpName } else { $DocName }
    $safe = ($base -replace '[^A-Za-z0-9_.-]', '_')
    $bmp = Join-Path $RenderDir ("$safe.bmp")
  }
  $r = Invoke-BpacRender -TemplatePath $TemplatePath -Fields $Fields -PrinterName $PrinterName -BmpPath $bmp `
    -LabelMediaName $LabelMediaName -FallbackMediaName $FallbackMediaName `
    -LabelWidthMm $LabelWidthMm -LabelLengthMm $LabelLengthMm -ToleranceMm $ToleranceMm
  if ($ExportBmp) {
    return @{ Ok = $true; Stage = 'exported'; Path = $r.Path; MediaName = $r.MediaName; WidthMm = $r.WidthMm; LengthMm = $r.LengthMm }
  }
  # From here on paper may come out - the caller must treat exceptions as "uncertain".
  $lenForDriver = if ($LabelLengthMm -gt 0) { $LabelLengthMm } else { $r.LengthMm }
  $widForDriver = if ($LabelWidthMm -gt 0) { $LabelWidthMm } else { $r.WidthMm }
  $p = Invoke-GdiPrint -BmpPath $r.Path -PrinterName $PrinterName -Copies $Copies -DocName $DocName `
    -PaperRawKind $PaperRawKind -WidthMm $widForDriver -LengthMm $lenForDriver
  return @{ Ok = $true; Stage = 'printed'; Copies = $Copies; Path = $r.Path; MediaName = $r.MediaName; WidthMm = $r.WidthMm; LengthMm = $r.LengthMm; PageInfo = $p.PageInfo }
}

<#
  Read config.json (or config.example.json when there is no config.json yet).
#>
function Get-PrintConfig {
  param([string] $Path = '')
  if (-not $Path) {
    $Path = Join-Path $script:PrintLabelDir 'config.json'
    if (-not (Test-Path $Path)) { $Path = Join-Path $script:PrintLabelDir 'config.example.json' }
  }
  if (-not (Test-Path $Path)) { throw "config not found: $Path" }
  $c = Get-Content $Path -Raw -Encoding UTF8 | ConvertFrom-Json
  foreach ($k in @('objects', 'templates', 'label')) { if (-not $c.$k) { throw "config is missing '$k': $Path" } }
  foreach ($k in @('productName', 'packQty', 'jan', 'fnsku')) { if (-not $c.objects.$k) { throw "config objects.$k is missing: $Path" } }
  foreach ($k in @('jan', 'fnsku')) { if (-not $c.templates.$k) { throw "config templates.$k is missing: $Path" } }
  foreach ($k in @('mediaName', 'widthMm', 'lengthMm', 'paperRawKind')) { if (-not $c.label.$k) { throw "config label.$k is missing: $Path" } }
  return $c
}

<#
  Build the objectName -> text table for one label from the config's object names.
  Only the barcode object of the chosen type is filled (for the box label both types name
  the same CODE128 object). "expiry" is optional: written only when config.objects.expiry
  names an object; an empty string blanks that object on the label.
#>
function New-LabelFields {
  param($Config, [string] $BarcodeType, [string] $Barcode, [string] $ProductName, [string] $PackQty, [string] $Expiry = '')
  $o = $Config.objects
  $f = @{}
  $f[[string]$o.productName] = [string]$ProductName
  $f[[string]$o.packQty]     = [string]$PackQty
  if ($o.expiry) { $f[[string]$o.expiry] = [string]$Expiry }
  if ($BarcodeType -eq 'jan')        { $f[[string]$o.jan]   = [string]$Barcode }
  elseif ($BarcodeType -eq 'fnsku')  { $f[[string]$o.fnsku] = [string]$Barcode }
  else { throw "unknown barcode type '$BarcodeType' (jan / fnsku)" }
  return $f
}

function Resolve-TemplatePath {
  param($Config, [string] $BarcodeType)
  $p = [string]$Config.templates.$BarcodeType
  if (-not $p) { throw "config has no template for barcode type '$BarcodeType'" }
  if (-not [IO.Path]::IsPathRooted($p)) { $p = Join-Path $script:PrintLabelDir $p }
  return $p
}

function Get-ConfigInt {
  param($Config, [string] $Name, [int] $Default)
  $v = $Config.$Name
  if ($null -eq $v -or [string]$v -eq '') { return $Default }
  return [int]$v
}

# The label geometry arguments for Invoke-BpacPrint, straight from config.label.
function Get-LabelArgs {
  param($Config)
  $l = $Config.label
  $tol = if ($null -ne $l.toleranceMm -and [string]$l.toleranceMm -ne '') { [double]$l.toleranceMm } else { 1.0 }
  return @{
    LabelMediaName = [string]$l.mediaName; LabelWidthMm = [double]$l.widthMm; LabelLengthMm = [double]$l.lengthMm
    ToleranceMm = $tol; PaperRawKind = [int]$l.paperRawKind
    # Optional. The driver's own continuous format to fall back on when the design format cannot
    # be selected (QL-800). A longer canvas is fine: Invoke-GdiPrint prints on a lengthMm page.
    FallbackMediaName = [string]$l.fallbackMediaName
  }
}

if ($Library) { return }

# ---------------------------------------------------------------- command-line use

[Console]::OutputEncoding = [Text.Encoding]::UTF8
$avail = Test-BpacAvailable
Write-Host $avail.Detail
if (-not $avail.Ok) {
  Write-Host 'Install the b-PAC Client Component (Brother developer site, free, needs user registration).'
  Write-Host 'Pick the bitness of the PowerShell that will run this (64-bit powershell.exe -> 64-bit component).'
  exit 2
}
if (-not $BarcodeType -or -not $Barcode) { Write-Host 'nothing to do: give -BarcodeType jan|fnsku and -Barcode (see the header for examples)'; exit 0 }
if (-not $ExportBmp -and -not $Print) { Write-Host 'refusing to print without -Print (use -ExportBmp <file> for a preview)'; exit 1 }

$cfg = Get-PrintConfig $ConfigPath
$fields = New-LabelFields $cfg $BarcodeType $Barcode $ProductName $PackQty $Expiry
$tpl = Resolve-TemplatePath $cfg $BarcodeType
$label = Get-LabelArgs $cfg
if (-not $DocName) { $DocName = 'hakolabel-manual-' + (Get-Date -Format 'HHmmss') }
if (-not $Printer) { $Printer = [string]$cfg.printerName }
if (-not $Printer) { throw 'give -Printer (or set printerName in config.json)' }

Write-Host ("template : {0}" -f $tpl)
Write-Host ("fields   : {0}" -f (($fields.Keys | ForEach-Object { "{0}=[{1}]" -f $_, $fields[$_] }) -join '  '))
$info = Get-BpacMediaInfo -PrinterName $Printer -FormatName $label.LabelMediaName
Write-Host ("driver   : format '{0}' {1}; printer online={2}; b-PAC says supported={3}; printer reports '{4}' ({5})" -f $label.LabelMediaName, $(if ($info.HasFormat) { 'registered' } else { 'NOT REGISTERED' }), $info.Online, $info.Supported, $info.ReportedName, $info.ReportedId)
try {
  if ($ExportBmp) {
    $r = Invoke-BpacPrint -TemplatePath $tpl -Fields $fields -PrinterName $Printer -ExportBmp $ExportBmp @label
    Write-Host ("exported : {0}  (paper '{1}' {2} x {3} mm)" -f $r.Path, $r.MediaName, $r.WidthMm, $r.LengthMm)
  } else {
    Write-Host ("printer  : {0}  copies={1}  docName={2}  paper raw={3} {4}x{5}mm" -f $Printer, $Copies, $DocName, $label.PaperRawKind, $label.LabelWidthMm, $label.LabelLengthMm)
    $r = Invoke-BpacPrint -TemplatePath $tpl -Fields $fields -PrinterName $Printer -Copies $Copies -DocName $DocName @label
    Write-Host ("printed  : {0} label(s) handed to the spooler ({1})" -f $r.Copies, $r.PageInfo)
  }
} catch {
  Write-Host ("FAILED   : {0}" -f $_.Exception.Message)
  exit 1
}
