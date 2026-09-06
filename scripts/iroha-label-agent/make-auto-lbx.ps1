#Requires -Version 5.1
<#
  Build the automation copies of the Iroha storage-box label template (.lbx).

  Why this exists:
    The hand-operated template is linked to a CSV (P-touch Editor database merge). When
    b-PAC opens a template that still carries that link it reads the CSV and may overwrite
    whatever we put into the objects. The agent therefore prints from copies that have NO
    database link. The layout is otherwise untouched.

  Why two copies:
    The agent (shared design with the warehouse nefuda agent) picks a template by barcode
    type: "jan" (digits only) or "fnsku" (Amazon code with letters). Each copy keeps exactly
    one barcode object - the one templates.json names for that type - and drops any other
    barcode object. The box label has a single CODE128 object, so both copies keep the same
    object and are identical; the two files exist only to keep the agent contract unchanged:
      <outDir>/<outNames.jan>    - used for JAN barcodes
      <outDir>/<outNames.fnsku>  - used for FNSKU barcodes

  The .lbx file is a plain ZIP holding label.xml + prop.xml (UTF-8, no BOM).

  Inputs come from templates.json next to this script (paths and object names contain
  Japanese, this file must stay ASCII):
    source      - the hand-operated template (never modified)
    outDir      - where to write the copies (relative to this folder or absolute)
    textObjects    - object names the agent writes text into; must exist in the template
    barcodeObjects - { jan, fnsku }: the barcode object to keep for each type
    outNames       - { jan, fnsku }: file names of the copies

  Usage:
    powershell -ExecutionPolicy Bypass -File make-auto-lbx.ps1
    powershell -ExecutionPolicy Bypass -File make-auto-lbx.ps1 -Source "X:\a.lbx" -OutDir ".\templates"

  Re-run whenever the hand-operated template changes layout, then re-run setup.
  ASCII only in this file (Task Scheduler + PowerShell 5.1 safety - same rule as agent.ps1).
#>
[CmdletBinding()]
param(
  [string] $Source = '',
  [string] $OutDir = ''
)

$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [Text.Encoding]::UTF8
$ScriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }

$cfgPath = Join-Path $ScriptDir 'templates.json'
if (-not (Test-Path $cfgPath)) { throw "templates.json not found next to this script: $cfgPath" }
$cfg = Get-Content $cfgPath -Raw -Encoding UTF8 | ConvertFrom-Json
if (-not $Source) { $Source = [string]$cfg.source }
if (-not $OutDir) { $OutDir = [string]$cfg.outDir }
$TextObjects = @($cfg.textObjects | ForEach-Object { [string]$_ })
if (-not $Source) { throw 'templates.json has no "source"' }
if (-not $OutDir) { $OutDir = 'templates' }
if ($TextObjects.Count -eq 0) { throw 'templates.json has no "textObjects"' }
$BarcodeObjects = $cfg.barcodeObjects
$OutNames = $cfg.outNames
if (-not $BarcodeObjects -or -not $BarcodeObjects.jan -or -not $BarcodeObjects.fnsku) { throw 'templates.json has no "barcodeObjects" (jan / fnsku object names)' }
if (-not $OutNames -or -not $OutNames.jan -or -not $OutNames.fnsku) { throw 'templates.json has no "outNames" (jan / fnsku file names)' }
if (-not [IO.Path]::IsPathRooted($OutDir)) { $OutDir = Join-Path $ScriptDir $OutDir }
if (-not (Test-Path $Source)) { throw "source template not found: $Source" }
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem

function Read-ZipEntryBytes {
  param([IO.Compression.ZipArchive] $Zip, [string] $Name)
  $e = $Zip.GetEntry($Name)
  if (-not $e) { throw "entry '$Name' missing in the source (is this really a .lbx?)" }
  $ms = New-Object IO.MemoryStream
  $s = $e.Open()
  try { $s.CopyTo($ms) } finally { $s.Dispose() }
  return $ms.ToArray()
}

$zip = [IO.Compression.ZipFile]::OpenRead($Source)
try {
  $labelBytes = Read-ZipEntryBytes $zip 'label.xml'
  $propBytes  = Read-ZipEntryBytes $zip 'prop.xml'
} finally { $zip.Dispose() }
$utf8 = New-Object Text.UTF8Encoding($false)
$labelXml = $utf8.GetString($labelBytes)
if ($labelXml.Length -gt 0 -and [int][char]$labelXml[0] -eq 0xFEFF) { $labelXml = $labelXml.Substring(1) }

function Convert-Template {
  param([string] $Xml, [string] $KeepBarcode, [string[]] $RequiredTexts)

  $doc = New-Object Xml.XmlDocument
  $doc.PreserveWhitespace = $true
  $doc.LoadXml($Xml)
  $ns = New-Object Xml.XmlNamespaceManager($doc.NameTable)
  $ns.AddNamespace('pt',       'http://schemas.brother.info/ptouch/2007/lbx/main')
  $ns.AddNamespace('barcode',  'http://schemas.brother.info/ptouch/2007/lbx/barcode')
  $ns.AddNamespace('database', 'http://schemas.brother.info/ptouch/2007/lbx/database')

  # 1. cut the database link (the whole <database:database> block)
  $dbNodes = @($doc.SelectNodes('//database:database', $ns))
  if ($dbNodes.Count -eq 0) { Write-Warning 'source has no database link (nothing to cut) - continuing' }
  foreach ($n in $dbNodes) { [void]$n.ParentNode.RemoveChild($n) }

  # 2. objects must not remember the merge field either
  $merged = @($doc.SelectNodes('//pt:expanded[@dbMergeFieldStyleName]', $ns))
  foreach ($n in $merged) {
    $n.RemoveAttribute('dbMergeFieldStyleName')
    if ($n.HasAttribute('dbRecordOffset')) { $n.RemoveAttribute('dbRecordOffset') }
  }

  # 3. keep exactly one barcode object (the one named $KeepBarcode); drop every other barcode object
  $found = @{}
  $bars = @($doc.SelectNodes('//barcode:barcode', $ns))
  foreach ($b in $bars) {
    $exp = $b.SelectSingleNode('pt:objectStyle/pt:expanded', $ns)
    $name = if ($exp) { [string]$exp.GetAttribute('objectName') } else { '' }
    $found[$name] = $true
    if ($name -ne $KeepBarcode) { [void]$b.ParentNode.RemoveChild($b) }
  }
  if (-not $found.ContainsKey($KeepBarcode)) { throw "source template has no barcode object named '$KeepBarcode' (found: $($found.Keys -join ', '))" }

  # 4. the text objects the agent writes into must exist by name
  $names = @($doc.SelectNodes('//pt:expanded', $ns) | ForEach-Object { [string]$_.GetAttribute('objectName') })
  foreach ($required in $RequiredTexts) {
    if ($names -notcontains $required) { throw "text object '$required' not found in source template (object names: $($names -join ', '))" }
  }

  # write back as UTF-8 without BOM, declaration included, no re-indentation
  $settings = New-Object Xml.XmlWriterSettings
  $settings.Encoding = New-Object Text.UTF8Encoding($false)
  $settings.OmitXmlDeclaration = $false
  $settings.Indent = $false
  $ms = New-Object IO.MemoryStream
  $xw = [Xml.XmlWriter]::Create($ms, $settings)
  $doc.Save($xw)
  $xw.Dispose()
  return $ms.ToArray()
}

function Write-Lbx {
  param([string] $Path, [byte[]] $LabelBytes, [byte[]] $PropBytes)
  $tmp = "$Path.tmp"
  if (Test-Path $tmp) { Remove-Item $tmp -Force }
  $fs = [IO.File]::Open($tmp, [IO.FileMode]::CreateNew)
  try {
    $za = New-Object IO.Compression.ZipArchive($fs, [IO.Compression.ZipArchiveMode]::Create, $false)
    try {
      foreach ($pair in @(@('label.xml', $LabelBytes), @('prop.xml', $PropBytes))) {
        $entry = $za.CreateEntry($pair[0], [IO.Compression.CompressionLevel]::Optimal)
        $s = $entry.Open()
        try { $s.Write($pair[1], 0, $pair[1].Length) } finally { $s.Dispose() }
      }
    } finally { $za.Dispose() }
  } finally { $fs.Dispose() }
  # write-then-move: never leave a half-written template where the agent could open it
  Move-Item $tmp $Path -Force
}

$janBytes   = Convert-Template -Xml $labelXml -KeepBarcode ([string]$BarcodeObjects.jan)   -RequiredTexts $TextObjects
$fnskuBytes = Convert-Template -Xml $labelXml -KeepBarcode ([string]$BarcodeObjects.fnsku) -RequiredTexts $TextObjects

$janPath   = Join-Path $OutDir ([string]$OutNames.jan)
$fnskuPath = Join-Path $OutDir ([string]$OutNames.fnsku)
Write-Lbx $janPath   $janBytes   $propBytes
Write-Lbx $fnskuPath $fnskuBytes $propBytes

# self-check: reopen and prove the link is gone and exactly one barcode object remains
foreach ($p in @($janPath, $fnskuPath)) {
  $z = [IO.Compression.ZipFile]::OpenRead($p)
  try { $x = $utf8.GetString((Read-ZipEntryBytes $z 'label.xml')) } finally { $z.Dispose() }
  if ($x -match 'database:database') { throw "database link still present in $p" }
  if ($x -match 'dbMergeFieldStyleName') { throw "merge field attribute still present in $p" }
  if ($x -notmatch '^<\?xml version="1.0" encoding="utf-8"\?>') { throw "unexpected XML declaration in $p : $($x.Substring(0, [Math]::Min(60, $x.Length)))" }
  $barCount = ([regex]::Matches($x, '<barcode:barcode>')).Count
  if ($barCount -ne 1) { throw "expected exactly 1 barcode object in $p, found $barCount" }
  $chk = New-Object Xml.XmlDocument
  $chk.LoadXml($x)   # throws if not well-formed
  Write-Host ("OK  {0}  ({1} bytes, 1 barcode object, no database link)" -f $p, (Get-Item $p).Length)
}
Write-Host ("source: {0} (modified {1:yyyy-MM-dd HH:mm})" -f $Source, (Get-Item $Source).LastWriteTime)
