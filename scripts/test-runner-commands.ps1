# test-runner-commands.ps1 - static check: every command a Claude runner calls must exist (ASCII only).
# Run: powershell -NoProfile -ExecutionPolicy Bypass -File scripts\test-runner-commands.ps1
#
# Why (2026-10-02): run-lp-compose.ps1 called Test-ClaudeStartable, a helper defined only inside
# run-ph-generate.ps1. With ErrorActionPreference=Continue the CommandNotFound error left the try via finally,
# Claude was never started, and the runner logged "nothing moved" every minute while the request waited.
# A runner that is only ever exercised on the miniPC needs this check here, before it is installed.
#
# A command name counts as defined when it is a function in the runner itself, a function in ClaudeGuard.ps1
# (all these runners dot-source it), or something Get-Command resolves (cmdlet / alias / exe on PATH).
# Dynamic calls (& $var) have no static name and are skipped.
$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent $PSScriptRoot
$Guard = Join-Path $PSScriptRoot 'claude-guard\ClaudeGuard.ps1'
$Runners = @(
  'scripts\ph-nightly\run-lp-compose.ps1',
  'scripts\ph-nightly\run-ph-generate.ps1',
  'scripts\product-idea-scout\ai\run-keywords.ps1'
)

function Parse([string]$path) {
  $tokens = $null; $errors = $null
  $ast = [System.Management.Automation.Language.Parser]::ParseFile($path, [ref]$tokens, [ref]$errors)
  return [pscustomobject]@{ Ast = $ast; Errors = @($errors) }
}
function FunctionNames($ast) {
  return @($ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] }, $true) | ForEach-Object { $_.Name })
}

$pass = 0; $fail = 0
function Ok([bool]$c, [string]$label) { if ($c) { $script:pass++; Write-Output ('  ok  ' + $label) } else { $script:fail++; Write-Output ('  NG  ' + $label) } }

$g = Parse $Guard
Ok ($g.Errors.Count -eq 0) 'ClaudeGuard.ps1 parses'
$guardFns = FunctionNames $g.Ast

foreach ($rel in $Runners) {
  $path = Join-Path $Root $rel
  Write-Output ('[' + $rel + ']')
  $r = Parse $path
  Ok ($r.Errors.Count -eq 0) 'parses without errors'
  $defined = @(FunctionNames $r.Ast) + $guardFns
  $calls = @($r.Ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.CommandAst] }, $true) |
    ForEach-Object { $_.GetCommandName() } | Where-Object { $_ } | Sort-Object -Unique)
  $missing = @($calls | Where-Object {
    $name = $_
    -not ($defined -contains $name) -and -not (Get-Command -Name $name -ErrorAction SilentlyContinue)
  })
  Ok ($missing.Count -eq 0) ('every called command exists (' + $calls.Count + ' names)' + $(if ($missing.Count) { ' - missing: ' + ($missing -join ', ') } else { '' }))
}

# The check itself must catch the 2026-10-02 bug: a runner calling a helper that only another runner defines.
Write-Output '[self-check]'
$tmp = Join-Path 'C:\tmp' ('runner-commands-test-' + [guid]::NewGuid().ToString('N').Substring(0, 8) + '.ps1')
try {
  Set-Content -LiteralPath $tmp -Encoding ascii -Value "if (-not (Test-ClaudeStartable)) { exit 1 }`r`n`$r = Test-ReadyToStartClaude"
  $bad = Parse $tmp
  $badCalls = @($bad.Ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.CommandAst] }, $true) | ForEach-Object { $_.GetCommandName() })
  $badMissing = @($badCalls | Where-Object { -not ($guardFns -contains $_) -and -not (Get-Command -Name $_ -ErrorAction SilentlyContinue) })
  Ok (($badMissing -join ',') -eq 'Test-ClaudeStartable') 'a helper defined only in another runner is reported; a ClaudeGuard function is not'
} finally {
  Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
}

Write-Output ''
Write-Output ("$pass PASS / $fail FAIL")
if ($fail -gt 0) { exit 1 }
