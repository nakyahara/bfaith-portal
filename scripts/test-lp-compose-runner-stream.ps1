# test-lp-compose-runner-stream.ps1 - unit tests for the model check in scripts\ph-nightly\run-lp-compose.ps1 (ASCII only).
# Run: powershell -NoProfile -ExecutionPolicy Bypass -File scripts\test-lp-compose-runner-stream.ps1
#
# The runner is a script with top-level code, so it cannot be dot-sourced. The two functions under test are taken
# out of its AST and defined here. Invoke-RestMethod is shadowed by a function (functions win over cmdlets) so no
# HTTP is sent. Fixtures follow the stream-json observed on the miniPC (Claude Code 2.1.280, 2026-10-02):
#   system init model = claude-opus-5-5[1m], assistant message.model = claude-opus-5-5 (no [1m]).
$ErrorActionPreference = 'Stop'
$Runner = Join-Path $PSScriptRoot 'ph-nightly\run-lp-compose.ps1'
$tokens = $null; $errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($Runner, [ref]$tokens, [ref]$errors)
foreach ($name in @('Read-ClaudeStream', 'New-ModelCheckBody', 'Send-ModelCheckBody')) {
  $fn = $ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name }, $true)
  if (-not $fn) { throw ('function not found in the runner: ' + $name) }
  . ([scriptblock]::Create($fn.Extent.Text))
}

$pass = 0; $fail = 0
function Ok([bool]$c, [string]$label) { if ($c) { $script:pass++; Write-Output ('  ok  ' + $label) } else { $script:fail++; Write-Output ('  NG  ' + $label) } }
$tmp = Join-Path 'C:\tmp' ('lp-stream-test-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
New-Item -ItemType Directory -Force -Path $tmp | Out-Null
function Fixture([string[]]$lines) {
  $f = Join-Path $tmp ([guid]::NewGuid().ToString('N') + '.jsonl')
  [IO.File]::WriteAllText($f, ($lines -join "`n") + "`n", (New-Object Text.UTF8Encoding($false)))
  return $f
}
$init = '{"type":"system","subtype":"init","model":"claude-opus-5-5[1m]"}'
$main = '{"type":"assistant","message":{"model":"claude-opus-5-5","content":[{"type":"text","text":"ok"}]},"parent_tool_use_id":null}'
$sub = '{"type":"assistant","message":{"model":"claude-haiku-4-5","content":[]},"parent_tool_use_id":"toolu_1"}'
# a tool result that QUOTES event-like text: escaped quotes inside a string must not be read as an event
$userQuote = '{"type":"user","message":{"content":[{"type":"tool_result","content":"{\"type\":\"assistant\",\"message\":{\"model\":\"claude-sonnet-5\"}}"}]}}'
$ok = '{"type":"result","subtype":"success","is_error":false,"result":"job=1 status=done","modelUsage":{"claude-opus-5-5[1m]":{},"claude-haiku-4-5-20251001":{}}}'

try {
  Write-Output '[1] Read-ClaudeStream'
  $r = Read-ClaudeStream (Fixture @($init, $main, $sub, $userQuote, $main, $ok))
  Ok (($r.Models -join ',') -eq 'claude-opus-5-5') 'main answer model only (sub-agent Haiku and init [1m] are not evidence)'
  Ok (-not $r.Unreadable -and $r.HasResult -and -not $r.IsError) 'a clean run is readable, has a result, no error'

  $r = Read-ClaudeStream (Fixture @($init, $main, '{"type":"assistant","message":{"model":"claude-sonnet-5"},"parent_tool_use_id":null}', $ok))
  Ok (($r.Models -join ',') -eq 'claude-opus-5-5,claude-sonnet-5') 'two main models are both reported (the server calls it a mismatch)'

  $r = Read-ClaudeStream (Fixture @($init, '{"type":"assistant","message":{"model":"gpt-5.6-sol"},"parent_tool_use_id":null}', $ok))
  Ok ($r.Unreadable) 'a model string of another shape makes the run unreadable (never a match)'

  $r = Read-ClaudeStream (Fixture @($init, '{"type":"assistant","message":{"model":"claude-opus-5-5\"],\"x\":[\""},"parent_tool_use_id":null}', $ok))
  Ok ($r.Unreadable -and $r.Models.Count -eq 0) 'quotes in a model string never reach the JSON body'

  $r = Read-ClaudeStream (Fixture @($init, '{"type":"result","is_error":true,"result":"API Error: 400 Claude Code 2.1.252 does not support this model"}'))
  Ok ($r.IsError -and $r.ErrorText -like 'API Error: 400*' -and $r.Models.Count -eq 0) 'an early error is reported with its text and no main model'

  $r = Read-ClaudeStream (Fixture @($init, $main, '{"type":"assistant", broken'))
  Ok ($r.Unreadable) 'a broken event line makes the run unreadable'

  $r = Read-ClaudeStream (Fixture @($init, $main))
  Ok (-not $r.HasResult) 'a killed run (no result event) is visible as HasResult=false'

  $r = Read-ClaudeStream (Join-Path $tmp 'missing.jsonl')
  Ok ($r.Unreadable) 'a missing output file is unreadable'

  Write-Output '[2] New-ModelCheckBody / Send-ModelCheckBody (Invoke-RestMethod is shadowed; nothing is sent)'
  $TokenFile = Join-Path $tmp 'token.txt'; Set-Content -LiteralPath $TokenFile -Value 'test-token' -Encoding ascii
  $Base = 'https://example.invalid/service-api'
  $script:sent = @()
  $script:failNext = $false
  function Invoke-RestMethod { param($Method, $Uri, $Headers, $ContentType, $Body, $TimeoutSec)
    if ($script:failNext) { throw 'simulated network error' }
    $script:sent += @{ Uri = $Uri; Body = $Body; Auth = $Headers.Authorization }
    return @{ checks = @(@{ generation_id = 1; model_check = 'match' }) } }
  $b = New-ModelCheckBody 'lpr-20261002-150000-abc123' @('claude-opus-5-5') | ConvertFrom-Json
  Ok ($b.runner_run_id -eq 'lpr-20261002-150000-abc123' -and @($b.actual_models).Count -eq 1 -and $b.actual_models[0] -eq 'claude-opus-5-5') 'one model is sent as a one-element array'
  $raw = New-ModelCheckBody 'lpr-x' @()
  Ok ($raw -match '"actual_models":\[\]' -and @(($raw | ConvertFrom-Json).actual_models).Count -eq 0) 'no model is sent as an empty array (= unknown on the server)'
  Ok (((New-ModelCheckBody 'lpr-x' @('claude-opus-5-5', 'claude-sonnet-5') | ConvertFrom-Json).actual_models -join ',') -eq 'claude-opus-5-5,claude-sonnet-5') 'two models are sent as two'
  $null = Send-ModelCheckBody $raw
  Ok ($script:sent[-1].Uri -eq 'https://example.invalid/service-api/lp-compose/model-check') 'posts to /lp-compose/model-check'
  Ok ($script:sent[-1].Auth -eq 'Bearer test-token') 'with the service token'

  Write-Output '[3] Send-PendingModelChecks (a check that could not be sent is re-sent until the server answers)'
  $fn = $ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'Send-PendingModelChecks' }, $true)
  . ([scriptblock]::Create($fn.Extent.Text))
  $StateDir = Join-Path $tmp 'state'; New-Item -ItemType Directory -Force -Path $StateDir | Out-Null
  $script:logs = @()
  function Log([string]$m) { $script:logs += $m }
  $p1 = Join-Path $StateDir 'lp-model-check-lpr-a.json'
  [IO.File]::WriteAllText($p1, (New-ModelCheckBody 'lpr-a' @('claude-opus-5-5')))
  $old = Join-Path $StateDir 'lp-model-check-lpr-old.json'
  [IO.File]::WriteAllText($old, (New-ModelCheckBody 'lpr-old' @()))
  (Get-Item $old).LastWriteTime = (Get-Date).AddDays(-2)
  $script:sent = @()
  $script:failNext = $true
  Send-PendingModelChecks
  Ok ((Test-Path $p1) -and $script:sent.Count -eq 0) 'a failed re-send keeps the file'
  Ok (-not (Test-Path $old)) 'a file older than 1 day is dropped (the server has closed it as unknown)'
  $script:failNext = $false
  Send-PendingModelChecks
  Ok (-not (Test-Path $p1)) 'a successful re-send removes the file'
  Ok ($script:sent.Count -eq 1 -and ($script:sent[0].Body | ConvertFrom-Json).runner_run_id -eq 'lpr-a') 'the stored body is sent as it was'
  Ok (($script:logs -join ' ') -match 'lpr-a.json -> match') 'the answer is logged'
} finally {
  Remove-Item -LiteralPath $tmp -Recurse -Force -ErrorAction SilentlyContinue
}
Write-Output ''
Write-Output ("$pass PASS / $fail FAIL")
if ($fail -gt 0) { exit 1 }
