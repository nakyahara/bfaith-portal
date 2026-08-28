/**
 * test-print-agent.mjs — 出荷PCの印刷エージェント (scripts/slip-print-agent/) の検証
 *
 * ここで守りたいのは2つ。
 *   ① プリンター名が**そのまま1つの引数として**印刷コマンドに渡ること
 *   ② スクリプトが ASCII だけで書かれ、構文が壊れていないこと
 *
 * ①が一番危ない。実測 (2026-08-28) では `Start-Process -ArgumentList` は配列で渡しても
 * スペース入りの名前を**分断する**:
 *     -ArgumentList @('-print-to','Munbyn ITPP941(300DPI)', ...)
 *       → 実行ファイルは [-print-to] [Munbyn] [ITPP941(300DPI)] を受け取る
 * これが起きると、存在しないプリンター宛に刷ろうとするか、最悪**別のプリンターから出る**。
 * 呼び出し演算子 + splat (`& $exe @argv`) なら日本語名 (ネコポス / 発払) も含めて壊れない。
 *
 * ②は PS5.1 + タスクスケジューラが日本語入り .ps1 を壊すため (過去2回ハマっている)。
 *
 * ⚠ HTTP を通した通しテストはここではやらない — この環境の PowerShell はローカルの
 *   HTTPサーバに到達できない (Node→Node は通る)。実機での確認手順は
 *   scripts/slip-print-agent/README.md の「動くか1回だけ試す」。
 *
 * PowerShell が無い環境ではスキップする。
 * 実行: node apps/packing/tests/test-print-agent.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const AGENT_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)),
  '..', '..', '..', 'scripts', 'slip-print-agent');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };

let psExe = null;
for (const cand of ['powershell.exe', 'powershell', 'pwsh']) {
  try {
    execFileSync(cand, ['-NoProfile', '-Command', '$PSVersionTable.PSVersion.Major'], { stdio: 'pipe' });
    psExe = cand; break;
  } catch { /* 次の候補 */ }
}

console.log('── スクリプトが ASCII だけで書かれているか ──');
for (const f of ['agent.ps1', 'install.ps1']) {
  const src = fs.readFileSync(path.join(AGENT_DIR, f), 'utf8');
  const bad = src.split('\n').map((l, i) => [i + 1, l])
    .filter(([, l]) => [...l].some((c) => c.charCodeAt(0) > 127));
  ok(bad.length === 0,
    `${f} は ASCII のみ (PS5.1+タスクスケジューラが日本語入り .ps1 を壊すため)`
    + (bad.length ? ` — ${bad.length}行が非ASCII (最初=${bad[0][0]}行目)` : ''));
}

if (!psExe) {
  console.log('\n⏭ PowerShell が無いので以降はスキップ (Windows 以外)');
  console.log(`\n${failed === 0 ? '✅ 全テスト PASS' : `❌ ${failed} 件失敗`}`);
  process.exitCode = failed === 0 ? 0 : 1;
} else {
  console.log('\n── PowerShell として構文が通るか ──');
  for (const f of ['agent.ps1', 'install.ps1']) {
    const p = path.join(AGENT_DIR, f).replace(/\\/g, '\\\\');
    const out = execFileSync(psExe, ['-NoProfile', '-Command',
      `$e = $null
       [void][System.Management.Automation.Language.Parser]::ParseFile("${p}", [ref]$null, [ref]$e)
       if ($e.Count -eq 0) { 'OK' } else { $e | ForEach-Object { $_.Message } }`],
      { encoding: 'utf8' }).trim();
    ok(out === 'OK', `${f} の構文${out === 'OK' ? '' : ` — ${out}`}`);
  }

  console.log('\n── 🚨 プリンター名が分断されずに渡るか (実機で誤ったプリンターに出さないため) ──');
  // ASCIIパスを使う: cmd.exe/バッチは CP932 で読まれるため日本語を含むパスだと化ける
  const tmp = fs.mkdtempSync(path.join('C:', 'tmp', 'agent-argtest-'));
  const out = path.join(tmp, 'got.txt');
  const echoPs1 = path.join(tmp, 'echo.ps1');
  fs.writeFileSync(echoPs1,
    '$a = $args | ForEach-Object { "[" + $_ + "]" }\n'
    + '[IO.File]::WriteAllLines(' + JSON.stringify(out) + ', $a, [Text.UTF8Encoding]::new($false))\n'
    + 'exit 0\n', 'utf8');

  // エージェント本体が使っている呼び方を、そのまま抜き出して試す
  const agentSrc = fs.readFileSync(path.join(AGENT_DIR, 'agent.ps1'), 'utf8');
  ok(/&\s+\$Sumatra\s+@argv/.test(agentSrc),
    'agent.ps1 が呼び出し演算子 + splat (& $Sumatra @argv) を使っている');
  ok(!/Start-Process\s+-FilePath\s+\$Sumatra/.test(agentSrc),
    'agent.ps1 が Start-Process -ArgumentList を使っていない (名前を分断するため)');

  for (const printer of ['Munbyn ITPP941(300DPI)', 'ネコポス', '発払']) {
    try { fs.unlinkSync(out); } catch { /* 初回 */ }
    const script = [
      '$ErrorActionPreference = "Stop"',
      '[Console]::OutputEncoding = [Text.Encoding]::UTF8',
      '$exe = ' + JSON.stringify(psExe),
      '$printer = ' + JSON.stringify(printer),
      '$argv = @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", ' + JSON.stringify(echoPs1) + ',',
      '          "-print-to", $printer, "-print-settings", "noscale", "-silent", "C:\\x\\a.pdf")',
      '& $exe @argv | Out-Null',
    ].join('\n');
    execFileSync(psExe, ['-NoProfile', '-Command', script], { encoding: 'utf8' });
    const got = fs.readFileSync(out, 'utf8').trim().split(/\r?\n/);
    ok(got.includes('[' + printer + ']'),
      `「${printer}」が1つの引数として渡る — ${JSON.stringify(got.slice(0, 4))}`);
  }
  fs.rmSync(tmp, { recursive: true, force: true });

  console.log(`\n${failed === 0 ? '✅ 全テスト PASS' : `❌ ${failed} 件失敗`}`);
  process.exitCode = failed === 0 ? 0 : 1;
}
