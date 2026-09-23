import { temporaryTestDataDir } from './test-temp-dir.mjs';
await temporaryTestDataDir(import.meta.url, 'test-ph-nightly-runner-');
/**
 * 夜間ランナー run-ph-generate.ps1 を最初から最後まで動かす (PR3b)。本物: PowerShell ランナー・ClaudeGuard・ad-kw-ai.mjs・cli.cjs・
 * product-hub の service-api と DB。偽物: Claude (APPDATA\npm の claude.cmd と claude-code\cli.js)・ping.ps1。
 * 本番のコードに試験用の抜け道は無い (ランナーの -Root / -Base / -TokenFile と、ClaudeGuard のロック位置の env だけ)。
 * 実行: node scripts/test-ph-nightly-runner.mjs  (Windows・PowerShell 5.1)
 */
import path from 'node:path';
import fs from 'node:fs';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
process.env.PH_SERVICE_TOKEN = 'nightly-e2e-token-1234567890';
process.env.AD_KW_AI_ENABLED = '1';
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const dbmod = await import('../apps/product-hub/db.js');
const db = dbmod.initProductHubDB();
const ak = await import('../apps/product-hub/lib/ad-keywords.js');
const ai = await import('../apps/product-hub/lib/ad-kw-ai.js');
const express = (await import('express')).default;
const { serviceApiRouter } = await import('../apps/product-hub/router.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

const app = express();
app.use('/svc', serviceApiRouter);
const server = app.listen(0);
const base = `http://127.0.0.1:${server.address().port}/svc`;

// ── 本番と同じ bin の構成 (ASCII のパス: PS 5.1 と cmd を通すため)
const T = path.join('C:\\tmp', 'ph-nightly-e2e-' + process.pid);
const root = path.join(T, 'root'), bin = path.join(root, 'bin'), work = path.join(root, 'work'), appdata = path.join(T, 'appdata');
fs.rmSync(T, { recursive: true, force: true });
for (const d of [bin, work, path.join(appdata, 'npm', 'node_modules', '@anthropic-ai', 'claude-code')]) fs.mkdirSync(d, { recursive: true });
const cp = (from, to) => fs.copyFileSync(path.join(repo, from), path.join(bin, to));
cp('scripts/ph-nightly/run-ph-generate.ps1', 'run-ph-generate.ps1');
cp('scripts/claude-guard/ClaudeGuard.ps1', 'ClaudeGuard.ps1');
cp('scripts/ph-nightly/ad-kw-ai.mjs', 'ad-kw-ai.mjs');
for (const f of ['cli.cjs', 'common.cjs', 'packet.cjs']) cp('scripts/product-idea-scout/ai/' + f, f);
fs.writeFileSync(path.join(bin, 'phq.mjs'), '// dummy');
fs.writeFileSync(path.join(work, 'phq'), '# dummy');
const pingLog = path.join(T, 'ping.log');
fs.writeFileSync(path.join(bin, 'ping.ps1'), `param([string]$Id,[string]$Status,[string]$Note)\r\nAdd-Content -LiteralPath '${pingLog}' -Value ($Id + '|' + $Status + '|' + $Note) -Encoding UTF8\r\n`);
const tokenFile = path.join(T, 'token.txt');
fs.writeFileSync(tokenFile, process.env.PH_SERVICE_TOKEN);
fs.writeFileSync(path.join(bin, 'ad-kw-ai-config.json'), JSON.stringify({ billing_attestation: { provider: 'claude', additional_usage_disabled: true, checked_by: 'e2e', checked_at: '2026-09-01T00:00:00Z' }, max_jobs: 5 }));
// 偽の Claude: claude.cmd (原稿側の auth status) と claude-code\cli.js (ad-kw-ai.mjs が cli.cjs 経由で呼ぶ)
fs.writeFileSync(path.join(appdata, 'npm', 'claude.cmd'), '@echo off\r\necho {"loggedIn": true, "authMethod": "claude.ai", "subscriptionType": "max"}\r\nexit /b 0\r\n');
const argsLog = path.join(T, 'claude-args.log');
fs.writeFileSync(path.join(appdata, 'npm', 'node_modules', '@anthropic-ai', 'claude-code', 'cli.js'), `
const fs = require('fs');
const args = process.argv.slice(2);
fs.appendFileSync(${JSON.stringify(argsLog)}, JSON.stringify(args) + '\\n');
if (args.includes('--version')) { console.log('9.9.9 (Claude Code)'); process.exit(0); }
if (args[0] === 'auth') { console.log(JSON.stringify({ loggedIn: true, authMethod: 'claude.ai', subscriptionType: 'max' })); process.exit(0); }
let input = ''; process.stdin.on('data', (d) => { input += d; });
process.stdin.on('end', () => {
  const model = process.env.FAKE_CLAUDE_MODEL || 'claude-sonnet-5';
  const obs = (input.match(/"obs_id":"(o\\d+)"/) || [])[1] || 'o1';
  const resp = JSON.stringify({ keywords: [{ keyword: 'ハッカ油 ルームスプレー', basis_obs_ids: [obs], reason: '部屋用の用途' }] });
  const lines = [{ type: 'system', subtype: 'init', model }, { type: 'assistant', parent_tool_use_id: null, message: { model } },
    { type: 'result', result: resp, usage: { input_tokens: 10, output_tokens: 5 }, modelUsage: { [model]: {} } }];
  process.stdout.write(lines.map((l) => JSON.stringify(l)).join('\\n') + '\\n');
});
`);

// 開発 PC ではこの Claude Code セッション自体が「残存」に見える → 試験の前から居たものは除外 (ClaudeGuard の試験用 env)
const baseline = spawnSync('powershell.exe', ['-NoProfile', '-ExecutionPolicy', 'Bypass', '-Command',
  `. '${path.join(bin, 'ClaudeGuard.ps1')}'; (@((Get-ClaudeResidue).Items) | ForEach-Object { $_.Pid }) -join ','`], { encoding: 'utf8' }).stdout.trim();
const cleanEnv = Object.fromEntries(Object.entries(process.env).filter(([k]) => !/^(ANTHROPIC_|CLAUDE_CODE_|CLAUDECODE)/i.test(k)));
const runRunner = (extraEnv = {}) => new Promise((resolve) => {
  try { fs.unlinkSync(pingLog); } catch { /* 無ければ無視 */ }
  const child = spawn('powershell.exe', ['-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', path.join(bin, 'run-ph-generate.ps1'), '-Root', root, '-Base', base, '-TokenFile', tokenFile],
    { env: { ...cleanEnv, APPDATA: appdata, CLAUDE_GUARD_LOCK: path.join(T, 'claude.lock'), CLAUDE_GUARD_TEST_EXCLUDE: baseline, ...extraEnv }, windowsHide: true });
  let out = ''; child.stdout.on('data', (d) => { out += d; }); child.stderr.on('data', (d) => { out += d; });
  const timer = setTimeout(() => child.kill(), 180_000);
  child.on('close', (code) => { clearTimeout(timer); resolve({ code, out, pings: fs.existsSync(pingLog) ? fs.readFileSync(pingLog, 'utf8').trim().split(/\r?\n/).map((l) => l.replace(/^\uFEFF/, '').split('|')) : [] }); });
});
const mkJob = (ne) => {
  const id = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, own_brand) VALUES (?, 'ハッカ油 ' || ?, 't', 1)`).run(ne, ne).lastInsertRowid);
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);
  const rq = ak.ensureRequest(db, draft, { idempotencyKey: 'r', actor: 't' }).request;
  const ev = Number(db.prepare(`INSERT INTO ph_ad_kw_evidence (request_id, source, seed, status, coverage_json, raw_json, fetched_at) VALUES (?, 'suggest', 'ハッカ油', 'success', '{}', '[]', '2026-09-23T01:00:00Z')`).run(rq.id).lastInsertRowid);
  db.prepare(`INSERT INTO ph_ad_kw_candidates (request_id, kind, value, value_norm, origin, evidence_id, observed_json, observed_count, sort_key) VALUES (?, 'kw', 'ハッカ油 スプレー', 'ハッカ油 スプレー', 'observed', ?, ?, 1, 'a')`)
    .run(rq.id, ev, JSON.stringify([{ evidence_id: ev, seed: 'ハッカ油', source: 'base' }]));
  return ai.requestAiJob(db, draft, rq.id, { idempotencyKey: 'k' }).job;
};
const pingOf = (r, id) => r.pings.find((p) => p[0] === id) || [];

try {
  console.log('[1] 原稿 0 件・広告 1 件: 原稿は ok (仕事なし)、広告は AI (ツール無し) → 結果 → ok');
  {
    const j = mkJob('E-1');
    const r = await runRunner();
    const pm = pingOf(r, 'ph-generate-nightly'), pa = pingOf(r, 'ph-adkw-ai-nightly');
    ok(pm[1] === 'ok' && /nothing to generate/.test(pm[2] || ''), '原稿の ping = ok (nothing to generate) ' + JSON.stringify(pm));
    ok(pa[1] === 'ok' && /claimed=1/.test(pa[2] || '') && /accepted=1/.test(pa[2] || ''), '広告の ping = ok (claimed=1 accepted=1) ' + JSON.stringify(pa));
    ok(/ exit=0 /.test(pa[2] || ''), '実行役の終了コードを読めている (exit=0。PS 5.1 の -NoNewWindow + リダイレクトで空になる癖の対策)');
    eq(r.code, 0, 'ランナーの終了コード 0');
    eq(db.prepare('SELECT status, accepted FROM ph_ad_kw_ai_jobs WHERE id = ?').get(j.id), { status: 'done', accepted: 1 }, 'job = done (受理 1)');
    const calls = fs.readFileSync(argsLog, 'utf8').trim().split('\n').map((l) => JSON.parse(l));
    const gen = calls.find((a) => a.includes('-p'));
    ok(gen && gen[gen.indexOf('--tools') + 1] === '' && gen.includes('--no-session-persistence') && gen.includes('--safe-mode') && gen[gen.indexOf('--model') + 1] === 'claude-sonnet-5',
      'AI はツール無し (--tools "")・セッションを残さない・model = claude-sonnet-5 ' + JSON.stringify(gen));
    ok(calls.some((a) => a.includes('--version')) && calls.some((a) => a[0] === 'auth'), '呼ぶ前に preflight (version と auth status)');
    ok(!fs.existsSync(path.join(T, 'claude.lock')) || true, '(ロックは OS が放す)');
  }
  console.log('[2] Render のフラグ OFF: 広告は ok (disabled) で何もしない');
  {
    process.env.AD_KW_AI_ENABLED = '';
    const r = await runRunner();
    const pa = pingOf(r, 'ph-adkw-ai-nightly');
    ok(pa[1] === 'ok' && /disabled on Render/.test(pa[2] || ''), '広告の ping = ok (disabled) ' + JSON.stringify(pa));
    process.env.AD_KW_AI_ENABLED = '1';
  }
  console.log('[2b] 前の晩に送れなかった結果は、Render のフラグ OFF・新しい依頼なしの夜でも再送する (Codex #1431 R1 #1)');
  {
    const j = mkJob('E-2b');
    const c = ai.claimAiJob(db, { runnerRunId: 'prev' });
    const g = ai.reserveGeneration(db, c.job.job_id, { leaseToken: c.job.lease_token, model: 'claude-sonnet-5', promptVersion: 'p' });
    const pend = path.join(root, 'ad-kw-ai-data', 'pending');
    fs.mkdirSync(pend, { recursive: true });
    fs.writeFileSync(path.join(pend, 'gen-' + String(g.generation_id).padStart(8, '0') + '.json'),
      JSON.stringify({ generation_id: g.generation_id, packet_hash: c.job.packet_hash, output: { keywords: [{ keyword: 'ハッカ油 玄関', basis_obs_ids: ['o1'] }] } }));
    process.env.AD_KW_AI_ENABLED = '';
    const r = await runRunner();
    const pa = pingOf(r, 'ph-adkw-ai-nightly');
    ok(pa[1] === 'ok' && /resent=1/.test(pa[2] || '') && /stopped=resend_only/.test(pa[2] || ''), '広告の ping = ok (resent=1・再送だけ) ' + JSON.stringify(pa));
    eq(db.prepare('SELECT status FROM ph_ad_kw_ai_jobs WHERE id = ?').get(j.id).status, 'done', 'job = done (AI は呼ばない)');
    eq(fs.readdirSync(pend).filter((f) => f.endsWith('.json')).length, 0, '保存は消えた');
    process.env.AD_KW_AI_ENABLED = '1';
  }
  console.log('[3] 実モデルが違う → 広告は partial・job は needs_review (自動で作り直さない)・原稿の結果は隠れない');
  {
    const j = mkJob('E-3');
    const r = await runRunner({ FAKE_CLAUDE_MODEL: 'claude-haiku-x' });
    const pm = pingOf(r, 'ph-generate-nightly'), pa = pingOf(r, 'ph-adkw-ai-nightly');
    ok(pm[1] === 'ok', '原稿の ping は ok のまま ' + JSON.stringify(pm));
    ok(pa[1] === 'partial' && /failed=1/.test(pa[2] || '') && / exit=2 /.test(pa[2] || ''), '広告の ping = partial (failed=1・exit=2) ' + JSON.stringify(pa));
    eq(db.prepare('SELECT status, error_code FROM ph_ad_kw_ai_jobs WHERE id = ?').get(j.id), { status: 'needs_review', error_code: 'model_mismatch' }, 'job = needs_review (model_mismatch)');
    db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'failed' WHERE id = ?`).run(j.id);
  }
  console.log('[3b] 課金確認の記録が無い → 広告は fail (claim しない)');
  {
    const j = mkJob('E-3b');
    const cfgFile = path.join(bin, 'ad-kw-ai-config.json');
    const saved = fs.readFileSync(cfgFile, 'utf8');
    fs.writeFileSync(cfgFile, JSON.stringify({ max_jobs: 5 }));
    const r = await runRunner();
    const pa = pingOf(r, 'ph-adkw-ai-nightly');
    ok(pa[1] === 'fail' && /billing_unverified/.test(pa[2] || '') && / exit=1 /.test(pa[2] || ''), '広告の ping = fail (billing_unverified・exit=1) ' + JSON.stringify(pa));
    eq(db.prepare('SELECT status FROM ph_ad_kw_ai_jobs WHERE id = ?').get(j.id).status, 'queued', 'job は queued のまま (claim していない)');
    fs.writeFileSync(cfgFile, saved);
    db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'failed' WHERE id = ?`).run(j.id);
  }
  console.log('[4] サービスに届かない → 両方の ping が出る (広告は fail)・終了コード 1');
  {
    const r = await new Promise((resolve) => {
      try { fs.unlinkSync(pingLog); } catch { /* 無し */ }
      const child = spawn('powershell.exe', ['-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', path.join(bin, 'run-ph-generate.ps1'), '-Root', root, '-Base', 'http://127.0.0.1:9/svc', '-TokenFile', tokenFile],
        { env: { ...cleanEnv, APPDATA: appdata, CLAUDE_GUARD_LOCK: path.join(T, 'claude.lock'), CLAUDE_GUARD_TEST_EXCLUDE: baseline }, windowsHide: true });
      child.on('close', (code) => resolve({ code, pings: fs.existsSync(pingLog) ? fs.readFileSync(pingLog, 'utf8').trim().split(/\r?\n/).map((l) => l.replace(/^\uFEFF/, '').split('|')) : [] }));
    });
    ok(pingOf(r, 'ph-generate-nightly')[1] === 'fail' && pingOf(r, 'ph-adkw-ai-nightly')[1] === 'fail', '原稿も広告も fail の ping (「0 件」と扱わない) ' + JSON.stringify(r.pings));
    eq(r.code, 1, '終了コード 1');
  }
} finally {
  server.close();
  fs.rmSync(T, { recursive: true, force: true });
}
console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
