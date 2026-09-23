import { temporaryTestDataDir } from './test-temp-dir.mjs';
await temporaryTestDataDir(import.meta.url, 'test-adkw-ai-runner-');
/**
 * SP広告KW の夜間 AI の実行役 (scripts/ph-nightly/ad-kw-ai.mjs・PR3b) を、本物の service-api (product-hub) と DB につないで試す。
 * AI 呼び出し (cli.invoke) と事前確認 (cli.preflight) だけ差し替える。
 * 実行: node scripts/test-ph-ad-kw-ai-runner.mjs
 */
import path from 'node:path';
import fs from 'node:fs';
process.env.PH_SERVICE_TOKEN = 'runner-token-1234567890';
process.env.AD_KW_AI_ENABLED = '1';
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const dbmod = await import('../apps/product-hub/db.js');
const db = dbmod.initProductHubDB();
const ak = await import('../apps/product-hub/lib/ad-keywords.js');
const ai = await import('../apps/product-hub/lib/ad-kw-ai.js');
const runner = await import('./ph-nightly/ad-kw-ai.mjs');
const express = (await import('express')).default;
const { serviceApiRouter } = await import('../apps/product-hub/router.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

const app = express();
let downNext = 0;   // 次の N 回の result を 503 にする (送信失敗の再現)
app.use('/svc', (req, res, next) => { if (downNext > 0 && /\/result$/.test(req.path)) { downNext -= 1; return res.status(503).json({ ok: false }); } next(); });
app.use('/svc', serviceApiRouter);
const server = app.listen(0);
const base = `http://127.0.0.1:${server.address().port}/svc`;
const token = process.env.PH_SERVICE_TOKEN;
const dataDir = path.join(process.env.DATA_DIR, 'runner-data');
const attestation = { provider: 'claude', additional_usage_disabled: true, checked_by: 'test', checked_at: '2026-09-01T00:00:00Z' };
const readyPre = async (provider, opts) => { seenEnv = opts.env; return { status: 'READY_FOR_BILLING_CHECK' }; };
let seenEnv = null, seenPrompt = null, seenOpts = null;
const okInvoke = (keywords) => async (stage, prompt, opts) => { seenPrompt = prompt; seenOpts = opts; return { status: 'OK', actual_model: 'claude-sonnet-5', response: '```json\n' + JSON.stringify({ keywords }) + '\n```' }; };

// 材料つきの依頼を 1 つ作る
const mkJob = (ne, key = 'j') => {
  const id = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, created_by, own_brand) VALUES (?, 'ハッカ油 ' || ?, 't', 1)`).run(ne, ne).lastInsertRowid);
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);
  const rq = ak.ensureRequest(db, draft, { idempotencyKey: 'r', actor: 't' }).request;
  const ev = Number(db.prepare(`INSERT INTO ph_ad_kw_evidence (request_id, source, seed, status, coverage_json, raw_json, fetched_at) VALUES (?, 'suggest', 'ハッカ油', 'success', '{}', '[]', '2026-09-23T01:00:00Z')`).run(rq.id).lastInsertRowid);
  db.prepare(`INSERT INTO ph_ad_kw_candidates (request_id, kind, value, value_norm, origin, evidence_id, observed_json, observed_count, sort_key) VALUES (?, 'kw', 'ハッカ油 スプレー', 'ハッカ油 スプレー', 'observed', ?, ?, 1, 'a')`)
    .run(rq.id, ev, JSON.stringify([{ evidence_id: ev, seed: 'ハッカ油', source: 'base' }]));
  return ai.requestAiJob(db, draft, rq.id, { idempotencyKey: key }).job;
};
const jobRow = (id) => db.prepare('SELECT status, result_kind, accepted, error_code FROM ph_ad_kw_ai_jobs WHERE id = ?').get(id);
const later = (min) => Date.now() + min * 60_000;
const run = (o = {}) => runner.runAdKwAi({ deadlineMs: later(30), runId: 't', base, token, dataDir, attestation, preflightImpl: readyPre,
  env: { ...process.env, PH_SERVICE_TOKEN: token, SOME_API_KEY: 'x', HOME_DIR: 'ok' }, ...o });

console.log('[1] 正常: claim → reserve → AI (ツール無し) → result');
{
  const j = mkJob('R-1');
  const r = await run({ invokeImpl: okInvoke([{ keyword: 'ハッカ油 ルームスプレー', basis_obs_ids: ['o1'], reason: '部屋用' }, { keyword: 'B0XXXXXXXX' }]) });
  eq([r.exit, r.summary.claimed, r.summary.submitted, r.summary.accepted, r.summary.rejected, r.summary.stopped], [0, 1, 1, 1, 1, 'empty'], '1 件処理して空で終わる (受理 1・棄却 1)');
  eq(jobRow(j.id).status, 'done', 'job = done');
  ok(seenPrompt.includes('<untrusted_data>') && seenPrompt.includes('"obs_id":"o1"') && seenPrompt.includes('ハッカ油 スプレー'), 'プロンプト = 固定の指示 + <untrusted_data> の材料 (観測語と obs_id)');
  ok(seenOpts.billing_attestation === attestation && seenOpts.timeout_ms <= 10 * 60_000 && seenOpts.timeout_ms > 60_000 && path.isAbsolute(seenOpts.cwd), '課金確認の記録・時間切れ (残り時間以内・10 分まで)・作業場所 (絶対パス) を渡す');
  ok(!('PH_SERVICE_TOKEN' in seenOpts.env) && !('SOME_API_KEY' in seenOpts.env) && seenOpts.env.HOME_DIR === 'ok' && !('PH_SERVICE_TOKEN' in seenEnv), '子プロセスの env から token / key を落とす');
  eq(fs.readdirSync(path.join(dataDir, 'pending')).length, 0, '送信できたので保存は消えている');
}

console.log('[2] 送信の失敗 → 保存した payload を次回に再送 (AI を再実行しない)');
{
  const j = mkJob('R-2');
  downNext = 1;
  let calls = 0;
  const inv = async (...a) => { calls += 1; return okInvoke([{ keyword: 'ハッカ油 お風呂', basis_obs_ids: ['o1'] }])(...a); };
  let r = await run({ invokeImpl: inv });
  eq([r.exit, r.summary.stopped, r.summary.pending_left], [1, 'server_unreachable', 1], '送れなかった → 止まる・保存は残る');
  eq(jobRow(j.id).status, 'running', 'job はまだ running (予約済み)');
  r = await run({ invokeImpl: inv });
  eq([r.summary.resent, r.summary.pending_left, calls], [1, 0, 1], '次回: 先に再送 (AI は 1 回だけ)');
  eq(jobRow(j.id).status, 'done', 'job = done');
}

console.log('[3] AI の失敗 → fail (予約後なので needs_review)・quota / billing は止まる');
{
  const j1 = mkJob('R-3a'); const j2 = mkJob('R-3b');
  let r = await run({ invokeImpl: async () => ({ status: 'QUOTA_BLOCKED' }) });
  eq([r.exit, r.summary.stopped, r.summary.failed], [2, 'ai:QUOTA_BLOCKED', 1], '利用上限 → 1 件目で止まる');
  eq([jobRow(j1.id).status, jobRow(j1.id).error_code], ['needs_review', 'quota'], '予約後の失敗 = needs_review (自動で作り直さない)');
  eq(jobRow(j2.id).status, 'queued', '2 件目は手を付けない');
  r = await run({ invokeImpl: async () => ({ status: 'MODEL_UNVERIFIED' }) });
  eq([r.exit, jobRow(j2.id).status, jobRow(j2.id).error_code], [2, 'needs_review', 'model_mismatch'], '実モデル不明 → 止めて needs_review');
}

console.log('[4] 出力が JSON でない → 送って rejected (AI を呼び直さない)');
{
  const j = mkJob('R-4');
  const r = await run({ invokeImpl: async () => ({ status: 'OK', actual_model: 'claude-sonnet-5', response: 'すみません、分かりません' }) });
  eq([r.exit, r.summary.submitted, r.summary.rejected_results], [2, 1, 1], '送信はする・全部棄却は exit 2 (ok にしない — Codex #1431 R1 #5)');
  eq([jobRow(j.id).status, jobRow(j.id).result_kind], ['failed', 'rejected'], 'job = failed / rejected');
}

console.log('[5] 始める前に止める: 課金確認の記録が無い / preflight 不合格 / 残り時間が短い');
{
  const j = mkJob('R-5');
  let called = 0;
  const inv = async () => { called += 1; return { status: 'OK', response: '{"keywords":[]}' }; };
  let r = await run({ attestation: null, invokeImpl: inv });
  eq([r.exit, r.summary.stopped, r.summary.claimed], [1, 'billing_unverified', 0], '課金確認の記録が無ければ claim しない');
  r = await run({ preflightImpl: async () => ({ status: 'BILLING_MODE_MISMATCH', blocked_names: ['ANTHROPIC_API_KEY'] }), invokeImpl: inv });
  eq([r.exit, r.summary.stopped, r.summary.claimed], [1, 'preflight:BILLING_MODE_MISMATCH', 0], '課金経路の不一致なら claim しない');
  r = await run({ deadlineMs: later(8), invokeImpl: inv });
  eq([r.exit, r.summary.stopped, r.summary.claimed, called], [0, 'deadline', 0, 0], '残り 8 分 (< 最短枠 + 余裕) なら予約しない');
  eq(jobRow(j.id).status, 'queued', 'job は queued のまま');
  r = await run({ invokeImpl: inv });
  ok(r.summary.claimed === 1 && jobRow(j.id).status === 'done', '時間があれば処理する');
}

console.log('[6] 1 日の上限 → release して止める / 抽出');
{
  process.env.AD_KW_AI_DAILY_CAP = '0';
  const j = mkJob('R-6');
  const r = await run({ invokeImpl: okInvoke([]) });
  eq([r.exit, r.summary.stopped], [2, 'daily_cap'], '上限 → 止まる (partial)');
  eq(jobRow(j.id).status, 'queued', 'release で queued に戻る (予約していない)');
  db.prepare("UPDATE ph_ad_kw_ai_jobs SET status = 'failed' WHERE id = ?").run(j.id);   // 後の場面が取らないように
  process.env.AD_KW_AI_DAILY_CAP = '10';
  eq(runner.extractJson('```json\n{"a":1}\n```'), { a: 1 }, 'extractJson: コードブロック');
  eq(runner.extractJson('前置き {"a":2} 後書き'), { a: 2 }, 'extractJson: 前後に文');
  eq(runner.extractJson('だめ'), null, 'extractJson: 無し');
  eq(['QUOTA_BLOCKED', 'BILLING_UNVERIFIED', 'MODEL_MISMATCH', 'TIMEOUT', 'WEIRD'].map(runner.failCodeOf), ['quota', 'billing', 'model_mismatch', 'timeout', 'other'], 'failCodeOf');
  ok(runner.childEnvironment({ A: '1', GITHUB_TOKEN: 'x', DB_PASSWORD: 'y' }).A === '1' && Object.keys(runner.childEnvironment({ GITHUB_TOKEN: 'x', DB_PASSWORD: 'y' })).length === 0, 'childEnvironment');
}

console.log('[7] Codex #1431 R1: 課金確認の完全な検査 / モデル不一致で止める / 再送だけ / 締め切り / 401 は保存を残す / cli の deadline');
{
  // #7 不完全な課金確認 (checked_by・checked_at が無い / 未来) → claim しない (予約して日次枠を使わない)
  const j = mkJob('R-7');
  let called = 0;
  const inv = async () => { called += 1; return { status: 'OK', response: '{"keywords":[]}' }; };
  for (const bad of [{ provider: 'claude', additional_usage_disabled: true }, { ...attestation, checked_at: '2099-01-01T00:00:00Z' }, { ...attestation, revoked: true }]) {
    const r = await run({ attestation: bad, invokeImpl: inv });
    ok(r.summary.stopped === 'billing_unverified' && r.summary.claimed === 0, '不完全な課金確認 → claim しない ' + JSON.stringify(Object.keys(bad)));
  }
  eq([called, jobRow(j.id).status], [0, 'queued'], 'AI も予約もしていない');
  db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'failed' WHERE id = ?`).run(j.id);
  // #2 モデル不一致 → 1 件目で止める (2 件目を予約・実行しない)
  const a = mkJob('R-8a'), b = mkJob('R-8b');
  let n = 0;
  const r2 = await run({ invokeImpl: async () => { n += 1; return { status: 'MODEL_MISMATCH' }; } });
  eq([r2.exit, r2.summary.claimed, n, r2.summary.stopped], [2, 1, 1, 'ai:MODEL_MISMATCH'], 'モデル不一致は 1 件目で止める');
  eq([jobRow(a.id).status, jobRow(b.id).status], ['needs_review', 'queued'], '2 件目は手を付けない');
  db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'failed' WHERE id IN (?, ?)`).run(a.id, b.id);
  // #8 401 → 保存を残して止まる (認証を直せば次回送れる) / #1 再送だけ (新しい依頼は取らない)
  const c = mkJob('R-9'); const q = mkJob('R-9q');
  const realFetch = fetch;
  let deny = true;
  const fetch401 = async (url, opts) => (deny && /\/result$/.test(url) ? new Response(JSON.stringify({ ok: false, error: 'unauthorized' }), { status: 401 }) : realFetch(url, opts));
  const r3 = await run({ maxJobs: 1, fetchImpl: fetch401, invokeImpl: okInvoke([{ keyword: 'ハッカ油 寝室', basis_obs_ids: ['o1'] }]) });
  eq([r3.exit, r3.summary.stopped, r3.summary.pending_left], [1, 'auth_rejected', 1], '401 → 保存を残して止まる (脇へ置かない)');
  ok(fs.readdirSync(path.join(dataDir, 'pending')).some((f) => /\.json$/.test(f)), '保存は .json のまま (次回の再送の対象)');
  deny = false;
  let claims = 0;
  const countClaim = async (url, opts) => { if (/\/claim$/.test(url)) claims += 1; return realFetch(url, opts); };
  const r4 = await run({ resendOnly: true, fetchImpl: countClaim, invokeImpl: async () => { throw new Error('must not run'); } });
  eq([r4.exit, r4.summary.resent, r4.summary.stopped, claims], [0, 1, 'resend_only', 0], '再送だけ: 送れた・claim しない');
  eq([jobRow(c.id).status, jobRow(q.id).status], ['done', 'queued'], '再送で job = done・ほかの依頼は取らない');
  db.prepare(`UPDATE ph_ad_kw_ai_jobs SET status = 'failed' WHERE id = ?`).run(q.id);
  // #3 締め切りを過ぎて起動 → 通信しない
  fs.writeFileSync(path.join(dataDir, 'pending', 'gen-99999999.json'), JSON.stringify({ generation_id: 99999999, packet_hash: 'x', output: null }));
  let calls = 0;
  const r5 = await run({ deadlineMs: Date.now() - 1000, fetchImpl: async (...x) => { calls += 1; return realFetch(...x); }, preflightImpl: async () => { calls += 100; return { status: 'READY_FOR_BILLING_CHECK' }; } });
  eq([r5.exit, r5.summary.stopped, calls], [1, 'deadline', 0], '締め切り後は再送も preflight もしない');
  fs.unlinkSync(path.join(dataDir, 'pending', 'gen-99999999.json'));
  // cli.cjs: deadline_ms が近ければ preflight のあとで呼ばずに DEADLINE
  const cli = (await import('module')).createRequire(import.meta.url)('./product-idea-scout/ai/cli.cjs');
  const fakeExec = async (cmd, args) => (args.includes('--version') ? { code: 0, stdout: '1.0' } : { code: 0, stdout: JSON.stringify({ loggedIn: true, authMethod: 'claude.ai', subscriptionType: 'max' }) });
  let ran = false;
  const res = await cli.invoke('ADKW1', 'x', { env: {}, cwd: process.env.DATA_DIR, billing_attestation: attestation, command: { file: 'x', prefix: [] },
    execute: async (c, a, o) => { if (a.includes('-p')) { ran = true; return { code: 0, stdout: '' }; } return fakeExec(c, a, o); },
    budget: { reserve: () => ({ id: 1 }), finish: () => {}, snapshot: () => ({}) }, save_budget: async () => {}, deadline_ms: Date.now() + 30_000 });
  eq([res.status, ran], ['DEADLINE', false], 'cli.invoke: preflight 後に残り 1 分未満なら呼ばない (DEADLINE)');
  eq(runner.failCodeOf('DEADLINE'), 'timeout', 'DEADLINE は timeout として報告');
}

server.close();
console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
