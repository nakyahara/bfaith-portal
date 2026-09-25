#!/usr/bin/env node
/**
 * 納品実績の 06:00 自動引き取りの修正 (2026-09-25) の試験。miniPC・SP-API には行かない。DATA_DIR は一時フォルダ。
 *   node scripts/test-fba-inbound-daily-pull.mjs
 *
 * 不具合: Render の cron が miniPC のジョブの応答 { ok: true, job: {...} } の status を一段浅く読み、
 *   「完了」を一度も見つけられず 25 分で時間切れ → Render への引き取りが 2026-08-05 (#704) から一度も走っていなかった。
 *   しかも ping は SKU 同期だけで ok にしていたので、監視にも出なかった。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-inbound-pull-'));
const imp = (p) => import(pathToFileURL(path.join(root, p)).href);

let pass = 0, fail = 0;
async function t(name, fn) {
  try { await fn(); pass++; console.log(`  ✅ ${name}`); }
  catch (e) { fail++; console.log(`  ❌ ${name}\n     ${e.stack.split('\n').slice(0, 4).join('\n     ')}`); }
}

const { jobOf, dailySyncPingStatus } = await imp('apps/fba-replenishment/router.js');

await t('🚨 ジョブの応答は { ok, job } = 実物の miniPC の口 (service-router.js) と同じ形から status を読む', async () => {
  const sr = fs.readFileSync(path.join(root, 'apps', 'warehouse', 'service-router.js'), 'utf8');
  assert.match(sr, /router\.get\('\/jobs\/:jobId'[\s\S]{0,400}?okResponse\(res, \{ job \}\);/, 'miniPC のジョブの口の応答の形が変わった');
  const eh = fs.readFileSync(path.join(root, 'apps', 'warehouse', 'error-handler.js'), 'utf8');
  assert.match(eh, /res\.status\(statusCode\)\.json\(\{ ok: true, \.\.\.data \}\)/, 'okResponse の形が変わった');
  assert.deepEqual(jobOf({ ok: true, job: { status: 'completed', result: {} } }), { status: 'completed', result: {} });
  assert.equal(jobOf({ ok: true, job: { status: 'failed', error: 'x' } }).status, 'failed');
  // 以前の読み方 (body.status) では completed を見つけられない = 時間切れになっていた
  assert.equal({ ok: true, job: { status: 'completed' } }.status, undefined);
  for (const bad of [null, undefined, 'x', {}, { ok: false, error: 'JOB_NOT_FOUND' }, { job: 'completed' }]) assert.equal(jobOf(bad), null);
});

await t('cron の待ち処理は jobOf を使い、404 (ジョブが消えた) は待たずに失敗にする', async () => {
  const src = fs.readFileSync(path.join(root, 'apps', 'fba-replenishment', 'router.js'), 'utf8');
  const fn = src.slice(src.indexOf('async function runInboundHistoryDailySync'), src.indexOf('// 日別 / 月別サマリ'));
  assert.ok(fn.length > 200, 'runInboundHistoryDailySync が見つからない');
  assert.match(fn, /const job = jobOf\(body\);/);
  assert.ok(fn.indexOf('resp.status === 404') < fn.indexOf('resp.json()'), '404 の判定が本文の解析より後ろ');
  assert.doesNotMatch(fn, /job = await resp\.json\(\);/, '応答をそのままジョブとして読んでいる');
  assert.match(fn, /pull = pullInboundFromMiniPC/);
  assert.match(fn, /const pulled = await pull\(false\);/);
});

await t('🚨 ping: SKU 失敗 = fail / SKU 成功・納品実績の失敗 = partial (ok の日付を進めない = 監視で知らせる) / 両方成功 = ok', async () => {
  assert.equal(dailySyncPingStatus({ skuOk: false, inboundOk: true }), 'fail');
  assert.equal(dailySyncPingStatus({ skuOk: false, inboundOk: false }), 'fail');
  assert.equal(dailySyncPingStatus({ skuOk: true, inboundOk: false }), 'partial');
  assert.equal(dailySyncPingStatus({ skuOk: true, inboundOk: true }), 'ok');
  const src = fs.readFileSync(path.join(root, 'apps', 'fba-replenishment', 'router.js'), 'utf8');
  assert.match(src, /pingJob\('fba-daily-sync', dailySyncPingStatus\(\{ skuOk, inboundOk \}\), notes\.join\(' '\)\);/);
  // 監視は partial を「生きているが ok ではない」として扱う (fba-daily-sync は partial_max_days なし = 締切で通知)
  const store = fs.readFileSync(path.join(root, 'apps', 'jobs-monitor', 'store.js'), 'utf8');
  assert.match(store, /last_ok_at\s+= CASE WHEN @status = 'ok' THEN @now ELSE job_state\.last_ok_at END/);
  const { JOBS_REGISTRY: jobs } = await imp('config/jobs-registry.mjs');
  const def = (jobs || []).find((j) => j.id === 'fba-daily-sync');
  assert.ok(def, '台帳に fba-daily-sync が無い');
  assert.equal(def.partial_max_days, undefined, 'partial を ok 扱いにする設定が入っている');
});

// ── 動かして確かめる (待つ → 引き取る)。待つ間隔は 1ms、通信は差し替え ──
const { runInboundHistoryDailySync, jobErrorText } = await imp('apps/fba-replenishment/router.js');
const jsonRes = (status, body) => ({ status, ok: status < 300, json: async () => body });
const htmlRes = (status) => ({ status, ok: status < 300, json: async () => { throw new SyntaxError('Unexpected token <'); } });
const started = async () => ({ ok: true, jobId: 'job-1', status: 'running' });
const run = (fetchSeq, over = {}) => {
  let i = 0; const seen = { polls: 0, pulled: 0 };
  const p = runInboundHistoryDailySync({
    pollMs: 1, deadlineMs: 2000,
    callMiniPCImpl: started,
    fetchImpl: async () => { seen.polls++; const f = fetchSeq[Math.min(i++, fetchSeq.length - 1)]; return f(); },
    pull: async () => { seen.pulled++; return { shipments: 12, items: 34, pages: 1, status: {} }; },
    ...over,
  });
  return { p, seen };
};
await t('🚨 動作: running → (通信断) → completed を見つけて引き取る。明細の失敗件数と例を返す (Codex #1451 R1 Medium 1)', async () => {
  const { p, seen } = run([
    () => jsonRes(200, { ok: true, job: { status: 'running' } }),
    () => { throw new TypeError('fetch failed'); },
    () => jsonRes(200, { ok: true, job: { status: 'completed', result: { items_failed: 2, errors: [{ shipment_id: 'FBA1', message: 'QuotaExceeded' }, { shipment_id: 'FBA2', message: 'x' }] } } }),
  ]);
  const r = await p;
  assert.deepEqual([r.shipments, r.items, r.items_failed, seen.pulled, seen.polls], [12, 34, 2, 1, 3]);
  assert.deepEqual(r.items_failed_sample, ['FBA1: QuotaExceeded', 'FBA2: x']);
  const clean = await run([() => jsonRes(200, { ok: true, job: { status: 'completed', result: { items_failed: 0 } } })]).p;
  assert.deepEqual([clean.items_failed, clean.items_failed_sample], [0, []]);
});
await t('動作: 失敗したジョブは理由つきで例外 ({ code, message } を [object Object] にしない) / 引き取らない (Codex #1451 R1 Low 1)', async () => {
  const { p, seen } = run([() => jsonRes(200, { ok: true, job: { status: 'failed', error: { code: 'SP_API_ERROR', message: 'Access denied' } } })]);
  await assert.rejects(p, /ミニPC側のジョブが失敗: SP_API_ERROR: Access denied/);
  assert.equal(seen.pulled, 0);
  assert.deepEqual([jobErrorText('x'), jobErrorText(null), jobErrorText({ message: 'm' })], ['x', '', 'm']);
});
await t('動作: 404 は本文が JSON でなくても待たずに失敗 (Codex #1451 R1 Low 2) / 形の違う応答や JSON でない 200 は待ち続けて時間切れ', async () => {
  const a = run([() => htmlRes(404)]);
  await assert.rejects(a.p, /miniPC から消えた/);
  assert.equal(a.seen.polls, 1);
  const b = run([() => jsonRes(200, { status: 'completed' })], { deadlineMs: 30 });
  await assert.rejects(b.p, /タイムアウト/, '以前の形 (body.status) を完了と読んでいる');
  assert.equal(b.seen.pulled, 0);
  const c = run([() => htmlRes(200)], { deadlineMs: 30 });
  await assert.rejects(c.p, /タイムアウト/);
});
await t('動作: miniPC 側で実行中 (already_running) なら待たずに引き取りだけ / 起動に失敗したら例外', async () => {
  const { p, seen } = run([() => { throw new Error('呼ばれないはず'); }], { callMiniPCImpl: async () => ({ ok: true, status: 'already_running' }) });
  const r = await p;
  assert.deepEqual([seen.polls, seen.pulled, r.items_failed], [0, 1, 0]);
  await assert.rejects(run([], { callMiniPCImpl: async () => ({ ok: false }) }).p, /取込ジョブの起動に失敗/);
});
await t('cron: 明細の失敗があれば inboundOk = false (= partial) で、note に件数と例', async () => {
  const src = fs.readFileSync(path.join(root, 'apps', 'fba-replenishment', 'router.js'), 'utf8');
  assert.match(src, /inboundOk = !ih\.items_failed;/);
  assert.match(src, /明細失敗\$\{ih\.items_failed\}/);
});

console.log(`\n${pass} passed / ${fail} failed`);
process.exit(fail ? 1 : 0);
