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
  assert.match(fn, /if \(resp\.status === 404\) throw/);
  assert.doesNotMatch(fn, /job = await resp\.json\(\);/, '応答をそのままジョブとして読んでいる');
  assert.match(fn, /return pullInboundFromMiniPC\(false\);/);
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

console.log(`\n${pass} passed / ${fail} failed`);
process.exit(fail ? 1 : 0);
