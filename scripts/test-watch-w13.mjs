/**
 * test-watch-w13.mjs — 見張りの W13 (マスタの照合 ①ロードの検証) を engine ごと通す (Company DB構想 10 §6.1.1 B3。Codex ③a-2 B-R0 #4 #5 #7)
 *
 * 固定する契約:
 *   1 照合の証跡 (complete) と全件 JSON (sha256 が合う) を読み、差 0 = pass / 差あり = breach。全案件を渡す (保存の明細は 200 行に間引くが、案件は 201 件以上も全部できる)
 *   2 翌日に差が消えた案件: 今回その種類 × SKU を比べた = 回復 / 比べていない (ロードの判断で対象外) = 回復にしない (監視期間外)
 *   3 判定できない = blocked (案件は保持): 証跡が無い・実行中・別の実行 ID・日付違い・全件 JSON が無い / ハッシュ違い / 証跡と食い違う・照合が blocked
 * 使い方: node scripts/test-watch-w13.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';

const DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'w13-'));
process.env.DATA_DIR = DIR;
const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const REAL = await import('../config/watch-checks.mjs');
const { runWatch } = await import('../apps/company-db/watch/engine.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const SYNC = 'ds_w13_1';
const CONFIG = { ...REAL, CHECKS: [REAL.checkById('W13')] };

let seq = 0;
/** 照合の実行口が書くのと同じ形の全件 JSON と証跡 */
function evidenceFor(asOf, { items = [], verdict = items.length ? 'breach' : 'pass', blockedReason = null, compared = {}, exclusions = {}, extra = {}, resultExtra = {}, corrupt = false } = {}) {
  const runId = `mc_20260923T000000${String(++seq).padStart(3, '0')}Z_abcdef`;
  const res = { format: 'mc-v1', as_of: asOf, compare_run_id: runId, verdict, blocked_reason: blockedReason, load: { ingest_run_id: 'load_n1', started_at: `${asOf}T17:00:00Z` },
    counts: { items: items.length, by_type: {}, compared: { value: 1000 } }, items, compared, exclusions, finished_at: `${asOf}T22:11:00Z`, ...resultExtra };
  const rel = `cdb-master-compare/${asOf}/${runId}.json`;
  fs.mkdirSync(path.join(DIR, 'cdb-master-compare', asOf), { recursive: true });
  const buf = Buffer.from(JSON.stringify(res), 'utf8');
  fs.writeFileSync(path.join(DIR, rel), corrupt ? Buffer.from('{}') : buf);
  return { name: 'master-compare', state: 'complete', compare_run_id: runId, as_of: asOf, json_path: rel, sha256: crypto.createHash('sha256').update(buf).digest('hex'),
    verdict, blocked_reason: blockedReason, counts: res.counts, load: { ingest_run_id: 'load_n1' }, sync_run_id: SYNC, ...extra };
}
const item = (type, norm, extra = {}) => ({ type, code: norm, norm, subject_key: `${type}:${norm}`, ...extra });
const pg = new PGlite(); const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
/** 証跡のファイル (W13 はファイルから読む) */
const evFile = (asOf) => path.join(DIR, 'company-db-evidence', asOf, 'master-compare.json');
const putEvidence = (asOf, ev) => { if (ev) { fs.mkdirSync(path.dirname(evFile(asOf)), { recursive: true }); fs.writeFileSync(evFile(asOf), JSON.stringify(ev)); } else fs.rmSync(evFile(asOf), { force: true }); };
const run = (asOf, ev, nowIso, hooks) => { putEvidence(asOf, ev); return runWatch({ db, writer: db, config: CONFIG, asOf, evidence: ev ? { 'master-compare': ev } : {}, now: new Date(nowIso), host: 'test', log: quiet, syncRunId: SYNC, hooks }); };
const w13 = (r) => r.results.find((x) => x.checkId === 'W13');
const issues = async (state) => (await db.query("select subject_key, state from ops.watch_issues where check_id = 'W13' and state = $1 order by subject_key", [state])).rows.map((x) => x.subject_key);

await ta('[1] 差 0 = pass / 差あり = breach。250 件の差は 250 件の案件 (保存の明細は間引いても案件は全部)', async () => {
  let r = await run('2026-09-23', evidenceFor('2026-09-23'), '2026-09-22T23:00:00Z');
  assert.equal(w13(r).verdict, 'pass', w13(r).reason);
  const many = Array.from({ length: 250 }, (_, i) => item('value', `sku${String(i).padStart(3, '0')}`, { diffs: [{ col: 'name', expected: 'a', actual: 'b' }] }));
  const compared = { value: many.map((x) => x.norm), missing: many.map((x) => x.norm) };
  r = await run('2026-09-24', evidenceFor('2026-09-24', { items: many, compared }), '2026-09-23T23:00:00Z');
  assert.equal(w13(r).verdict, 'breach'); assert.equal(w13(r).severity, 'info');
  assert.equal((await issues('open')).length, 250);
  const saved = (await db.query("select count(*)::int as n from ops.watch_result_items x join ops.watch_results r on r.watch_result_id = x.watch_result_id where r.check_id = 'W13' and r.watch_run_id = $1", [r.runId])).rows[0].n;
  assert.ok(saved <= 200, String(saved));
});

await ta('[2] 翌日に差が消えた案件: 比べた = 回復 / 比べていない (ロードの判断で対象外) = 回復にしない', async () => {
  // sku000 は比べて差が無い (回復)、sku001 は今回比べていない (代表の仕入先を触らなかった等 = 対象外)、残り 248 件は続く
  const rest = Array.from({ length: 248 }, (_, i) => item('value', `sku${String(i + 2).padStart(3, '0')}`));
  const compared = { value: ['sku000', ...rest.map((x) => x.norm)] };
  const r = await run('2026-09-25', evidenceFor('2026-09-25', { items: rest, compared, exclusions: { 'value:sku001': 'norm_collision' } }), '2026-09-24T23:00:00Z');
  assert.equal(w13(r).verdict, 'breach');
  assert.deepEqual(await issues('recovered'), ['value:sku000']);
  assert.deepEqual(await issues('out_of_window'), ['value:sku001']);
  assert.equal((await issues('open')).length, 248);
  assert.deepEqual(w13(r).observed.out_of_scope, { 'value:sku001': 'norm_collision' });
});

await ta('[3] 判定できない = blocked (案件は保持): 証跡が無い・実行中・別の実行・日付違い・JSON が無い / ハッシュ違い / 証跡と食い違う・照合が blocked', async () => {
  const d = '2026-09-26', now = '2026-09-25T23:00:00Z';
  const cases = [
    [null, /証跡が無い/],
    [{ ...evidenceFor(d), state: 'running' }, /終わっていない/],
    [{ ...evidenceFor(d), sync_run_id: 'ds_other' }, /今朝の実行のものでない/],
    [{ ...evidenceFor(d), as_of: '2026-09-25' }, /日が違う/],
    [{ ...evidenceFor(d), json_path: 'cdb-master-compare/none.json' }, /読めない/],
    [evidenceFor(d, { corrupt: true }), /ハッシュ/],
    [{ ...evidenceFor(d), verdict: 'breach' }, /食い違う/],
    [evidenceFor(d, { verdict: 'blocked', blockedReason: 'rule_mismatch' }), /判定できない \(rule_mismatch\)/],
  ];
  for (const [ev, re] of cases) {
    const r = await run(d, ev, now);
    assert.equal(w13(r).verdict, 'blocked', `${re}: ${w13(r).reason}`);
    assert.match(w13(r).reason, re);
    assert.equal((await issues('open')).length, 248);   // 案件は回復も継続もしない (保持)
  }
});

await ta('[4] 評価の途中で証跡が差し替わったら、再評価は新しい証跡を使う (旧 pass → 新 breach。Codex #1456 R1 High-2)', async () => {
  const d = '2026-09-27', now = '2026-09-26T23:00:00Z';
  const fresh = evidenceFor(d, { items: [item('cost', 'newsku')], compared: { cost: ['newsku'] } });
  const r = await run(d, evidenceFor(d), now, { afterSnapshot: async (n) => { if (n === 1) putEvidence(d, fresh); } });
  assert.equal(r.attempts, 2);
  assert.equal(w13(r).verdict, 'breach', w13(r).reason);
  assert.equal(w13(r).inputGeneration.compare_run_id, fresh.compare_run_id);
  assert.ok((await issues('open')).includes('cost:newsku'));
});

await pg.close();
try { fs.rmSync(DIR, { recursive: true, force: true }); } catch { /* Windows は OS に任せる */ }
console.log(`\n${passed} 件 PASS`);
process.exit(process.exitCode || 0);
