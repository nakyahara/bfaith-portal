/**
 * test-watch-w13-ne.mjs — 見張りの W13:ne (マスタの照合 ②NE との照合) を engine ごと通す (Company DB構想 10 §6.1.1 C2 v5-4・v5-5・v6-4 = C2b)
 *
 * 固定する契約:
 *   1 W13 の評価キーは load と ne の 2 つ。ne は全件 JSON の ne 節の items を案件に (差 0 = pass / 差あり = breach・info)
 *   2 明示の回復: 明細に無い open の案件は recoverable にあるものだけ回復 / held にある = 保持 (理由つき) / どちらにも無い = 保持 (not_confirmed) /
 *     out_of_scope = 監視期間外 (本当に対象から外れた案件だけ)
 *   3 ② が判定できない・落ちた・② の節が無い = W13:ne は blocked (案件は全部保持)・W13:load は ① で判定する
 *   4 証跡と全件 JSON の ② の判定が食い違う・件数が食い違う = blocked
 *   5 朝の要約: W13:ne の案件は「新・継続」に混ぜず「NE との差 N 件 (新 M)」にまとめる (他の見張りの知らせを埋もれさせない)
 * 使い方: node scripts/test-watch-w13-ne.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';

const DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'w13ne-'));
process.env.DATA_DIR = DIR;
const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const REAL = await import('../config/watch-checks.mjs');
const { runWatch, summarize } = await import('../apps/company-db/watch/engine.mjs');
const { plannedKeys } = await import('../apps/company-db/watch/checks.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const SYNC = 'ds_w13ne_1';
const CONFIG = { ...REAL, CHECKS: [REAL.checkById('W13')] };

let seq = 0;
/** 照合の実行口が書くのと同じ形 (mc-v2) の全件 JSON と証跡。ne = null なら ② の節なし */
function evidenceFor(asOf, { ne = {}, neVerdictInEvidence, corruptCount = false } = {}) {
  const runId = `mc_20260926T000000${String(++seq).padStart(3, '0')}Z_abcdef`;
  const neItems = ne ? (ne.items || []) : [];
  const neSec = ne ? { format: ne.format ?? 'mc-ne-v1', verdict: ne.verdict ?? (neItems.length ? 'breach' : 'pass'), blocked_reason: ne.blocked_reason ?? null, error: ne.error ?? null,
    items: neItems, held: ne.held ?? {}, recoverable: ne.recoverable ?? [], out_of_scope: ne.out_of_scope ?? {},
    counts: { items: corruptCount ? neItems.length + 1 : neItems.length, ne_skus: 100, by_class: { lag: neItems.length }, decisions: 0 } } : undefined;
  const res = { format: ne ? 'mc-v2' : 'mc-v1', as_of: asOf, compare_run_id: runId, verdict: 'pass', blocked_reason: null, load: { ingest_run_id: 'load_n1', started_at: `${asOf}T17:00:00Z` },
    counts: { items: 0, by_type: {}, compared: { value: 1000 } }, items: [], compared: {}, exclusions: {}, finished_at: `${asOf}T22:11:00Z`, ...(neSec ? { ne: neSec } : {}) };
  const rel = `cdb-master-compare/${asOf}/${runId}.json`;
  fs.mkdirSync(path.join(DIR, 'cdb-master-compare', asOf), { recursive: true });
  const buf = Buffer.from(JSON.stringify(res), 'utf8');
  fs.writeFileSync(path.join(DIR, rel), buf);
  return { name: 'master-compare', state: 'complete', compare_run_id: runId, as_of: asOf, json_path: rel, sha256: crypto.createHash('sha256').update(buf).digest('hex'),
    verdict: 'pass', blocked_reason: null, counts: res.counts, load: { ingest_run_id: 'load_n1' }, sync_run_id: SYNC,
    ...(neSec ? { ne: { verdict: neVerdictInEvidence ?? neSec.verdict, blocked_reason: neSec.blocked_reason, counts: neSec.counts } } : {}) };
}
const item = (type, norm, cls = 'lag') => ({ type, code: norm, norm, kind: 'single', subject_key: `${type}:${norm}`, classes: [cls], columns: [{ col: 'name', cls, n: 'x', c: 'y' }] });
const pg = new PGlite(); const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
const evFile = (asOf) => path.join(DIR, 'company-db-evidence', asOf, 'master-compare.json');
const putEvidence = (asOf, ev) => { fs.mkdirSync(path.dirname(evFile(asOf)), { recursive: true }); fs.writeFileSync(evFile(asOf), JSON.stringify(ev)); };
const run = (asOf, ev, nowIso) => { putEvidence(asOf, ev); return runWatch({ db, writer: db, config: CONFIG, asOf, evidence: { 'master-compare': ev }, now: new Date(nowIso), host: 'test', log: quiet, syncRunId: SYNC }); };
const res = (r, scope) => r.results.find((x) => x.checkId === 'W13' && x.scopeKey === scope);
const issues = async (state) => (await db.query("select subject_key from ops.watch_issues where check_id = 'W13' and scope_key = 'ne' and state = $1 order by subject_key", [state])).rows.map((x) => x.subject_key);

await ta('[1] 評価キーは load と ne。ne の差 = breach (info)・案件は scope ne で開く / W13:load は ① で pass', async () => {
  assert.deepEqual(plannedKeys(CONFIG).map((k) => k.scopeKey), ['load', 'ne']);
  const r = await run('2026-09-27', evidenceFor('2026-09-27', { ne: { items: [item('value', 'a'), item('value', 'b', 'incomparable'), item('components', 'c'), item('kind', 'd')] } }), '2026-09-26T23:00:00Z');
  assert.deepEqual([res(r, 'load').verdict, res(r, 'ne').verdict, res(r, 'ne').severity], ['pass', 'breach', 'info']);
  assert.deepEqual(await issues('open'), ['components:c', 'kind:d', 'value:a', 'value:b']);
  assert.match(res(r, 'ne').reason, /NE との差 4 件/);
});

await ta('[2] 明示の回復: recoverable = 回復 / held = 保持 (理由) / どちらにも無い = 保持 (not_confirmed) / out_of_scope = 監視期間外', async () => {
  const r = await run('2026-09-28', evidenceFor('2026-09-28', { ne: { items: [], recoverable: ['value:a'], held: { 'value:b': 'incomparable' }, out_of_scope: { 'kind:d': 'exception_item' } } }), '2026-09-27T23:00:00Z');
  assert.equal(res(r, 'ne').verdict, 'pass');
  assert.deepEqual(await issues('recovered'), ['value:a']);
  assert.deepEqual(await issues('out_of_window'), ['kind:d']);
  assert.deepEqual(await issues('open'), ['components:c', 'value:b']);   // 回復にしない
  const held = Object.fromEntries(r.notes.held.filter((h) => h.scopeKey === 'ne').map((h) => [h.subjectKey, h.reason]));
  assert.deepEqual(held, { 'value:b': 'incomparable', 'components:c': 'not_confirmed' });
});

await ta('[3] ② が判定できない・落ちた・節が無い = W13:ne blocked (案件は保持)・W13:load は判定する', async () => {
  for (const [ne, re] of [[{ verdict: 'blocked', blocked_reason: 'stale_ne' }, /判定できない \(stale_ne\)/], [{ verdict: 'error', error: 'boom' }, /落ちた \(boom\)/], [null, /節が無い/]]) {
    const r = await run('2026-09-29', evidenceFor('2026-09-29', { ne }), '2026-09-28T23:00:00Z');
    assert.equal(res(r, 'load').verdict, 'pass');
    assert.equal(res(r, 'ne').verdict, 'blocked'); assert.match(res(r, 'ne').reason, re);
    assert.deepEqual(await issues('open'), ['components:c', 'value:b']);
  }
});
await ta('[3b] ① が判定できない朝でも ② は判定する (評価キーは別)', async () => {
  const ev = evidenceFor('2026-09-29', { ne: { items: [item('value', 'b', 'incomparable'), item('components', 'c')] } });
  const j = path.join(DIR, ev.json_path); const x = JSON.parse(fs.readFileSync(j, 'utf8'));
  x.verdict = 'blocked'; x.blocked_reason = 'material_not_matched';
  const buf = Buffer.from(JSON.stringify(x), 'utf8'); fs.writeFileSync(j, buf);
  Object.assign(ev, { verdict: 'blocked', blocked_reason: 'material_not_matched', sha256: crypto.createHash('sha256').update(buf).digest('hex') });
  const r = await run('2026-09-29', ev, '2026-09-28T23:30:00Z');
  assert.deepEqual([res(r, 'load').verdict, res(r, 'ne').verdict], ['blocked', 'breach']);
});

await ta('[4] 証跡と全件 JSON の ② の判定・件数が食い違う = blocked', async () => {
  let r = await run('2026-09-30', evidenceFor('2026-09-30', { ne: { items: [item('value', 'b')] }, neVerdictInEvidence: 'pass' }), '2026-09-29T23:00:00Z');
  assert.match(res(r, 'ne').reason, /証跡と食い違う/);
  r = await run('2026-09-30', evidenceFor('2026-09-30', { ne: { items: [item('value', 'b')] }, corruptCount: true }), '2026-09-29T23:10:00Z');
  assert.match(res(r, 'ne').reason, /件数が食い違う/);
  assert.deepEqual(await issues('open'), ['components:c', 'value:b']);
});

await ta('[5] 朝の要約: W13:ne の案件は「NE との差 N 件 (新 M)」にまとめ、「新・継続」と明細に混ぜない', async () => {
  const r = await run('2026-10-01', evidenceFor('2026-10-01', { ne: { items: [item('value', 'b'), item('components', 'c'), item('value', 'e'), item('value', 'f')] } }), '2026-09-30T23:00:00Z');
  assert.match(r.lastLine, /異常 1 \(新 0 \/ 継続 0\)/);
  assert.match(r.lastLine, /NE との差 4 件 \(新 2\)/);
  assert.doesNotMatch(r.lastLine, /value:e/);
  // separate が無ければ今までどおり「新」に数える
  const s = summarize({ asOf: '2026-10-01', planned: [{}, {}], results: r.results, notes: r.notes, deadlineHit: false });
  assert.match(s.lastLine, /新 2 \/ 継続 2/);
});

await pg.close();
try { fs.rmSync(DIR, { recursive: true, force: true }); } catch { /* */ }
console.log(`\n${passed} 件 PASS`);
process.exit(process.exitCode || 0);
